#!/usr/bin/env python3
"""
Utility functions for the Linode Object Storage Monitoring System.
With Redis Sentinel support for high availability.
"""

import json
import logging
import os
import time
import yaml
import redis
from redis.sentinel import Sentinel
from datetime import datetime

# Configure logging
def setup_logging(name, level=None):
    """Set up logging with appropriate format and level."""
    if level is None:
        level = os.environ.get("LOG_LEVEL", "INFO").upper()
    
    numeric_level = getattr(logging, level, logging.INFO)
    
    logging.basicConfig(
        level=numeric_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    return logging.getLogger(name)

# Logger for this module
logger = setup_logging("utils")

# Load and merge configurations
def load_config():
    """Load configuration from files and environment variables."""
    # Base configuration from configmap
    config_path = os.environ.get("CONFIG_PATH", "/app/config/config.yaml")
    config = {}
    
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded base configuration from {config_path}")
    except Exception as e:
        logger.error(f"Error loading configuration from {config_path}: {e}")
    
    # Load bucket configurations from secrets
    buckets_path = os.environ.get("BUCKETS_PATH", "/app/secrets/buckets.yaml")
    try:
        if os.path.exists(buckets_path):
            with open(buckets_path, 'r') as f:
                buckets_config = yaml.safe_load(f)
                if buckets_config and "buckets" in buckets_config:
                    config["buckets"] = buckets_config["buckets"]
                    logger.info(f"Loaded {len(config['buckets'])} buckets from {buckets_path}")
    except Exception as e:
        logger.error(f"Error loading buckets from {buckets_path}: {e}")
    
    # Override with environment variables
    if "POLLING_INTERVAL" in os.environ:
        try:
            interval = int(os.environ["POLLING_INTERVAL"])
            if "defaults" not in config:
                config["defaults"] = {}
            config["defaults"]["polling_interval"] = interval
            logger.info(f"Overriding polling interval to {interval}s from environment")
        except ValueError:
            logger.error(f"Invalid POLLING_INTERVAL value: {os.environ['POLLING_INTERVAL']}")
    
    if "REDIS_HOST" in os.environ:
        if "redis" not in config:
            config["redis"] = {}
        config["redis"]["host"] = os.environ["REDIS_HOST"]
    
    if "WEBHOOK_URL" in os.environ:
        if "webhook" not in config:
            config["webhook"] = {}
        config["webhook"]["url"] = os.environ["WEBHOOK_URL"]
    
    return config

# Redis client creation
def create_redis_client(config):
    """Create Redis clients with Sentinel support for HA."""
    redis_config = config.get("redis", {})
    
    # Get Sentinel configuration
    sentinel_host = redis_config.get("sentinel_host", "redis-sentinel")
    sentinel_port = redis_config.get("sentinel_port", 26379)
    master_name = redis_config.get("master_name", "mymaster")
    password = redis_config.get("password", None)
    db = redis_config.get("db", 0)
    
    # Set up connection pools for better performance
    socket_timeout = 5.0
    socket_connect_timeout = 5.0
    
    try:
        # Create Sentinel manager
        sentinel = Sentinel(
            [(sentinel_host, sentinel_port)],
            socket_timeout=socket_timeout,
            password=password
        )
        
        # Create Redis clients - one for master (writes), one for slave (reads)
        master = sentinel.master_for(
            master_name,
            socket_timeout=socket_timeout,
            db=db,
            password=password,
            decode_responses=True
        )
        
        slave = sentinel.slave_for(
            master_name,
            socket_timeout=socket_timeout,
            db=db,
            password=password,
            decode_responses=True
        )
        
        logger.info("Successfully connected to Redis via Sentinel")
        
        # Return both clients
        return {
            "master": master,  # Use for writes
            "slave": slave     # Use for reads
        }
    except Exception as e:
        logger.error(f"Error connecting to Redis via Sentinel: {e}")
        
        # Fallback to direct connection if Sentinel fails
        logger.warning("Falling back to direct Redis connection")
        try:
            # Try to connect directly to Redis master as fallback
            fallback_host = redis_config.get("host", "redis-0.redis")
            fallback_port = redis_config.get("port", 6379)
            
            redis_client = redis.Redis(
                host=fallback_host,
                port=fallback_port,
                db=db,
                password=password,
                decode_responses=True,
                socket_timeout=socket_timeout,
                socket_connect_timeout=socket_connect_timeout
            )
            
            # Simple connection test
            redis_client.ping()
            
            # Return the same client for both reads and writes in fallback mode
            return {
                "master": redis_client,
                "slave": redis_client
            }
        except Exception as fallback_error:
            logger.error(f"Fallback Redis connection also failed: {fallback_error}")
            raise

# Bucket notification status functions
def is_bucket_notifications_enabled(redis_client, bucket_name):
    """Check if notifications are enabled for a bucket."""
    # Use slave for reads when available
    client = redis_client.get("slave", redis_client) if isinstance(redis_client, dict) else redis_client
    
    result = client.get(f"linode:objstore:config:{bucket_name}:notifications_disabled")
    return result is None  # Enabled if not explicitly disabled
    
def disable_bucket_notifications(redis_client, bucket_name):
    """Disable notifications for a bucket."""
    # Use master for writes
    client = redis_client.get("master", redis_client) if isinstance(redis_client, dict) else redis_client
    
    client.set(f"linode:objstore:config:{bucket_name}:notifications_disabled", "true")
    logger.info(f"Notifications disabled for bucket {bucket_name}")
    
def enable_bucket_notifications(redis_client, bucket_name):
    """Enable notifications for a bucket."""
    # Use master for writes
    client = redis_client.get("master", redis_client) if isinstance(redis_client, dict) else redis_client
    
    client.delete(f"linode:objstore:config:{bucket_name}:notifications_disabled")
    logger.info(f"Notifications enabled for bucket {bucket_name}")

# State management functions
def get_object_state(redis_client, config, bucket, key):
    """Get stored state for an object."""
    state_prefix = config.get("redis", {}).get("state_prefix", "linode:objstore:state:")
    redis_key = f"{state_prefix}{bucket}:{key}"
    
    # Use slave for reads if available, otherwise use what we have
    client = redis_client.get("slave", redis_client) if isinstance(redis_client, dict) else redis_client
    
    try:
        state_json = client.get(redis_key)
        if state_json:
            try:
                return json.loads(state_json)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in Redis for {redis_key}")
    except redis.RedisError as e:
        logger.error(f"Redis error getting state for {redis_key}: {e}")
        
    return None

def save_object_state(redis_client, config, bucket, key, state):
    """Save state for an object with TTL."""
    state_prefix = config.get("redis", {}).get("state_prefix", "linode:objstore:state:")
    redis_key = f"{state_prefix}{bucket}:{key}"
    
    # Get TTL from config or use default (30 days)
    ttl = config.get("redis", {}).get("ttl", 2592000)
    
    # Use master for writes
    client = redis_client.get("master", redis_client) if isinstance(redis_client, dict) else redis_client
    
    try:
        # Set with expiration to prevent unlimited growth
        client.setex(redis_key, ttl, json.dumps(state))
        return True
    except Exception as e:
        logger.error(f"Error saving state to Redis for {redis_key}: {e}")
        return False

# Queue operations
def publish_notification(redis_client, config, message):
    """Publish a notification to the Redis queue."""
    queue_name = config.get("redis", {}).get("queue_name", "linode:notifications:queue")
    
    # Use master for writes
    client = redis_client.get("master", redis_client) if isinstance(redis_client, dict) else redis_client
    
    try:
        # Convert message to JSON string
        message_json = json.dumps(message)
        
        # Push to Redis list used as queue
        client.rpush(queue_name, message_json)
        
        # Optional: Set TTL on queue to prevent unbounded growth
        client.expire(queue_name, 604800)  # 7 days
        
        return True
    except Exception as e:
        logger.error(f"Error publishing to queue: {e}")
        return False

def get_notifications(redis_client, config, batch_size=10):
    """Get a batch of notifications from the Redis queue with atomic operations."""
    queue_name = config.get("redis", {}).get("queue_name", "linode:notifications:queue")
    
    # Use master for queue operations (atomic LRANGE+LTRIM)
    client = redis_client.get("master", redis_client) if isinstance(redis_client, dict) else redis_client
    
    # Create a pipeline to execute commands atomically
    pipe = client.pipeline()
    pipe.lrange(queue_name, 0, batch_size - 1)
    pipe.ltrim(queue_name, batch_size, -1)
    
    try:
        # Execute both commands atomically
        result = pipe.execute()
        messages = result[0]
        
        if not messages:
            return []
            
        # Parse JSON messages
        parsed_messages = []
        for message_json in messages:
            try:
                parsed_messages.append(json.loads(message_json))
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in queue message: {message_json[:100]}...")
                continue
                
        return parsed_messages
    except Exception as e:
        logger.error(f"Error getting notifications from queue: {e}")
        return []

# Health check functions
def check_redis_health(redis_client):
    """Check if Redis is healthy."""
    try:
        # Check both master and slave if available
        if isinstance(redis_client, dict):
            master_ok = redis_client["master"].ping()
            slave_ok = redis_client["slave"].ping()
            return master_ok and slave_ok
        else:
            return redis_client.ping()
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        return False

def check_sentinel_health(config):
    """Check Sentinel status."""
    redis_config = config.get("redis", {})
    sentinel_host = redis_config.get("sentinel_host", "redis-sentinel")
    sentinel_port = redis_config.get("sentinel_port", 26379)
    master_name = redis_config.get("master_name", "mymaster")
    
    try:
        # Connect to Sentinel
        sentinel = Sentinel(
            [(sentinel_host, sentinel_port)],
            socket_timeout=1.0
        )
        
        # Get master address
        master = sentinel.discover_master(master_name)
        
        # Get slave addresses
        slaves = sentinel.discover_slaves(master_name)
        
        return {
            "status": "ok",
            "master": f"{master[0]}:{master[1]}",
            "slaves": [f"{slave[0]}:{slave[1]}" for slave in slaves],
            "slave_count": len(slaves)
        }
    except Exception as e:
        logger.error(f"Sentinel health check failed: {e}")
        return {"status": "error", "error": str(e)}

def check_queue_stats(redis_client, config):
    """Get statistics about the notification queue."""
    queue_name = config.get("redis", {}).get("queue_name", "linode:notifications:queue")
    
    # Use slave for reads when possible
    client = redis_client.get("slave", redis_client) if isinstance(redis_client, dict) else redis_client
    
    try:
        queue_length = client.llen(queue_name)
        return {
            "queue_length": queue_length,
            "queue_name": queue_name
        }
    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
        return {"error": str(e)}
