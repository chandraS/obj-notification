#!/usr/bin/env python3
"""
Utility functions for the Linode Object Storage Monitoring System.
"""

import json
import logging
import os
import time
import yaml
import redis
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
    """Create a Redis client with connection pooling."""
    redis_config = config.get("redis", {})
    
    # Connection pool settings
    pool = redis.ConnectionPool(
        host=redis_config.get("host", "localhost"),
        port=redis_config.get("port", 6379),
        db=redis_config.get("db", 0),
        password=redis_config.get("password"),
        decode_responses=True,
        max_connections=20,
        socket_timeout=5.0,
        socket_connect_timeout=5.0
    )
    
    return redis.Redis(connection_pool=pool)

# State management functions
def get_object_state(redis_client, config, bucket, key):
    """Get stored state for an object."""
    state_prefix = config.get("redis", {}).get("state_prefix", "linode:objstore:state:")
    redis_key = f"{state_prefix}{bucket}:{key}"
    
    state_json = redis_client.get(redis_key)
    if state_json:
        try:
            return json.loads(state_json)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in Redis for {redis_key}")
    
    return None

def save_object_state(redis_client, config, bucket, key, state):
    """Save state for an object with TTL."""
    state_prefix = config.get("redis", {}).get("state_prefix", "linode:objstore:state:")
    redis_key = f"{state_prefix}{bucket}:{key}"
    
    # Get TTL from config or use default (30 days)
    ttl = config.get("redis", {}).get("ttl", 2592000)
    
    try:
        # Set with expiration to prevent unlimited growth
        redis_client.setex(redis_key, ttl, json.dumps(state))
        return True
    except Exception as e:
        logger.error(f"Error saving state to Redis for {redis_key}: {e}")
        return False

# Queue operations
def publish_notification(redis_client, config, message):
    """Publish a notification to the Redis queue."""
    queue_name = config.get("redis", {}).get("queue_name", "linode:notifications:queue")
    
    try:
        # Convert message to JSON string
        message_json = json.dumps(message)
        
        # Push to Redis list used as queue
        redis_client.rpush(queue_name, message_json)
        
        # Optional: Set TTL on queue to prevent unbounded growth
        redis_client.expire(queue_name, 604800)  # 7 days
        
        return True
    except Exception as e:
        logger.error(f"Error publishing to queue: {e}")
        return False

def get_notifications(redis_client, config, batch_size=10):
    """Get a batch of notifications from the Redis queue with atomic operations."""
    queue_name = config.get("redis", {}).get("queue_name", "linode:notifications:queue")
    
    # Create a pipeline to execute commands atomically
    pipe = redis_client.pipeline()
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
        return redis_client.ping()
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        return False

def check_queue_stats(redis_client, config):
    """Get statistics about the notification queue."""
    queue_name = config.get("redis", {}).get("queue_name", "linode:notifications:queue")
    
    try:
        queue_length = redis_client.llen(queue_name)
        return {
            "queue_length": queue_length,
            "queue_name": queue_name
        }
    except Exception as e:
        logger.error(f"Error getting queue stats: {e}")
        return {"error": str(e)}
