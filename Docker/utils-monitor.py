#!/usr/bin/env python3
"""
Utility functions for the Linode Object Storage Monitoring System.
With Redis Sentinel support for high availability.
"""

import json
import logging
import base64
import requests
import os
import time
import yaml
import jwt
from typing import Dict, Optional
import redis
from datetime import datetime, timedelta
from redis.sentinel import Sentinel
from datetime import datetime, timedelta

INFISICAL_API_URL = os.getenv("INFISICAL_API_URL", "https://hmsinfvault.akamai-mco.com").rstrip("/")
INFISICAL_TOKEN = os.getenv("INFISICAL_TOKEN")
INFISICAL_PROJECT_ID = os.getenv("INFISICAL_PROJECT_ID")
INFISICAL_ENV = os.getenv("INFISICAL_ENV", "dev")

_infisical_cache = {}
_cache_ttl = 300  # 5 minutes

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

# OAuth token cache
oauth_token_cache = {}

# Default OAuth token URL (same for all webhooks)
#DEFAULT_OAUTH_TOKEN_URL = "https://oauth/token"

def get_oauth_token(client_id, client_secret, token_url):
    """Get OAuth token using client credentials grant.
    
    Args:
        client_id: OAuth client ID
        client_secret: OAuth client secret
        token_url: OAuth token endpoint URL
        
    Returns:
        str: OAuth access token or None if failed
    """
    if not token_url:
        logger.error("No token URL provided for OAuth authentication")
        return None
        
    cache_key = f"{client_id}:{token_url}"
    
    # Check cache first
    current_time = time.time()
    if cache_key in oauth_token_cache:
        token_data = oauth_token_cache[cache_key]
        # Check if token is still valid (with 5-minute buffer)
        if current_time < token_data["expires_at"] - 300:  # 5-minute buffer
            logger.debug(f"Using cached OAuth token for {client_id}")
            return token_data["access_token"]
    
    try:
        # Prepare Basic Auth header
        auth_str = f"{client_id}:{client_secret}"
        auth_bytes = auth_str.encode('ascii')
        base64_bytes = base64.b64encode(auth_bytes)
        base64_auth = base64_bytes.decode('ascii')
        
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Basic {base64_auth}"
        }
        
        data = {
            "grant_type": "client_credentials"
        }
        
        logger.debug(f"Requesting OAuth token from {token_url}")
        response = requests.post(token_url, headers=headers, data=data, timeout=10)
        
        if response.status_code == 200:
            token_data = response.json()
            access_token = token_data.get("access_token")
            expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
            
            # Cache the token with expiration time
            oauth_token_cache[cache_key] = {
                "access_token": access_token,
                "expires_at": current_time + expires_in
            }
            
            logger.info(f"Successfully obtained OAuth token for {client_id}")
            return access_token
        else:
            logger.error(f"Failed to obtain OAuth token: {response.status_code}, {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"Error obtaining OAuth token for {client_id}: {e}")
        return None

def clear_oauth_token_cache(client_id=None, token_url=None):
    """Clear OAuth token cache for specific client or all clients."""
    global oauth_token_cache
    
    if client_id and token_url:
        cache_key = f"{client_id}:{token_url}"
        if cache_key in oauth_token_cache:
            del oauth_token_cache[cache_key]
            logger.debug(f"Cleared OAuth token cache for {client_id} and {token_url}")
    else:
        oauth_token_cache = {}
        logger.debug("Cleared all OAuth token cache")

# Get webhook URL for a specific bucket
def get_bucket_webhook_url(redis_client, config, bucket_name):
    """Get the webhook URL for a specific bucket."""
    # Check if we have a bucket-specific webhook URL in Redis
    client = redis_client.get("slave", redis_client) if isinstance(redis_client, dict) else redis_client
    
    # Try to get from Redis first (for dynamic updates)
    webhook_url = client.get(f"linode:objstore:config:{bucket_name}:webhook_url")
    
    # If not in Redis, check bucket configs
    if not webhook_url:
        for bucket in config.get("buckets", []):
            if bucket["name"] == bucket_name:
                webhook_url = bucket.get("webhook_url")
                if webhook_url:
                    break
    
    return webhook_url

# JWT Authentication functions
def generate_jwt_token(client_id, expiry_hours=24):
    """Generate a JWT token for the given client_id."""
    now = datetime.utcnow()
    payload = {
        'sub': client_id,
        'iat': now,
        'exp': now + timedelta(hours=expiry_hours)
    }
    
    jwt_secret = os.environ.get('JWT_SECRET', 'default-secret-key')
    token = jwt.encode(payload, jwt_secret, algorithm='HS256')
    
    # PyJWT > 2.0.0 returns string instead of bytes
    if isinstance(token, bytes):
        return token.decode('utf-8')
    return token

def validate_jwt_token(token):
    """Validate a JWT token and return the payload if valid."""
    jwt_secret = os.environ.get('JWT_SECRET', 'default-secret-key')
    try:
        payload = jwt.decode(token, jwt_secret, algorithms=['HS256'])
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("JWT token expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid JWT token: {e}")
        return None

def validate_client_credentials(client_id, client_secret):
    """Validate client credentials against configured values."""
    expected_client_id = os.environ.get('API_CLIENT_ID', 'default-client-id')
    expected_client_secret = os.environ.get('API_CLIENT_SECRET', 'default-client-secret')
    
    return client_id == expected_client_id and client_secret == expected_client_secret


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

def add_bucket_info_to_redis(redis_client, bucket_info):
    """Store bucket information in Redis for administrative review."""
    # Use master for writes
    client = redis_client.get("master", redis_client) if isinstance(redis_client, dict) else redis_client
    
    # Create a Redis key based on bucket name
    redis_key = f"linode:objstore:pending_buckets:{bucket_info['name']}"
    
    # Store the full information as JSON
    client.set(redis_key, json.dumps(bucket_info))
    
    # Set a TTL (e.g., 7 days) so pending configs don't stay forever
    client.expire(redis_key, 7 * 24 * 60 * 60)
    
    return True

def get_pending_bucket_configs(redis_client):
    """Get all pending bucket configurations from Redis."""
    # Use slave for reads
    client = redis_client.get("slave", redis_client) if isinstance(redis_client, dict) else redis_client
    
    # Get all pending bucket keys
    pending_keys = client.keys("linode:objstore:pending_buckets:*")
    
    # Get the information for each key
    pending_buckets = []
    for key in pending_keys:
        data = client.get(key)
        if data:
            try:
                bucket_info = json.loads(data)
                pending_buckets.append(bucket_info)
            except json.JSONDecodeError:
                continue
                
    return pending_buckets

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
    
# Functions for storing configuration (NO TTL)
def save_bucket_config(redis_client, bucket_config):
    """Save bucket configuration to Redis with NO TTL."""
    # Use master for writes
    client = redis_client.get("master", redis_client) if isinstance(redis_client, dict) else redis_client
    
    bucket_name = bucket_config["name"]
    redis_key = f"linode:objstore:config:bucket:{bucket_name}"
    
    # Store as JSON without TTL
    client.set(redis_key, json.dumps(bucket_config))
    logger.info(f"Saved bucket configuration for {bucket_name} to Redis (persistent)")
    return True

def get_bucket_config(redis_client, bucket_name):
    """Get bucket configuration from Redis."""
    # Use slave for reads when available
    client = redis_client.get("slave", redis_client) if isinstance(redis_client, dict) else redis_client
    
    redis_key = f"linode:objstore:config:bucket:{bucket_name}"
    config_json = client.get(redis_key)
    
    if config_json:
        try:
            return json.loads(config_json)
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in Redis for {redis_key}")
    
    return None

def load_bucket_configs(redis_client):
    """Load all bucket configurations from Redis."""
    # Use slave for reads when available
    client = redis_client.get("slave", redis_client) if isinstance(redis_client, dict) else redis_client
    
    # Get all bucket configurations
    bucket_keys = client.keys("linode:objstore:config:bucket:*")
    bucket_configs = []
    
    for key in bucket_keys:
        config_json = client.get(key)
        if config_json:
            try:
                bucket_config = json.loads(config_json)
                bucket_configs.append(bucket_config)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in Redis for {key}")
    
    return bucket_configs

# Functions for object state WITH TTL
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

def get_bucket_credentials_from_infisical(bucket_name: str) -> Optional[Dict[str, str]]:
    """Retrieve bucket credentials from Infisical"""
    global _infisical_cache
    
    # Check cache first
    cache_key = f"{bucket_name}:{INFISICAL_ENV}"
    if cache_key in _infisical_cache:
        cached_data, cache_time = _infisical_cache[cache_key]
        if time.time() - cache_time < _cache_ttl:
            return cached_data
    
    if not INFISICAL_TOKEN or not INFISICAL_PROJECT_ID:
        logger.error("Infisical credentials not configured")
        return None
    
    try:
        # Get both access and secret keys
        headers = {
            "Authorization": f"Bearer {INFISICAL_TOKEN}",
            "Content-Type": "application/json"
        }
        
        credentials = {}
        
        # Retrieve access key
        access_key_name = f"{bucket_name.upper().replace('-', '_')}_ACCESS_KEY"
        url = f"{INFISICAL_API_URL}/api/v3/secrets/raw/{access_key_name}"
        params = {
            "workspaceId": INFISICAL_PROJECT_ID,
            "environment": INFISICAL_ENV,
            "secretPath": "/"
        }
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            credentials['access_key'] = data.get('secret', {}).get('secretValue')
        
        # Retrieve secret key
        secret_key_name = f"{bucket_name.upper().replace('-', '_')}_SECRET_KEY"
        url = f"{INFISICAL_API_URL}/api/v3/secrets/raw/{secret_key_name}"
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            credentials['secret_key'] = data.get('secret', {}).get('secretValue')
        
        if credentials.get('access_key') and credentials.get('secret_key'):
            # Cache the credentials
            _infisical_cache[cache_key] = (credentials, time.time())
            return credentials
        else:
            logger.error(f"Failed to retrieve complete credentials for bucket {bucket_name}")
            return None
            
    except Exception as e:
        logger.error(f"Error retrieving credentials from Infisical: {e}")
        return None

def get_bucket_config(redis_client, bucket_name):
    """Get bucket configuration from Redis, but fetch credentials from Infisical"""
    # Get base config from Redis
    client = redis_client.get("slave", redis_client) if isinstance(redis_client, dict) else redis_client
    
    redis_key = f"linode:objstore:config:bucket:{bucket_name}"
    config_json = client.get(redis_key)
    
    if config_json:
        try:
            bucket_config = json.loads(config_json)
            
            # Override credentials with Infisical values
            infisical_creds = get_bucket_credentials_from_infisical(bucket_name)
            if infisical_creds:
                bucket_config['access_key'] = infisical_creds['access_key']
                bucket_config['secret_key'] = infisical_creds['secret_key']
                logger.debug(f"Using credentials from Infisical for bucket {bucket_name}")
            else:
                logger.warning(f"Failed to get Infisical credentials for {bucket_name}, using Redis fallback")
            
            return bucket_config
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in Redis for {redis_key}")
    
    return None

def add_credentials_to_infisical(bucket_name: str, access_key: str, secret_key: str) -> bool:
    """Add S3 credentials to Infisical"""
    url_base = f"{INFISICAL_API_URL}/api/v3/secrets/raw"
    
    headers = {
        "Authorization": f"Bearer {INFISICAL_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Create both secrets
    secrets_to_create = [
        {
            "name": f"{bucket_name.upper().replace('-', '_')}_ACCESS_KEY",
            "value": access_key
        },
        {
            "name": f"{bucket_name.upper().replace('-', '_')}_SECRET_KEY",
            "value": secret_key
        }
    ]
    
    for secret in secrets_to_create:
        url = f"{url_base}/{secret['name']}"
        payload = {
            "workspaceId": INFISICAL_PROJECT_ID,
            "environment": INFISICAL_ENV,
            "secretPath": "/",
            "secretValue": secret['value'],
            "type": "shared",
            "skipMultilineEncoding": True
        }
        
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code not in [200, 201]:
            logger.error(f"Failed to create secret {secret['name']}: {response.text}")
            return False
    
    return True

def delete_credentials_from_infisical(bucket_name: str) -> bool:
    """Delete S3 credentials from Infisical"""

    cache_key = f"{bucket_name}:{INFISICAL_ENV}"
    
    if cache_key in _infisical_cache:
        logger.info(f"Clearing cached credentials for {bucket_name}")
        del _infisical_cache[cache_key]

    headers = {
        "Authorization": f"Bearer {INFISICAL_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Delete both secrets
    secret_names = [
        f"{bucket_name.upper().replace('-', '_')}_ACCESS_KEY",
        f"{bucket_name.upper().replace('-', '_')}_SECRET_KEY"
    ]

    all_deleted = True
    
    for secret_name in secret_names:
        url = f"{INFISICAL_API_URL}/api/v3/secrets/raw/{secret_name}"
        payload = {
            "workspaceId": INFISICAL_PROJECT_ID,
            "environment": INFISICAL_ENV,
            "secretPath": "/"
        }
        
        try:
            response = requests.delete(url, headers=headers, json=payload)
        
            if response.status_code in [200, 204]:
                logger.info(f"Successfully deleted secret {secret_name}")
            elif response.status_code == 404:
                logger.info(f"Secret {secret_name} already deleted or doesn't exist")
            else:
                logger.error(f"Failed to delete secret {secret_name}: Status {response.status_code}, Response: {response.text}")
                all_deleted = False

        except Exception as e:
            logger.error(f"Exception deleting secret {secret_name}: {e}")
            all_deleted = False
    
    return all_deleted
