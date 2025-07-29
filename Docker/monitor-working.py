#!/usr/bin/env python3
"""
Linode Object Storage Monitor

This script monitors Linode Object Storage buckets across multiple regions,
detects new or updated objects, and sends notifications via a Redis queue.
It uses optimized thread pools and staggered scanning for efficiency.

Supports Redis Sentinel for high availability.
"""

import boto3
import os
import time
import yaml
import json
import threading
import queue
import signal
import random
import http.server
import socketserver
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from botocore.client import Config

# Import utility functions
from utils import (
    setup_logging,
    load_config,
    create_redis_client,
    get_object_state,
    save_object_state,
    publish_notification,
    check_redis_health,
    check_queue_stats,
    check_sentinel_health,
    is_bucket_notifications_enabled,
    enable_bucket_notifications,
    disable_bucket_notifications,
    get_pending_bucket_configs,
    save_bucket_config,
    get_bucket_credentials_from_infisical,
    add_credentials_to_infisical,
    delete_credentials_from_infisical,
    add_bucket_info_to_redis,
    get_bucket_webhook_url,
    generate_jwt_token,
    validate_jwt_token,
    validate_client_credentials
)

# Set up logging
logger = setup_logging("bucket-monitor")

class S3ClientCache:
    """Cache for S3 clients to prevent recreation."""
    
    def __init__(self):
        """Initialize an empty client cache."""
        self.clients = {}
        self.lock = threading.Lock()  # Thread-safe access
        self.stats = {
            "cache_hits": 0,
            "cache_misses": 0,
            "total_clients": 0
        }
        
    def get_client_key(self, endpoint, access_key, secret_key):
        """Generate a unique key for the client cache."""
        return f"{endpoint}:{access_key}"
        
    def get_client(self, endpoint, access_key, secret_key):
        """Get or create an S3 client for the given parameters."""
        client_key = self.get_client_key(endpoint, access_key, secret_key)
        
        with self.lock:
            # Check if we have a cached client
            if client_key in self.clients:
                self.stats["cache_hits"] += 1
                return self.clients[client_key]
            
            # Create a new client if not in cache
            try:
                client = boto3.client(
                    's3',
                    endpoint_url=f"https://{endpoint}",
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    config=Config(signature_version='s3v4')
                )
                
                # Store in cache
                self.clients[client_key] = client
                self.stats["cache_misses"] += 1
                self.stats["total_clients"] += 1
                
                logger.info(f"Created new S3 client for endpoint {endpoint}")
                return client
                
            except Exception as e:
                logger.error(f"Error creating S3 client for endpoint {endpoint}: {e}")
                return None
                
    def clear_cache(self):
        """Clear the client cache."""
        with self.lock:
            self.clients.clear()
            logger.info("S3 client cache cleared")
            
    def get_stats(self):
        """Get cache statistics."""
        with self.lock:
            return self.stats.copy()

class BucketScanner(threading.Thread):
    """Thread for scanning a single bucket."""
    
    def __init__(self, bucket_config, redis_client, global_config, result_queue, client_cache):
        """Initialize the bucket scanner."""
        threading.Thread.__init__(self)
        self.bucket_config = bucket_config
        self.redis_client = redis_client
        self.global_config = global_config
        self.result_queue = result_queue
        self.client_cache = client_cache
        
    def run(self):
        """Run the bucket scan."""
        bucket_name = self.bucket_config["name"]
        start_time = time.time()
        
        try:
            # Get S3 client for this bucket from cache
            endpoint = self.bucket_config.get("endpoint", 
                    self.global_config.get("defaults", {}).get("endpoint", "us-east-1.linodeobjects.com"))
            access_key = self.bucket_config.get("access_key")
            secret_key = self.bucket_config.get("secret_key")
            
            s3_client = self.client_cache.get_client(endpoint, access_key, secret_key)
            if not s3_client:
                raise Exception(f"Failed to get S3 client for bucket {bucket_name}")
            
            # Record scan time
            scan_time = datetime.now().timestamp()
            
            # Track counts for this bucket
            stats = {
                "bucket": bucket_name,
                "objects_scanned": 0,
                "new_objects": 0,
                "updated_objects": 0,
                "detected_objects": 0,
                "deleted_objects": 0,  
                "errors": 0,
                "scan_duration": 0
            }
            
            # CHANGE: First, collect all current objects before processing
            all_current_objects = []
            
            # Use paginator to handle buckets with many objects
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name)
            
            for page in page_iterator:
                if "Contents" in page:
                    all_current_objects.extend(page["Contents"])
                    
                    # Check if we've hit the batch limit
                    max_objects = self.global_config.get("defaults", {}).get("max_objects_per_batch", 1000)
                    if len(all_current_objects) >= max_objects:
                        logger.info(f"Reached max objects per batch for {bucket_name}, will resume in next scan")
                        all_current_objects = all_current_objects[:max_objects]
                        break
            
            # ADD: Detect and clean up deleted objects
            monitor = self.global_config.get('monitor_instance')
            if monitor:
                deleted_count = monitor.detect_and_clean_deletions(bucket_name, all_current_objects)
                stats["deleted_objects"] = deleted_count
            else:
                logger.warning("Monitor instance not available for deletion detection")
            
            # CHANGE: Now process the collected objects
            for obj in all_current_objects:
                stats["objects_scanned"] += 1
                
                object_key = obj["Key"]
                object_etag = obj["ETag"].strip('"')
                object_modified = obj["LastModified"].timestamp()
                
                # Get stored state
                state = get_object_state(self.redis_client, self.global_config, bucket_name, object_key)
                
                # Determine if object is new or updated
                is_missing_state = state is None
                is_updated = False
                
                if not is_missing_state:
                    # We have state, check if updated
                    is_updated = (state.get("etag") != object_etag or 
                                state.get("last_modified", 0) < object_modified)
                
                # For objects missing state, determine if truly new based on timestamp
                current_time = time.time()
                recent_threshold = 86400  # 24 hours in seconds
                is_truly_new = is_missing_state and (current_time - object_modified <= recent_threshold)
                
                # Determine appropriate event type
                if is_truly_new:
                    event_type = "created"
                elif is_missing_state:
                    # CHANGE: Check if it's a recent upload (within last 5 minutes)
                    if current_time - object_modified <= 300:  # 5 minutes
                        event_type = "created"  # Treat as new if recently uploaded
                    else:
                        event_type = "detected"
                elif is_updated:
                    event_type = "updated"
                else:
                    # No change detected, skip
                    continue
                
                # Process change
                # Get object details (only if needed)
                details = {}
                try:
                    details = s3_client.head_object(
                        Bucket=bucket_name, 
                        Key=object_key
                    )
                except Exception as e:
                    logger.error(f"Error getting details for {bucket_name}/{object_key}: {e}")
                    stats["errors"] += 1
                
                # Create notification message
                message = {
                    "bucket": bucket_name,
                    "key": object_key,
                    "region": endpoint,
                    "etag": object_etag,
                    "size": obj["Size"],
                    "last_modified": object_modified,
                    "event_type": event_type,
                    "content_type": details.get("ContentType", "application/octet-stream"),
                    "metadata": details.get("Metadata", {}),
                    "detection_time": scan_time,
                    "is_recent": current_time - object_modified <= recent_threshold,
                    "retry_count": 0
                }
                
                # Check if notifications are enabled for this bucket
                if is_bucket_notifications_enabled(self.redis_client, bucket_name):
                    # Publish to queue
                    if publish_notification(self.redis_client, self.global_config, message):
                        # Update state after successful queue publish
                        save_object_state(self.redis_client, self.global_config, bucket_name, object_key, {
                            "etag": object_etag,
                            "last_modified": object_modified,
                            "size": obj["Size"],
                            "last_processed": scan_time,
                            "last_seen": scan_time 
                        })
                        
                        # Update statistics based on event type
                        if event_type == "created":
                            stats["new_objects"] += 1
                        elif event_type == "updated":
                            stats["updated_objects"] += 1
                        elif event_type == "detected":
                            stats["detected_objects"] += 1
                    else:
                        logger.debug(f"Failed to publish notification for {bucket_name}/{object_key}")
                else:
                    logger.debug(f"Skipping notification for {bucket_name}/{object_key} (notifications disabled)")
            
            # Update last scan time
            # Use redis master explicitly for this write operation
            master_client = self.redis_client.get("master", self.redis_client) if isinstance(self.redis_client, dict) else self.redis_client
            master_client.set(f"linode:objstore:last_scan:{bucket_name}", scan_time)
            
            # Calculate scan duration
            stats["scan_duration"] = time.time() - start_time
            
            # Add result to queue
            self.result_queue.put(stats)
            
            # CHANGE: Updated log message to include deleted objects
            logger.info(
                f"Bucket {bucket_name} scan complete: "
                f"{stats['new_objects']} new, {stats['updated_objects']} updated, "
                f"{stats['detected_objects']} detected, {stats['deleted_objects']} deleted, "
                f"{stats['objects_scanned']} scanned, "
                f"in {stats['scan_duration']:.2f}s"
            )
            
        except Exception as e:
            scan_duration = time.time() - start_time
            logger.error(f"Error scanning bucket {bucket_name}: {e}")
            # Report failure
            self.result_queue.put({
                "bucket": bucket_name,
                "error": str(e),
                "scan_duration": scan_duration
            })

class MultiRegionMonitor:
    """Monitor for hundreds of buckets across multiple regions with optimized resource usage."""
    
    def __init__(self):
        """Initialize the monitor with configuration."""
        self.config = load_config()
        self.redis_client = create_redis_client(self.config)

        self.refresh_config_from_redis()

        self.client_cache = S3ClientCache()
        
        # Track last scan times for each bucket
        self.last_scan_times = {}
        self.load_last_scan_times()
        
        # Initialize bucket offsets for staggered scanning
        self.bucket_offsets = {}
        self.initialize_staggered_schedule()
        
        # Initialize thread pools for parallel bucket scanning
        self.init_thread_pools()
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)
        
        # Flag for shutdown
        self.running = True
        
        # Start health check server
        self.start_health_server()
        
        # Track scan metrics
        self.scan_stats = {
            "total_scans": 0,
            "total_objects_scanned": 0,
            "total_new_objects": 0,
            "total_updated_objects": 0,
            "total_detected_objects": 0,
            "errors": 0,
            "last_scan_time": 0,
            "scan_durations": []  # Last 10 scan durations
        }

    def refresh_config_from_redis(self):
        """Refresh configuration from Redis but get credentials from Infisical"""
        logger.info("Refreshing configuration from Redis and Infisical")
        
        client = self.redis_client.get("master", self.redis_client) if isinstance(self.redis_client, dict) else self.redis_client
        all_bucket_keys = client.keys("linode:objstore:config:bucket:*")
        
        new_buckets = []
        
        for key in all_bucket_keys:
            bucket_data = client.get(key)
            if bucket_data:
                try:
                    if isinstance(bucket_data, bytes):
                        bucket_data = bucket_data.decode('utf-8')
                    bucket_config = json.loads(bucket_data)
                    
                    # Get credentials from Infisical
                    bucket_name = bucket_config['name']
                    infisical_creds = get_bucket_credentials_from_infisical(bucket_name)
                    
                    if infisical_creds:
                        bucket_config['access_key'] = infisical_creds['access_key']
                        bucket_config['secret_key'] = infisical_creds['secret_key']
                        logger.debug(f"Loaded credentials from Infisical for {bucket_name}")
                    else:
                        logger.warning(f"No Infisical credentials for {bucket_name}, using stored values")
                    
                    new_buckets.append(bucket_config)
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON from Redis for key {key}")
        
        self.config["buckets"] = new_buckets
        logger.info(f"Refreshed configuration: {len(new_buckets)} buckets loaded")
    
    def handle_signal(self, signum, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        
    def load_last_scan_times(self):
        """Load last scan times for all buckets from Redis."""
        try:
            # Use slave client for reads when available
            client = self.redis_client.get("slave", self.redis_client) if isinstance(self.redis_client, dict) else self.redis_client
            
            for bucket_config in self.config.get("buckets", []):
                bucket_name = bucket_config["name"]
                last_scan = client.get(f"linode:objstore:last_scan:{bucket_name}")
                if last_scan:
                    self.last_scan_times[bucket_name] = float(last_scan)
        except Exception as e:
            logger.error(f"Error loading last scan times: {e}")
    
    def initialize_staggered_schedule(self):
        """Distribute buckets evenly across the polling interval using hash-based staggering."""
        buckets = self.config.get("buckets", [])
        polling_interval = self.config.get("defaults", {}).get("polling_interval", 60)
        stagger_method = self.config.get("parallel", {}).get("stagger_method", "hash")
        
        logger.info(f"Initializing {stagger_method} staggered schedule for {len(buckets)} buckets across {polling_interval}s interval")
        
        # Track distribution for logging
        offset_distribution = [0] * polling_interval
        
        for bucket_config in buckets:
            bucket_name = bucket_config["name"]
            offset = 0
            
            if stagger_method == "hash":
                # Generate consistent hash value from bucket name
                hash_value = 0
                for char in bucket_name:
                    hash_value = (hash_value * 31 + ord(char)) & 0xFFFFFFFF
                
                # Map hash to the polling interval (0 to polling_interval-1)
                offset = hash_value % polling_interval
            elif stagger_method == "equal":
                # Equal distribution based on position in list
                index = buckets.index(bucket_config)
                offset = (index * polling_interval) // len(buckets)
            else:
                # Random distribution (less predictable)
                offset = random.randint(0, polling_interval - 1)
            
            # Store the offset
            self.bucket_offsets[bucket_name] = offset
            
            # Track distribution
            offset_distribution[offset] += 1
            
            logger.debug(f"Bucket {bucket_name} offset: {offset}s")
        
        # Log distribution statistics
        avg_buckets_per_second = len(buckets) / polling_interval
        max_buckets_in_one_second = max(offset_distribution) if offset_distribution else 0
        
        logger.info(f"Staggering complete: avg {avg_buckets_per_second:.2f} buckets/second, " 
                    f"max {max_buckets_in_one_second} buckets in any one second")
    
    def init_thread_pools(self):
        """Initialize thread pools for parallel bucket scanning."""
        parallel_config = self.config.get("parallel", {})
        
        # Main thread pool for overall concurrency
        self.max_workers = parallel_config.get("max_workers", 60)
        logger.info(f"Initializing main thread pool with {self.max_workers} workers")
        
        # Create separate thread pools for each region (for rate limiting)
        self.region_thread_pools = {}
        rate_limit_per_region = parallel_config.get("rate_limit_per_region", 15)
        
        for region in self.get_all_regions():
            logger.info(f"Initializing thread pool for region {region} with {rate_limit_per_region} workers")

            self.region_thread_pools[region] = ThreadPoolExecutor(max_workers=rate_limit_per_region)
    
    def get_all_regions(self):
        """Get all unique regions from bucket configurations."""
        regions = set()
        default_endpoint = self.config.get("defaults", {}).get("endpoint", "us-east-1.linodeobjects.com")
        
        for bucket_config in self.config.get("buckets", []):
            endpoint = bucket_config.get("endpoint", default_endpoint)
            regions.add(endpoint)
            
        return regions
    
    def is_bucket_due_for_scan(self, bucket_config):
        """Check if a bucket is due for scanning including offset."""
        bucket_name = bucket_config["name"]
        
        # Get bucket-specific polling interval or use default
        polling_interval = self.config.get("defaults", {}).get("polling_interval", 60)
        
        # Get stagger offset for this bucket
        offset = self.bucket_offsets.get(bucket_name, 0)
        
        # Check if enough time has passed since last scan
        last_scan = self.last_scan_times.get(bucket_name, 0)
        current_time = time.time()
        
        # Apply offset to the check
        adjusted_due_time = last_scan + polling_interval
        
        # For the first scan, add the offset to current time
        if last_scan == 0:
            is_due = current_time >= offset  # Only becomes due after initial offset
            if is_due:
                logger.debug(f"Bucket {bucket_name} due for first scan after {offset:.2f}s offset")
        else:
            # For subsequent scans, check against normal interval
            is_due = current_time >= adjusted_due_time
            
        return is_due
    
    def get_buckets_due_for_scan(self):
        """Get list of buckets due for scanning."""
        due_buckets = []
        
        for bucket_config in self.config.get("buckets", []):
            if self.is_bucket_due_for_scan(bucket_config):
                due_buckets.append(bucket_config)
                
        return due_buckets
    
    def scan_bucket_with_region_pool(self, bucket_config, region_pool):
        """Submit bucket scan to the region-specific thread pool."""
        # This function runs in the main thread pool and delegates to region pool
        future = region_pool.submit(self.scan_bucket, bucket_config)
        return future.result()  # Wait for region-pool task to complete
    
    def scan_bucket(self, bucket_config):
        """Scan a bucket using a thread from the region's thread pool."""
        result_queue = queue.Queue()

        config_with_monitor = self.config.copy()
        config_with_monitor['monitor_instance'] = self
        scanner = BucketScanner(
            bucket_config,
            self.redis_client,
            config_with_monitor,
            self.config,
            result_queue,
            self.client_cache
        )
        
        # Run scanner in the current thread (which is from the region pool)
        scanner.run()
        
        # Get result
        if not result_queue.empty():
            return result_queue.get()
        else:
            return {"bucket": bucket_config["name"], "error": "No result returned from scanner"}
    
    def process_due_buckets(self):
        """Process all buckets that are due for scanning using thread pools."""
        # Get buckets due for scanning
        due_buckets = self.get_buckets_due_for_scan()
        logger.info(f"{len(due_buckets)} buckets due for scanning")
        
        if not due_buckets:
            return
            
        # Group buckets by region for regional rate limiting
        buckets_by_region = {}
        for bucket_config in due_buckets:
            endpoint = bucket_config.get("endpoint", self.config.get("defaults", {}).get("endpoint"))
            if endpoint not in buckets_by_region:
                buckets_by_region[endpoint] = []
            buckets_by_region[endpoint].append(bucket_config)
        
        # Process each region's buckets with its dedicated thread pool
        all_futures = []
        
        # Use the main thread pool to manage overall concurrency
        with ThreadPoolExecutor(max_workers=self.max_workers) as main_executor:
            # Process each region
            for region, region_buckets in buckets_by_region.items():
                region_pool = self.region_thread_pools.get(region)
                
                if not region_pool:
                    logger.warning(f"No thread pool for region {region}, creating one")
                    rate_limit = self.config.get("parallel", {}).get("rate_limit_per_region", 15)
                    region_pool = ThreadPoolExecutor(max_workers=rate_limit)
                    self.region_thread_pools[region] = region_pool
                
                # Submit each bucket to the region pool
                for bucket_config in region_buckets:
                    # We use the main executor to submit tasks that will use the region pool
                    # This gives us two levels of concurrency control
                    future = main_executor.submit(
                        self.scan_bucket_with_region_pool,
                        bucket_config,
                        region_pool
                    )
                    all_futures.append(future)
            
            # Collect results as they complete
            results = []
            for future in as_completed(all_futures):
                try:
                    # Get the result (will raise exception if the task failed)
                    result = future.result()
                    if result:
                        results.append(result)
                        if "error" in result:
                            logger.error(f"Error scanning bucket {result.get('bucket', 'unknown')}: {result.get('error')}")
                            self.scan_stats["errors"] += 1
                except Exception as e:
                    logger.error(f"Exception in bucket scan task: {e}")
                    self.scan_stats["errors"] += 1
            
            return results
    
    def update_scan_stats(self, results):
        """Update overall scan statistics from bucket scan results."""
        if not results:
            return
            
        self.scan_stats["total_scans"] += 1
        self.scan_stats["last_scan_time"] = time.time()
        
        # Initialize deleted objects counter if it doesn't exist
        if "total_deleted_objects" not in self.scan_stats:
            self.scan_stats["total_deleted_objects"] = 0
        
        # Track scan durations (last 10)
        scan_durations = [r.get("scan_duration", 0) for r in results if "scan_duration" in r]
        if scan_durations:
            avg_duration = sum(scan_durations) / len(scan_durations)
            self.scan_stats["scan_durations"].append(avg_duration)
            if len(self.scan_stats["scan_durations"]) > 10:
                self.scan_stats["scan_durations"].pop(0)
        
        # Sum up object counts
        for result in results:
            if "error" in result:
                continue
                
            self.scan_stats["total_objects_scanned"] += result.get("objects_scanned", 0)
            self.scan_stats["total_new_objects"] += result.get("new_objects", 0)
            self.scan_stats["total_updated_objects"] += result.get("updated_objects", 0)
            self.scan_stats["total_detected_objects"] += result.get("detected_objects", 0)
            self.scan_stats["total_deleted_objects"] += result.get("deleted_objects", 0) 

    def add_bucket_to_active_config(self, bucket_info):
        """Add a new bucket to the active configuration."""
        try:
            logger.info(f"Starting to add bucket: {bucket_info.get('name')}")
            # Format the bucket info correctly

            logger.info(f"Input bucket_info keys: {list(bucket_info.keys())}")

            bucket_config = bucket_info.copy()

            #logger.info(f"Has client_id: {'client_id' in bucket_info}, Value: {bucket_info.get('client_id', 'None')}")
            #logger.info(f"Has client_secret: {'client_secret' in bucket_info}, Value: {'<present>' if 'client_secret' in bucket_info else 'None'}")
            #logger.info(f"Has token_url: {'token_url' in bucket_info}, Value: {bucket_info.get('token_url', 'None')}")

            """bucket_config = {
                "name": bucket_info["name"],
                "access_key": bucket_info["access_key"],
                "secret_key": bucket_info["secret_key"],
                "endpoint": bucket_info.get("region", bucket_info.get("endpoint")),
                "webhook_url": bucket_info["webhook_url"],
                "status": "active",
                "added_at": datetime.now().isoformat()
            }"""


            
            # Add authentication details if provided
            """
            if "client_id" in bucket_info and "client_secret" in bucket_info and "token_url" in bucket_info:
                bucket_config["webhook_auth"] = {
                    "type": "oauth2",
                    "client_id": bucket_info["client_id"],
                    "client_secret": bucket_info["client_secret"],
                    "token_url": bucket_info.get("token_url")
                }"""
            # Add authentication details if provided - SIMPLIFIED APPROACH
            if "client_id" in bucket_info:
                # Initialize webhook_auth if not present
                if "webhook_auth" not in bucket_config:
                    bucket_config["webhook_auth"] = {"type": "oauth2"}

                bucket_config["webhook_auth"]["client_id"] = bucket_info["client_id"]

            if "client_secret" in bucket_info:
                # Initialize webhook_auth if not present
                if "webhook_auth" not in bucket_config:
                    bucket_config["webhook_auth"] = {"type": "oauth2"}

                bucket_config["webhook_auth"]["client_secret"] = bucket_info["client_secret"]

            if "token_url" in bucket_info:
                # Initialize webhook_auth if not present
                if "webhook_auth" not in bucket_config:
                    bucket_config["webhook_auth"] = {"type": "oauth2"}

                bucket_config["webhook_auth"]["token_url"] = bucket_info["token_url"]

            # Log the final bucket config before saving
            logger.info(f"Final bucket config before Redis save: {json.dumps(bucket_config, default=str)}")
            
            # Add to Redis for persistence (NO TTL)
            logger.info(f"Saving bucket config to Redis: {bucket_config.get('name')}")
            redis_result = save_bucket_config(self.redis_client, bucket_config)

            if not redis_result:
                logger.error(f"Failed to save bucket config to Redis: {bucket_config.get('name')}")
                return False
            
            # Add to in-memory configuration
            logger.info(f"Adding bucket to in-memory config: {bucket_config.get('name')}")
            self.config.setdefault("buckets", []).append(bucket_config)
            
            # Initialize scan metadata for this bucket
            logger.info(f"Initializing scan metadata for bucket: {bucket_config.get('name')}")
            polling_interval = self.config.get("defaults", {}).get("polling_interval", 60)
            self.bucket_offsets[bucket_config["name"]] = random.randint(0, polling_interval - 1)
            #self.bucket_offsets[bucket_info["name"]] = random.randint(
             #   0, self.config.get("defaults", {}).get("polling_interval", 60) - 1
            #)
            
            # If the bucket's region is new, create a thread pool for it
            endpoint = bucket_config["endpoint"]
            logger.info(f"Checking thread pool for region: {endpoint}")
            if endpoint not in self.region_thread_pools:
                rate_limit = self.config.get("parallel", {}).get("rate_limit_per_region", 15)
                self.region_thread_pools[endpoint] = ThreadPoolExecutor(max_workers=rate_limit)
                logger.info(f"Created new thread pool for region {endpoint}")
            
            logger.info(f"Added new bucket {bucket_config['name']} to active configuration")
            return True
            
        except Exception as e:
            logger.error(f"Error adding bucket to active configuration: {e}", exc_info=True)
            return False
        
    def update_bucket_config(self, bucket_info):
        """Update an existing bucket configuration."""
        try:
            bucket_name = bucket_info["name"]
            
            # Find existing bucket in memory
            bucket_found = False
            for i, bucket in enumerate(self.config.get("buckets", [])):
                if bucket["name"] == bucket_name:
                    bucket_found = True
                    
                    # Update fields that are present in the request
                    if "access_key" in bucket_info:
                        self.config["buckets"][i]["access_key"] = bucket_info["access_key"]
                    if "secret_key" in bucket_info:
                        self.config["buckets"][i]["secret_key"] = bucket_info["secret_key"]
                    if "region" in bucket_info:
                        self.config["buckets"][i]["endpoint"] = bucket_info["region"]
                    elif "endpoint" in bucket_info:
                        self.config["buckets"][i]["endpoint"] = bucket_info["endpoint"]
                    if "webhook_url" in bucket_info:
                        self.config["buckets"][i]["webhook_url"] = bucket_info["webhook_url"]
                    
                    # Update authentication if provided
                    if "client_id" in bucket_info and "client_secret" in bucket_info:
                        self.config["buckets"][i]["webhook_auth"] = {
                            "type": "oauth2",
                            "client_id": bucket_info["client_id"],
                            "client_secret": bucket_info["client_secret"],
                            "token_url": bucket_info.get("token_url")
                        }
                    
                    # Save to Redis
                    save_bucket_config(self.redis_client, self.config["buckets"][i])
                    
                    # If endpoint changed, clear client cache
                    if "region" in bucket_info or "endpoint" in bucket_info:
                        self.client_cache.clear_cache()
                    
                    logger.info(f"Updated configuration for bucket {bucket_name}")
                    break
            
            return bucket_found
            
        except Exception as e:
            logger.error(f"Error updating bucket configuration: {e}")
            return False
            
    def delete_bucket_config(self, bucket_name):
        """Delete a bucket from the configuration."""
        try:
            # Find and remove from in-memory config
            self.config["buckets"] = [b for b in self.config.get("buckets", []) if b["name"] != bucket_name]
            
            # Remove from Redis
            client = self.redis_client.get("master", self.redis_client) if isinstance(self.redis_client, dict) else self.redis_client
            client.delete(f"linode:objstore:config:bucket:{bucket_name}")
            
            # Clean up any state associated with this bucket
            client.delete(f"linode:objstore:last_scan:{bucket_name}")
            client.delete(f"linode:objstore:config:{bucket_name}:notifications_disabled")
            
            # Clean up offset
            if bucket_name in self.bucket_offsets:
                del self.bucket_offsets[bucket_name]
            
            logger.info(f"Deleted bucket {bucket_name} from configuration")
            return True
            
        except Exception as e:
            logger.error(f"Error deleting bucket configuration: {e}")
            return False

    def detect_and_clean_deletions(self, bucket_name, current_objects):
        """Detect objects that were deleted and clean up their state"""
        try:
            state_prefix = self.config.get("redis", {}).get("state_prefix", "linode:objstore:state:")
            pattern = f"{state_prefix}{bucket_name}:*"
        
            # Get all stored states for this bucket
            client = self.redis_client.get("slave", self.redis_client) if isinstance(self.redis_client, dict) else self.redis_client
            stored_keys = client.keys(pattern)
        
            if not stored_keys:
                return 0
        
            # Extract object keys from Redis keys
            stored_objects = {}
            prefix_len = len(f"{state_prefix}{bucket_name}:")
        
            for redis_key in stored_keys:
                # Extract object key from Redis key
                object_key = redis_key[prefix_len:]
                stored_objects[object_key] = redis_key
        
            # Find current objects
            current_object_keys = set(obj["Key"] for obj in current_objects)
        
            # Find deleted objects
            deleted_objects = set(stored_objects.keys()) - current_object_keys
        
            # Clean up state for deleted objects
            if deleted_objects:
                master_client = self.redis_client.get("master", self.redis_client) if isinstance(self.redis_client, dict) else self.redis_client
            
                # Use pipeline for efficient deletion
                pipe = master_client.pipeline()
                for object_key in deleted_objects:
                    redis_key = stored_objects[object_key]
                    pipe.delete(redis_key)
                    logger.info(f"Cleaning up state for deleted object: {bucket_name}/{object_key}")
            
                pipe.execute()
                logger.info(f"Cleaned up {len(deleted_objects)} deleted objects from bucket {bucket_name}")
            
                return len(deleted_objects)
        
            return 0
        
        except Exception as e:
            logger.error(f"Error detecting deletions for bucket {bucket_name}: {e}")
            return 0


    def start_health_server(self):
        """Start a simple HTTP server for health checks and API."""
        class APIHandler(http.server.SimpleHTTPRequestHandler):
            def __init__(self2, *args, **kwargs):
                self2.monitor = self
                super().__init__(*args, **kwargs)
            
            def authenticate_request(self2):
                """Authenticate a request using JWT."""
                auth_header = self2.headers.get('Authorization')
                if not auth_header or not auth_header.startswith('Bearer '):
                    self2.send_response(401)
                    self2.send_header('Content-Type', 'application/json')
                    self2.end_headers()
                    self2.wfile.write(json.dumps({"error": "Authentication required", "message": "Use Bearer token authentication"}).encode())
                    return None
                
                token = auth_header[7:]  # Remove 'Bearer ' prefix
                payload = validate_jwt_token(token)
                
                if not payload:
                    self2.send_response(401)
                    self2.send_header('Content-Type', 'application/json')
                    self2.end_headers()
                    self2.wfile.write(json.dumps({"error": "Invalid or expired token"}).encode())
                    return None
                
                return payload
            
            def do_GET(self2):
                if self2.path == '/health':
                    # Basic health check
                    health_status = {"status": "ok"}
                    self2.send_response(200)
                    self2.send_header('Content-Type', 'application/json')
                    self2.end_headers()
                    self2.wfile.write(json.dumps(health_status).encode())
                    
                elif self2.path == '/ready':
                    # Check if Redis is available with Sentinel status
                    redis_ok = check_redis_health(self.redis_client)
                    sentinel_ok = check_sentinel_health(self.config)["status"] == "ok"
                    
                    status_code = 200 if (redis_ok and sentinel_ok) else 503
                    ready_status = {
                        "status": "ready" if (redis_ok and sentinel_ok) else "not_ready",
                        "redis": "ok" if redis_ok else "error",
                        "sentinel": "ok" if sentinel_ok else "error"
                    }
                    self2.send_response(status_code)
                    self2.send_header('Content-Type', 'application/json')
                    self2.end_headers()
                    self2.wfile.write(json.dumps(ready_status).encode())
                    
                elif self2.path == '/metrics':
                    # Return current metrics with Sentinel status
                    redis_stats = check_queue_stats(self.redis_client, self.config)
                    sentinel_stats = check_sentinel_health(self.config)
                    client_stats = self.client_cache.get_stats()
                    
                    metrics = {
                        "scan_stats": self.scan_stats,
                        "client_cache": client_stats,
                        "redis": redis_stats,
                        "sentinel": sentinel_stats,
                        "buckets": {
                            "total": len(self.config.get("buckets", [])),
                            "scanned": len(self.last_scan_times)
                        }
                    }
                    
                    self2.send_response(200)
                    self2.send_header('Content-Type', 'application/json')
                    self2.end_headers()
                    self2.wfile.write(json.dumps(metrics).encode())
                
                elif self2.path == '/api/bucket-notifications/status':

                    # Authenticate request
                    payload = self2.authenticate_request()
                    if not payload:
                        return
                    
                    self.refresh_config_from_redis()
                    
                    # Get notification status for all buckets
                    statuses = []
                    for bucket_config in self.config.get("buckets", []):
                        bucket_name = bucket_config["name"]
                        enabled = is_bucket_notifications_enabled(self.redis_client, bucket_name)
                        webhook_url = bucket_config.get("webhook_url", "Not configured")
                        statuses.append({
                            "name": bucket_name,
                            "notifications_enabled": enabled,
                            "webhook_url": webhook_url
                        })
                    
                    self2.send_response(200)
                    self2.send_header('Content-Type', 'application/json')
                    self2.send_header('Access-Control-Allow-Origin', '*')
                    self2.end_headers()
                    self2.wfile.write(json.dumps({"buckets": statuses}).encode())

                elif self2.path == '/api/buckets/pending':
                    # Get pending bucket configurations
                    pending_buckets = get_pending_bucket_configs(self.redis_client)
                    
                    self2.send_response(200)
                    self2.send_header('Content-Type', 'application/json')
                    self2.end_headers()
                    self2.wfile.write(json.dumps({
                        "pending_buckets": pending_buckets,
                        "count": len(pending_buckets)
                    }).encode())
                    
                else:
                    self2.send_response(404)
                    self2.end_headers()
            
            def do_POST(self2):
                import urllib.parse
                
                parsed_url = urllib.parse.urlparse(self2.path)
                query_params = urllib.parse.parse_qs(parsed_url.query)

                if parsed_url.path == '/api/auth/token':
                    logger.info("Token request received")
                    try:
                        # Handle token generation without authentication
                        content_length = int(self2.headers.get('Content-Length', 0))
                        logger.debug(f"Content length: {content_length}")
                        
                        if content_length == 0:
                            logger.error("No content provided in token request")
                            self2.send_response(400)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({"error": "No content provided"}).encode())
                            return
                        
                        post_data = self2.rfile.read(content_length)
                        logger.debug(f"Raw data: {post_data}")
                        
                        try:
                            data = json.loads(post_data.decode('utf-8'))
                            logger.debug(f"Parsed data: {data}")
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON decode error: {e}")
                            self2.send_response(400)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({"error": f"Invalid JSON: {str(e)}"}).encode())
                            return
                        
                        client_id = data.get('client_id')
                        client_secret = data.get('client_secret')
                        
                        logger.debug(f"Received client_id: {client_id}")
                        # Don't log the actual secret, but log if it's present
                        logger.debug(f"client_secret provided: {client_secret is not None}")
                        
                        if not client_id or not client_secret:
                            logger.error("Missing credentials in token request")
                            self2.send_response(400)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({"error": "Missing credentials", "message": "client_id and client_secret are required"}).encode())
                            return
                        
                        # Validate credentials
                        logger.debug("Validating credentials...")
                        valid = validate_client_credentials(client_id, client_secret)
                        logger.debug(f"Credentials valid: {valid}")
                        
                        if not valid:
                            logger.error("Invalid credentials provided")
                            self2.send_response(401)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({"error": "Invalid credentials"}).encode())
                            return
                        
                        # Generate JWT token
                        logger.debug("Generating token...")
                        try:
                            token = generate_jwt_token(client_id)
                            logger.debug("Token generated successfully")
                        except Exception as e:
                            logger.error(f"Error generating token: {e}")
                            self2.send_response(500)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({"error": f"Token generation failed: {str(e)}"}).encode())
                            return
                        
                        # Send response
                        logger.debug("Sending token response")
                        self2.send_response(200)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        response_data = {
                            "access_token": token,
                            "token_type": "Bearer",
                            "expires_in": 86400  # 24 hours
                        }
                        response_json = json.dumps(response_data)
                        logger.debug(f"Response prepared: {len(response_json)} bytes")
                        self2.wfile.write(response_json.encode())
                        logger.info("Token response sent successfully")
                        
                    except Exception as e:
                        logger.error(f"Unhandled exception in token endpoint: {e}", exc_info=True)
                        try:
                            self2.send_response(500)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({"error": f"Server error: {str(e)}"}).encode())
                        except:
                            logger.critical("Failed to send error response", exc_info=True)

                    return

                
                if parsed_url.path == '/api/bucket-notifications/disable':

                    # Authenticate request
                    payload = self2.authenticate_request()
                    if not payload:
                        return
                    
                    # Get bucket parameter
                    bucket_name = query_params.get('bucket', [None])[0]
                    
                    if not bucket_name:
                        self2.send_response(400)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        self2.wfile.write(json.dumps({"error": "Missing bucket parameter"}).encode())
                        return
                    
                    # Check if bucket exists
                    bucket_exists = any(b["name"] == bucket_name for b in self.config.get("buckets", []))
                    if not bucket_exists:
                        self2.send_response(404)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        self2.wfile.write(json.dumps({"error": "Bucket not found"}).encode())
                        return
                    
                    # Disable notifications
                    disable_bucket_notifications(self.redis_client, bucket_name)
                    logger.info(f"Notifications disabled for bucket {bucket_name} via API")
                    
                    self2.send_response(200)
                    self2.send_header('Content-Type', 'application/json')
                    self2.send_header('Access-Control-Allow-Origin', '*')
                    self2.end_headers()
                    self2.wfile.write(json.dumps({
                        "status": "success",
                        "bucket": bucket_name,
                        "notifications": "disabled"
                    }).encode())
                    
                elif parsed_url.path == '/api/bucket-notifications/enable':

                    # Authenticate request
                    payload = self2.authenticate_request()
                    if not payload:
                        return
                    
                    # Get bucket parameter
                    bucket_name = query_params.get('bucket', [None])[0]
                    
                    if not bucket_name:
                        self2.send_response(400)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        self2.wfile.write(json.dumps({"error": "Missing bucket parameter"}).encode())
                        return
                    
                    # Check if bucket exists
                    bucket_exists = any(b["name"] == bucket_name for b in self.config.get("buckets", []))
                    if not bucket_exists:
                        self2.send_response(404)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        self2.wfile.write(json.dumps({"error": "Bucket not found"}).encode())
                        return
                    
                    # Enable notifications
                    enable_bucket_notifications(self.redis_client, bucket_name)
                    logger.info(f"Notifications enabled for bucket {bucket_name} via API")
                    
                    self2.send_response(200)
                    self2.send_header('Content-Type', 'application/json')
                    self2.send_header('Access-Control-Allow-Origin', '*')
                    self2.end_headers()
                    self2.wfile.write(json.dumps({
                        "status": "success",
                        "bucket": bucket_name,
                        "notifications": "enabled"
                    }).encode())

                # Add a new bucket
                elif parsed_url.path == '/api/buckets/add':

                    # Authenticate request
                    payload = self2.authenticate_request()
                    if not payload:
                        return
                    
                    # Parse request body
                    content_length = int(self2.headers['Content-Length'])
                    post_data = self2.rfile.read(content_length)
                    
                    try:
                        data = json.loads(post_data.decode('utf-8'))
                        
                        # Validate required fields
                        required_fields = ['name', 'access_key', 'secret_key', 'webhook_url']
                        for field in required_fields:
                            if field not in data:
                                self2.send_response(400)
                                self2.send_header('Content-Type', 'application/json')
                                self2.end_headers()
                                self2.wfile.write(json.dumps({
                                    "error": f"Missing required field: {field}"
                                }).encode())
                                return
                        
                        # Check if region or endpoint is provided
                        if 'region' not in data and 'endpoint' not in data:
                            self2.send_response(400)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({
                                "error": "Either 'region' or 'endpoint' field is required"
                            }).encode())
                            return
                        
                        # Check if bucket already exists
                        bucket_exists = any(b["name"] == data["name"] for b in self.config.get("buckets", []))
                        if bucket_exists:
                            self2.send_response(409)  # Conflict
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({
                                "error": f"Bucket {data['name']} already exists"
                            }).encode())
                            return
                        
                        # Extract S3 credentials for Infisical
                        bucket_name = data["name"]
                        access_key = data["access_key"]
                        secret_key = data["secret_key"]
                        
                        # Step 1: Add S3 credentials to Infisical
                        try:
                            if not add_credentials_to_infisical(bucket_name, access_key, secret_key):
                                raise Exception("Failed to store credentials in Infisical")
                            logger.info(f"Successfully stored credentials in Infisical for bucket {bucket_name}")
                        except Exception as e:
                            logger.error(f"Failed to store credentials in Infisical: {e}")
                            self2.send_response(500)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({
                                "error": f"Failed to store credentials securely: {str(e)}"
                            }).encode())
                            return
                            
                        # Step 2: Create bucket configuration WITHOUT S3 credentials
                        bucket_config = {
                            "name": data["name"],
                            # NO access_key or secret_key stored here
                            "endpoint": data.get("region", data.get("endpoint")),
                            "webhook_url": data["webhook_url"],
                            "status": "active",
                            "added_at": datetime.now().isoformat()
                        }
                        
                        # Add authentication if provided (OAuth stays in Redis)
                        if "client_id" in data and "client_secret" in data and "token_url" in data:
                            bucket_config["webhook_auth"] = {
                                "type": "oauth2",
                                "client_id": data["client_id"],
                                "client_secret": data["client_secret"],
                                "token_url": data.get("token_url")
                            }
                        
                        # Step 3: Add to active configuration
                        success = self.add_bucket_to_active_config(bucket_config)

                        if success:
                            # Refresh from Redis to ensure consistency
                            self.refresh_config_from_redis()
                            
                            self2.send_response(201)  # Created
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({
                                "status": "success",
                                "message": f"Bucket {data['name']} added to active configuration",
                                "bucket": data['name'],
                                "note": "S3 credentials stored securely in vault"
                            }).encode())
                        else:
                            # Rollback: Try to remove credentials from Infisical if Redis save failed
                            try:
                                delete_credentials_from_infisical(bucket_name)
                                logger.warning(f"Rolled back Infisical credentials for {bucket_name} after Redis failure")
                            except Exception as rollback_error:
                                logger.error(f"Failed to rollback Infisical credentials: {rollback_error}")
                            
                            self2.send_response(500)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({
                                "status": "error",
                                "message": "Failed to add bucket to active configuration"
                            }).encode())
                    
                    except json.JSONDecodeError:
                        self2.send_response(400)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        self2.wfile.write(json.dumps({
                            "error": "Invalid JSON payload"
                        }).encode())

                # Update an existing bucket
                elif parsed_url.path == '/api/buckets/update':

                    # Authenticate request
                    payload = self2.authenticate_request()
                    if not payload:
                        return
                    
                    # Parse request body
                    content_length = int(self2.headers['Content-Length'])
                    post_data = self2.rfile.read(content_length)
                    
                    try:
                        data = json.loads(post_data.decode('utf-8'))
                        
                        # Validate required fields
                        if 'name' not in data:
                            self2.send_response(400)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({
                                "error": "Missing required field: name"
                            }).encode())
                            return
                        
                        # Find existing bucket
                        bucket_name = data['name']
                        bucket_exists = False
                        
                        # Check if S3 credentials are being updated
                        updating_s3_creds = "access_key" in data or "secret_key" in data
                        
                        if updating_s3_creds:
                            # If updating S3 credentials, both must be provided
                            if not ("access_key" in data and "secret_key" in data):
                                self2.send_response(400)
                                self2.send_header('Content-Type', 'application/json')
                                self2.end_headers()
                                self2.wfile.write(json.dumps({
                                    "error": "Both access_key and secret_key must be provided when updating credentials"
                                }).encode())
                                return
                            
                            # Update credentials in Infisical
                            try:
                                if not add_credentials_to_infisical(bucket_name, data["access_key"], data["secret_key"]):
                                    raise Exception("Failed to update credentials in Infisical")
                                logger.info(f"Successfully updated credentials in Infisical for bucket {bucket_name}")
                            except Exception as e:
                                logger.error(f"Failed to update credentials in Infisical: {e}")
                                self2.send_response(500)
                                self2.send_header('Content-Type', 'application/json')
                                self2.end_headers()
                                self2.wfile.write(json.dumps({
                                    "error": f"Failed to update credentials securely: {str(e)}"
                                }).encode())
                                return
                        
                        for i, bucket in enumerate(self.config.get("buckets", [])):
                            if bucket["name"] == bucket_name:
                                bucket_exists = True
                                
                                # Update non-sensitive fields only
                                # DO NOT update access_key or secret_key in Redis
                                if "region" in data:
                                    self.config["buckets"][i]["endpoint"] = data["region"]
                                elif "endpoint" in data:
                                    self.config["buckets"][i]["endpoint"] = data["endpoint"]
                                if "webhook_url" in data:
                                    self.config["buckets"][i]["webhook_url"] = data["webhook_url"]
                                
                                # Update authentication details if provided
                                if "client_id" in data and "client_secret" in data:
                                    # Create or update webhook_auth section
                                    self.config["buckets"][i]["webhook_auth"] = {
                                        "type": "oauth2",
                                        "client_id": data["client_id"],
                                        "client_secret": data["client_secret"]
                                    }
                                    # Add token URL if provided
                                    if "token_url" in data:
                                        self.config["buckets"][i]["webhook_auth"]["token_url"] = data["token_url"]
                                
                                # Mark update time
                                self.config["buckets"][i]["updated_at"] = datetime.now().isoformat()
                                
                                # Save to Redis (without S3 credentials)
                                save_bucket_config(self.redis_client, self.config["buckets"][i])

                                self.refresh_config_from_redis()
                                
                                # If endpoint changed, clear client cache
                                if "region" in data or "endpoint" in data:
                                    self.client_cache.clear_cache()
                                
                                break
                        
                        if not bucket_exists:
                            self2.send_response(404)
                            self2.send_header('Content-Type', 'application/json')
                            self2.end_headers()
                            self2.wfile.write(json.dumps({
                                "error": f"Bucket {bucket_name} not found"
                            }).encode())
                            return
                        
                        self2.send_response(200)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        response_message = {
                            "status": "success",
                            "message": f"Bucket {bucket_name} updated"
                        }
                        if updating_s3_creds:
                            response_message["note"] = "S3 credentials updated in vault"
                        self2.wfile.write(json.dumps(response_message).encode())
                    
                    except json.JSONDecodeError:
                        self2.send_response(400)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        self2.wfile.write(json.dumps({
                            "error": "Invalid JSON payload"
                        }).encode()) 


                elif parsed_url.path == '/api/buckets/delete':
                    # Authenticate request
                    payload = self2.authenticate_request()
                    if not payload:
                        return
                    
                    # Get bucket parameter
                    bucket_name = query_params.get('bucket', [None])[0]
                    
                    if not bucket_name:
                        self2.send_response(400)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        self2.wfile.write(json.dumps({"error": "Missing bucket parameter"}).encode())
                        return
                    
                    # Check if bucket exists
                    bucket_exists = any(b["name"] == bucket_name for b in self.config.get("buckets", []))
                    if not bucket_exists:
                        self2.send_response(404)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        self2.wfile.write(json.dumps({"error": "Bucket not found"}).encode())
                        return
                    
                    # Step 1: Delete from Infisical
                    try:
                        delete_credentials_from_infisical(bucket_name)
                        logger.info(f"Deleted credentials from Infisical for bucket {bucket_name}")
                    except Exception as e:
                        logger.error(f"Failed to delete credentials from Infisical: {e}")
                        # Continue with deletion even if Infisical fails
                    
                    # Step 2: Delete from Redis and in-memory config
                    success = self.delete_bucket_config(bucket_name)
                    
                    if success:
                        self2.send_response(200)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        self2.wfile.write(json.dumps({
                            "status": "success",
                            "message": f"Bucket {bucket_name} deleted completely"
                        }).encode())
                    else:
                        self2.send_response(500)
                        self2.send_header('Content-Type', 'application/json')
                        self2.end_headers()
                        self2.wfile.write(json.dumps({
                            "error": "Failed to delete bucket configuration"
                        }).encode())       

                else:
                    self2.send_response(404)
                    self2.end_headers()
            
            def do_OPTIONS(self2):
                # Handle CORS preflight requests
                self2.send_response(200)
                self2.send_header('Access-Control-Allow-Origin', '*')
                self2.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
                self2.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
                self2.end_headers()
                
            def log_message(self2, format, *args):
                # Suppress logs from the HTTP server
                logger.debug(f"API request: {format % args}")
        
        def start_server():
            httpd = socketserver.ThreadingTCPServer(('', 8080), APIHandler)
            httpd.serve_forever()
            
        # Start in a separate thread
        server_thread = threading.Thread(target=start_server, daemon=True)
        server_thread.start()
        logger.info("Health check and API server started on port 8080")
        
    def run(self):
        """Run the monitor in a continuous loop."""
        logger.info("Starting Multi-Region Object Storage Monitor with Redis Sentinel Support")
        logger.info(f"Monitoring {len(self.config.get('buckets', []))} buckets across {len(self.get_all_regions())} regions")
        logger.info(f"Polling interval: {self.config.get('defaults', {}).get('polling_interval', 60)}s")
        
        # Check Redis Sentinel status
        sentinel_status = check_sentinel_health(self.config)

        if sentinel_status["status"] == "ok":
            logger.info(f"Redis Sentinel active: Master at {sentinel_status.get('master')}, {sentinel_status.get('slave_count')} slaves")
        else:
            logger.warning(f"Redis Sentinel not available: {sentinel_status.get('error')}")
        
        while self.running:
            try:
                start_time = time.time()
                
                # Process due buckets
                results = self.process_due_buckets()
                
                # Update stats
                if results:
                    self.update_scan_stats(results)
                
                # Update last scan times from Redis
                self.load_last_scan_times()
                
                # Log client cache stats occasionally
                if self.scan_stats["total_scans"] % 10 == 0:
                    cache_stats = self.client_cache.get_stats()
                    logger.info(
                        f"Client cache stats: "
                        f"{cache_stats['total_clients']} total clients, "
                        f"{cache_stats['cache_hits']} hits, "
                        f"{cache_stats['cache_misses']} misses"
                    )
                
                # Brief sleep before next check for due buckets
                elapsed = time.time() - start_time
                logger.debug(f"Scan cycle completed in {elapsed:.2f}s")
                
                # Sleep a short time before checking again
                time.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in monitor main loop: {e}")
                # Don't crash, sleep and retry
                time.sleep(30)
        
        logger.info("Monitor shutting down")

if __name__ == "__main__":
    monitor = MultiRegionMonitor()
    monitor.run()
