#!/usr/bin/env python3
"""
Linode Object Storage Monitor

This script monitors Linode Object Storage buckets across multiple regions,
detects new or updated objects, and sends notifications via a Redis queue.
It uses optimized thread pools and staggered scanning for efficiency.
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
    check_queue_stats
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
                "errors": 0,
                "scan_duration": 0
            }
            
            # Use paginator to handle buckets with many objects
            paginator = s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket_name)
            
            for page in page_iterator:
                if "Contents" not in page:
                    continue
                    
                for obj in page["Contents"]:
                    stats["objects_scanned"] += 1
                    
                    # Check if we've hit the batch limit
                    max_objects = self.global_config.get("defaults", {}).get("max_objects_per_batch", 1000)
                    if stats["objects_scanned"] >= max_objects:
                        logger.info(f"Reached max objects per batch for {bucket_name}, will resume in next scan")
                        break
                    
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
                        # Object is not recent but missing state - likely state expired
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
                    
                    # Publish to queue
                    if publish_notification(self.redis_client, self.global_config, message):
                        # Update state after successful queue publish
                        save_object_state(self.redis_client, self.global_config, bucket_name, object_key, {
                            "etag": object_etag,
                            "last_modified": object_modified,
                            "size": obj["Size"],
                            "last_processed": scan_time
                        })
                        
                        # Update statistics based on event type
                        if event_type == "created":
                            stats["new_objects"] += 1
                        elif event_type == "updated":
                            stats["updated_objects"] += 1
                        elif event_type == "detected":
                            stats["detected_objects"] += 1
            
            # Update last scan time
            self.redis_client.set(f"linode:objstore:last_scan:{bucket_name}", scan_time)
            
            # Calculate scan duration
            stats["scan_duration"] = time.time() - start_time
            
            # Add result to queue
            self.result_queue.put(stats)
            
            logger.info(
                f"Bucket {bucket_name} scan complete: "
                f"{stats['new_objects']} new, {stats['updated_objects']} updated, "
                f"{stats['detected_objects']} detected, {stats['objects_scanned']} scanned, "
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
    
    def handle_signal(self, signum, frame):
        """Handle termination signals."""
        logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
        
    def load_last_scan_times(self):
        """Load last scan times for all buckets from Redis."""
        try:
            for bucket_config in self.config.get("buckets", []):
                bucket_name = bucket_config["name"]
                last_scan = self.redis_client.get(f"linode:objstore:last_scan:{bucket_name}")
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
        scanner = BucketScanner(
            bucket_config,
            self.redis_client,
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
    
    def start_health_server(self):
        """Start a simple HTTP server for health checks."""
        class HealthHandler(http.server.SimpleHTTPRequestHandler):
            def __init__(self2, *args, **kwargs):
                self2.monitor = self
                super().__init__(*args, **kwargs)
                
            def do_GET(self2):
                if self2.path == '/health':
                    # Basic health check
                    health_status = {"status": "ok"}
                    self2.send_response(200)
                    self2.send_header('Content-Type', 'application/json')
                    self2.end_headers()
                    self2.wfile.write(json.dumps(health_status).encode())
                    
                elif self2.path == '/ready':
                    # Check if Redis is available
                    redis_ok = check_redis_health(self.redis_client)
                    status_code = 200 if redis_ok else 503
                    ready_status = {
                        "status": "ready" if redis_ok else "not_ready",
                        "redis": "ok" if redis_ok else "error"
                    }
                    self2.send_response(status_code)
                    self2.send_header('Content-Type', 'application/json')
                    self2.end_headers()
                    self2.wfile.write(json.dumps(ready_status).encode())
                    
                elif self2.path == '/metrics':
                    # Return current metrics
                    redis_stats = check_queue_stats(self.redis_client, self.config)
                    client_stats = self.client_cache.get_stats()
                    
                    metrics = {
                        "scan_stats": self.scan_stats,
                        "client_cache": client_stats,
                        "redis": redis_stats,
                        "buckets": {
                            "total": len(self.config.get("buckets", [])),
                            "scanned": len(self.last_scan_times)
                        }
                    }
                    
                    self2.send_response(200)
                    self2.send_header('Content-Type', 'application/json')
                    self2.end_headers()
                    self2.wfile.write(json.dumps(metrics).encode())
                    
                else:
                    self2.send_response(404)
                    self2.end_headers()
                    
            def log_message(self2, format, *args):
                # Suppress logs from the HTTP server
                pass
        
        def start_server():
            httpd = socketserver.ThreadingTCPServer(('', 8080), HealthHandler)
            httpd.serve_forever()
            
        # Start in a separate thread
        server_thread = threading.Thread(target=start_server, daemon=True)
        server_thread.start()
        logger.info("Health check server started on port 8080")
    
    def run(self):
        """Run the monitor in a continuous loop."""
        logger.info("Starting Multi-Region Object Storage Monitor")
        logger.info(f"Monitoring {len(self.config.get('buckets', []))} buckets across {len(self.get_all_regions())} regions")
        logger.info(f"Polling interval: {self.config.get('defaults', {}).get('polling_interval', 60)}s")
        
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
