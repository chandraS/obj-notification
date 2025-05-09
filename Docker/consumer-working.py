#!/usr/bin/env python3
"""
Webhook Consumer for Linode Object Storage Monitor

This script reads notifications from the Redis queue and
delivers them to the configured webhook endpoint.
It handles retries, circuit breaking, and rate limiting.

Supports Redis Sentinel for high availability.
"""

import json
import os
import time
import base64
import threading
import http.server
import socketserver
import requests
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Import utility functions
from utils import (
    setup_logging,
    load_config,
    create_redis_client,
    get_notifications,
    check_redis_health,
    check_queue_stats,
    check_sentinel_health,
    get_bucket_webhook_url,
    get_bucket_config,
    clear_oauth_token_cache,
    get_oauth_token
)

# Set up logging
logger = setup_logging("webhook-consumer")

class CircuitBreaker:
    """Circuit breaker pattern implementation for webhook endpoints."""
    
    def __init__(self, failure_threshold=5, reset_timeout=60):
        """Initialize the circuit breaker."""
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failures = 0
        self.last_failure_time = 0
        self.state = "closed"  # closed, open, half-open
        self.lock = threading.Lock()
    
    def allow_request(self):
        """Check if a request should be allowed based on circuit state."""
        with self.lock:
            if self.state == "closed":
                return True
            elif self.state == "open":
                # Check if it's time to try again
                if time.time() - self.last_failure_time > self.reset_timeout:
                    logger.info("Circuit half-open, allowing test request")
                    self.state = "half-open"
                    return True
                return False
            elif self.state == "half-open":
                return True
    
    def record_success(self):
        """Record a successful request."""
        with self.lock:
            if self.state == "half-open":
                logger.info("Circuit closed after successful test request")
                self.state = "closed"
            self.failures = 0
    
    def record_failure(self):
        """Record a failed request."""
        with self.lock:
            self.failures += 1
            self.last_failure_time = time.time()
            
            if self.state == "half-open" or (self.state == "closed" and self.failures >= self.failure_threshold):
                logger.warning(f"Circuit opened after {self.failures} failures")
                self.state = "open"

class WebhookConsumer:
    """Consumer that processes queue messages and delivers to webhooks."""
    
    def __init__(self):
        """Initialize the consumer with configuration."""
        self.config = load_config()
        self.redis_client = create_redis_client(self.config)
        
        # Configure webhook settings
        webhook_config = self.config.get("webhook", {})
        self.timeout = webhook_config.get("timeout", 10)
        self.max_retries = webhook_config.get("max_retries", 3)
        self.backoff_factor = webhook_config.get("backoff_factor", 2)
        
        # Create circuit breakers dictionary (one per webhook URL)
        self.circuit_breakers = {}
        
        # Configure consumer settings
        consumer_config = self.config.get("consumer", {})
        self.polling_interval = consumer_config.get("polling_interval", 1)
        self.batch_size = consumer_config.get("batch_size", 10)
        self.webhook_threads = consumer_config.get("webhook_threads", 20)
        self.max_empty_polls = consumer_config.get("max_empty_polls", 10)
        
        # Track statistics
        self.stats = {
            "messages_processed": 0,
            "successful_deliveries": 0,
            "failed_deliveries": 0,
            "retries": 0,
            "circuit_breaks": 0,
            "start_time": time.time()
        }
        
        # Flag for shutdown
        self.running = True
        
        # Start health check server
        self.start_health_server()
        
        # Check Redis Sentinel status
        sentinel_status = check_sentinel_health(self.config)
        if sentinel_status["status"] == "ok":
            logger.info(f"Redis Sentinel active: Master at {sentinel_status.get('master')}, {sentinel_status.get('slave_count')} slaves")
        else:
            logger.warning(f"Redis Sentinel not available: {sentinel_status.get('error')}")
    
    def deliver_webhook(self, message):
        """Send a notification to the bucket-specific webhook with OAuth authentication."""
        bucket_name = message.get("bucket")
        
        # Look up the bucket configuration
        bucket_config = get_bucket_config(self.redis_client, bucket_name)
        if not bucket_config:
            # Fallback to in-memory config
            bucket_config = next((b for b in self.config.get("buckets", []) if b["name"] == bucket_name), None)
        
        if not bucket_config or "webhook_url" not in bucket_config:
            logger.error(f"No webhook URL configured for bucket {bucket_name}")
            return False
        
        webhook_url = bucket_config["webhook_url"]
        logger.debug(f"Preparing webhook request to {webhook_url} for bucket {bucket_name}")

        
        # Check circuit breaker
        if webhook_url not in self.circuit_breakers:
            webhook_config = self.config.get("webhook", {})
            self.circuit_breakers[webhook_url] = CircuitBreaker(
                failure_threshold=webhook_config.get("circuit_threshold", 5),
                reset_timeout=webhook_config.get("circuit_reset_time", 60)
            )
        
        circuit_breaker = self.circuit_breakers[webhook_url]
        
        if not circuit_breaker.allow_request():
            logger.warning(f"Circuit breaker open for {webhook_url}, skipping webhook request")
            self.stats["circuit_breaks"] += 1
            return False
        
        # Add delivery timestamp
        message["delivery_attempt_time"] = datetime.now().isoformat()
        
        # Set up headers
        headers = {
            "Content-Type": "application/json",
            "X-Bucket-Name": bucket_name,
            "X-Event-Type": message.get("event_type", "unknown")
        }
        
        # Check for webhook authentication
        if "webhook_auth" in bucket_config and bucket_config["webhook_auth"].get("type") == "oauth2":
            auth_config = bucket_config["webhook_auth"]
            client_id = auth_config.get("client_id")
            client_secret = auth_config.get("client_secret")
            token_url = auth_config.get("token_url")

            logger.debug(f"OAuth auth configured for bucket {bucket_name} with token URL: {token_url}")
            
            if client_id and client_secret and token_url:

                logger.debug(f"Requesting OAuth token from {token_url} for client ID {client_id}")

                try:

                    auth_str = f"{client_id}:{client_secret}"
                    auth_bytes = auth_str.encode('ascii')
                    base64_bytes = base64.b64encode(auth_bytes)
                    base64_auth = base64_bytes.decode('ascii')
                    
                    headers_token_request = {
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Authorization": f"Basic {base64_auth}"
                    }
                    
                    data_token_request = {
                        "grant_type": "client_credentials"
                    }
                    
                    logger.debug(f"Making token request to {token_url}")
                    
                    token_response = requests.post(
                        token_url, 
                        headers=headers_token_request, 
                        data=data_token_request,
                        timeout=self.timeout
                    )
                    
                    logger.info(f"Token response status: {token_response.status_code}")

                    if token_response.status_code == 200:
                        token_data = token_response.json()
                        logger.debug(f"Token response data keys: {list(token_data.keys())}")
                        
                        access_token = token_data.get("access_token")
                        #logger.info(f"Received access token: {access_token}")

                        if access_token:
                            token_length = len(access_token)
                            token_stripped = access_token.strip()
                            stripped_length = len(token_stripped)
                            
                            if token_length != stripped_length:
                                logger.warning(f"Token contains whitespace! Original length: {token_length}, Stripped length: {stripped_length}")
                                logger.warning(f"First 5 chars: '{access_token[:5]}', Last 5 chars: '{access_token[-5:]}'")
                                # Use the stripped token instead
                                access_token = token_stripped
                        
                        logger.info(f"Received access token: {access_token}")
                        
                        # Try uppercase "Bearer" as in curl
                        headers["Authorization"] = f"Bearer {access_token}" 
                        
                        # Check for whitespace in the full Authorization header
                        auth_header = headers["Authorization"]
                        auth_header_stripped = auth_header.strip()
                        if len(auth_header) != len(auth_header_stripped):
                            logger.warning(f"Authorization header contains whitespace! Original: '{auth_header}', Stripped: '{auth_header_stripped}'")
                            # Use the stripped header
                            headers["Authorization"] = auth_header_stripped
                        
                        logger.debug(f"Added Authorization header: '{headers['Authorization']}'")
                        # END OF NEW CODE
                        
                    else:
                        logger.error(f"Token request failed: {token_response.status_code}, {token_response.text}")
                except Exception as e:
                    logger.error(f"Exception during token request: {type(e).__name__}: {str(e)}")
                        

        # Log the final headers and message
        logger.debug(f"Final webhook request headers: {headers}")
        logger.debug(f"Webhook request payload: {json.dumps(message)[:200]}... (truncated if longer)")
                
                # Get OAuth token
                #access_token = get_oauth_token(client_id, client_secret, token_url)
                
                #if access_token:
                    # Add token to headers
                   # headers["Authorization"] = f"Bearer {access_token}"
                   # logger.debug(f"Added OAuth bearer token to webhook request for {bucket_name}")
                #else:
                    #logger.error(f"Failed to get OAuth token for webhook delivery to {bucket_name}")
        
        try:
            # Send to webhook
            logger.info(f"Sending webhook request to {webhook_url}")
            response = requests.post(
                webhook_url,
                json=message,
                headers=headers,
                timeout=self.timeout
            )

            logger.info(f"Webhook response status: {response.status_code}")
            logger.debug(f"Webhook response headers: {dict(response.headers)}")
            logger.info(f"Webhook response body: {response.text}")
            
            if response.status_code >= 200 and response.status_code < 300:
                logger.info(f"Successfully delivered notification for {message['bucket']}/{message['key']} to {webhook_url}")
                circuit_breaker.record_success()
                return True
            else:
                logger.warning(f"Webhook {webhook_url} returned error {response.status_code}: {response.text}")
                
                # Clear token cache if authentication error
                if (response.status_code == 401 or response.status_code == 403) and "webhook_auth" in bucket_config:
                    auth_config = bucket_config["webhook_auth"]
                    client_id = auth_config.get("client_id")
                    token_url = auth_config.get("token_url")
                    
                    if client_id and token_url:
                        clear_oauth_token_cache(client_id, token_url)
                        logger.info(f"Cleared OAuth token for {client_id} due to authentication error")
                
                circuit_breaker.record_failure()
                return False
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request to webhook {webhook_url} failed: {e}")
            circuit_breaker.record_failure()
            return False
    
    def process_message(self, message):
        """Process a message and send to webhook with retries."""
        retry_count = message.get("retry_count", 0)
        message["retry_count"] = retry_count + 1
        
        # Attempt delivery
        success = self.deliver_webhook(message)
        
        if success:
            self.stats["successful_deliveries"] += 1
            return True
        else:
            self.stats["failed_deliveries"] += 1

            message["retry_count"] = retry_count + 1
            
            # Check if we should retry
            if retry_count < self.max_retries:
                # Calculate backoff time
                backoff = (self.backoff_factor ** retry_count) + random.random()
                
                logger.info(f"Will retry message for {message['bucket']}/{message['key']} "
                           f"after {backoff:.2f}s (attempt {retry_count+1}/{self.max_retries})")
                  
                # Sleep for backoff period
                time.sleep(backoff)
                self.stats["retries"] += 1
                
                # Retry
                return self.process_message(message)
            else:
                # Max retries exceeded
                logger.error(f"Max retries exceeded for {message['bucket']}/{message['key']}")
                return False
    
    def process_batch(self):
        """Process a batch of messages from the queue."""
        messages = get_notifications(self.redis_client, self.config, self.batch_size)
        
        if not messages:
            return 0
        
        # Use thread pool to process messages in parallel
        with ThreadPoolExecutor(max_workers=self.webhook_threads) as executor:
            # Submit tasks
            futures = [executor.submit(self.process_message, message) for message in messages]
            
            # Wait for all to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        # Update statistics
        self.stats["messages_processed"] += len(messages)
        
        logger.info(f"Processed batch of {len(messages)} messages")
        return len(messages)
    
    def start_health_server(self):
        """Start a simple HTTP server for health checks."""
        class HealthHandler(http.server.SimpleHTTPRequestHandler):
            def __init__(self2, *args, **kwargs):
                self2.consumer = self
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
                    # Check if Redis is available, Sentinel is available, and webhook URL is configured
                    redis_ok = check_redis_health(self.redis_client)
                    sentinel_ok = check_sentinel_health(self.config)["status"] == "ok"
                    
                    # For webhook readiness, check if we have buckets with webhook URLs
                    webhook_ok = any(bucket.get("webhook_url") for bucket in self.config.get("buckets", []))
                    
                    status_code = 200 if (redis_ok and webhook_ok and sentinel_ok) else 503
                    ready_status = {
                        "status": "ready" if (redis_ok and webhook_ok and sentinel_ok) else "not_ready",
                        "redis": "ok" if redis_ok else "error",
                        "sentinel": "ok" if sentinel_ok else "error",
                        "webhook": "ok" if webhook_ok else "missing"
                    }
                    self2.send_response(status_code)
                    self2.send_header('Content-Type', 'application/json')
                    self2.end_headers()
                    self2.wfile.write(json.dumps(ready_status).encode())
                    
                elif self2.path == '/metrics':
                    # Return current metrics
                    uptime = time.time() - self.stats["start_time"]
                    queue_stats = check_queue_stats(self.redis_client, self.config)
                    sentinel_stats = check_sentinel_health(self.config)
                    
                    # Add circuit breaker stats per webhook
                    circuit_stats = {}
                    for webhook_url, circuit in self.circuit_breakers.items():
                        circuit_stats[webhook_url] = {
                            "state": circuit.state,
                            "failures": circuit.failures
                        }
                    
                    metrics = {
                        "consumer_stats": self.stats,
                        "queue": queue_stats,
                        "sentinel": sentinel_stats,
                        "uptime_seconds": uptime,
                        "circuit_breakers": circuit_stats
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
        """Run the consumer in a continuous loop."""
        logger.info("Starting webhook consumer for bucket-specific webhook delivery")
        
        empty_polls = 0
        last_stats_time = time.time()
        
        while self.running:
            try:
                # Process a batch
                processed = self.process_batch()
                
                if processed == 0:
                    empty_polls += 1
                    # Exponential backoff for empty polls to reduce Redis load
                    sleep_time = min(
                        self.polling_interval * (2 if empty_polls > self.max_empty_polls else 1),
                        5  # Cap at 5 seconds
                    )
                    time.sleep(sleep_time)
                else:
                    empty_polls = 0
                    # Brief pause to prevent CPU spinning
                    time.sleep(0.1)
                
                # Log stats periodically (every minute)
                if time.time() - last_stats_time > 60:
                    logger.info(
                        f"Statistics: processed {self.stats['messages_processed']}, "
                        f"success {self.stats['successful_deliveries']}, "
                        f"failed {self.stats['failed_deliveries']}, "
                        f"retries {self.stats['retries']}, "
                        f"circuit breaks {self.stats['circuit_breaks']}"
                    )
                        
                    last_stats_time = time.time()
                    
                    # Also check sentinel status periodically
                    sentinel_status = check_sentinel_health(self.config)
                    if sentinel_status["status"] == "ok":
                        logger.debug(f"Redis Sentinel active: {sentinel_status.get('slave_count')} slaves")
                    else:
                        logger.warning(f"Redis Sentinel issue detected: {sentinel_status.get('error')}")
                    
            except KeyboardInterrupt:
                logger.info("Consumer stopped by user")
                self.running = False
                break
            except Exception as e:
                logger.error(f"Error in consumer main loop: {e}")
                # Don't crash, sleep and retry
                time.sleep(5)
        
        logger.info("Consumer shutting down")

if __name__ == "__main__":
    consumer = WebhookConsumer()
    consumer.run()
