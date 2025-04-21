!/usr/bin/env python3
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
    OAuthTokenCache,
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
        self.webhook_url = webhook_config.get("url")
        if not self.webhook_url and "WEBHOOK_URL" in os.environ:
            self.webhook_url = os.environ["WEBHOOK_URL"]
            
        self.timeout = webhook_config.get("timeout", 10)
        self.max_retries = webhook_config.get("max_retries", 3)
        self.backoff_factor = webhook_config.get("backoff_factor", 2)
        
        # Create circuit breaker
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=webhook_config.get("circuit_threshold", 5),
            reset_timeout=webhook_config.get("circuit_reset_time", 60)
        )
        
        # Configure OAuth if enabled
        oauth_config = self.config.get("oauth", {})
        self.oauth_enabled = oauth_config.get("enabled", False)
        if self.oauth_enabled:
            self.client_id = oauth_config.get("client_id")
            self.client_secret = oauth_config.get("client_secret")
            self.token_url = oauth_config.get("token_url")
            self.token_cache = OAuthTokenCache()
            logger.info("OAuth authentication enabled for webhook delivery")
        
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
            "oauth_token_failures": 0,
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
        """Send a notification to the webhook with circuit breaker pattern."""
        if not self.webhook_url:
            logger.error("No webhook URL configured")
            return False
        
        # Check circuit breaker
        if not self.circuit_breaker.allow_request():
            logger.warning("Circuit breaker open, skipping webhook request")
            self.stats["circuit_breaks"] += 1
            return False
        
        # Add delivery timestamp
        message["delivery_attempt_time"] = datetime.now().isoformat()
        
        # Set up headers
        headers = {"Content-Type": "application/json"}
        
        # Add OAuth token if enabled
        if self.oauth_enabled:
            token = self.token_cache.get_token(
                self.client_id, 
                self.client_secret, 
                self.token_url
            )
            
            if token:
                headers["Authorization"] = f"bearer {token}"
                logger.debug("Added OAuth bearer token to request")
            else:
                logger.error("Failed to get OAuth token, cannot proceed with webhook delivery")
                self.stats["oauth_token_failures"] += 1
                self.circuit_breaker.record_failure()
                return False
        
        try:
            # Send to webhook
            response = requests.post(
                self.webhook_url,
                json=message,
                headers=headers,
                timeout=self.timeout
            )
            
            if response.status_code >= 200 and response.status_code < 300:
                logger.info(f"Successfully delivered notification for {message['bucket']}/{message['key']}")
                self.circuit_breaker.record_success()
                return True
            else:
                logger.warning(f"Webhook returned error {response.status_code}: {response.text}")
                self.circuit_breaker.record_failure()
                return False
                
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request to webhook failed: {e}")
            self.circuit_breaker.record_failure()
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
                    webhook_ok = self.webhook_url is not None
                    
                    # For OAuth, check if we can get a token
                    oauth_ok = True
                    if self.oauth_enabled:
                        token = self.token_cache.get_token(
                            self.client_id,
                            self.client_secret,
                            self.token_url
                        )
                        oauth_ok = token is not None
                    
                    status_code = 200 if (redis_ok and webhook_ok and sentinel_ok and oauth_ok) else 503
                    ready_status = {
                        "status": "ready" if (redis_ok and webhook_ok and sentinel_ok and oauth_ok) else "not_ready",
                        "redis": "ok" if redis_ok else "error",
                        "sentinel": "ok" if sentinel_ok else "error",
                        "webhook": "ok" if webhook_ok else "missing",
                        "oauth": "ok" if oauth_ok else "error" if self.oauth_enabled else "disabled"
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
                    
                    metrics = {
                        "consumer_stats": self.stats,
                        "queue": queue_stats,
                        "sentinel": sentinel_stats,
                        "uptime_seconds": uptime,
                        "circuit_breaker": {
                            "state": self.circuit_breaker.state,
                            "failures": self.circuit_breaker.failures
                        },
                        "oauth": {
                            "enabled": self.oauth_enabled,
                            "token_failures": self.stats.get("oauth_token_failures", 0)
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
        """Run the consumer in a continuous loop."""
        webhook_url_display = self.webhook_url if self.webhook_url else "not configured"
        logger.info(f"Starting webhook consumer, delivering to: {webhook_url_display}")
        
        # Log OAuth status
        if self.oauth_enabled:
            logger.info(f"OAuth authentication enabled, token URL: {self.token_url}")
        
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
                    if self.oauth_enabled:
                        logger.info(f"OAuth token failures: {self.stats.get('oauth_token_failures', 0)}")
                        
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
