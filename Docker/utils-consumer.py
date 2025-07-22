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
import uuid
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
base_logger = setup_logging("webhook-consumer")

class ContextLogger:
    def __init__(self, logger):
        self.logger = logger
    
    def info(self, msg, **kwargs):
        self.logger.info(msg, extra=kwargs)
    
    def debug(self, msg, **kwargs):
        self.logger.debug(msg, extra=kwargs)
    
    def warning(self, msg, **kwargs):
        self.logger.warning(msg, extra=kwargs)
    
    def error(self, msg, **kwargs):
        self.logger.error(msg, extra=kwargs)
    
    def critical(self, msg, **kwargs):
        self.logger.critical(msg, extra=kwargs)

logger = ContextLogger(base_logger)


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
                    logger.debug("Circuit half-open, allowing test request")
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
    
    def log_with_context(self, level, message, **extra):
        """Log with the current request context."""
        
        # Call the appropriate logger method with context as extra
        if level == "debug":
            logger.debug(message, **extra)
        elif level == "info":
            logger.info(message, **extra)
        elif level == "warning":
            logger.warning(message, **extra)
        elif level == "error":
            logger.error(message, **extra)
        elif level == "critical":
            logger.critical(message, **extra)

    def deliver_webhook(self, message):
        """Send a notification to the bucket-specific webhook with OAuth authentication."""
        start_time = time.time()
        bucket_name = message.get("bucket", "unknown")
        object_key = message.get("key", "unknown")
        event_type = message.get("event_type", "unknown")
        request_id = message.get("request_id", "unknown")
        retry_count = message.get("retry_count", 0)
        
        context = {
            "request_id": request_id,
            "bucket": bucket_name,
            "object_key": object_key,
            "event_type": event_type,
            "retry_count": retry_count
        }
        
        # Look up the bucket configuration
        bucket_config = get_bucket_config(self.redis_client, bucket_name)
        if not bucket_config:
            # Fallback to in-memory config
            bucket_config = next((b for b in self.config.get("buckets", []) if b["name"] == bucket_name), None)
        
        if not bucket_config or "webhook_url" not in bucket_config:
            self.log_with_context("error", f"No webhook URL configured", **context)
            return False
        
        webhook_url = bucket_config["webhook_url"]
        self.log_with_context("debug", f"Preparing webhook request", 
                            webhook_url=webhook_url, **context)
        
        # Check circuit breaker
        if webhook_url not in self.circuit_breakers:
            webhook_config = self.config.get("webhook", {})
            self.circuit_breakers[webhook_url] = CircuitBreaker(
                failure_threshold=webhook_config.get("circuit_threshold", 5),
                reset_timeout=webhook_config.get("circuit_reset_time", 60)
            )
        
        circuit_breaker = self.circuit_breakers[webhook_url]
        
        if not circuit_breaker.allow_request():
            self.log_with_context("warning", f"Circuit breaker open, skipping webhook request", 
                                webhook_url=webhook_url, **context)
            self.stats["circuit_breaks"] += 1
            return False
        
        # Add delivery timestamp
        message["delivery_attempt_time"] = datetime.now().isoformat()
        
        # Set up headers
        headers = {
            "Content-Type": "application/json",
            "X-Bucket-Name": bucket_name,
            "X-Event-Type": message.get("event_type", "unknown"),
            "X-Request-ID": request_id
        }
        
        # Check for webhook authentication
        if "webhook_auth" in bucket_config and bucket_config["webhook_auth"].get("type") == "oauth2":
            auth_config = bucket_config["webhook_auth"]
            client_id = auth_config.get("client_id")
            client_secret = auth_config.get("client_secret")
            token_url = auth_config.get("token_url")

            self.log_with_context("debug", f"OAuth auth configured", 
                                webhook_url=webhook_url, token_url=token_url, **context)
            
            if client_id and client_secret and token_url:
                self.log_with_context("debug", f"Requesting OAuth token from {token_url} for client ID {client_id}", **context)

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
                    
                    self.log_with_context("debug", f"Making token request to {token_url}", **context)
                    
                    token_response = requests.post(
                        token_url, 
                        headers=headers_token_request, 
                        data=data_token_request,
                        timeout=self.timeout
                    )
                    
                    self.log_with_context("info", f"Token response status", 
                                        status_code=token_response.status_code, **context)

                    if token_response.status_code == 200:
                        token_data = token_response.json()
                        self.log_with_context("debug", f"Token response data keys", 
                                            keys=list(token_data.keys()), **context)
                        
                        access_token = token_data.get("access_token")

                        if access_token:
                            token_length = len(access_token)
                            token_stripped = access_token.strip()
                            stripped_length = len(token_stripped)
                            
                            if token_length != stripped_length:
                                self.log_with_context("warning", f"Token contains whitespace!", 
                                                original_length=token_length, 
                                                stripped_length=stripped_length,
                                                first_chars=access_token[:5], 
                                                last_chars=access_token[-5:], **context)
                                # Use the stripped token instead
                                access_token = token_stripped
                        
                        self.log_with_context("info", f"Received access token", token_prefix=access_token[:5], **context)
                        
                        # Try uppercase "Bearer" as in curl
                        headers["Authorization"] = f"Bearer {access_token}" 
                        
                        # Check for whitespace in the full Authorization header
                        auth_header = headers["Authorization"]
                        auth_header_stripped = auth_header.strip()
                        if len(auth_header) != len(auth_header_stripped):
                            self.log_with_context("warning", f"Authorization header contains whitespace!", 
                                            original=auth_header, 
                                            stripped=auth_header_stripped, **context)
                            # Use the stripped header
                            headers["Authorization"] = auth_header_stripped
                        
                        self.log_with_context("debug", f"Added Authorization header", 
                                            auth_header=headers["Authorization"], **context)
                        
                    else:
                        self.log_with_context("error", f"Token request failed", 
                                            status_code=token_response.status_code, 
                                            response=token_response.text, **context)
                except Exception as e:
                    self.log_with_context("error", f"Exception during token request", 
                                        error_type=type(e).__name__,
                                        error=str(e), **context)

        # Log the final headers and message
        self.log_with_context("debug", f"Final webhook request headers", 
                            headers=headers, **context)
        self.log_with_context("debug", f"Webhook request payload", 
                            payload=json.dumps(message)[:200], **context)
        
        try:
            # Send to webhook
            self.log_with_context("info", f"Sending webhook request", 
                                webhook_url=webhook_url, **context)
            
            response = requests.post(
                webhook_url,
                json=message,
                headers=headers,
                timeout=self.timeout
            )
            
            duration_ms = int((time.time() - start_time) * 1000)
            self.log_with_context("info", f"Webhook response received", 
                                response_code=response.status_code,
                                duration_ms=duration_ms, 
                                response_body=response.text[:200], **context)
            
            if response.status_code >= 200 and response.status_code < 300:
                self.log_with_context("info", f"Successfully delivered notification", **context)
                circuit_breaker.record_success()
                return True
            else:
                self.log_with_context("warning", f"Webhook returned error", 
                                    response_code=response.status_code,
                                    response_body=response.text[:200], **context)
                
                # Clear token cache if authentication error
                if (response.status_code == 401 or response.status_code == 403) and "webhook_auth" in bucket_config:
                    auth_config = bucket_config["webhook_auth"]
                    client_id = auth_config.get("client_id")
                    token_url = auth_config.get("token_url")
                    
                    if client_id and token_url:
                        clear_oauth_token_cache(client_id, token_url)
                        self.log_with_context("info", f"Cleared OAuth token due to authentication error", 
                                            client_id=client_id, **context)
                
                circuit_breaker.record_failure()
                return False
                
        except requests.exceptions.RequestException as e:
            self.log_with_context("warning", f"Request to webhook failed", 
                                error=str(e), webhook_url=webhook_url, **context)
            circuit_breaker.record_failure()
            return False
    
    def process_message(self, message):
        """Process a message and send to webhook with retries."""

        if "request_id" not in message:
            message["request_id"] = str(uuid.uuid4())
        
        request_id = message["request_id"]
        retry_count = message.get("retry_count", 0)
        message["retry_count"] = retry_count + 1

        bucket = message.get("bucket", "unknown")
        object_key = message.get("key", "unknown")
        event_type = message.get("event_type", "unknown")

        context = {
            "request_id": request_id,
            "bucket": bucket,
            "object_key": object_key,
            "event_type": event_type, 
            "retry_count": retry_count
        }


        self.log_with_context("info", f"Processing webhook delivery", **context)
        
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
                
                self.log_with_context("info", f"Will retry message after {backoff:.2f}s", 
                                   backoff_seconds=backoff, **context)
                  
                # Sleep for backoff period
                time.sleep(backoff)
                self.stats["retries"] += 1
                
                # Retry
                return self.process_message(message)
            else:
                # Max retries exceeded
                self.log_with_context("error", f"Max retries exceeded", **context)
                return False
    
    def process_batch(self):
        """Process a batch of messages from the queue."""
        messages = get_notifications(self.redis_client, self.config, self.batch_size)
        
        if not messages:
            return 0
        
        batch_id = str(uuid.uuid4())
        self.log_with_context("info", f"Processing batch of {len(messages)} messages", 
                             batch_size=len(messages), batch_id=batch_id)
        
        # Use thread pool to process messages in parallel
        with ThreadPoolExecutor(max_workers=self.webhook_threads) as executor:
            # Submit tasks
            futures = [executor.submit(self.process_message, message) for message in messages]
            
            # Wait for all to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    self.log_with_context("error", f"Error processing message: {e}", 
                                         error=str(e), error_type=type(e).__name__)
        
        # Update statistics
        self.stats["messages_processed"] += len(messages)
        
        self.log_with_context("info", f"Completed batch processing", 
                             batch_size=len(messages), batch_id=batch_id)
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
                    self.log_with_context("info", 
                        f"Statistics: processed {self.stats['messages_processed']}, "
                        f"success {self.stats['successful_deliveries']}, "
                        f"failed {self.stats['failed_deliveries']}, "
                        f"retries {self.stats['retries']}, "
                        f"circuit breaks {self.stats['circuit_breaks']}",
                        stats=self.stats
                    )
                        
                    last_stats_time = time.time()
                    
                    # Also check sentinel status periodically
                    sentinel_status = check_sentinel_health(self.config)
                    if sentinel_status["status"] == "ok":
                        self.log_with_context("debug", f"Redis Sentinel active", slave_count=sentinel_status.get('slave_count'))
                    else:
                        self.log_with_context("warning", f"Redis Sentinel issue detected", error=sentinel_status.get('error'))
                    
            except KeyboardInterrupt:
                logger.info("Consumer stopped by user")
                self.running = False
                break
            except Exception as e:
                self.log_with_context("error", f"Error in consumer main loop: {e}", 
                                     error=str(e), error_type=type(e).__name__)
                # Don't crash, sleep and retry
                time.sleep(5)
        
        logger.info("Consumer shutting down")

if __name__ == "__main__":
    consumer = WebhookConsumer()
    consumer.run()
