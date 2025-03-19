# obj-notification

# Linode Object Storage Monitor Deployment Guide

This guide walks you through deploying the Object Storage Monitor on your 5-node LKE cluster.

## Prerequisites

1. A Linode Kubernetes Engine (LKE) cluster with 5 nodes (16 CPU, 32GB RAM each)
2. `kubectl` configured to access your cluster
3. Docker installed for building images
4. A webhook endpoint to receive notifications

## Deployment Steps

### 1. Create the Bucket Configuration

First, create your bucket configuration file with credentials:

```yaml
buckets:
  - name: "bucket-1"
    access_key: "ACCESS_KEY_1"
    secret_key: "SECRET_KEY_1"
    endpoint: "us-east-1.linodeobjects.com"
  
  # Add all your buckets here...
```

### 2. Build and Push Docker Images

```bash
# Build the monitor image
docker build -f Dockerfile.monitor -t your-registry/bucket-monitor:latest .

# Build the consumer image
docker build -f Dockerfile.consumer -t your-registry/bucket-consumer:latest .

# Push images to your registry
docker push your-registry/bucket-monitor:latest
docker push your-registry/bucket-consumer:latest
```

### 3. Apply Kubernetes Manifests

```bash
# Create namespace and deploy Redis
kubectl apply -f 00-namespace.yaml
kubectl apply -f 01-redis.yaml

# Create ConfigMap
kubectl apply -f 02-config.yaml

# Create Secrets (update with your actual webhook URL)
# Edit 03-secrets.yaml first to add your bucket configurations
kubectl apply -f 03-secrets.yaml

# Deploy the monitor and consumer
kubectl apply -f 04-monitor-deployment.yaml
kubectl apply -f 05-consumer-deployment.yaml
```

### 4. Verify Deployment

```bash
# Check pods are running
kubectl -n object-monitor get pods

# Check logs for the monitor
kubectl -n object-monitor logs -l app=bucket-monitor

# Check logs for the consumer
kubectl -n object-monitor logs -l app=webhook-consumer
```

### 5. Monitor Health and Metrics

```bash
# Port-forward to access health endpoints
kubectl -n object-monitor port-forward svc/bucket-monitor 8080:8080

# Access metrics in browser or curl
curl http://localhost:8080/metrics
```

## Configuration Parameters

### Polling Interval

To change the polling interval after deployment:

```bash
# Edit the ConfigMap
kubectl -n object-monitor edit configmap monitor-config

# Or update via environment variable
kubectl -n object-monitor set env deployment/bucket-monitor POLLING_INTERVAL=60
```

### Webhook URL

To update the webhook URL:

```bash
kubectl -n object-monitor edit secret bucket-secrets
# Update the webhook-url field (remember it needs to be base64 encoded)
```

### Thread Pool Sizing

Thread pool sizes are configured in the ConfigMap:

```yaml
parallel:
  max_workers: 60            # Main thread pool size
  rate_limit_per_region: 15  # Per-region concurrency limit
```

## Scaling Considerations

The deployment starts with:
- 3 monitor replicas
- 2 consumer replicas

The HPAs will automatically scale based on CPU and memory usage.

For manual scaling:

```bash
kubectl -n object-monitor scale deployment/bucket-monitor --replicas=5
kubectl -n object-monitor scale deployment/webhook-consumer --replicas=3
```

## Troubleshooting

### Pod Crashes or Restarts

Check for errors in the logs:

```bash
kubectl -n object-monitor logs --previous <pod-name>
```

### Webhook Delivery Issues

Check the consumer logs and the circuit breaker state:

```bash
kubectl -n object-monitor exec -it <consumer-pod> -- curl localhost:8080/metrics
```

### Redis Performance

Monitor Redis metrics:

```bash
kubectl -n object-monitor exec -it redis-0 -- redis-cli info
```

## Resource Monitoring

To monitor resource usage across your cluster:

```bash
kubectl top nodes
kubectl -n object-monitor top pods
```

This will help you verify if the 5-node cluster with 16 CPU per node is appropriately sized for your workload.
