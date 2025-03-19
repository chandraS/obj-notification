FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create config directory
RUN mkdir -p /app/config /app/secrets

# Copy application code
COPY utils.py monitor.py ./

# Expose health check port
EXPOSE 8080

# Set up environment
ENV PYTHONUNBUFFERED=1 \
    CONFIG_PATH=/app/config/config.yaml \
    BUCKETS_PATH=/app/secrets/buckets.yaml

# Run the monitor
CMD ["python", "monitor.py"]
