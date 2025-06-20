# Multi-stage build for advanced Kafka consumer with Dynatrace agent
FROM python:3.11-slim as builder

# Set build arguments for Dynatrace
ARG DT_TENANT
ARG DT_API_TOKEN
ARG DT_PAAS_TOKEN
ARG DT_CONNECTION_POINT

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    make \
    wget \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt requirements_advanced.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r requirements_advanced.txt

# Install PyInstaller for compilation
RUN pip install --no-cache-dir pyinstaller

# Copy source code
COPY advanced_kafka_consumer.py ./
COPY *.py ./

# Compile the advanced Kafka consumer to a single executable
RUN pyinstaller --onefile \
    --name advanced_kafka_consumer \
    --add-data "*.py:." \
    --hidden-import confluent_kafka \
    --hidden-import kafka \
    --hidden-import threading \
    --hidden-import queue \
    --hidden-import signal \
    --hidden-import json \
    --hidden-import logging \
    --hidden-import time \
    --hidden-import os \
    --hidden-import sys \
    --hidden-import dataclasses \
    --hidden-import typing \
    --collect-all confluent_kafka \
    advanced_kafka_consumer.py

# Production stage
FROM python:3.11-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV DT_TENANT=${DT_TENANT}
ENV DT_API_TOKEN=${DT_API_TOKEN}
ENV DT_PAAS_TOKEN=${DT_PAAS_TOKEN}
ENV DT_CONNECTION_POINT=${DT_CONNECTION_POINT}

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    unzip \
    procps \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Create application user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Download and install Dynatrace OneAgent
RUN if [ -n "$DT_TENANT" ] && [ -n "$DT_PAAS_TOKEN" ]; then \
    echo "Installing Dynatrace OneAgent..." && \
    wget -O Dynatrace-OneAgent.sh "https://${DT_TENANT}.live.dynatrace.com/api/v1/deployment/installer/agent/unix/default/latest?arch=x86&flavor=default" \
    --header="Authorization: Api-Token ${DT_PAAS_TOKEN}" && \
    chmod +x Dynatrace-OneAgent.sh && \
    ./Dynatrace-OneAgent.sh --set-app-log-content-access=true \
    --set-infra-only=false \
    --set-host-group=kafka-consumers \
    --set-host-name=advanced-kafka-consumer && \
    rm Dynatrace-OneAgent.sh; \
    else \
    echo "Dynatrace environment variables not set, skipping OneAgent installation"; \
    fi

# Copy compiled executable from builder stage
COPY --from=builder /app/dist/advanced_kafka_consumer /app/

# Copy configuration files
COPY .env.example /app/.env
COPY requirements.txt requirements_advanced.txt /app/

# Create logs directory
RUN mkdir -p /app/logs && chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep -f advanced_kafka_consumer || exit 1

# Expose metrics port (if using Prometheus metrics)
EXPOSE 8080

# Set default environment variables for Kafka consumer
ENV KAFKA_BOOTSTRAP_SERVERS=localhost:9092
ENV KAFKA_GROUP_ID=advanced-consumer-group
ENV KAFKA_TOPICS=test-topic
ENV KAFKA_MAX_WORKERS=4
ENV KAFKA_BATCH_SIZE=50
ENV KAFKA_BATCH_TIMEOUT_MS=5000
ENV LOG_LEVEL=INFO

# Labels for better container management
LABEL maintainer="Konekt Team"
LABEL version="1.0"
LABEL description="Advanced Kafka Consumer with Dynatrace monitoring"
LABEL kafka.consumer.type="advanced"
LABEL monitoring.agent="dynatrace"

# Entry point script
COPY <<EOF /app/entrypoint.sh
#!/bin/bash
set -e

# Start Dynatrace OneAgent if installed
if [ -f /opt/dynatrace/oneagent/agent/lib64/liboneagentproc.so ]; then
    echo "Starting with Dynatrace OneAgent..."
    export LD_PRELOAD="/opt/dynatrace/oneagent/agent/lib64/liboneagentproc.so"
    export DT_LOGSTREAM=stdout
fi

# Set up logging
export PYTHONUNBUFFERED=1

# Validate required environment variables
if [ -z "\$KAFKA_BOOTSTRAP_SERVERS" ]; then
    echo "Error: KAFKA_BOOTSTRAP_SERVERS environment variable is required"
    exit 1
fi

if [ -z "\$KAFKA_TOPICS" ]; then
    echo "Error: KAFKA_TOPICS environment variable is required"
    exit 1
fi

# Log startup information
echo "Starting Advanced Kafka Consumer..."
echo "Kafka Brokers: \$KAFKA_BOOTSTRAP_SERVERS"
echo "Consumer Group: \$KAFKA_GROUP_ID"
echo "Topics: \$KAFKA_TOPICS"
echo "Max Workers: \$KAFKA_MAX_WORKERS"
echo "Batch Size: \$KAFKA_BATCH_SIZE"
echo "Log Level: \$LOG_LEVEL"

# Start the consumer
exec ./advanced_kafka_consumer
EOF

RUN chmod +x /app/entrypoint.sh

# Default command
ENTRYPOINT ["/app/entrypoint.sh"]
