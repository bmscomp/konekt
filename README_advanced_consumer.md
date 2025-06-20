# Advanced Kafka Consumer

A comprehensive, production-ready Kafka consumer implementation with partition-aware multi-threading, fault tolerance, and resilience features.

## Features

### Core Capabilities
- **Partition-Aware Processing**: Dedicated worker threads for each partition
- **Multi-Threading**: Configurable threading per partition and overall
- **Fault Tolerance**: Comprehensive error handling and recovery mechanisms
- **Resilience**: Automatic retry logic and partition pausing on errors
- **Monitoring**: Real-time statistics and health checks

### Advanced Features
- **Batch Processing**: Configurable batch sizes and timeouts
- **Graceful Shutdown**: Proper cleanup and signal handling
- **Error Thresholds**: Automatic partition pausing when error limits reached
- **Health Monitoring**: Background health checks and activity monitoring
- **Statistics Tracking**: Detailed metrics and performance monitoring

## Installation

```bash
# Install dependencies
pip install -r requirements_advanced.txt

# Or install confluent-kafka directly
pip install confluent-kafka>=2.3.0
```

## Configuration

The `KafkaConfig` class provides comprehensive configuration options:

```python
config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="your-consumer-group",
    topics=["your-topic"],
    
    # Threading
    max_workers=4,
    partition_workers=1,
    
    # Batch processing
    batch_size=100,
    batch_timeout_ms=5000,
    
    # Fault tolerance
    max_retries=3,
    retry_backoff_ms=1000,
    error_threshold=10,
    
    # Consumer settings
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    session_timeout_ms=30000,
)
```

## Usage

### Basic Usage

```python
from advanced_kafka_consumer import AdvancedKafkaConsumer, KafkaConfig

def my_message_handler(message):
    """Your custom message processing logic"""
    print(f"Processing: {message}")
    return True  # Return True for success, False for failure

config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    topics=["my-topic"],
    group_id="my-consumer-group"
)

consumer = AdvancedKafkaConsumer(config, my_message_handler)
consumer.start()
```

### Custom Message Handler

```python
def advanced_message_handler(message):
    """Example of a more complex message handler"""
    try:
        if isinstance(message, bytes):
            message = message.decode('utf-8')
        
        if isinstance(message, str):
            data = json.loads(message)
            # Process your JSON data here
            process_data(data)
        
        return True
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return False
```

### Running the Program

```bash
# Run with default configuration
python advanced_kafka_consumer.py

# Or customize the main() function for your specific needs
```

## Architecture

### Components

1. **AdvancedKafkaConsumer**: Main consumer class managing overall lifecycle
2. **PartitionWorker**: Individual worker threads for each partition
3. **MessageBatch**: Data structure for batched message processing
4. **ConsumerStats**: Statistics and metrics tracking
5. **KafkaConfig**: Comprehensive configuration management

### Threading Model

```
Main Consumer Thread
├── Partition 0 Worker Thread
├── Partition 1 Worker Thread
├── Partition N Worker Thread
├── Health Monitor Thread
└── Stats Reporter Thread
```

### Message Flow

1. Main consumer polls messages from Kafka
2. Messages are batched by partition
3. Batches are queued to partition-specific workers
4. Workers process messages with retry logic
5. Failed messages are logged and counted
6. Offsets are committed after successful processing

## Error Handling

### Retry Logic
- Configurable retry attempts per message
- Exponential backoff between retries
- Per-partition error tracking

### Partition Pausing
- Automatic pausing when error threshold reached
- Prevents cascading failures
- Maintains overall consumer health

### Health Monitoring
- Background health checks
- Activity monitoring
- Worker thread resurrection

## Monitoring

### Statistics Available
- Messages processed/failed counts
- Processing rates and times
- Partition-specific error counts
- Uptime and activity timestamps
- Batch processing metrics

### Example Stats Output
```json
{
  "uptime_seconds": 3600.5,
  "messages_processed": 15420,
  "messages_failed": 12,
  "processing_rate_per_second": 4.28,
  "average_processing_time_ms": 15.6,
  "error_rate_percent": 0.08,
  "partition_errors": {"0": 2, "1": 0, "2": 3}
}
```

## Production Considerations

### Performance Tuning
- Adjust `batch_size` based on message size and processing time
- Tune `max_workers` based on CPU cores and workload
- Configure `fetch_min_bytes` and `fetch_max_wait_ms` for throughput

### Reliability
- Set appropriate `session_timeout_ms` and `heartbeat_interval_ms`
- Configure `max_retries` and `error_threshold` for your use case
- Monitor partition lag and processing rates

### Scaling
- Use multiple consumer instances with same `group_id`
- Partition your topics appropriately
- Consider partition assignment strategies

## Troubleshooting

### Common Issues

1. **Consumer Lag**: Increase `max_workers` or optimize message handler
2. **Connection Issues**: Check `bootstrap_servers` and network connectivity
3. **Partition Imbalance**: Review partition assignment strategy
4. **Memory Usage**: Adjust `batch_size` and queue sizes

### Debugging

Enable debug logging:
```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

### Health Checks

Monitor the health check logs for:
- Worker thread status
- Processing activity
- Error rates per partition

## License

This is a standalone implementation for educational and production use.
