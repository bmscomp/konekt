# Kafka to SQL Server Consumer

A robust, partition-aware Kafka consumer that processes messages and stores them in SQL Server with transaction management.

## Features

- **Partition Awareness**: Dedicated thread pools for each Kafka partition
- **Transaction Management**: Ensures data consistency between Kafka and SQL Server
- **Dual Library Support**: Works with both `confluent-kafka` and `kafka-python`
- **Idempotency**: Prevents duplicate message processing
- **Graceful Shutdown**: Handles interruption signals properly
- **Comprehensive Monitoring**: Built-in statistics and logging
- **Configurable**: Environment variable and programmatic configuration
- **Error Handling**: Robust error handling and retry mechanisms

## Requirements

```bash
pip install confluent-kafka kafka-python sqlalchemy pyodbc python-dotenv
```

## Quick Start

1. **Setup Environment Variables**:
```bash
cp .env.example .env
# Edit .env with your configuration
```

2. **Basic Usage**:
```python
from kafka_sqlserver_consumer import PartitionAwareKafkaConsumer, KafkaConfig, SQLServerConfig

# Configure Kafka
kafka_config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="my_consumer_group",
    topics=["my-topic"],
    auto_offset_reset="earliest"
)

# Configure SQL Server
sqlserver_config = SQLServerConfig(
    server="localhost",
    database="kafka_messages",
    username="sa",
    password="YourPassword123!",
    table_name="messages"
)

# Create and start consumer
consumer = PartitionAwareKafkaConsumer(
    kafka_config=kafka_config,
    sqlserver_config=sqlserver_config,
    max_workers_per_partition=2
)

consumer.start()
```

3. **Run with Command Line**:
```bash
python kafka_sqlserver_consumer.py
```

## Configuration

### Kafka Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bootstrap_servers` | `localhost:9092` | Kafka broker addresses |
| `group_id` | `kafka_sqlserver_consumer` | Consumer group ID |
| `topics` | `["test-topic"]` | List of topics to consume |
| `auto_offset_reset` | `earliest` | Offset reset strategy |
| `enable_auto_commit` | `False` | Enable automatic offset commits |
| `max_poll_records` | `500` | Maximum records per poll |
| `session_timeout_ms` | `30000` | Session timeout |
| `heartbeat_interval_ms` | `10000` | Heartbeat interval |
| `max_poll_interval_ms` | `300000` | Maximum poll interval |

### SQL Server Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `server` | `localhost` | SQL Server host |
| `port` | `1433` | SQL Server port |
| `database` | `kafka_messages` | Database name |
| `username` | `sa` | Database username |
| `password` | `YourPassword123!` | Database password |
| `driver` | `ODBC Driver 18 for SQL Server` | ODBC driver |
| `table_name` | `kafka_messages` | Table name for messages |
| `trust_server_certificate` | `True` | Trust server certificate |
| `encrypt` | `True` | Enable encryption |
| `connection_timeout` | `30` | Connection timeout |

### Environment Variables

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=kafka_sqlserver_consumer
KAFKA_TOPICS=topic1,topic2,topic3
KAFKA_AUTO_OFFSET_RESET=earliest
KAFKA_ENABLE_AUTO_COMMIT=false

# SQL Server Configuration
SQLSERVER_HOST=localhost
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=kafka_messages
SQLSERVER_USERNAME=sa
SQLSERVER_PASSWORD=YourPassword123!
SQLSERVER_DRIVER=ODBC Driver 18 for SQL Server
SQLSERVER_TABLE=kafka_messages

# Consumer Configuration
MAX_WORKERS_PER_PARTITION=2
```

## Database Schema

The consumer automatically creates the following table structure:

```sql
CREATE TABLE kafka_messages (
    id VARCHAR(36) PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    offset BIGINT NOT NULL,
    message_key TEXT,
    message_value TEXT NOT NULL,
    message_headers TEXT,
    consumer_group VARCHAR(255) NOT NULL,
    processing_timestamp DATETIME NOT NULL,
    processing_status VARCHAR(50) NOT NULL,
    error_message TEXT
);

-- Indexes for performance
CREATE INDEX IX_kafka_messages_topic_partition_offset 
ON kafka_messages (topic, partition, offset);

CREATE INDEX IX_kafka_messages_processing_timestamp 
ON kafka_messages (processing_timestamp);
```

## Transaction Management

The consumer implements transaction management between Kafka and SQL Server:

1. **Message Processing**: Each message is processed in isolation
2. **Database Transaction**: Database operations are wrapped in transactions
3. **Offset Commit**: Kafka offsets are committed only after successful database insertion
4. **Idempotency**: Duplicate messages are detected and skipped
5. **Error Handling**: Failed messages don't block partition processing

## Partition Awareness

- **Dedicated Executors**: Each partition gets its own thread pool
- **Parallel Processing**: Messages from different partitions are processed concurrently
- **Order Preservation**: Message order is maintained within each partition
- **Independent Scaling**: Each partition can have different worker counts

## Monitoring and Statistics

The consumer provides comprehensive monitoring:

```python
# Access statistics
stats = consumer.stats
print(f"Messages processed: {stats['messages_processed']}")
print(f"Messages failed: {stats['messages_failed']}")
print(f"Partition stats: {stats['partition_stats']}")
```

## Examples

### Basic Consumer
```bash
python example_usage.py basic
```

### Multi-Topic Consumer
```bash
python example_usage.py multi
```

### High Throughput Consumer
```bash
python example_usage.py throughput
```

### Consumer with Monitoring
```bash
python example_usage.py monitoring
```

### Environment-based Configuration
```bash
python example_usage.py env
```

## Error Handling

The consumer handles various error scenarios:

- **Kafka Connection Issues**: Automatic reconnection
- **Database Connection Issues**: Connection pooling and retry
- **Message Processing Errors**: Individual message failures don't stop processing
- **Serialization Errors**: Graceful handling of malformed messages
- **Transaction Failures**: Automatic rollback and retry

## Performance Tuning

### High Throughput Configuration

```python
kafka_config = KafkaConfig(
    max_poll_records=2000,  # Larger batches
    session_timeout_ms=45000,  # Longer timeouts
    max_poll_interval_ms=600000  # 10 minutes
)

consumer = PartitionAwareKafkaConsumer(
    kafka_config=kafka_config,
    sqlserver_config=sqlserver_config,
    max_workers_per_partition=8  # More workers
)
```

### Memory Optimization

- Use smaller batch sizes for memory-constrained environments
- Reduce the number of workers per partition
- Enable auto-commit for less strict consistency requirements

## Troubleshooting

### Common Issues

1. **SQL Server Connection Issues**:
   - Verify ODBC driver installation
   - Check connection string parameters
   - Ensure SQL Server allows remote connections

2. **Kafka Connection Issues**:
   - Verify broker addresses
   - Check network connectivity
   - Validate topic existence

3. **Performance Issues**:
   - Monitor partition distribution
   - Adjust worker counts
   - Check database performance

### Logging

Enable debug logging for troubleshooting:

```python
import logging
logging.getLogger().setLevel(logging.DEBUG)
```

## License

This project is part of the konekt learning repository.
