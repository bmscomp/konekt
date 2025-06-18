# Konekt make you data synched with Kafka and Kafka connect

This project demonstrates a scalable data processing pipeline using Kafka, MongoDB, and Python. It includes several Kafka consumer implementations and a data seeder for MongoDB.

## Prerequisites

- [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/install/)
- [Python](https://www.python.org/downloads/) 3.8 or higher
- [Git](https://git-scm.com/downloads) (optional)

## Project Structure

```
.
├── docker-compose.yml          # Docker services configuration
├── requirements.txt            # Python dependencies
├── .gitignore                  # Git ignore file
├── Seeder.py                   # Data seeder for MongoDB
├── ScalableKafkaConsumer.py    # Base Kafka consumer class
├── DynamicScalableKafkaConsumer.py  # Consumer with dynamic scaling
├── PartitionAwareConsumer.py   # Consumer with partition awareness
└── mongo-init/                 # MongoDB initialization scripts
    └── 01-init.js              # Initial data and user setup
```

## Setting Up Docker Environment

### 1. Start Docker Services

The project uses Docker Compose to set up the following services:
- SQL Server
- MongoDB
- Kafka (broker)
- Schema Registry
- Kafka Connect
- Control Center

To start all services:

```bash
docker-compose up -d
```

This command starts all services in detached mode. The `-d` flag runs containers in the background.

### 2. Check Service Status

Verify that all services are running:

```bash
docker-compose ps
```

### 3. Access Service UIs

- **Kafka Control Center**: http://localhost:9021
- **Schema Registry**: http://localhost:8081
- **Kafka Connect**: http://localhost:8083

### 4. Stop Docker Services

When you're done working with the project:

```bash
docker-compose down
```

To remove volumes as well (this will delete all data):

```bash
docker-compose down -v
```

## Setting Up Python Virtual Environment

### 1. Create a Virtual Environment

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Kaggle API (for Seeder.py)

To use the Seeder.py script, you need to set up Kaggle API credentials:

1. Create a Kaggle account if you don't have one: https://www.kaggle.com/
2. Go to your Kaggle account settings and create an API token
3. Download the `kaggle.json` file and place it in `~/.kaggle/` directory:

```bash
# Create the directory if it doesn't exist
mkdir -p ~/.kaggle

# Move the downloaded kaggle.json file
mv path/to/downloaded/kaggle.json ~/.kaggle/

# Set proper permissions
chmod 600 ~/.kaggle/kaggle.json
```

## Running Python Scripts

Ensure your virtual environment is activated before running any scripts.

### 1. Seed Data to MongoDB

```bash
python Seeder.py
```

This script:
- Authenticates with the Kaggle API
- Downloads a dataset from Kaggle
- Processes JSON files (supports both JSON arrays and JSON Lines format)
- Inserts the data into MongoDB in batches

### 2. Run Kafka Consumers

#### Basic Scalable Consumer

```bash
python ScalableKafkaConsumer.py
```

#### Dynamic Scalable Consumer

```bash
python DynamicScalableKafkaConsumer.py
```

#### Partition-Aware Consumer

```bash
python PartitionAwareConsumer.py
```

## Customizing Consumers

You can customize the consumer behavior by modifying the following parameters in the scripts:

- `TOPIC`: The Kafka topic to consume from
- `BOOTSTRAP_SERVERS`: Kafka broker addresses
- `GROUP_ID`: Consumer group ID
- `NUM_WORKERS`: Number of worker threads (for ScalableKafkaConsumer)
- `min_workers` and `max_workers`: Worker thread limits (for DynamicScalableKafkaConsumer)
- `num_workers_per_partition`: Workers per partition (for PartitionAwareConsumer)

## MongoDB Connection Details

- **Host**: localhost
- **Port**: 27017
- **Username**: storeuser
- **Password**: secret
- **Database**: store

## Troubleshooting

### Docker Issues

1. **Services not starting**: Check Docker logs with `docker-compose logs [service_name]`
2. **Port conflicts**: Ensure the required ports (1433, 27017, 9092, etc.) are not in use

### Python Issues

1. **Module not found errors**: Ensure all dependencies are installed with `pip install -r requirements.txt`
2. **Kafka connection errors**: Verify Kafka broker is running with `docker-compose ps broker`
3. **MongoDB connection errors**: Check MongoDB status with `docker-compose ps mongo`

## Extending the Project

- Add new consumer implementations by extending the base `ScalableKafkaConsumer` class
- Modify the MongoDB initialization script to create additional collections
- Add new data sources to the Seeder.py script

## Create a connector from curl command

curl -X POST -H "Content-Type: application/json" --data @mongo-source-connector.json http://localhost:8083/connectors

## Mongo Key file for replicaSet

openssl rand -base64 756 > mongodb-keyfile
chmod 400 mongodb-keyfile  # Restrict permissions
chmod +x scripts/setup.sh


## Mongo db connection

mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0&authSource=admin

## Create a connector on localhost instance.

curl -X POST -H "Content-Type: application/json" --data @mongo-source-connector.json http://localhost:8083/connectors



# Partition-Aware and Resilient Kafka Consumers with Multi-Threading

This guide provides comprehensive implementations of partition-aware and resilient Kafka consumers using both `confluent-kafka` and `kafka-python` libraries with multi-threaded models in Python.

## Table of Contents

1. [Overview](#overview)
2. [Key Features](#key-features)
3. [Architecture](#architecture)
4. [Implementation Details](#implementation-details)
5. [Configuration](#configuration)
6. [Usage Examples](#usage-examples)
7. [Best Practices](#best-practices)
8. [Dependencies](#dependencies)
9. [Installation](#installation)

## Overview

The implementation provides two robust Kafka consumer implementations that handle partition awareness, resilience, and multi-threading efficiently. Both consumers are designed for production use with comprehensive error handling, monitoring, and graceful shutdown capabilities.

## Key Features

### Partition Awareness

- **Dedicated partition workers**: Each assigned partition gets its own worker thread
- **Partition-specific queues**: Messages are queued per partition to maintain ordering
- **Dynamic partition handling**: Automatically handles partition assignment/revocation
- **Batch processing**: Configurable batch sizes for efficient processing

### Resilience

- **Retry logic**: Configurable retry attempts with exponential backoff
- **Graceful shutdown**: Proper signal handling and resource cleanup  
- **Error handling**: Comprehensive error handling at all levels
- **Offset management**: Manual offset commits only after successful processing
- **Health monitoring**: Built-in statistics and monitoring

### Multi-threading

- **ThreadPoolExecutor**: For managing worker threads efficiently
- **Per-partition workers**: Maintains message ordering within partitions
- **Thread-safe operations**: Proper synchronization for shared resources
- **Configurable concurrency**: Adjustable number of worker threads

## Architecture

### Base Consumer Architecture

```
┌─────────────────┐
│  Main Consumer  │
│     Thread      │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ Partition Mgmt  │
│   & Batching    │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Partition 0     │    │ Partition 1     │    │ Partition N     │
│ Worker Queue    │    │ Worker Queue    │    │ Worker Queue    │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                      │                      │
          ▼                      ▼                      ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│ Message Batch   │    │ Message Batch   │    │ Message Batch   │
│ Processor       │    │ Processor       │    │ Processor       │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Confluent Kafka Consumer

The Confluent Kafka implementation leverages:

- **Callback-based partition assignment**: Uses `on_assign` and `on_revoke` callbacks
- **High-performance C library**: Built on librdkafka for optimal performance
- **Advanced configuration**: Extensive configuration options for fine-tuning
- **Admin client integration**: Cluster metadata and administration capabilities

```python
class ConfluentKafkaConsumer(BaseKafkaConsumer):
    def _on_assign(self, consumer, partitions):
        # Create worker threads for new partitions
        
    def _on_revoke(self, consumer, partitions):
        # Cleanup workers for revoked partitions
```

### Kafka Python Consumer

The Kafka Python implementation features:

- **Pure Python implementation**: No external C dependencies
- **Poll-based consumption**: Explicit polling with configurable timeouts
- **Partition monitoring**: Periodic checks for partition assignment changes
- **Topic partition management**: Direct control over partition assignment

```python
class KafkaPythonConsumer(BaseKafkaConsumer):
    def _setup_partition_workers(self):
        # Monitor and setup workers for partition changes
```

## Implementation Details

### Consumer Configuration

```python
@dataclass
class ConsumerConfig:
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "default-group"
    topics: List[str] = field(default_factory=list)
    max_workers: int = 4
    batch_size: int = 10
    max_retries: int = 3
    # ... additional configuration options
```

### Message Batch Processing

```python
@dataclass
class MessageBatch:
    partition: int
    topic: str
    messages: List[Any]
    offsets: List[int]
```

Messages are batched per partition to:
- Improve processing efficiency
- Maintain message ordering within partitions
- Enable bulk offset commits
- Reduce overhead of individual message processing

### Error Handling and Retries

```python
def process_message_with_retry(self, message: Any) -> bool:
    for attempt in range(self.config.max_retries + 1):
        try:
            success = self.message_handler(message)
            if success:
                self.update_stats(processed=1)
                return True
        except Exception as e:
            if attempt < self.config.max_retries:
                time.sleep(self.config.retry_backoff_ms / 1000.0)
            else:
                self.update_stats(failed=1)
                return False
```

### Graceful Shutdown

Both implementations support graceful shutdown through:

- **Signal handlers**: SIGINT and SIGTERM handling
- **Shutdown events**: Thread coordination for clean shutdown
- **Resource cleanup**: Proper cleanup of threads, connections, and queues
- **Final processing**: Completion of in-flight messages before shutdown

## Configuration

### Basic Configuration

```python
config = ConsumerConfig(
    bootstrap_servers="localhost:9092",
    group_id="my-consumer-group",
    topics=["my-topic"],
    max_workers=4,
    batch_size=10
)
```

### Advanced Configuration

```python
config = ConsumerConfig(
    bootstrap_servers="broker1:9092,broker2:9092,broker3:9092",
    group_id="advanced-consumer-group",
    topics=["topic1", "topic2"],
    max_workers=8,
    batch_size=50,
    max_retries=5,
    retry_backoff_ms=2000,
    session_timeout_ms=30000,
    heartbeat_interval_ms=10000,
    max_poll_interval_ms=300000,
    partition_assignment_strategy="range"
)
```

### Environment-Based Configuration

```python
import os

config = ConsumerConfig(
    bootstrap_servers=os.getenv("KAFKA_BROKERS", "localhost:9092"),
    group_id=os.getenv("CONSUMER_GROUP", "default-group"),
    topics=os.getenv("KAFKA_TOPICS", "").split(","),
    max_workers=int(os.getenv("MAX_WORKERS", "4")),
    batch_size=int(os.getenv("BATCH_SIZE", "10"))
)
```

## Usage Examples

### Basic Usage

```python
def simple_message_handler(message: Any) -> bool:
    print(f"Processing message: {message}")
    return True

# Confluent Kafka Consumer
if CONFLUENT_AVAILABLE:
    config = ConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="simple-group",
        topics=["test-topic"]
    )
    consumer = ConfluentKafkaConsumer(config, simple_message_handler)
    consumer.start()
```

### Advanced Message Handler

```python
import json
import logging

def advanced_message_handler(message: Any) -> bool:
    try:
        # Parse JSON message
        if isinstance(message, str):
            data = json.loads(message)
        else:
            data = message
        
        # Business logic processing
        user_id = data.get('user_id')
        event_type = data.get('event_type')
        
        if event_type == 'user_signup':
            process_user_signup(user_id, data)
        elif event_type == 'user_purchase':
            process_user_purchase(user_id, data)
        else:
            logging.warning(f"Unknown event type: {event_type}")
        
        return True
        
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON message: {message}")
        return False
    except Exception as e:
        logging.error(f"Processing error: {e}")
        return False

def process_user_signup(user_id: str, data: dict):
    # Implement user signup logic
    pass

def process_user_purchase(user_id: str, data: dict):
    # Implement purchase processing logic
    pass
```

### Running with Monitoring

```python
import threading
import time

def run_consumer_with_monitoring():
    config = ConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="monitored-group",
        topics=["events"],
        max_workers=6,
        batch_size=20
    )
    
    consumer = ConfluentKafkaConsumer(config, advanced_message_handler)
    
    # Stats reporting thread
    def report_stats():
        while consumer.running:
            time.sleep(30)
            stats = consumer.get_stats()
            logging.info(f"Consumer Stats: {stats}")
    
    stats_thread = threading.Thread(target=report_stats, daemon=True)
    stats_thread.start()
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        logging.info("Shutdown requested")
    finally:
        consumer.shutdown()
```

### Command Line Interface

```python
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer')
    parser.add_argument('--library', choices=['confluent', 'kafka-python'], 
                       required=True, help='Kafka library to use')
    parser.add_argument('--brokers', default='localhost:9092', 
                       help='Kafka brokers')
    parser.add_argument('--group', required=True, help='Consumer group ID')
    parser.add_argument('--topics', required=True, nargs='+', 
                       help='Topics to consume')
    parser.add_argument('--workers', type=int, default=4, 
                       help='Number of worker threads')
    
    args = parser.parse_args()
    
    config = ConsumerConfig(
        bootstrap_servers=args.brokers,
        group_id=args.group,
        topics=args.topics,
        max_workers=args.workers
    )
    
    if args.library == 'confluent':
        consumer = ConfluentKafkaConsumer(config, advanced_message_handler)
    else:
        consumer = KafkaPythonConsumer(config, advanced_message_handler)
    
    consumer.start()

if __name__ == "__main__":
    main()
```

## Best Practices

### 1. Partition Ordering
- Messages within a partition are processed in order
- Use partition keys strategically to ensure related messages are in the same partition
- Avoid cross-partition transactions

### 2. Offset Management
- Manual offset commits ensure at-least-once delivery
- Commit offsets only after successful processing
- Use batch commits for better performance

### 3. Error Handling
- Implement proper retry logic with exponential backoff
- Use dead letter queues for messages that consistently fail
- Log errors with sufficient context for debugging

### 4. Resource Management
- Properly size worker thread pools based on workload
- Monitor memory usage, especially with large batches
- Implement proper cleanup in shutdown handlers

### 5. Monitoring and Observability

```python
# Example metrics collection
class ConsumerMetrics:
    def __init__(self):
        self.messages_processed = 0
        self.messages_failed = 0
        self.processing_time = []
        self.last_commit_time = time.time()
    
    def record_processing_time(self, duration):
        self.processing_time.append(duration)
        # Keep only last 1000 measurements
        self.processing_time = self.processing_time[-1000:]
    
    def get_avg_processing_time(self):
        return sum(self.processing_time) / len(self.processing_time) if self.processing_time else 0
```

### 6. Configuration Management

```python
# Use environment-specific configurations
class EnvironmentConfig:
    @classmethod
    def for_development(cls):
        return ConsumerConfig(
            bootstrap_servers="localhost:9092",
            max_workers=2,
            batch_size=5
        )
    
    @classmethod
    def for_production(cls):
        return ConsumerConfig(
            bootstrap_servers=os.getenv("KAFKA_BROKERS"),
            max_workers=8,
            batch_size=50,
            session_timeout_ms=30000,
            max_poll_interval_ms=300000
        )
```

## Performance Tuning

### Batch Size Optimization

```python
# Small batches: Lower latency, higher overhead
config.batch_size = 1-10

# Medium batches: Balanced performance
config.batch_size = 10-50

# Large batches: Higher throughput, higher latency
config.batch_size = 50-500
```

### Worker Thread Tuning

```python
# CPU-bound processing
config.max_workers = multiprocessing.cpu_count()

# I/O-bound processing
config.max_workers = multiprocessing.cpu_count() * 2-4

# Mixed workload
config.max_workers = multiprocessing.cpu_count() * 1.5
```

## Dependencies

### Core Dependencies

```txt
# Kafka client libraries
confluent-kafka>=2.3.0
kafka-python>=2.0.2

# Threading and async operations
threading-utils>=0.3
concurrent-futures>=3.1.1

# Logging and monitoring
structlog>=23.2.0
python-json-logger>=2.0.7

# Configuration management
pydantic>=2.5.0
python-dotenv>=1.0.0
```

### Optional Dependencies

```txt
# Serialization formats
msgpack>=1.0.7
avro-python3>=1.11.3
fastavro>=1.9.4
protobuf>=4.25.1

# Monitoring and metrics
prometheus-client>=0.19.0
statsd>=4.0.1

# Schema registry support
confluent-kafka[avro]>=2.3.0
confluent-kafka[json]>=2.3.0
confluent-kafka[protobuf]>=2.3.0
```

### Development Dependencies

```txt
# Testing
pytest>=7.4.3
pytest-asyncio>=0.21.1
pytest-mock>=3.12.0
pytest-cov>=4.1.0

# Code quality
black>=23.11.0
flake8>=6.1.0
mypy>=1.7.1
isort>=5.12.0
```

## Installation

### Basic Installation

```bash
# Install core dependencies
pip install confluent-kafka kafka-python

# Install from requirements file
pip install -r requirements.txt
```

### Development Installation

```bash
# Install all dependencies including development tools
pip install -r requirements.txt

# Install in development mode (if using setup.py)
pip install -e .
```

### Docker Installation

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "kafka_consumer.py", "confluent"]
```

## Troubleshooting

### Common Issues

1. **Partition Rebalancing**
   - Increase `session_timeout_ms` for slow processing
   - Reduce `max_poll_interval_ms` for faster detection
   - Monitor consumer lag

2. **Memory Issues**
   - Reduce batch sizes
   - Limit worker thread count
   - Monitor queue sizes

3. **Connection Issues**
   - Verify broker connectivity
   - Check security settings (SSL/SASL)
   - Review network configuration

4. **Performance Issues**
   - Profile message handlers
   - Optimize batch processing
   - Consider async processing for I/O operations

### Debugging

```python
# Enable debug logging
logging.getLogger('confluent_kafka').setLevel(logging.DEBUG)
logging.getLogger('kafka').setLevel(logging.DEBUG)

# Add performance monitoring
import time

class TimedMessageHandler:
    def __init__(self, handler):
        self.handler = handler
        self.processing_times = []
    
    def __call__(self, message):
        start_time = time.time()
        result = self.handler(message)
        processing_time = time.time() - start_time
        self.processing_times.append(processing_time)
        
        if len(self.processing_times) % 100 == 0:
            avg_time = sum(self.processing_times[-100:]) / 100
            logging.info(f"Average processing time (last 100): {avg_time:.3f}s")
        
        return result
```

This comprehensive implementation provides production-ready Kafka consumers with partition awareness, resilience, and multi-threading capabilities suitable for high-throughput, mission-critical applications.

## script_db.py Implementation

`script_db.py` is a robust Kafka consumer implementation that supports both confluent-kafka and kafka-python libraries with SQLite database integration. It provides reliable message consumption and storage with proper transactional integrity.

### Key Features

1. **Dual Kafka Client Support**
   - Compatible with both confluent-kafka and kafka-python libraries
   - Seamless switching between implementations
   - Consistent interface across both clients

2. **SQLite Database Integration**
   - Transactional message storage
   - Automatic table creation
   - Efficient batch processing
   - Thread-safe database operations

3. **Resilient Message Processing**
   - Configurable retry mechanism
   - Error handling with proper logging
   - At-least-once delivery guarantee
   - Proper offset management

4. **Multi-threaded Architecture**
   - Per-partition worker threads
   - Thread-safe message queues
   - Controlled concurrency
   - Graceful shutdown handling

### Usage

```bash
# Run with kafka-python implementation
python script_db.py kafka-python

# Run with confluent-kafka implementation
python script_db.py confluent
```

### Configuration

```python
# Consumer configuration
config = ConsumerConfig(
    bootstrap_servers="localhost:9092",
    group_id="my-group",
    topics=["test-topic"],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=500,
    session_timeout_ms=30000,
    heartbeat_interval_ms=10000,
    max_poll_interval_ms=300000,
    retry_count=3,
    retry_interval=1.0
)

# Database configuration
db_config = DatabaseConfig(
    database="kafka_messages.db",
    table_name="processed_messages",
    create_table_if_not_exists=True
)
```

### Message Processing Flow

1. **Message Consumption**
   - Consumer polls messages from Kafka
   - Messages are validated and decoded
   - Metadata is extracted (topic, partition, offset)

2. **Processing**
   - Messages are processed by worker threads
   - Each partition has a dedicated worker
   - Retry logic handles transient failures

3. **Database Storage**
   - Messages are stored in SQLite database
   - Transactions ensure data consistency
   - Offsets are committed after successful storage

4. **Monitoring**
   - Processing statistics are collected
   - Worker thread health is monitored
   - Error conditions are logged

### Error Handling

1. **Kafka Errors**
   - Connection issues
   - Partition rebalancing
   - Message deserialization

2. **Processing Errors**
   - Invalid message format
   - Business logic failures
   - Resource constraints

3. **Database Errors**
   - Connection issues
   - Transaction failures
   - Schema violations

### Best Practices

1. **Configuration**
   - Adjust batch sizes based on message size and processing time
   - Set appropriate timeout values for your use case
   - Configure retry parameters based on error patterns

2. **Monitoring**
   - Watch for consumer lag
   - Monitor processing times
   - Track error rates
   - Check database performance

3. **Maintenance**
   - Regularly clean up old data
   - Monitor disk space
   - Review error logs
   - Update configurations as needed

## script_db_sqlserver.py Implementation

`script_db_sqlserver.py` is a SQL Server-specific implementation of the Kafka consumer that provides robust message consumption and storage capabilities with Microsoft SQL Server as the backend database.

### Key Features

1. **SQL Server Integration**
   - Full SQL Server support using pyodbc driver
   - Optimized connection pooling for SQL Server
   - SQL Server-specific schema and data types
   - Proper index creation for performance

2. **Dual Kafka Client Support**
   - Compatible with both confluent-kafka and kafka-python libraries
   - Consistent interface across both client implementations
   - Client-specific configuration handling

3. **Transactional Message Processing**
   - ACID-compliant database operations
   - Proper transaction management with commit/rollback
   - Offset commits synchronized with database transactions
   - Bulk insert operations for efficiency

4. **Resilient Architecture**
   - Configurable retry mechanism
   - Error handling with proper logging
   - Connection error recovery
   - Graceful shutdown handling

### Usage

```bash
# Run with confluent-kafka implementation
python script_db_sqlserver.py confluent

# Run with kafka-python implementation
python script_db_sqlserver.py kafka-python
```

### Configuration

```python
# SQL Server configuration
db_config = DatabaseConfig(
    server=os.getenv("SQLSERVER_HOST", "localhost"),
    port=int(os.getenv("SQLSERVER_PORT", "1433")),
    database=os.getenv("SQLSERVER_DB", "kafka_messages"),
    username=os.getenv("SQLSERVER_USER", "sa"),
    password=os.getenv("SQLSERVER_PASSWORD", "Seeqwa1!Passw0rd"),
    driver=os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
    table_name="processed_messages",
    create_table_if_not_exists=True,
    trust_server_certificate=True,
    encrypt=True,
    connection_timeout=30
)

# Consumer configuration
consumer_config = ConsumerConfig(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    group_id="sqlserver-consumer-group",
    topics=["test-topic"],
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    max_poll_records=500,
    session_timeout_ms=30000,
    heartbeat_interval_ms=10000,
    max_poll_interval_ms=300000,
    max_workers=4,
    batch_size=100,
    database_config=db_config,
    enable_database_storage=True
)
```

### Database Schema

The script automatically creates the following SQL Server table structure:

```sql
CREATE TABLE processed_messages (
    id VARCHAR(36) PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    offset INT NOT NULL,
    message_key VARCHAR(255),
    message_value TEXT NOT NULL,
    message_headers TEXT,
    consumer_group VARCHAR(255) NOT NULL,
    processing_time FLOAT,
    processing_status VARCHAR(50) NOT NULL,
    error_message TEXT,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL
);

-- Indexes for better performance
CREATE INDEX idx_processed_messages_topic_partition_offset ON processed_messages (topic, partition, offset);
CREATE INDEX idx_processed_messages_status ON processed_messages (processing_status);
CREATE INDEX idx_processed_messages_created_at ON processed_messages (created_at);
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|--------|
| `SQLSERVER_HOST` | SQL Server hostname | localhost |
| `SQLSERVER_PORT` | SQL Server port | 1433 |
| `SQLSERVER_DB` | Database name | kafka_messages |
| `SQLSERVER_USER` | Database username | sa |
| `SQLSERVER_PASSWORD` | Database password | Seeqwa1!Passw0rd |
| `SQLSERVER_DRIVER` | ODBC driver name | ODBC Driver 18 for SQL Server |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | localhost:9092 |

### Best Practices

1. **SQL Server Configuration**
   - Use connection pooling for better performance
   - Set appropriate connection timeouts
   - Configure proper authentication
   - Use parameterized queries for security

2. **Performance Optimization**
   - Use bulk inserts for better throughput
   - Create appropriate indexes for your query patterns
   - Monitor SQL Server query performance
   - Consider table partitioning for large datasets

3. **Maintenance**
   - Implement regular data cleanup
   - Monitor SQL Server logs
   - Check for database growth
   - Maintain index statistics
