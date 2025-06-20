# Konekt: Data Synchronization with Kafka and Kafka Connect

A comprehensive data processing framework integrating Kafka, MongoDB, and SQL Server with Python. This project provides multiple robust Kafka consumer implementations, Docker configurations, and data management tools.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Project Structure](#project-structure)
4. [Prerequisites](#prerequisites)
5. [Setting Up the Environment](#setting-up-the-environment)
   - [Docker Environment](#docker-environment)
   - [Python Virtual Environment](#python-virtual-environment)
   - [Configuration Files](#configuration-files)
6. [Scripts and Programs](#scripts-and-programs)
   - [Basic Kafka Consumer](#basic-kafka-consumer)
   - [Advanced Kafka Consumer](#advanced-kafka-consumer)
   - [Kafka SQL Server Consumer](#kafka-sql-server-consumer)
   - [Database Scripts](#database-scripts)
   - [Utility Scripts](#utility-scripts)
7. [Docker Configurations](#docker-configurations)
   - [Docker Compose](#docker-compose)
   - [MongoDB Configuration](#mongodb-configuration)
8. [Usage Examples](#usage-examples)
9. [Best Practices](#best-practices)
10. [Performance Tuning](#performance-tuning)
11. [Troubleshooting](#troubleshooting)
12. [License](#license)

## Overview

This project demonstrates a scalable data processing pipeline using Kafka, MongoDB, SQL Server, and Python. It includes several Kafka consumer implementations with different features:

- Basic scalable consumers
- Advanced partition-aware consumers
- Database integration with SQLite and SQL Server
- Multi-threading and resilience features
- Docker configurations for all required services

The framework provides reliable message processing with transaction support, fault tolerance, and performance monitoring.

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

## Project Structure

```
.
├── advanced_kafka_consumer.py       # Advanced partition-aware Kafka consumer implementation
├── check_drivers.py                 # Utility to verify database drivers
├── data.py                          # Data processing utilities
├── docker-compose-mongo.yaml        # Docker Compose for MongoDB services
├── docker-compose.yml               # Main Docker Compose configuration
├── example_usage.py                 # Usage examples for the consumers
├── kafka_sqlserver_consumer.py      # SQL Server integrated Kafka consumer
├── loader.py                        # Data loading utilities
├── mongo-source-connector.json      # MongoDB source connector configuration
├── mongo_health.py                  # MongoDB health check script
├── requirements.txt                 # Main Python dependencies
├── requirements_advanced.txt        # Additional dependencies for advanced features
├── script.py                        # Basic Kafka consumer implementation
├── script_db.py                     # SQLite database integrated Kafka consumer
├── script_db_sqlserver.py           # SQL Server integrated consumer implementation
├── setup_sqlserver_driver.sh        # SQL Server driver setup script
├── .env.example                     # Example environment variables
├── mongo-init/                      # MongoDB initialization scripts
│   └── 01-init.js                   # Initial MongoDB setup
└── scripts/                         # Utility scripts
    └── setup.sh                     # Environment setup script
```

## Prerequisites

### Software Requirements

- **Docker and Docker Compose**: For running Kafka, MongoDB, and SQL Server services
  - Docker version 20.10.0 or higher
  - Docker Compose version 2.0.0 or higher

- **Python**: For running the consumer scripts and utilities
  - Python 3.8 or higher
  - pip package manager

- **Database Drivers**:
  - ODBC Driver for SQL Server (v17 or v18)
  - SQLite (included with Python)

### Hardware Recommendations

- **Memory**: Minimum 8GB RAM (16GB recommended for full stack)
- **CPU**: 4+ cores recommended for optimal performance
- **Disk**: 10GB minimum free space

### Network Requirements

- Available ports:
  - Kafka Broker: 9092
  - Zookeeper: 2181
  - Schema Registry: 8081
  - Kafka Connect: 8083
  - Control Center: 9021
  - MongoDB: 27017, 27018, 27019
  - SQL Server: 1433

## Setting Up the Environment

### Docker Environment

#### 1. Start Docker Services

The project uses Docker Compose to set up the following services:
- SQL Server
- MongoDB
- Kafka (broker)
- Schema Registry
- Kafka Connect
- Control Center

To start all services:

```bash
# Start all services in detached mode
docker-compose up -d

# For MongoDB replica set only
docker-compose -f docker-compose-mongo.yaml up -d
```

#### 2. Check Service Status

Verify that all services are running:

```bash
docker-compose ps
```

#### 3. Access Service UIs

- **Kafka Control Center**: http://localhost:9021
- **Schema Registry**: http://localhost:8081
- **Kafka Connect**: http://localhost:8083

#### 4. Stop Docker Services

When you're done working with the project:

```bash
# Stop services without removing data
docker-compose down

# Stop services and remove volumes (deletes all data)
docker-compose down -v
```

### Python Virtual Environment

#### 1. Create a Virtual Environment

```bash
# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
# On Windows:
.venv\Scripts\activate
# On macOS/Linux:
source .venv/bin/activate
```

#### 2. Install Dependencies

```bash
# Install base requirements
pip install -r requirements.txt

# For advanced consumer features
pip install -r requirements_advanced.txt
```

#### 3. SQL Server Driver Setup

For SQL Server integration, install the required ODBC drivers:

```bash
# On Linux/macOS (requires sudo)
chmod +x setup_sqlserver_driver.sh
./setup_sqlserver_driver.sh

# On Windows
# Download and install the Microsoft ODBC Driver from Microsoft's website
```

### Configuration Files

#### Environment Variables

Copy the example environment file and customize it for your setup:

```bash
cp .env.example .env
# Edit .env with your configuration
```

Key environment variables:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=kafka_consumer
KAFKA_TOPICS=topic1,topic2,topic3

# SQL Server Configuration
SQLSERVER_HOST=localhost
SQLSERVER_PORT=1433
SQLSERVER_DB=kafka_messages
SQLSERVER_USER=sa
SQLSERVER_PASSWORD=YourPassword123!
SQLSERVER_DRIVER=ODBC Driver 18 for SQL Server
```

#### MongoDB Replica Set Configuration

Set up MongoDB key file for authentication:

```bash
# Generate keyfile
openssl rand -base64 756 > mongodb-keyfile
chmod 400 mongodb-keyfile

# Run initialization script
chmod +x scripts/setup.sh
./scripts/setup.sh
```

#### Kafka Connect Configuration

Configure MongoDB source connector:

```bash
# Review and modify mongo-source-connector.json as needed

# Create connector
curl -X POST -H "Content-Type: application/json" --data @mongo-source-connector.json http://localhost:8083/connectors
```

## Scripts and Programs

### Basic Kafka Consumer

`script.py` provides a basic Kafka consumer implementation with support for both `confluent-kafka` and `kafka-python` libraries.

#### Key Features

- Dual library support (confluent-kafka and kafka-python)
- Multi-threading with partition awareness
- Error handling and retry logic
- Graceful shutdown handling

#### Usage

```bash
# Run with confluent-kafka implementation
python script.py confluent

# Run with kafka-python implementation
python script.py kafka-python
```

#### Configuration

```python
# Example configuration
config = ConsumerConfig(
    bootstrap_servers="localhost:9092",
    group_id="my-consumer-group",
    topics=["my-topic"],
    auto_offset_reset="earliest",
    max_workers=4
)
```

### Advanced Kafka Consumer

`advanced_kafka_consumer.py` provides a production-ready Kafka consumer with advanced features for resilience and scaling.

#### Key Features

- **Partition-Aware Threading**: Dedicated worker threads per partition
- **Fault Tolerance**: Automatic error recovery and partition pausing
- **Batch Processing**: Configurable batch sizes and timeouts
- **Health Monitoring**: Background health checks and statistics
- **Error Thresholds**: Configurable error thresholds and recovery

#### Usage

```bash
# Run with default configuration
python advanced_kafka_consumer.py
```

#### Configuration

```python
# Advanced configuration example
config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="advanced-consumer-group",
    topics=["my-topic"],
    max_workers=4,
    partition_workers=2,  # Workers per partition
    batch_size=50,
    batch_timeout_ms=5000,
    max_retries=3,
    error_threshold=10
)
```

### Kafka SQL Server Consumer

`kafka_sqlserver_consumer.py` and `script_db_sqlserver.py` provide integration between Kafka and SQL Server with transaction support.

#### Key Features

- **SQL Server Integration**: Full SQL Server support using pyodbc driver
- **Transaction Management**: ACID-compliant database operations
- **Partition Awareness**: Dedicated thread pools for each partition
- **Idempotency**: Prevents duplicate message processing

#### Usage

```bash
# Run kafka_sqlserver_consumer.py
python kafka_sqlserver_consumer.py

# Or run script_db_sqlserver.py with specific client
python script_db_sqlserver.py confluent
python script_db_sqlserver.py kafka-python
```

#### Database Schema

The consumers automatically create the following table structure:

```sql
CREATE TABLE processed_messages (
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
```

### Database Scripts

`script_db.py` is a SQLite-based implementation of the Kafka consumer that provides local database integration.

#### Key Features

- **SQLite Integration**: Local database storage with transaction support
- **Thread-Safe Operations**: Proper database connection handling
- **Batch Processing**: Efficient batch insertions

#### Usage

```bash
# Run with confluent-kafka implementation
python script_db.py confluent

# Run with kafka-python implementation
python script_db.py kafka-python
```

### Utility Scripts

#### MongoDB Health Check

`mongo_health.py` verifies MongoDB replica set status and connectivity.

```bash
python mongo_health.py
```

#### Driver Checker

`check_drivers.py` verifies the availability of database drivers.

```bash
python check_drivers.py
```

#### Example Usage

`example_usage.py` provides examples of different consumer configurations.

```bash
# Run basic example
python example_usage.py basic

# Run multi-topic example
python example_usage.py multi

# Run monitoring example
python example_usage.py monitoring
```

## Docker Configurations

This project includes Docker Compose files for setting up the required infrastructure.

### Docker Compose

#### Main Docker Compose File (`docker-compose.yml`)

Sets up the core services including Kafka, Schema Registry, SQL Server, and Kafka Connect.

```yaml
# Key services defined in docker-compose.yml:
#  - zookeeper: Kafka coordination service
#  - kafka: Kafka broker
#  - schema-registry: Schema validation and compatibility
#  - kafka-connect: Integration platform for data pipelines
#  - control-center: Confluent Control Center for monitoring
#  - sqlserver: Microsoft SQL Server instance
```

#### MongoDB Docker Compose File (`docker-compose-mongo.yaml`)

Sets up a MongoDB replica set for advanced scenarios.

```yaml
# Key services in docker-compose-mongo.yaml:
#  - mongo1, mongo2, mongo3: MongoDB replica set nodes
```

### MongoDB Configuration

#### Replica Set Initialization

MongoDB is configured with a replica set for high availability:

1. The MongoDB containers initialize with the replica set configuration
2. Authentication is set up using the keyfile
3. Initial data is loaded through the `mongo-init/01-init.js` script

#### Connection String

After initialization, connect to MongoDB using:

```
mongodb://username:password@mongo1:27017,mongo2:27018,mongo3:27019/database?replicaSet=rs0&authSource=admin
```

### Volume Management

Docker volumes are used for persistent storage:

```yaml
volumes:
  zookeeper-data: # Stores ZooKeeper data
  kafka-data: # Stores Kafka data
  mongo1-data: # MongoDB primary data
  mongo2-data: # MongoDB secondary data
  mongo3-data: # MongoDB secondary data
  sqlserver-data: # SQL Server data
```

To clean up and start fresh:

```bash
docker-compose down -v
```

## Usage Examples

### Basic Message Processing

Here's an example of a simple message handler function for the advanced consumer:

```python
def handle_message_batch(messages):
    for msg in messages:
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        key = msg.key().decode('utf-8') if msg.key() else None
        value = msg.value().decode('utf-8')
        
        # Process message
        print(f"Processing: Topic={topic}, Partition={partition}, Offset={offset}")
        print(f"Message: Key={key}, Value={value}")
        
        # Your business logic here
    
    # Return True if processing succeeded, False otherwise
    return True
```

### SQL Server Consumer Example

Example of using the SQL Server integrated consumer:

```python
from script_db_sqlserver import ConsumerConfig, DatabaseConfig, run_confluent_consumer

# Configure the consumer
consumer_config = ConsumerConfig(
    bootstrap_servers="localhost:9092",
    group_id="sqlserver-group",
    topics=["orders", "users"],
    auto_offset_reset="earliest"
)

# Configure the database connection
db_config = DatabaseConfig(
    server="localhost",
    port=1433,
    database="kafka_db",
    username="sa",
    password="YourPassword123!",
    driver="ODBC Driver 18 for SQL Server",
    trust_server_certificate=True
)

# Run the consumer
run_confluent_consumer(consumer_config, db_config)
```

### Advanced Consumer with Custom Configuration

```python
from advanced_kafka_consumer import KafkaConfig, AdvancedKafkaConsumer

# Configure the consumer
config = KafkaConfig(
    bootstrap_servers="localhost:9092",
    group_id="advanced-group",
    topics=["high-volume-topic"],
    batch_size=100,             # Process 100 messages per batch
    batch_timeout_ms=30000,     # Wait up to 30 seconds for a batch
    error_threshold=5,          # Pause partition after 5 consecutive errors
    error_timeout_ms=60000,     # Resume partition after 1 minute
    stats_interval_ms=10000,    # Report stats every 10 seconds
    commit_interval_ms=5000,    # Commit offsets every 5 seconds
    partition_workers=2,        # Dedicate 2 threads per partition
    max_retries=3              # Retry failed batches 3 times
)

# Define a message handler
def process_messages(messages):
    # Your processing logic here
    for msg in messages:
        print(f"Processing message: {msg.value().decode('utf-8')}")
    return True

# Create and run the consumer
consumer = AdvancedKafkaConsumer(config, process_messages)
consumer.start()

# To shutdown gracefully
# consumer.stop()
```

## Best Practices

### Consumer Configuration

1. **Group ID Management**
   - Use unique group IDs for different applications
   - Use consistent group IDs for scaled instances of the same application

2. **Offset Management**
   - Commit offsets only after successful processing
   - Use at-least-once semantics for critical data

3. **Error Handling**
   - Implement proper retry logic with backoff
   - Use dead-letter topics for unprocessable messages

### Database Integration

1. **Connection Pooling**
   - Use connection pools for better performance
   - Monitor and tune pool sizes based on workload

2. **Transaction Management**
   - Use transactions to ensure data consistency
   - Commit database transactions and Kafka offsets atomically when possible

3. **Idempotency**
   - Design your database operations to be idempotent
   - Use unique message IDs to prevent duplicate processing

### Docker Environment

1. **Volume Management**
   - Use named volumes for data persistence
   - Regularly backup volume data

2. **Resource Allocation**
   - Set appropriate memory limits for containers
   - Monitor container resource usage

3. **Networking**
   - Use internal Docker networks for service communication
   - Expose only necessary ports to the host

## Performance Tuning

### Kafka Consumer Tuning

1. **Batch Size**
   - Increase batch size for higher throughput
   - Default: `50`
   - High volume: `100-200`

2. **Worker Threads**
   - Base thread count on available CPU cores
   - Default: `4`
   - Formula: `2 × CPU cores`

3. **Partition Workers**
   - Balance between partition count and available resources
   - Default: `1`
   - High throughput: `2-3`

4. **Commit Interval**
   - Balance between throughput and potential duplicate processing
   - Default: `5000ms`
   - Low latency: `1000-2000ms`

### SQL Server Tuning

1. **Connection Pool**
   - Set appropriate pool size
   - Default: `5`
   - Formula: `2 × worker_threads + 1`

2. **Bulk Inserts**
   - Use batch operations for better performance
   - Optimal batch size: `500-1000` rows

3. **Indexing**
   - Create indexes on frequently queried columns
   - Essential indexes:
     - Primary key on message ID
     - Composite index on (topic, partition, offset)

### Docker Service Tuning

1. **Kafka Broker**
   - Increase heap size: `-Xmx4g -Xms4g`
   - Adjust `num.partitions` based on consumer count

2. **MongoDB**
   - Increase WiredTiger cache: `--wiredTigerCacheSizeGB 2`
   - Enable journaling for data safety

## Troubleshooting

### Common Issues

#### Kafka Consumer Issues

| Issue | Symptoms | Resolution |
|-------|----------|------------|
| Consumer rebalancing too frequently | Processing delays, log messages about rebalancing | Increase `session.timeout.ms` and `heartbeat.interval.ms` |
| Slow message processing | High lag, consumer group falling behind | Increase batch size, add more worker threads |
| Duplicate message processing | Same message processed multiple times | Check for proper offset commits, ensure idempotent operations |
| Out of memory errors | Consumer crashes with OOM errors | Reduce batch size, check for memory leaks |

#### Database Connection Issues

| Issue | Symptoms | Resolution |
|-------|----------|------------|
| Connection pool exhaustion | "No available connections" errors | Increase pool size, reduce connection holding time |
| SQL Server connection failures | Connection timeout errors | Check network, credentials, and SQL Server state |
| Deadlocks | Transaction failures, deadlock victim errors | Review transaction isolation level, optimize query patterns |

#### Docker Issues

| Issue | Symptoms | Resolution |
|-------|----------|------------|
| Container fails to start | "Exit code 1" or specific error messages | Check logs with `docker-compose logs <service>` |
| Service unreachable | Connection refused errors | Verify service is running and ports are exposed correctly |
| Volume permission issues | Access denied errors | Check file permissions, use appropriate user in containers |

### Diagnostic Commands

#### Kafka

```bash
# List consumer groups
docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Check consumer group lag
docker-compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group-id>

# List topics
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

#### MongoDB

```bash
# Check replica set status
docker-compose exec mongo1 mongosh --eval "rs.status()"

# Verify connectivity
python mongo_health.py
```

#### SQL Server

```bash
# Check if SQL Server container is running
docker-compose ps sqlserver

# Test database connection
python check_drivers.py
```
