#!/usr/bin/env python3
"""
Kafka to SQL Server Consumer with Transaction Management and Partition Awareness

This module provides a robust Kafka consumer that:
1. Consumes messages from Kafka topics
2. Inserts messages into SQL Server database
3. Manages transactions between Kafka and SQL Server
4. Provides partition-aware processing capabilities
5. Handles failures and retries gracefully
"""

import json
import logging
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Callable
import uuid
import os
from contextlib import contextmanager

# Kafka imports
try:
    from confluent_kafka import Consumer as ConfluentConsumer, KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient
    CONFLUENT_AVAILABLE = True
except ImportError:
    CONFLUENT_AVAILABLE = False

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError as KafkaPythonError
    from kafka.structs import TopicPartition
    KAFKA_PYTHON_AVAILABLE = True
except ImportError:
    KAFKA_PYTHON_AVAILABLE = False

# Database imports
try:
    import sqlalchemy
    from sqlalchemy import create_engine, text, Table, Column, String, DateTime, Text, Integer, MetaData, BigInteger
    from sqlalchemy.orm import sessionmaker, Session
    from sqlalchemy.exc import SQLAlchemyError
    import pyodbc
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    print("SQLAlchemy or pyodbc not available. Database functionality will be disabled.")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class KafkaConfig:
    """Configuration for Kafka consumer"""
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "kafka_sqlserver_consumer"
    topics: List[str] = field(default_factory=lambda: ["test-topic"])
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_interval_ms: int = 300000
    consumer_timeout_ms: int = 5000


@dataclass
class SQLServerConfig:
    """Configuration for SQL Server database"""
    server: str = "localhost"
    port: int = 1433
    database: str = "kafka_messages"
    username: str = "sa"
    password: str = "YourPassword123!"
    driver: str = "auto"  # Changed to auto-detect
    table_name: str = "kafka_messages"
    trust_server_certificate: bool = True
    encrypt: bool = True
    connection_timeout: int = 30
    
    def _detect_available_driver(self) -> str:
        """Detect available SQL Server ODBC driver"""
        try:
            import pyodbc
            available_drivers = pyodbc.drivers()
            
            # Preferred drivers in order of preference
            preferred_drivers = [
                "ODBC Driver 18 for SQL Server",
                "ODBC Driver 17 for SQL Server",
                "ODBC Driver 13 for SQL Server",
                "ODBC Driver 11 for SQL Server",
                "SQL Server Native Client 11.0",
                "SQL Server Native Client 10.0",
                "SQL Server"
            ]
            
            for driver in preferred_drivers:
                if driver in available_drivers:
                    logger.info(f"Using SQL Server driver: {driver}")
                    return driver
            
            # If no preferred driver found, list available drivers
            sql_drivers = [d for d in available_drivers if 'SQL' in d.upper()]
            if sql_drivers:
                logger.warning(f"No preferred SQL Server driver found. Using: {sql_drivers[0]}")
                logger.info(f"Available SQL-related drivers: {sql_drivers}")
                return sql_drivers[0]
            
            # No SQL Server drivers found
            logger.error("No SQL Server ODBC drivers found!")
            logger.error(f"Available drivers: {available_drivers}")
            logger.error("Please install Microsoft ODBC Driver for SQL Server")
            logger.error("Run: ./setup_sqlserver_driver.sh")
            raise RuntimeError("No SQL Server ODBC driver available")
            
        except ImportError:
            logger.error("pyodbc not installed. Please install: pip install pyodbc")
            raise RuntimeError("pyodbc not available")
    
    def get_connection_string(self) -> str:
        """Generate SQLAlchemy connection string"""
        # Auto-detect driver if needed
        actual_driver = self.driver
        if self.driver == "auto":
            actual_driver = self._detect_available_driver()
        
        params = [
            f"DRIVER={{{actual_driver}}}",
            f"SERVER={self.server},{self.port}",
            f"DATABASE={self.database}",
            f"UID={self.username}",
            f"PWD={self.password}",
            f"TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}",
            f"Encrypt={'yes' if self.encrypt else 'no'}",
            f"Connection Timeout={self.connection_timeout}"
        ]
        connection_string = "mssql+pyodbc://?" + "&".join(params)
        return connection_string


@dataclass
class ProcessedMessage:
    """Represents a processed Kafka message"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    topic: str = ""
    partition: int = 0
    offset: int = 0
    message_key: Optional[str] = None
    message_value: str = ""
    message_headers: Optional[str] = None
    consumer_group: str = ""
    processing_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    processing_status: str = "SUCCESS"
    error_message: Optional[str] = None


class SQLServerManager:
    """Manages SQL Server database operations with transaction support"""
    
    def __init__(self, config: SQLServerConfig):
        self.config = config
        self.engine = None
        self.session_factory = None
        self.metadata = MetaData()
        self._setup_database()
    
    def _setup_database(self):
        """Initialize database connection and create tables"""
        try:
            connection_string = self.config.get_connection_string()
            self.engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            self.session_factory = sessionmaker(bind=self.engine)
            self._create_table_if_not_exists()
            logger.info("Database connection established successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup database: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create the messages table if it doesn't exist"""
        try:
            # Define table structure
            messages_table = Table(
                self.config.table_name,
                self.metadata,
                Column('id', String(36), primary_key=True),
                Column('topic', String(255), nullable=False),
                Column('partition', Integer, nullable=False),
                Column('offset', BigInteger, nullable=False),
                Column('message_key', Text, nullable=True),
                Column('message_value', Text, nullable=False),
                Column('message_headers', Text, nullable=True),
                Column('consumer_group', String(255), nullable=False),
                Column('processing_timestamp', DateTime, nullable=False),
                Column('processing_status', String(50), nullable=False),
                Column('error_message', Text, nullable=True)
            )
            
            # Create table and indexes
            self.metadata.create_all(self.engine)
            
            # Create indexes for better performance
            with self.engine.connect() as conn:
                try:
                    conn.execute(text(f"""
                        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_{self.config.table_name}_topic_partition_offset')
                        CREATE INDEX IX_{self.config.table_name}_topic_partition_offset 
                        ON {self.config.table_name} (topic, partition, offset)
                    """))
                    
                    conn.execute(text(f"""
                        IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_{self.config.table_name}_processing_timestamp')
                        CREATE INDEX IX_{self.config.table_name}_processing_timestamp 
                        ON {self.config.table_name} (processing_timestamp)
                    """))
                    
                    conn.commit()
                    logger.info(f"Table {self.config.table_name} and indexes created successfully")
                except Exception as e:
                    logger.warning(f"Could not create indexes: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions with automatic cleanup"""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def insert_messages_batch(self, messages: List[ProcessedMessage]) -> bool:
        """Insert a batch of messages with transaction management"""
        if not messages:
            return True
            
        try:
            with self.get_session() as session:
                # Convert messages to dict format for bulk insert
                message_dicts = []
                for msg in messages:
                    message_dicts.append({
                        'id': msg.id,
                        'topic': msg.topic,
                        'partition': msg.partition,
                        'offset': msg.offset,
                        'message_key': msg.message_key,
                        'message_value': msg.message_value,
                        'message_headers': msg.message_headers,
                        'consumer_group': msg.consumer_group,
                        'processing_timestamp': msg.processing_timestamp,
                        'processing_status': msg.processing_status,
                        'error_message': msg.error_message
                    })
                
                # Bulk insert using raw SQL for better performance
                insert_sql = text(f"""
                    INSERT INTO {self.config.table_name} 
                    (id, topic, partition, offset, message_key, message_value, 
                     message_headers, consumer_group, processing_timestamp, 
                     processing_status, error_message)
                    VALUES (:id, :topic, :partition, :offset, :message_key, 
                            :message_value, :message_headers, :consumer_group, 
                            :processing_timestamp, :processing_status, :error_message)
                """)
                
                session.execute(insert_sql, message_dicts)
                logger.info(f"Successfully inserted {len(messages)} messages into database")
                return True
                
        except Exception as e:
            logger.error(f"Failed to insert messages batch: {e}")
            return False
    
    def check_message_exists(self, topic: str, partition: int, offset: int) -> bool:
        """Check if a message already exists in the database"""
        try:
            with self.get_session() as session:
                result = session.execute(
                    text(f"""
                        SELECT COUNT(*) FROM {self.config.table_name} 
                        WHERE topic = :topic AND partition = :partition AND offset = :offset
                    """),
                    {'topic': topic, 'partition': partition, 'offset': offset}
                ).scalar()
                return result > 0
        except Exception as e:
            logger.error(f"Failed to check message existence: {e}")
            return False
    
    def close(self):
        """Close database connections"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")


class PartitionAwareKafkaConsumer:
    """Partition-aware Kafka consumer with SQL Server integration"""
    
    def __init__(self, kafka_config: KafkaConfig, sqlserver_config: SQLServerConfig, 
                 max_workers_per_partition: int = 2):
        self.kafka_config = kafka_config
        self.sqlserver_config = sqlserver_config
        self.max_workers_per_partition = max_workers_per_partition
        
        # Initialize components
        self.db_manager = SQLServerManager(sqlserver_config)
        self.consumer = None
        self.partition_executors = {}
        self.running = False
        self.stats = {
            'messages_processed': 0,
            'messages_failed': 0,
            'start_time': None,
            'partition_stats': {}
        }
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown()
    
    def _setup_consumer(self):
        """Setup Kafka consumer based on available libraries"""
        if CONFLUENT_AVAILABLE:
            self._setup_confluent_consumer()
        elif KAFKA_PYTHON_AVAILABLE:
            self._setup_kafka_python_consumer()
        else:
            raise RuntimeError("No Kafka library available")
    
    def _setup_confluent_consumer(self):
        """Setup confluent-kafka consumer"""
        config = {
            'bootstrap.servers': self.kafka_config.bootstrap_servers,
            'group.id': self.kafka_config.group_id,
            'auto.offset.reset': self.kafka_config.auto_offset_reset,
            'enable.auto.commit': self.kafka_config.enable_auto_commit,
            'session.timeout.ms': self.kafka_config.session_timeout_ms,
            'heartbeat.interval.ms': self.kafka_config.heartbeat_interval_ms,
            'max.poll.interval.ms': self.kafka_config.max_poll_interval_ms
        }
        
        self.consumer = ConfluentConsumer(config)
        self.consumer.subscribe(self.kafka_config.topics)
        logger.info("Confluent Kafka consumer initialized")
    
    def _setup_kafka_python_consumer(self):
        """Setup kafka-python consumer"""
        config = {
            'bootstrap_servers': self.kafka_config.bootstrap_servers,
            'group_id': self.kafka_config.group_id,
            'auto_offset_reset': self.kafka_config.auto_offset_reset,
            'enable_auto_commit': self.kafka_config.enable_auto_commit,
            'max_poll_records': self.kafka_config.max_poll_records,
            'session_timeout_ms': self.kafka_config.session_timeout_ms,
            'heartbeat_interval_ms': self.kafka_config.heartbeat_interval_ms,
            'max_poll_interval_ms': self.kafka_config.max_poll_interval_ms,
            'consumer_timeout_ms': self.kafka_config.consumer_timeout_ms
        }
        
        self.consumer = KafkaConsumer(*self.kafka_config.topics, **config)
        logger.info("Kafka-Python consumer initialized")
    
    def _setup_partition_executors(self):
        """Setup thread executors for each partition"""
        if CONFLUENT_AVAILABLE and hasattr(self.consumer, 'list_topics'):
            # Get partition information for confluent-kafka
            metadata = self.consumer.list_topics(timeout=10)
            for topic in self.kafka_config.topics:
                if topic in metadata.topics:
                    for partition_id in metadata.topics[topic].partitions:
                        executor_key = f"{topic}:{partition_id}"
                        self.partition_executors[executor_key] = ThreadPoolExecutor(
                            max_workers=self.max_workers_per_partition,
                            thread_name_prefix=f"partition-{topic}-{partition_id}"
                        )
                        self.stats['partition_stats'][executor_key] = {
                            'processed': 0, 'failed': 0
                        }
        elif KAFKA_PYTHON_AVAILABLE:
            # Get partition information for kafka-python
            for topic in self.kafka_config.topics:
                partitions = self.consumer.partitions_for_topic(topic)
                if partitions:
                    for partition_id in partitions:
                        executor_key = f"{topic}:{partition_id}"
                        self.partition_executors[executor_key] = ThreadPoolExecutor(
                            max_workers=self.max_workers_per_partition,
                            thread_name_prefix=f"partition-{topic}-{partition_id}"
                        )
                        self.stats['partition_stats'][executor_key] = {
                            'processed': 0, 'failed': 0
                        }
        
        logger.info(f"Initialized {len(self.partition_executors)} partition executors")
    
    def _process_message(self, message, topic: str, partition: int, offset: int, 
                        key: Optional[str] = None, headers: Optional[Dict] = None) -> bool:
        """Process a single message with transaction management"""
        try:
            # Check if message already processed (idempotency)
            if self.db_manager.check_message_exists(topic, partition, offset):
                logger.debug(f"Message {topic}:{partition}:{offset} already processed, skipping")
                return True
            
            # Process message content
            if isinstance(message, bytes):
                message_value = message.decode('utf-8')
            else:
                message_value = str(message)
            
            # Create processed message record
            processed_msg = ProcessedMessage(
                topic=topic,
                partition=partition,
                offset=offset,
                message_key=key,
                message_value=message_value,
                message_headers=json.dumps(headers) if headers else None,
                consumer_group=self.kafka_config.group_id,
                processing_status='SUCCESS'
            )
            
            # Insert into database with transaction management
            success = self.db_manager.insert_messages_batch([processed_msg])
            
            if success:
                # Update statistics
                executor_key = f"{topic}:{partition}"
                self.stats['messages_processed'] += 1
                if executor_key in self.stats['partition_stats']:
                    self.stats['partition_stats'][executor_key]['processed'] += 1
                
                logger.debug(f"Successfully processed message {topic}:{partition}:{offset}")
                return True
            else:
                self.stats['messages_failed'] += 1
                if executor_key in self.stats['partition_stats']:
                    self.stats['partition_stats'][executor_key]['failed'] += 1
                return False
                
        except Exception as e:
            logger.error(f"Error processing message {topic}:{partition}:{offset}: {e}")
            self.stats['messages_failed'] += 1
            executor_key = f"{topic}:{partition}"
            if executor_key in self.stats['partition_stats']:
                self.stats['partition_stats'][executor_key]['failed'] += 1
            return False
    
    def _consume_confluent(self):
        """Consume messages using confluent-kafka"""
        logger.info("Starting message consumption with confluent-kafka")
        
        while self.running:
            try:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"End of partition reached {msg.topic()}:{msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Extract message information
                topic = msg.topic()
                partition = msg.partition()
                offset = msg.offset()
                key = msg.key().decode('utf-8') if msg.key() else None
                value = msg.value()
                headers = dict(msg.headers()) if msg.headers() else None
                
                # Submit to appropriate partition executor
                executor_key = f"{topic}:{partition}"
                if executor_key in self.partition_executors:
                    future = self.partition_executors[executor_key].submit(
                        self._process_message, value, topic, partition, offset, key, headers
                    )
                    
                    # Commit offset only after successful processing
                    def commit_on_success(fut):
                        try:
                            if fut.result():  # If processing was successful
                                self.consumer.commit(msg)
                        except Exception as e:
                            logger.error(f"Error in commit callback: {e}")
                    
                    future.add_done_callback(commit_on_success)
                
            except KafkaException as e:
                logger.error(f"Kafka exception: {e}")
                if not self.running:
                    break
            except Exception as e:
                logger.error(f"Unexpected error in consumer loop: {e}")
                time.sleep(1)
    
    def _consume_kafka_python(self):
        """Consume messages using kafka-python"""
        logger.info("Starting message consumption with kafka-python")
        
        while self.running:
            try:
                message_batch = self.consumer.poll(timeout_ms=1000, max_records=100)
                
                if not message_batch:
                    continue
                
                # Process messages by partition
                futures = []
                for topic_partition, messages in message_batch.items():
                    topic = topic_partition.topic
                    partition = topic_partition.partition
                    executor_key = f"{topic}:{partition}"
                    
                    if executor_key in self.partition_executors:
                        for message in messages:
                            key = message.key.decode('utf-8') if message.key else None
                            headers = dict(message.headers) if message.headers else None
                            
                            future = self.partition_executors[executor_key].submit(
                                self._process_message, message.value, topic, partition, 
                                message.offset, key, headers
                            )
                            futures.append((future, message))
                
                # Wait for all messages in batch to be processed before committing
                all_successful = True
                for future, message in futures:
                    try:
                        if not future.result(timeout=30):  # 30 second timeout
                            all_successful = False
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        all_successful = False
                
                # Commit offsets only if all messages were processed successfully
                if all_successful and not self.kafka_config.enable_auto_commit:
                    self.consumer.commit()
                
            except Exception as e:
                logger.error(f"Error in consumer loop: {e}")
                if not self.running:
                    break
                time.sleep(1)
    
    def start(self):
        """Start the consumer"""
        logger.info("Starting Kafka to SQL Server consumer")
        self.running = True
        self.stats['start_time'] = datetime.now(timezone.utc)
        
        try:
            # Setup consumer and partition executors
            self._setup_consumer()
            self._setup_partition_executors()
            
            # Start consumption based on available library
            if CONFLUENT_AVAILABLE and isinstance(self.consumer, ConfluentConsumer):
                self._consume_confluent()
            elif KAFKA_PYTHON_AVAILABLE:
                self._consume_kafka_python()
            
        except Exception as e:
            logger.error(f"Error starting consumer: {e}")
            raise
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Shutdown the consumer gracefully"""
        if not self.running:
            return
            
        logger.info("Shutting down consumer...")
        self.running = False
        
        # Close consumer
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
        
        # Shutdown partition executors
        for executor_key, executor in self.partition_executors.items():
            try:
                executor.shutdown(wait=True, timeout=30)
                logger.info(f"Executor {executor_key} shutdown complete")
            except Exception as e:
                logger.error(f"Error shutting down executor {executor_key}: {e}")
        
        # Close database connections
        self.db_manager.close()
        
        # Print final statistics
        self._print_final_stats()
        logger.info("Consumer shutdown complete")
    
    def _print_final_stats(self):
        """Print final processing statistics"""
        if self.stats['start_time']:
            runtime = datetime.now(timezone.utc) - self.stats['start_time']
            logger.info(f"Consumer Statistics:")
            logger.info(f"  Runtime: {runtime}")
            logger.info(f"  Messages Processed: {self.stats['messages_processed']}")
            logger.info(f"  Messages Failed: {self.stats['messages_failed']}")
            logger.info(f"  Success Rate: {(self.stats['messages_processed'] / (self.stats['messages_processed'] + self.stats['messages_failed']) * 100):.2f}%" if (self.stats['messages_processed'] + self.stats['messages_failed']) > 0 else "N/A")
            
            logger.info("  Partition Statistics:")
            for partition, stats in self.stats['partition_stats'].items():
                logger.info(f"    {partition}: Processed={stats['processed']}, Failed={stats['failed']}")


def main():
    """Main function to run the consumer"""
    # Load configuration from environment variables
    kafka_config = KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id=os.getenv("KAFKA_GROUP_ID", "kafka_sqlserver_consumer"),
        topics=os.getenv("KAFKA_TOPICS", "test-topic").split(","),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        enable_auto_commit=os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false").lower() == "true"
    )
    
    sqlserver_config = SQLServerConfig(
        server=os.getenv("SQLSERVER_HOST", "localhost"),
        port=int(os.getenv("SQLSERVER_PORT", "1433")),
        database=os.getenv("SQLSERVER_DATABASE", "kafka_messages"),
        username=os.getenv("SQLSERVER_USERNAME", "sa"),
        password=os.getenv("SQLSERVER_PASSWORD", "YourPassword123!"),
        driver=os.getenv("SQLSERVER_DRIVER", "auto"),
        table_name=os.getenv("SQLSERVER_TABLE", "kafka_messages")
    )
    
    max_workers_per_partition = int(os.getenv("MAX_WORKERS_PER_PARTITION", "2"))
    
    # Create and start consumer
    consumer = PartitionAwareKafkaConsumer(
        kafka_config=kafka_config,
        sqlserver_config=sqlserver_config,
        max_workers_per_partition=max_workers_per_partition
    )
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
