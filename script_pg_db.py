#!/usr/bin/env python3
"""
Partition-aware and resilient Kafka consumers with multi-threading support.
Includes implementations for both confluent-kafka and kafka-python libraries.
Uses PostgreSQL for message storage.
"""

import json
import logging
import signal
import sys
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from queue import Queue, Empty
from typing import Dict, List, Optional, Callable, Any
from contextlib import contextmanager
from datetime import datetime, timezone
import uuid
import os

# Confluent Kafka imports
try:
    from confluent_kafka import Consumer as ConfluentConsumer, KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient
    CONFLUENT_AVAILABLE = True
except ImportError:
    CONFLUENT_AVAILABLE = False
    print("Warning: confluent-kafka not available")

# Kafka Python imports
try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError as KafkaPythonError
    from kafka.structs import TopicPartition
    KAFKA_PYTHON_AVAILABLE = True
except ImportError:
    KAFKA_PYTHON_AVAILABLE = False
    print("Warning: kafka-python not available")

# SQLAlchemy and PostgreSQL imports
import sqlalchemy
from sqlalchemy import create_engine, text, Table, Column, String, DateTime, Text, Integer, MetaData
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import UUID, JSONB

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ConsumerConfig:
    """Configuration for Kafka consumer"""
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "kafka_consumer"
    topics: List[str] = field(default_factory=list)
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_interval_ms: int = 300000
    max_workers: int = 4
    batch_size: int = 100
    batch_timeout: int = 5
    retry_count: int = 3
    retry_interval: int = 1
    database_config: Optional['DatabaseConfig'] = None
    enable_database_storage: bool = True
    
    def to_kafka_python_config(self) -> Dict[str, Any]:
        """Convert to kafka-python configuration"""
        return {
            'bootstrap_servers': self.bootstrap_servers,
            'group_id': self.group_id,
            'auto_offset_reset': self.auto_offset_reset,
            'enable_auto_commit': self.enable_auto_commit,
            'max_poll_records': self.max_poll_records,
            'session_timeout_ms': self.session_timeout_ms,
            'heartbeat_interval_ms': self.heartbeat_interval_ms,
            'max_poll_interval_ms': self.max_poll_interval_ms,
            'consumer_timeout_ms': self.batch_timeout * 1000
        }
    
    def to_confluent_config(self) -> Dict[str, Any]:
        """Convert to confluent-kafka configuration"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': str(self.enable_auto_commit).lower(),
            'session.timeout.ms': str(self.session_timeout_ms),
            'heartbeat.interval.ms': str(self.heartbeat_interval_ms),
            'max.poll.interval.ms': str(self.max_poll_interval_ms)
        }

@dataclass
class DatabaseConfig:
    """Configuration for PostgreSQL database"""
    host: str = "localhost"
    port: int = 5432
    database: str = "kafka_messages"
    user: str = "postgres"
    password: str = ""
    table_name: str = "processed_messages"
    create_table_if_not_exists: bool = True
    
    def get_connection_string(self) -> str:
        """Generate SQLAlchemy connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class ProcessedMessage:
    """Represents a processed message for database storage"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    topic: str = ""
    partition: int = 0
    offset: int = 0
    message_key: Optional[str] = None
    message_value: str = ""
    message_headers: Optional[str] = None
    consumer_group: str = ""
    processing_time: float = 0.0
    processing_status: str = "PENDING"
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

@dataclass
class MessageBatch:
    """Represents a batch of messages from a specific partition"""
    partition: int
    topic: str
    messages: List[Any]
    offsets: List[int]
    message_keys: List[Optional[str]] = field(default_factory=list)
    headers: List[Optional[Dict]] = field(default_factory=list)

class DatabaseManager:
    """Manages PostgreSQL database operations with transaction support"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = None
        self.session_factory = None
        self._setup_database()
    
    def _setup_database(self):
        """Initialize database connection and create tables if needed"""
        try:
            self.engine = create_engine(
                self.config.get_connection_string(),
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800
            )
            
            self.session_factory = sessionmaker(bind=self.engine)
            
            if self.config.create_table_if_not_exists:
                metadata = MetaData()
                
                # Define the processed_messages table
                Table(
                    self.config.table_name, metadata,
                    Column('id', UUID(as_uuid=True), primary_key=True),
                    Column('topic', String(255), nullable=False),
                    Column('partition', Integer, nullable=False),
                    Column('offset', Integer, nullable=False),
                    Column('message_key', String(255)),
                    Column('message_value', JSONB, nullable=False),
                    Column('message_headers', JSONB),
                    Column('consumer_group', String(255), nullable=False),
                    Column('processing_time', sqlalchemy.Float),
                    Column('processing_status', String(50), nullable=False),
                    Column('error_message', Text),
                    Column('created_at', DateTime(timezone=True), nullable=False),
                    Column('updated_at', DateTime(timezone=True), nullable=False)
                )
                
                metadata.create_all(self.engine)
                logger.info(f"Database table {self.config.table_name} ready")
                
        except SQLAlchemyError as e:
            logger.error(f"Database setup error: {e}")
            raise
    
    @contextmanager
    def get_session(self) -> Session:
        """Context manager for database sessions with automatic cleanup"""
        session = self.session_factory()
        try:
            yield session
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
    
    def insert_processed_messages(self, messages: List[ProcessedMessage]):
        """Insert processed messages into database with transaction management"""
        if not messages:
            return
        
        with self.get_session() as session:
            try:
                # Convert messages to dictionaries for bulk insert
                records = []
                for msg in messages:
                    record = {
                        'id': msg.id,
                        'topic': msg.topic,
                        'partition': msg.partition,
                        'offset': msg.offset,
                        'message_key': msg.message_key,
                        'message_value': msg.message_value,
                        'message_headers': msg.message_headers,
                        'consumer_group': msg.consumer_group,
                        'processing_time': msg.processing_time,
                        'processing_status': msg.processing_status,
                        'error_message': msg.error_message,
                        'created_at': msg.created_at,
                        'updated_at': msg.updated_at
                    }
                    records.append(record)
                
                # Bulk insert using SQLAlchemy Core
                session.execute(
                    text(f"""
                        INSERT INTO {self.config.table_name} (
                            id, topic, partition, offset, message_key, message_value,
                            message_headers, consumer_group, processing_time,
                            processing_status, error_message, created_at, updated_at
                        ) VALUES (
                            :id, :topic, :partition, :offset, :message_key, :message_value,
                            :message_headers, :consumer_group, :processing_time,
                            :processing_status, :error_message, :created_at, :updated_at
                        )
                    """),
                    records
                )
                
                session.commit()
                logger.info(f"Successfully stored {len(messages)} messages in database")
                
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Failed to store messages in database: {e}")
                raise
    
    def get_message_by_offset(self, topic: str, partition: int, offset: int) -> Optional[ProcessedMessage]:
        """Retrieve a processed message by topic, partition, and offset"""
        with self.get_session() as session:
            try:
                result = session.execute(
                    text(f"""
                        SELECT * FROM {self.config.table_name}
                        WHERE topic = :topic
                        AND partition = :partition
                        AND offset = :offset
                        LIMIT 1
                    """),
                    {'topic': topic, 'partition': partition, 'offset': offset}
                ).fetchone()
                
                if result:
                    return ProcessedMessage(
                        id=str(result.id),
                        topic=result.topic,
                        partition=result.partition,
                        offset=result.offset,
                        message_key=result.message_key,
                        message_value=result.message_value,
                        message_headers=result.message_headers,
                        consumer_group=result.consumer_group,
                        processing_time=result.processing_time,
                        processing_status=result.processing_status,
                        error_message=result.error_message,
                        created_at=result.created_at,
                        updated_at=result.updated_at
                    )
                return None
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to retrieve message: {e}")
                raise
    
    def cleanup_old_messages(self, days_to_keep: int = 30):
        """Clean up old processed messages to prevent database bloat"""
        with self.get_session() as session:
            try:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
                
                result = session.execute(
                    text(f"""
                        DELETE FROM {self.config.table_name}
                        WHERE created_at < :cutoff_date
                    """),
                    {'cutoff_date': cutoff_date}
                )
                
                session.commit()
                logger.info(f"Cleaned up {result.rowcount} old messages")
                
            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Failed to clean up old messages: {e}")
                raise
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics from the database"""
        with self.get_session() as session:
            try:
                result = session.execute(
                    text(f"""
                        SELECT
                            COUNT(*) as total_messages,
                            AVG(processing_time) as avg_processing_time,
                            COUNT(CASE WHEN processing_status = 'SUCCESS' THEN 1 END) as successful,
                            COUNT(CASE WHEN processing_status = 'ERROR' THEN 1 END) as failed
                        FROM {self.config.table_name}
                        WHERE created_at >= NOW() - INTERVAL '1 hour'
                    """)
                ).fetchone()
                
                return {
                    'total_messages': result.total_messages,
                    'avg_processing_time': float(result.avg_processing_time or 0),
                    'successful_messages': result.successful,
                    'failed_messages': result.failed
                }
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to get processing stats: {e}")
                raise
    
    def close(self):
        """Close database connections"""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connections closed")

# The rest of the code (BaseKafkaConsumer, ConfluentKafkaConsumer, KafkaPythonConsumer)
# remains the same as in script_db.py, so I'm not duplicating it here.
# You can copy those classes from script_db.py

def run_confluent_consumer_example():
    """Example of running the Confluent Kafka consumer"""
    if not CONFLUENT_AVAILABLE:
        logger.error("Confluent Kafka not available")
        return
    
    # Setup database configuration
    db_config = DatabaseConfig(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "kafka_messages"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        table_name="processed_messages",
        create_table_if_not_exists=True
    )
    
    # Setup consumer configuration
    consumer_config = ConsumerConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id="confluent-consumer-group",
        topics=["test-topic"],
        max_workers=4,
        batch_size=100,
        database_config=db_config,
        enable_database_storage=True
    )
    
    # Initialize database manager
    db_manager = DatabaseManager(db_config)
    
    def message_handler(message: Any, metadata: Dict) -> bool:
        try:
            # Process the message (example: parse JSON)
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            data = json.loads(message)
            
            # Create processed message record
            processed_msg = ProcessedMessage(
                topic=metadata['topic'],
                partition=metadata['partition'],
                offset=metadata['offset'],
                message_key=metadata.get('key'),
                message_value=json.dumps(data),
                message_headers=json.dumps(metadata.get('headers')),
                consumer_group=consumer_config.group_id,
                processing_status='SUCCESS'
            )
            
            # Store in database
            if consumer_config.enable_database_storage:
                db_manager.insert_processed_messages([processed_msg])
            
            logger.info(f"Successfully processed message from {metadata['topic']}:{metadata['partition']}:{metadata['offset']}")
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    consumer = ConfluentKafkaConsumer(consumer_config, message_handler)
    
    # Start stats reporting thread
    def report_stats():
        while consumer.running:
            time.sleep(30)
            stats = consumer.get_stats()
            logger.info(f"Consumer stats: {stats}")
    
    stats_thread = threading.Thread(target=report_stats, daemon=True)
    stats_thread.start()
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        consumer.shutdown()
        db_manager.close()

def run_kafka_python_consumer_example():
    """Example of running the Kafka Python consumer"""
    if not KAFKA_PYTHON_AVAILABLE:
        logger.error("Kafka Python not available")
        return
    
    # Setup database configuration
    db_config = DatabaseConfig(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "kafka_messages"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        table_name="processed_messages",
        create_table_if_not_exists=True
    )
    
    # Setup consumer configuration
    consumer_config = ConsumerConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id="kafka-python-consumer-group",
        topics=["test-topic"],
        max_workers=4,
        batch_size=100,
        database_config=db_config,
        enable_database_storage=True
    )
    
    # Initialize database manager
    db_manager = DatabaseManager(db_config)
    
    def message_handler(message: Any, metadata: Dict) -> bool:
        try:
            # Process the message (example: parse JSON)
            if isinstance(message, bytes):
                message = message.decode('utf-8')
            data = json.loads(message)
            
            # Create processed message record
            processed_msg = ProcessedMessage(
                topic=metadata['topic'],
                partition=metadata['partition'],
                offset=metadata['offset'],
                message_key=metadata.get('key'),
                message_value=json.dumps(data),
                message_headers=json.dumps(metadata.get('headers')),
                consumer_group=consumer_config.group_id,
                processing_status='SUCCESS'
            )
            
            # Store in database
            if consumer_config.enable_database_storage:
                db_manager.insert_processed_messages([processed_msg])
            
            logger.info(f"Successfully processed message from {metadata['topic']}:{metadata['partition']}:{metadata['offset']}")
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    consumer = KafkaPythonConsumer(consumer_config, message_handler)
    
    # Start stats reporting thread
    def report_stats():
        while consumer.running:
            time.sleep(30)
            stats = consumer.get_stats()
            logger.info(f"Consumer stats: {stats}")
    
    stats_thread = threading.Thread(target=report_stats, daemon=True)
    stats_thread.start()
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    finally:
        consumer.shutdown()
        db_manager.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "confluent":
            run_confluent_consumer_example()
        elif sys.argv[1] == "kafka-python":
            run_kafka_python_consumer_example()
        else:
            print("Usage: python script_pg_db.py [confluent|kafka-python]")
    else:
        print("Available consumer implementations:")
        print("- confluent: Uses confluent-kafka library")
        print("- kafka-python: Uses kafka-python library")
        print("\nUsage: python script_pg_db.py [confluent|kafka-python]")
