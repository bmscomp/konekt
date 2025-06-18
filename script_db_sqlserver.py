#!/usr/bin/env python3
"""
Partition-aware and resilient Kafka consumers with multi-threading support.
Includes implementations for both confluent-kafka and kafka-python libraries.
Uses SQL Server for message storage.
"""

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
from datetime import datetime, timezone, timedelta
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

# Check Python version for SQLAlchemy compatibility
PYTHON_VERSION = sys.version_info
ENABLE_DATABASE = os.getenv('ENABLE_DATABASE', 'true').lower() == 'true'

# SQLAlchemy has compatibility issues with Python 3.13
if PYTHON_VERSION >= (3, 13) and ENABLE_DATABASE:
    print("WARNING: SQLAlchemy is not compatible with Python 3.13+. Database functionality will be disabled.")
    print("To run without this warning, set environment variable ENABLE_DATABASE=false")
    ENABLE_DATABASE = False

# SQLAlchemy and SQL Server imports - conditional based on Python version
SQLALCHEMY_AVAILABLE = False
try:
    if ENABLE_DATABASE:
        import sqlalchemy
        from sqlalchemy import create_engine, text, Table, Column, String, DateTime, Text, Integer, MetaData
        from sqlalchemy.orm import sessionmaker, Session
        from sqlalchemy.exc import SQLAlchemyError
        import pyodbc
        SQLALCHEMY_AVAILABLE = True
except ImportError:
    print("SQLAlchemy or pyodbc not installed. Database functionality will be disabled.")
    ENABLE_DATABASE = False

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
    """Configuration for SQL Server database"""
    server: str = "localhost"
    port: int = 1433
    database: str = "kafka_messages"
    username: str = "sa"
    password: str = "Seeqwa1!Passw0rd"
    driver: str = "ODBC Driver 18 for SQL Server"
    table_name: str = "processed_messages"
    create_table_if_not_exists: bool = True
    trust_server_certificate: bool = True
    encrypt: bool = True
    connection_timeout: int = 30
    
    def get_connection_string(self) -> str:
        """Generate SQLAlchemy connection string"""
        conn_str = (
            f"mssql+pyodbc://{self.username}:{self.password}@{self.server}:{self.port}"
            f"/{self.database}?driver={self.driver.replace(' ', '+')}"
            f"&TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}"
            f"&encrypt={'yes' if self.encrypt else 'no'}"
            f"&connection+timeout={self.connection_timeout}"
        )
        return conn_str

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
    """Manages SQL Server database operations with transaction support"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.engine = None
        self.session_factory = None
        
        # Check if SQLAlchemy is available
        if not SQLALCHEMY_AVAILABLE:
            logger.warning("SQLAlchemy is not available. Database functionality will be disabled.")
            return
            
        self._setup_database()
    
    def _setup_database(self):
        """Initialize database connection and create tables if needed"""
        if not SQLALCHEMY_AVAILABLE:
            logger.warning("SQLAlchemy is not available. Skipping database setup.")
            return
            
        try:
            # Create SQL Server engine with connection pooling
            self.engine = create_engine(
                self.config.get_connection_string(),
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800
            )
            
            # Test connection
            logger.info(f"Testing connection to SQL Server at {self.config.server}:{self.config.port}")
            with self.engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                logger.info("SQL Server connection successful")
            
            # Create session factory
            self.session_factory = sessionmaker(bind=self.engine)
            
            # Create table if needed
            if self.config.create_table_if_not_exists:
                self._create_table_if_not_exists()
                
        except Exception as e:
            logger.error(f"Database setup error: {e}")
            logger.error(f"Connection failed to {self.config.server}:{self.config.port}/{self.config.database} as user {self.config.username}")
            self.engine = None
            self.session_factory = None
    
    def _create_table_if_not_exists(self):
        """Create the messages table and indexes if they don't exist"""
        if not SQLALCHEMY_AVAILABLE or not self.engine:
            return
            
        try:
            metadata = MetaData()
            
            # Define the processed_messages table
            Table(
                self.config.table_name, metadata,
                Column('id', String(36), primary_key=True),
                Column('topic', String(255), nullable=False),
                Column('partition', Integer, nullable=False),
                Column('offset', Integer, nullable=False),
                Column('message_key', String(255)),
                Column('message_value', Text, nullable=False),
                Column('message_headers', Text),
                Column('consumer_group', String(255), nullable=False),
                Column('processing_time', sqlalchemy.Float),
                Column('processing_status', String(50), nullable=False),
                Column('error_message', Text),
                Column('created_at', DateTime, nullable=False),
                Column('updated_at', DateTime, nullable=False)
            )
            
            metadata.create_all(self.engine)
            
            # Create indexes for better performance
            with self.get_session() as session:
                # Index for topic, partition, offset lookups
                session.execute(text(f"""
                    IF NOT EXISTS (
                        SELECT * FROM sys.indexes 
                        WHERE name = 'idx_{self.config.table_name}_topic_partition_offset'
                    )
                    CREATE INDEX idx_{self.config.table_name}_topic_partition_offset 
                    ON {self.config.table_name} (topic, partition, offset)
                """))
                
                # Index for status-based queries
                session.execute(text(f"""
                    IF NOT EXISTS (
                        SELECT * FROM sys.indexes 
                        WHERE name = 'idx_{self.config.table_name}_status'
                    )
                    CREATE INDEX idx_{self.config.table_name}_status 
                    ON {self.config.table_name} (processing_status)
                """))
                
                # Index for time-based queries
                session.execute(text(f"""
                    IF NOT EXISTS (
                        SELECT * FROM sys.indexes 
                        WHERE name = 'idx_{self.config.table_name}_created_at'
                    )
                    CREATE INDEX idx_{self.config.table_name}_created_at 
                    ON {self.config.table_name} (created_at)
                """))
                
                session.commit()
            
            logger.info(f"Database table {self.config.table_name} ready")
            
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions with automatic cleanup"""
        if not SQLALCHEMY_AVAILABLE or not self.session_factory:
            logger.warning("SQLAlchemy is not available. Cannot create database session.")
            yield None
            return
            
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
            
        if not SQLALCHEMY_AVAILABLE or not self.session_factory:
            # Log messages instead of storing them when database is not available
            logger.info(f"Database not available. Would have stored {len(messages)} messages.")
            for msg in messages:
                logger.debug(f"Message: topic={msg.topic}, partition={msg.partition}, offset={msg.offset}, status={msg.processing_status}")
            return
        
        with self.get_session() as session:
            if session is None:
                return
                
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
            except Exception as e:
                logger.error(f"Failed to store messages in database: {e}")
                session.rollback()
                # Continue execution even if database insertion fails
    
    def get_message_by_offset(self, topic: str, partition: int, offset: int) -> Optional[ProcessedMessage]:
        """Retrieve a processed message by topic, partition, and offset"""
        if not SQLALCHEMY_AVAILABLE or not self.session_factory:
            logger.warning("SQLAlchemy is not available. Cannot retrieve message from database.")
            return None
            
        with self.get_session() as session:
            if session is None:
                return None
                
            try:
                result = session.execute(
                    text(f"""
                        SELECT * FROM {self.config.table_name}
                        WHERE topic = :topic
                        AND partition = :partition
                        AND offset = :offset
                        ORDER BY created_at DESC
                        OFFSET 0 ROWS
                        FETCH NEXT 1 ROWS ONLY
                    """),
                    {'topic': topic, 'partition': partition, 'offset': offset}
                ).fetchone()
                
                if result:
                    return ProcessedMessage(
                        id=result.id,
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
                logger.info(f"Cleaned up old messages older than {days_to_keep} days")
                
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
                            SUM(CASE WHEN processing_status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
                            SUM(CASE WHEN processing_status = 'ERROR' THEN 1 ELSE 0 END) as failed
                        FROM {self.config.table_name}
                        WHERE created_at >= DATEADD(HOUR, -1, GETUTCDATE())
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

class BaseKafkaConsumer(ABC):
    """Base class for Kafka consumers with common functionality"""
    
    def __init__(self, config: ConsumerConfig, message_handler: Callable[[Any, Dict], bool]):
        self.config = config
        self.message_handler = message_handler
        self.running = False
        self.stats = {
            'messages_processed': 0,
            'messages_failed': 0,
            'processing_time': 0.0,
            'last_message_time': None,
            'partitions': {}
        }
        self.stats_lock = threading.Lock()
        self.workers = {}
        self.worker_queues = {}
        self.db_manager = None
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        if config.enable_database_storage and config.database_config:
            self.db_manager = DatabaseManager(config.database_config)
    
    def _signal_handler(self, sig, frame):
        """Handle termination signals"""
        logger.info(f"Received signal {sig}, shutting down...")
        self.shutdown()
    
    def update_stats(self, metadata: Dict, success: bool, processing_time: float):
        """Update processing statistics"""
        with self.stats_lock:
            if success:
                self.stats['messages_processed'] += 1
            else:
                self.stats['messages_failed'] += 1
            
            self.stats['processing_time'] += processing_time
            self.stats['last_message_time'] = datetime.now()
            
            partition_key = f"{metadata['topic']}:{metadata['partition']}"
            if partition_key not in self.stats['partitions']:
                self.stats['partitions'][partition_key] = {
                    'messages_processed': 0,
                    'last_offset': None
                }
            
            if success:
                self.stats['partitions'][partition_key]['messages_processed'] += 1
                self.stats['partitions'][partition_key]['last_offset'] = metadata['offset']
    
    def get_stats(self) -> Dict:
        """Get current processing statistics"""
        with self.stats_lock:
            stats_copy = self.stats.copy()
            if stats_copy['messages_processed'] > 0:
                stats_copy['avg_processing_time'] = stats_copy['processing_time'] / stats_copy['messages_processed']
            else:
                stats_copy['avg_processing_time'] = 0.0
            return stats_copy
    
    @abstractmethod
    def start(self):
        """Start the consumer"""
        pass
    
    @abstractmethod
    def shutdown(self):
        """Shutdown the consumer gracefully"""
        pass
    
    def process_message_with_retry(self, message: Any, metadata: Dict[str, Any]):
        """Process a message with retry logic and optional database storage"""
        start_time = time.time()
        processed_message = None
        
        if self.config.enable_database_storage:
            processed_message = ProcessedMessage(
                topic=metadata['topic'],
                partition=metadata['partition'],
                offset=metadata['offset'],
                message_key=metadata.get('key'),
                consumer_group=self.config.group_id
            )
            
            if isinstance(message, bytes):
                try:
                    # Try to decode and store as JSON
                    decoded = message.decode('utf-8')
                    processed_message.message_value = decoded
                except UnicodeDecodeError:
                    # If not valid UTF-8, store as base64
                    import base64
                    processed_message.message_value = base64.b64encode(message).decode('ascii')
                    processed_message.processing_status = 'BINARY'
            else:
                processed_message.message_value = str(message)
            
            if metadata.get('headers'):
                processed_message.message_headers = metadata['headers']
        
        for attempt in range(self.config.retry_count + 1):
            try:
                success = self.message_handler(message, metadata)
                if success:
                    if processed_message:
                        processed_message.processing_status = 'SUCCESS'
                        processed_message.processing_time = time.time() - start_time
                    break
                else:
                    logger.warning(f"Message handler returned False for message: {message}")
                    if processed_message:
                        processed_message.processing_status = 'HANDLER_REJECTED'
                        processed_message.error_message = 'Message handler returned False'
            except Exception as e:
                logger.error(f"Error processing message (attempt {attempt + 1}): {e}")
                if attempt < self.config.retry_count:
                    time.sleep(self.config.retry_interval)
                else:
                    # Final failure - store with error status
                    if processed_message:
                        processed_message.processing_status = 'ERROR'
                        processed_message.error_message = str(e)
                    success = False
        
        # Update statistics
        processing_time = time.time() - start_time
        self.update_stats(metadata, success, processing_time)
        
        # Store in database if enabled
        if self.config.enable_database_storage and processed_message:
            try:
                self.db_manager.insert_processed_messages([processed_message])
            except Exception as e:
                logger.error(f"Failed to store message in database: {e}")

class ConfluentKafkaConsumer(BaseKafkaConsumer):
    """Partition-aware consumer using confluent-kafka"""
    
    def __init__(self, config: ConsumerConfig, message_handler: Callable[[Any, Dict], bool]):
        super().__init__(config, message_handler)
        self.consumer = None
        self.admin_client = None
    
    def _create_consumer(self) -> ConfluentConsumer:
        """Create and configure confluent-kafka consumer"""
        conf = self.config.to_confluent_config()
        return ConfluentConsumer(conf)
    
    def start(self):
        """Start the Confluent Kafka consumer"""
        logger.info("Starting Confluent Kafka consumer...")
        self.running = True
        self.consumer = self._create_consumer()
        self.admin_client = AdminClient({'bootstrap.servers': self.config.bootstrap_servers})
        
        # Subscribe to topics
        self.consumer.subscribe(self.config.topics)
        
        while self.running:
            message = self.consumer.poll(timeout=1.0)
            if message is None:
                continue
            
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"Reached end of partition {message.topic()}/{message.partition()}")
                else:
                    logger.error(f"Kafka error: {message.error()}")
                continue
            
            metadata = {
                'topic': message.topic(),
                'partition': message.partition(),
                'offset': message.offset(),
                'key': message.key().decode('utf-8') if message.key() else None,
                'headers': dict(message.headers()) if message.headers() else None
            }
            
            self.process_message_with_retry(message.value(), metadata)
            
            # Manually commit offset after successful processing
            if not self.config.enable_auto_commit:
                self.consumer.commit(message, asynchronous=False)
    
    def shutdown(self):
        """Shutdown the consumer gracefully"""
        logger.info("Shutting down Confluent Kafka consumer...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            logger.info("Confluent Kafka consumer closed")
        
        if self.db_manager:
            self.db_manager.close()

class KafkaPythonConsumer(BaseKafkaConsumer):
    """Partition-aware consumer using kafka-python"""
    
    def __init__(self, config: ConsumerConfig, message_handler: Callable[[Any, Dict], bool]):
        super().__init__(config, message_handler)
        self.consumer = None
        self.executor = None
    
    def _create_consumer(self) -> KafkaConsumer:
        """Create and configure kafka-python consumer"""
        conf = self.config.to_kafka_python_config()
        return KafkaConsumer(*self.config.topics, **conf)
    
    def start(self):
        """Start the Kafka Python consumer"""
        logger.info("Starting Kafka Python consumer...")
        self.running = True
        self.consumer = self._create_consumer()
        self.executor = ThreadPoolExecutor(max_workers=self.config.max_workers)
        
        # Process messages
        for message in self.consumer:
            if not self.running:
                break
            
            metadata = {
                'topic': message.topic,
                'partition': message.partition,
                'offset': message.offset,
                'key': message.key.decode('utf-8') if message.key else None,
                'headers': dict(message.headers) if hasattr(message, 'headers') else None
            }
            
            self.process_message_with_retry(message.value, metadata)
            
            # Manually commit offset after successful processing
            if not self.config.enable_auto_commit:
                self.consumer.commit()
    
    def shutdown(self):
        """Shutdown the consumer gracefully"""
        logger.info("Shutting down Kafka Python consumer...")
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka Python consumer closed")
        
        if self.executor:
            self.executor.shutdown(wait=True)
            logger.info("Thread executor shut down")
        
        if self.db_manager:
            self.db_manager.close()

def run_confluent_consumer_example():
    """Example of running the Confluent Kafka consumer"""
    if not CONFLUENT_AVAILABLE:
        logger.error("Confluent Kafka not available")
        return
    
    # Setup database configuration
    db_config = DatabaseConfig(
        server=os.getenv("SQLSERVER_HOST", "localhost"),
        port=int(os.getenv("SQLSERVER_PORT", "1433")),
        database=os.getenv("SQLSERVER_DB", "kafka_messages"),
        username=os.getenv("SQLSERVER_USER", "sa"),
        password=os.getenv("SQLSERVER_PASSWORD", "Seeqwa1!Passw0rd"),
        driver=os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
        table_name="processed_messages",
        create_table_if_not_exists=True
    )
    
    # Setup consumer configuration
    consumer_config = ConsumerConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id="confluent-consumer-group",
        topics=["tmongo.pages_topic"],
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
            data = message
            
            # Create processed message record
            processed_msg = ProcessedMessage(
                topic=metadata['topic'],
                partition=metadata['partition'],
                offset=metadata['offset'],
                message_key=metadata.get('key'),
                message_value=data,
                message_headers=metadata.get('headers'),
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
        server=os.getenv("SQLSERVER_HOST", "localhost"),
        port=int(os.getenv("SQLSERVER_PORT", "1433")),
        database=os.getenv("SQLSERVER_DB", "kafka_messages"),
        username=os.getenv("SQLSERVER_USER", "sa"),
        password=os.getenv("SQLSERVER_PASSWORD", "Seeqwa1!Passw0rd"),
        driver=os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server"),
        table_name="processed_messages",
        create_table_if_not_exists=True
    )
    
    # Setup consumer configuration
    consumer_config = ConsumerConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        group_id="kafka-python-consumer-group",
        topics=["mongo.pages_topic"],
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
            data = message
            
            # Create processed message record
            processed_msg = ProcessedMessage(
                topic=metadata['topic'],
                partition=metadata['partition'],
                offset=metadata['offset'],
                message_key=metadata.get('key'),
                message_value=data,
                message_headers=metadata.get('headers'),
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
            print("Usage: python script_db_sqlserver.py [confluent|kafka-python]")
    else:
        print("Available consumer implementations:")
        print("- confluent: Uses confluent-kafka library")
        print("- kafka-python: Uses kafka-python library")
        print("\nUsage: python script_db_sqlserver.py [confluent|kafka-python]")
