#!/usr/bin/env python3
"""
Partition-aware and resilient Kafka consumers with multi-threading support.
Includes implementations for both confluent-kafka and kafka-python libraries.
"""

import json
import logging
import signal
import sys
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from queue import Queue, Empty
from typing import Dict, List, Optional, Callable, Any
from contextlib import contextmanager
from datetime import datetime, timezone
import uuid

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

# MS SQL Server imports
try:
    import pyodbc
    import sqlalchemy
    from sqlalchemy import create_engine, text, Table, Column, String, DateTime, Text, Integer, MetaData
    from sqlalchemy.orm import sessionmaker, Session
    from sqlalchemy.exc import SQLAlchemyError
    MSSQL_AVAILABLE = True
except ImportError:
    MSSQL_AVAILABLE = False
    print("Warning: MS SQL Server dependencies not available")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class DatabaseConfig:
    """Configuration for MS SQL Server database"""
    server: str = "localhost"
    database: str = "kafka_messages"
    username: Optional[str] = None
    password: Optional[str] = None
    driver: str = "ODBC Driver 17 for SQL Server"
    trusted_connection: bool = True
    connection_timeout: int = 30
    command_timeout: int = 30
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600
    
    # Table configuration
    table_name: str = "processed_messages"
    schema: str = "dbo"
    create_table_if_not_exists: bool = True
    
    def get_connection_string(self) -> str:
        """Generate SQLAlchemy connection string"""
        if self.trusted_connection:
            return (
                f"mssql+pyodbc://@{self.server}/{self.database}"
                f"?driver={self.driver.replace(' ', '+')}"
                f"&trusted_connection=yes"
                f"&timeout={self.connection_timeout}"
            )
        else:
            return (
                f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}"
                f"?driver={self.driver.replace(' ', '+')}"
                f"&timeout={self.connection_timeout}"
            )

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
    processed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    consumer_group: str = ""
    processing_status: str = "SUCCESS"
    error_message: Optional[str] = None
    retry_count: int = 0
    """Configuration for Kafka consumers"""
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "default-group"
    topics: List[str] = field(default_factory=list)
    max_workers: int = 4
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_records: int = 500
    max_poll_interval_ms: int = 300000
    retry_backoff_ms: int = 1000
    max_retries: int = 3
    
    # Partition assignment strategy
    partition_assignment_strategy: str = "range"
    
    # Custom processing configuration
    batch_size: int = 10
    processing_timeout: int = 30
    
    # Database configuration
    database_config: Optional[DatabaseConfig] = None
    enable_database_storage: bool = False
    
    def to_confluent_config(self) -> Dict[str, Any]:
        """Convert to confluent-kafka configuration"""
        return {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit,
            'session.timeout.ms': self.session_timeout_ms,
            'heartbeat.interval.ms': self.heartbeat_interval_ms,
            'max.poll.interval.ms': self.max_poll_interval_ms,
            'partition.assignment.strategy': self.partition_assignment_strategy,
        }
    
    def to_kafka_python_config(self) -> Dict[str, Any]:
        """Convert to kafka-python configuration"""
        return {
            'bootstrap_servers': self.bootstrap_servers.split(','),
            'group_id': self.group_id,
            'auto_offset_reset': self.auto_offset_reset,
            'enable_auto_commit': self.enable_auto_commit,
            'session_timeout_ms': self.session_timeout_ms,
            'heartbeat_interval_ms': self.heartbeat_interval_ms,
            'max_poll_records': self.max_poll_records,
            'max_poll_interval_ms': self.max_poll_interval_ms,
        }

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
    """Manages MS SQL Server database operations with transaction support"""
    
    def __init__(self, config: DatabaseConfig):
        if not MSSQL_AVAILABLE:
            raise ImportError("MS SQL Server dependencies not available")
        
        self.config = config
        self.engine = None
        self.session_factory = None
        self.metadata = MetaData()
        self.processed_messages_table = None
        self._setup_database()
    
    def _setup_database(self):
        """Initialize database connection and create tables if needed"""
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                self.config.get_connection_string(),
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_timeout=self.config.pool_timeout,
                pool_recycle=self.config.pool_recycle,
                echo=False  # Set to True for SQL debugging
            )
            
            # Create session factory
            self.session_factory = sessionmaker(bind=self.engine)
            
            # Define table schema
            self.processed_messages_table = Table(
                self.config.table_name,
                self.metadata,
                Column('id', String(36), primary_key=True),
                Column('topic', String(255), nullable=False),
                Column('partition', Integer, nullable=False),
                Column('offset', Integer, nullable=False),
                Column('message_key', String(500), nullable=True),
                Column('message_value', Text, nullable=False),
                Column('message_headers', Text, nullable=True),
                Column('processed_at', DateTime, nullable=False),
                Column('consumer_group', String(255), nullable=False),
                Column('processing_status', String(50), nullable=False),
                Column('error_message', Text, nullable=True),
                Column('retry_count', Integer, default=0),
                schema=self.config.schema
            )
            
            # Create table if it doesn't exist
            if self.config.create_table_if_not_exists:
                self.metadata.create_all(self.engine)
                logger.info(f"Database table {self.config.schema}.{self.config.table_name} ready")
            
        except Exception as e:
            logger.error(f"Failed to setup database: {e}")
            raise
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions with automatic cleanup"""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def insert_processed_messages(self, messages: List[ProcessedMessage]) -> bool:
        """Insert processed messages into database with transaction management"""
        if not messages:
            return True
        
        try:
            with self.get_session() as session:
                # Prepare data for bulk insert
                message_data = []
                for msg in messages:
                    message_data.append({
                        'id': msg.id,
                        'topic': msg.topic,
                        'partition': msg.partition,
                        'offset': msg.offset,
                        'message_key': msg.message_key,
                        'message_value': msg.message_value,
                        'message_headers': msg.message_headers,
                        'processed_at': msg.processed_at,
                        'consumer_group': msg.consumer_group,
                        'processing_status': msg.processing_status,
                        'error_message': msg.error_message,
                        'retry_count': msg.retry_count
                    })
                
                # Bulk insert using SQLAlchemy Core for better performance
                session.execute(self.processed_messages_table.insert(), message_data)
                session.commit()
                
                logger.debug(f"Successfully inserted {len(messages)} messages into database")
                return True
                
        except SQLAlchemyError as e:
            logger.error(f"Database error inserting messages: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error inserting messages: {e}")
            return False
    
    def get_message_by_offset(self, topic: str, partition: int, offset: int) -> Optional[ProcessedMessage]:
        """Retrieve a processed message by topic, partition, and offset"""
        try:
            with self.get_session() as session:
                result = session.execute(
                    text(f"""
                        SELECT * FROM {self.config.schema}.{self.config.table_name}
                        WHERE topic = :topic AND partition = :partition AND offset = :offset
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
                        processed_at=result.processed_at,
                        consumer_group=result.consumer_group,
                        processing_status=result.processing_status,
                        error_message=result.error_message,
                        retry_count=result.retry_count
                    )
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving message: {e}")
            return None
    
    def cleanup_old_messages(self, days_to_keep: int = 30) -> int:
        """Clean up old processed messages to prevent database bloat"""
        try:
            with self.get_session() as session:
                cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
                
                result = session.execute(
                    text(f"""
                        DELETE FROM {self.config.schema}.{self.config.table_name}
                        WHERE processed_at < :cutoff_date
                    """),
                    {'cutoff_date': cutoff_date}
                )
                
                deleted_count = result.rowcount
                session.commit()
                
                logger.info(f"Cleaned up {deleted_count} old messages")
                return deleted_count
                
        except Exception as e:
            logger.error(f"Error cleaning up old messages: {e}")
            return 0
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics from the database"""
        try:
            with self.get_session() as session:
                result = session.execute(
                    text(f"""
                        SELECT 
                            processing_status,
                            COUNT(*) as count,
                            AVG(retry_count) as avg_retries
                        FROM {self.config.schema}.{self.config.table_name}
                        WHERE processed_at >= DATEADD(day, -1, GETDATE())
                        GROUP BY processing_status
                    """)
                ).fetchall()
                
                stats = {}
                for row in result:
                    stats[row.processing_status] = {
                        'count': row.count,
                        'avg_retries': float(row.avg_retries) if row.avg_retries else 0
                    }
                
                return stats
                
        except Exception as e:
            logger.error(f"Error getting processing stats: {e}")
            return {}
    
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
        self.shutdown_event = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=config.max_workers)
        self.partition_queues: Dict[int, Queue] = {}
        self.partition_threads: Dict[int, threading.Thread] = {}
        self.stats = {
            'messages_processed': 0,
            'messages_failed': 0,
            'messages_stored': 0,
            'database_errors': 0,
            'partitions_assigned': 0,
            'last_activity': time.time()
        }
        self.stats_lock = threading.Lock()
        
        # Database manager
        self.db_manager = None
        if config.enable_database_storage and config.database_config:
            if not MSSQL_AVAILABLE:
                raise ImportError("MS SQL Server dependencies required for database storage")
            self.db_manager = DatabaseManager(config.database_config)
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown()
    
    def update_stats(self, processed: int = 0, failed: int = 0, stored: int = 0, db_errors: int = 0):
        """Thread-safe stats update"""
        with self.stats_lock:
            self.stats['messages_processed'] += processed
            self.stats['messages_failed'] += failed
            self.stats['messages_stored'] += stored
            self.stats['database_errors'] += db_errors
            self.stats['last_activity'] = time.time()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current consumer statistics"""
        with self.stats_lock:
            return self.stats.copy()
    
    @abstractmethod
    def start(self):
        """Start the consumer"""
        pass
    
    @abstractmethod
    def shutdown(self):
        """Shutdown the consumer gracefully"""
        pass
    
    def process_message_with_retry(self, message: Any, metadata: Dict[str, Any]) -> bool:
        """Process a message with retry logic and optional database storage"""
        processed_msg = None
        
        # Create ProcessedMessage object if database storage is enabled
        if self.config.enable_database_storage and self.db_manager:
            processed_msg = ProcessedMessage(
                topic=metadata.get('topic', ''),
                partition=metadata.get('partition', 0),
                offset=metadata.get('offset', 0),
                message_key=metadata.get('key'),
                message_value=str(message) if message is not None else '',
                message_headers=json.dumps(metadata.get('headers', {})) if metadata.get('headers') else None,
                consumer_group=self.config.group_id,
                processing_status='PROCESSING'
            )
        
        for attempt in range(self.config.max_retries + 1):
            try:
                success = self.message_handler(message, metadata)
                if success:
                    # Update processed message status
                    if processed_msg:
                        processed_msg.processing_status = 'SUCCESS'
                        processed_msg.retry_count = attempt
                        
                        # Store in database
                        if self.db_manager.insert_processed_messages([processed_msg]):
                            self.update_stats(processed=1, stored=1)
                        else:
                            self.update_stats(processed=1, db_errors=1)
                    else:
                        self.update_stats(processed=1)
                    
                    return True
                else:
                    logger.warning(f"Message handler returned False for message: {message}")
                    
            except Exception as e:
                logger.error(f"Error processing message (attempt {attempt + 1}): {e}")
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_backoff_ms / 1000.0)
                else:
                    # Final failure - store with error status
                    if processed_msg:
                        processed_msg.processing_status = 'FAILED'
                        processed_msg.error_message = str(e)
                        processed_msg.retry_count = attempt + 1
                        
                        if self.db_manager.insert_processed_messages([processed_msg]):
                            self.update_stats(failed=1, stored=1)
                        else:
                            self.update_stats(failed=1, db_errors=1)
                    else:
                        self.update_stats(failed=1)
                    
                    return False
        
        return False

class ConfluentKafkaConsumer(BaseKafkaConsumer):
    """Partition-aware consumer using confluent-kafka"""
    
    def __init__(self, config: ConsumerConfig, message_handler: Callable[[Any, Dict], bool]):
        if not CONFLUENT_AVAILABLE:
            raise ImportError("confluent-kafka is not available")
        
        super().__init__(config, message_handler)
        self.consumer = None
        self.admin_client = None
        
    def _create_consumer(self) -> ConfluentConsumer:
        """Create and configure confluent-kafka consumer"""
        confluent_config = self.config.to_confluent_config()
        consumer = ConfluentConsumer(confluent_config)
        
        # Set up partition assignment callbacks
        consumer.subscribe(
            self.config.topics,
            on_assign=self._on_assign,
            on_revoke=self._on_revoke
        )
        
        return consumer
    
    def _on_assign(self, consumer, partitions):
        """Callback when partitions are assigned"""
        logger.info(f"Partitions assigned: {[p.partition for p in partitions]}")
        
        with self.stats_lock:
            self.stats['partitions_assigned'] = len(partitions)
        
        # Create queues and worker threads for each partition
        for partition in partitions:
            partition_id = partition.partition
            if partition_id not in self.partition_queues:
                self.partition_queues[partition_id] = Queue(maxsize=1000)
                
                # Start partition-specific worker thread
                worker_thread = threading.Thread(
                    target=self._partition_worker,
                    args=(partition_id,),
                    name=f"partition-{partition_id}-worker"
                )
                worker_thread.daemon = True
                worker_thread.start()
                self.partition_threads[partition_id] = worker_thread
        
        # Commit the assignment
        consumer.assign(partitions)
    
    def _on_revoke(self, consumer, partitions):
        """Callback when partitions are revoked"""
        logger.info(f"Partitions revoked: {[p.partition for p in partitions]}")
        
        # Stop partition workers
        for partition in partitions:
            partition_id = partition.partition
            if partition_id in self.partition_queues:
                # Signal worker to stop by putting None
                self.partition_queues[partition_id].put(None)
                
                # Wait for worker to finish
                if partition_id in self.partition_threads:
                    self.partition_threads[partition_id].join(timeout=5)
                    del self.partition_threads[partition_id]
                
                del self.partition_queues[partition_id]
    
    def _partition_worker(self, partition_id: int):
        """Worker thread for processing messages from a specific partition"""
        logger.info(f"Started worker for partition {partition_id}")
        queue = self.partition_queues[partition_id]
        
        while not self.shutdown_event.is_set():
            try:
                batch = queue.get(timeout=1.0)
                if batch is None:  # Shutdown signal
                    break
                
                # Process batch of messages
                self._process_batch(batch)
                
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in partition {partition_id} worker: {e}")
        
        logger.info(f"Stopped worker for partition {partition_id}")
    
    def _process_batch(self, batch: MessageBatch):
        """Process a batch of messages with database transaction support"""
        successful_offsets = []
        processed_messages = []
        
        for i, (message, offset) in enumerate(zip(batch.messages, batch.offsets)):
            try:
                # Prepare metadata for message handler
                metadata = {
                    'topic': batch.topic,
                    'partition': batch.partition,
                    'offset': offset,
                    'key': batch.message_keys[i] if i < len(batch.message_keys) else None,
                    'headers': batch.headers[i] if i < len(batch.headers) else None
                }
                
                if self.process_message_with_retry(message, metadata):
                    successful_offsets.append(offset)
                else:
                    logger.error(f"Failed to process message at offset {offset}")
                    
            except Exception as e:
                logger.error(f"Unexpected error processing message at offset {offset}: {e}")
        
        # Commit successful offsets
        if successful_offsets and self.consumer:
            try:
                # Commit the highest successful offset
                max_offset = max(successful_offsets)
                tp = TopicPartition(batch.topic, batch.partition, max_offset + 1)
                self.consumer.commit(offsets=[tp], asynchronous=False)
                logger.debug(f"Committed offset {max_offset + 1} for partition {batch.partition}")
                
            except Exception as e:
                logger.error(f"Failed to commit offsets: {e}")
    
    def start(self):
        """Start the confluent-kafka consumer"""
        logger.info("Starting Confluent Kafka consumer...")
        self.running = True
        
        try:
            self.consumer = self._create_consumer()
            self.admin_client = AdminClient(self.config.to_confluent_config())
            
            # Main consumption loop
            batch_messages = {}
            batch_offsets = {}
            batch_keys = {}
            batch_headers = {}
            
            while self.running and not self.shutdown_event.is_set():
                try:
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        # Process any pending batches
                        self._flush_batches(batch_messages, batch_offsets, batch_keys, batch_headers)
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            logger.debug(f"Reached end of partition {msg.partition()}")
                        else:
                            logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    # Add message to batch
                    partition = msg.partition()
                    if partition not in batch_messages:
                        batch_messages[partition] = []
                        batch_offsets[partition] = []
                        batch_keys[partition] = []
                        batch_headers[partition] = []
                    
                    batch_messages[partition].append(msg.value())
                    batch_offsets[partition].append(msg.offset())
                    batch_keys[partition].append(msg.key().decode('utf-8') if msg.key() else None)
                    batch_headers[partition].append(dict(msg.headers()) if msg.headers() else None)
                    
                    # Send batch when it reaches the configured size
                    if len(batch_messages[partition]) >= self.config.batch_size:
                        self._send_batch_to_worker(
                            partition, msg.topic(), 
                            batch_messages[partition], 
                            batch_offsets[partition],
                            batch_keys[partition],
                            batch_headers[partition]
                        )
                        batch_messages[partition] = []
                        batch_offsets[partition] = []
                        batch_keys[partition] = []
                        batch_headers[partition] = []_messages[partition] = []
                        batch_offsets[partition] = []
                
                except KafkaException as e:
                    logger.error(f"Kafka exception: {e}")
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    time.sleep(1)
            
            # Process final batches
            self._flush_batches(batch_messages, batch_offsets)
            
        except Exception as e:
            logger.error(f"Fatal error in consumer: {e}")
        finally:
            self.shutdown()
    
    def _flush_batches(self, batch_messages: Dict, batch_offsets: Dict):
        """Send any remaining batches to workers"""
        for partition in list(batch_messages.keys()):
            if batch_messages[partition]:
                self._send_batch_to_worker(
                    partition, self.config.topics[0],  # Assuming single topic for simplicity
                    batch_messages[partition],
                    batch_offsets[partition]
                )
                batch_messages[partition] = []
                batch_offsets[partition] = []
    
    def _send_batch_to_worker(self, partition: int, topic: str, messages: List, offsets: List):
        """Send a batch to the appropriate partition worker"""
        if partition in self.partition_queues:
            batch = MessageBatch(partition, topic, messages.copy(), offsets.copy())
            try:
                self.partition_queues[partition].put(batch, timeout=5.0)
            except Exception as e:
                logger.error(f"Failed to queue batch for partition {partition}: {e}")
    
    def shutdown(self):
        """Shutdown the consumer gracefully"""
        if not self.running:
            return
        
        logger.info("Shutting down Confluent Kafka consumer...")
        self.running = False
        self.shutdown_event.set()
        
        # Stop all partition workers
        for partition_id, queue in self.partition_queues.items():
            queue.put(None)  # Signal shutdown
        
        # Wait for workers to finish
        for thread in self.partition_threads.values():
            thread.join(timeout=5)
        
        # Close consumer
        if self.consumer:
            self.consumer.close()
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        logger.info("Confluent Kafka consumer shutdown complete")

class KafkaPythonConsumer(BaseKafkaConsumer):
    """Partition-aware consumer using kafka-python"""
    
    def __init__(self, config: ConsumerConfig, message_handler: Callable[[Any], bool]):
        if not KAFKA_PYTHON_AVAILABLE:
            raise ImportError("kafka-python is not available")
        
        super().__init__(config, message_handler)
        self.consumer = None
        self.current_partitions = set()
    
    def _create_consumer(self) -> KafkaConsumer:
        """Create and configure kafka-python consumer"""
        kafka_config = self.config.to_kafka_python_config()
        
        consumer = KafkaConsumer(
            *self.config.topics,
            **kafka_config,
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        return consumer
    
    def _setup_partition_workers(self):
        """Setup workers for currently assigned partitions"""
        if not self.consumer:
            return
        
        # Get current partition assignment
        partitions = self.consumer.assignment()
        new_partitions = {tp.partition for tp in partitions}
        
        # Stop workers for revoked partitions
        revoked_partitions = self.current_partitions - new_partitions
        for partition_id in revoked_partitions:
            if partition_id in self.partition_queues:
                self.partition_queues[partition_id].put(None)
                if partition_id in self.partition_threads:
                    self.partition_threads[partition_id].join(timeout=5)
                    del self.partition_threads[partition_id]
                del self.partition_queues[partition_id]
        
        # Start workers for new partitions
        assigned_partitions = new_partitions - self.current_partitions
        for partition_id in assigned_partitions:
            self.partition_queues[partition_id] = Queue(maxsize=1000)
            
            worker_thread = threading.Thread(
                target=self._partition_worker,
                args=(partition_id,),
                name=f"partition-{partition_id}-worker"
            )
            worker_thread.daemon = True
            worker_thread.start()
            self.partition_threads[partition_id] = worker_thread
        
        self.current_partitions = new_partitions
        
        with self.stats_lock:
            self.stats['partitions_assigned'] = len(new_partitions)
        
        if assigned_partitions or revoked_partitions:
            logger.info(f"Partition assignment changed. Current: {sorted(new_partitions)}")
    
    def _partition_worker(self, partition_id: int):
        """Worker thread for processing messages from a specific partition"""
        logger.info(f"Started worker for partition {partition_id}")
        queue = self.partition_queues[partition_id]
        
        while not self.shutdown_event.is_set():
            try:
                batch = queue.get(timeout=1.0)
                if batch is None:  # Shutdown signal
                    break
                
                # Process batch of messages
                self._process_batch(batch)
                
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in partition {partition_id} worker: {e}")
        
        logger.info(f"Stopped worker for partition {partition_id}")
    
    def _process_batch(self, batch: MessageBatch):
        """Process a batch of messages"""
        successful_offsets = []
        
        for message, offset in zip(batch.messages, batch.offsets):
            try:
                if self.process_message_with_retry(message):
                    successful_offsets.append(offset)
                else:
                    logger.error(f"Failed to process message at offset {offset}")
                    
            except Exception as e:
                logger.error(f"Unexpected error processing message at offset {offset}: {e}")
        
        # Commit successful offsets
        if successful_offsets and self.consumer:
            try:
                # Commit the highest successful offset
                max_offset = max(successful_offsets)
                tp = TopicPartition(batch.topic, batch.partition)
                self.consumer.commit({tp: max_offset + 1})
                logger.debug(f"Committed offset {max_offset + 1} for partition {batch.partition}")
                
            except Exception as e:
                logger.error(f"Failed to commit offsets: {e}")
    
    def start(self):
        """Start the kafka-python consumer"""
        logger.info("Starting Kafka Python consumer...")
        self.running = True
        
        try:
            self.consumer = self._create_consumer()
            
            # Main consumption loop
            batch_messages = {}
            batch_offsets = {}
            last_partition_check = 0
            
            while self.running and not self.shutdown_event.is_set():
                try:
                    # Periodically check for partition changes
                    current_time = time.time()
                    if current_time - last_partition_check > 10:  # Check every 10 seconds
                        self._setup_partition_workers()
                        last_partition_check = current_time
                    
                    # Poll for messages
                    message_batch = self.consumer.poll(timeout_ms=1000, max_records=self.config.max_poll_records)
                    
                    if not message_batch:
                        # Process any pending batches
                        self._flush_batches(batch_messages, batch_offsets)
                        continue
                    
                    # Process messages by partition
                    for tp, messages in message_batch.items():
                        partition = tp.partition
                        topic = tp.topic
                        
                        if partition not in batch_messages:
                            batch_messages[partition] = []
                            batch_offsets[partition] = []
                        
                        for message in messages:
                            batch_messages[partition].append(message.value)
                            batch_offsets[partition].append(message.offset)
                        
                        # Send batch when it reaches the configured size
                        if len(batch_messages[partition]) >= self.config.batch_size:
                            self._send_batch_to_worker(
                                partition, topic,
                                batch_messages[partition],
                                batch_offsets[partition]
                            )
                            batch_messages[partition] = []
                            batch_offsets[partition] = []
                
                except KafkaPythonError as e:
                    logger.error(f"Kafka error: {e}")
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Unexpected error: {e}")
                    time.sleep(1)
            
            # Process final batches
            self._flush_batches(batch_messages, batch_offsets)
            
        except Exception as e:
            logger.error(f"Fatal error in consumer: {e}")
        finally:
            self.shutdown()
    
    def _flush_batches(self, batch_messages: Dict, batch_offsets: Dict):
        """Send any remaining batches to workers"""
        for partition in list(batch_messages.keys()):
            if batch_messages[partition]:
                topic = self.config.topics[0] if self.config.topics else "unknown"
                self._send_batch_to_worker(
                    partition, topic,
                    batch_messages[partition],
                    batch_offsets[partition]
                )
                batch_messages[partition] = []
                batch_offsets[partition] = []
    
    def _send_batch_to_worker(self, partition: int, topic: str, messages: List, offsets: List):
        """Send a batch to the appropriate partition worker"""
        if partition in self.partition_queues:
            batch = MessageBatch(partition, topic, messages.copy(), offsets.copy())
            try:
                self.partition_queues[partition].put(batch, timeout=5.0)
            except Exception as e:
                logger.error(f"Failed to queue batch for partition {partition}: {e}")
    
    def shutdown(self):
        """Shutdown the consumer gracefully"""
        if not self.running:
            return
        
        logger.info("Shutting down Kafka Python consumer...")
        self.running = False
        self.shutdown_event.set()
        
        # Stop all partition workers
        for partition_id, queue in self.partition_queues.items():
            queue.put(None)  # Signal shutdown
        
        # Wait for workers to finish
        for thread in self.partition_threads.values():
            thread.join(timeout=5)
        
        # Close consumer
        if self.consumer:
            self.consumer.close()
        
        # Shutdown executor
        self.executor.shutdown(wait=True)
        
        logger.info("Kafka Python consumer shutdown complete")

# Example usage and message handlers
def sample_message_handler(message: Any) -> bool:
    """Sample message handler that processes messages"""
    try:
        # Simulate processing time
        time.sleep(0.1)
        
        # Example: Process JSON messages
        if isinstance(message, str):
            data = json.loads(message)
            logger.info(f"Processed message: {data.get('id', 'unknown')}")
        else:
            logger.info(f"Processed message: {message}")
        
        return True
    except Exception as e:
        logger.error(f"Error in message handler: {e}")
        return False

def run_confluent_consumer_example():
    """Example of running the Confluent Kafka consumer"""
    if not CONFLUENT_AVAILABLE:
        logger.error("Confluent Kafka not available")
        return
    
    config = ConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="confluent-consumer-group",
        topics=["mongo.pages_topic"],
        max_workers=4,
        batch_size=10
    )
    
    consumer = ConfluentKafkaConsumer(config, sample_message_handler)
    
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

def run_kafka_python_consumer_example():
    """Example of running the Kafka Python consumer"""
    if not KAFKA_PYTHON_AVAILABLE:
        logger.error("Kafka Python not available")
        return
    
    config = ConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="kafka-python-consumer-group",
        topics=["mongo.pages_topic"],
        max_workers=4,
        batch_size=10
    )
    
    consumer = KafkaPythonConsumer(config, sample_message_handler)
    
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

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == "confluent":
            run_confluent_consumer_example()
        elif sys.argv[1] == "kafka-python":
            run_kafka_python_consumer_example()
        else:
            print("Usage: python script.py [confluent|kafka-python]")
    else:
        print("Available consumer implementations:")
        print("- confluent: Uses confluent-kafka library")
        print("- kafka-python: Uses kafka-python library")
        print("\nUsage: python script.py [confluent|kafka-python]")
