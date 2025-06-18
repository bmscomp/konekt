#!/usr/bin/env python3
"""
Partition-aware and resilient Kafka consumers with multi-threading support.
Includes implementations for both confluent-kafka and kafka-python libraries.
"""

import logging
import signal
import sys
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from queue import Queue, Empty
from typing import Dict, List, Callable, Any
from contextlib import contextmanager

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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ConsumerConfig:
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

class BaseKafkaConsumer(ABC):
    """Base class for Kafka consumers with common functionality"""
    
    def __init__(self, config: ConsumerConfig, message_handler: Callable[[Any], bool]):
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
            'partitions_assigned': 0,
            'last_activity': time.time()
        }
        self.stats_lock = threading.Lock()
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown()
    
    def update_stats(self, processed: int = 0, failed: int = 0):
        """Thread-safe stats update"""
        with self.stats_lock:
            self.stats['messages_processed'] += processed
            self.stats['messages_failed'] += failed
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
    
    def process_message_with_retry(self, message: Any) -> bool:
        """Process a message with retry logic"""
        for attempt in range(self.config.max_retries + 1):
            try:
                success = self.message_handler(message)
                if success:
                    self.update_stats(processed=1)
                    return True
                else:
                    logger.warning(f"Message handler returned False for message: {message}")
                    
            except Exception as e:
                logger.error(f"Error processing message (attempt {attempt + 1}): {e}")
                if attempt < self.config.max_retries:
                    time.sleep(self.config.retry_backoff_ms / 1000.0)
                else:
                    self.update_stats(failed=1)
                    return False
        
        return False

class ConfluentKafkaConsumer(BaseKafkaConsumer):
    """Partition-aware consumer using confluent-kafka"""
    
    def __init__(self, config: ConsumerConfig, message_handler: Callable[[Any], bool]):
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
            
            while self.running and not self.shutdown_event.is_set():
                try:
                    msg = self.consumer.poll(timeout=1.0)
                    
                    if msg is None:
                        # Process any pending batches
                        self._flush_batches(batch_messages, batch_offsets)
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
                    
                    batch_messages[partition].append(msg.value())
                    batch_offsets[partition].append(msg.offset())
                    
                    # Send batch when it reaches the configured size
                    if len(batch_messages[partition]) >= self.config.batch_size:
                        self._send_batch_to_worker(
                            partition, msg.topic(), 
                            batch_messages[partition], 
                            batch_offsets[partition]
                        )
                        batch_messages[partition] = []
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
            data = message
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
    
    # First check if Kafka is accessible
    try:
        admin_client = AdminClient({'bootstrap.servers': 'localhost:9092'})
        topics = admin_client.list_topics(timeout=5)
        if not topics:
            logger.error("Failed to connect to Kafka broker")
            return
        
        # Check if test-topic exists
        if 'test-topic' not in topics.topics:
            logger.error("test-topic does not exist")
            return
            
    except KafkaException as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        return
    
    config = ConsumerConfig(
        bootstrap_servers="localhost:9092",
        group_id="confluent-consumer-group",
        topics=["test-topic"],
        max_workers=4,
        batch_size=10
    )
    
    try:
        consumer = ConfluentKafkaConsumer(config, sample_message_handler)
    except Exception as e:
        logger.error(f"Failed to create consumer: {e}")
        return
    
    stats_thread = None
    
    try:
        # Start stats reporting thread
        def report_stats():
            while consumer.running:
                try:
                    time.sleep(30)
                    stats = consumer.get_stats()
                    logger.info(f"Consumer stats: {stats}")
                except Exception as e:
                    logger.error(f"Error in stats reporting: {e}")
                    break
        
        stats_thread = threading.Thread(target=report_stats, daemon=True)
        stats_thread.start()
        
        # Start the consumer
        consumer.start()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Error running consumer: {e}")
    finally:
        consumer.shutdown()
        if stats_thread and stats_thread.is_alive():
            consumer.running = False
            stats_thread.join(timeout=5)

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
