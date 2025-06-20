#!/usr/bin/env python3
"""
Advanced Partition-Aware Kafka Consumer with Multi-Threading and Fault Tolerance
Standalone implementation using confluent-kafka library.
"""

import json
import logging
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from queue import Queue, Empty
from typing import Dict, List, Callable, Any, Optional
from contextlib import contextmanager
import traceback
from collections import defaultdict


try:
    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
    from confluent_kafka.admin import AdminClient, ConfigResource, ConfigSource
    CONFLUENT_AVAILABLE = True
except ImportError:
    print("Error: confluent-kafka library not installed. Run: pip install confluent-kafka")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class KafkaConfig:
    """Advanced Kafka consumer configuration"""
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "advanced-consumer-group"
    topics: List[str] = field(default_factory=list)
    
    # Threading configuration
    max_workers: int = 4
    partition_workers: int = 1  # Workers per partition
    
    # Consumer settings
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_interval_ms: int = 300000
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    
    # Fault tolerance settings
    max_retries: int = 3
    retry_backoff_ms: int = 1000
    message_timeout_seconds: int = 30
    health_check_interval: int = 60
    
    # Batch processing
    batch_size: int = 100
    batch_timeout_ms: int = 5000
    
    # Error handling
    dead_letter_topic: Optional[str] = None
    error_threshold: int = 10  # Max errors before partition pause
    
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
            'fetch.min.bytes': self.fetch_min_bytes,
            'partition.assignment.strategy': 'range',
            'isolation.level': 'read_committed',
        }

@dataclass
class MessageBatch:
    """Represents a batch of messages from a partition"""
    partition: int
    topic: str
    messages: List[Any]
    offsets: List[int]
    timestamps: List[int]
    received_at: float = field(default_factory=time.time)

@dataclass
class ConsumerStats:
    """Consumer statistics and metrics"""
    messages_processed: int = 0
    messages_failed: int = 0
    batches_processed: int = 0
    partitions_assigned: int = 0
    last_activity: float = field(default_factory=time.time)
    start_time: float = field(default_factory=time.time)
    error_count_by_partition: Dict[int, int] = field(default_factory=lambda: defaultdict(int))
    processing_times: List[float] = field(default_factory=list)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics"""
        uptime = time.time() - self.start_time
        avg_processing_time = sum(self.processing_times[-100:]) / max(len(self.processing_times[-100:]), 1)
        
        return {
            'uptime_seconds': round(uptime, 2),
            'messages_processed': self.messages_processed,
            'messages_failed': self.messages_failed,
            'batches_processed': self.batches_processed,
            'partitions_assigned': self.partitions_assigned,
            'processing_rate_per_second': round(self.messages_processed / max(uptime, 1), 2),
            'average_processing_time_ms': round(avg_processing_time * 1000, 2),
            'error_rate_percent': round((self.messages_failed / max(self.messages_processed + self.messages_failed, 1)) * 100, 2),
            'partition_errors': dict(self.error_count_by_partition)
        }

class PartitionWorker:
    """Worker thread for processing messages from a specific partition"""
    
    def __init__(self, partition_id: int, topic: str, config: KafkaConfig, 
                 message_handler: Callable[[Any], bool], consumer_stats: ConsumerStats):
        self.partition_id = partition_id
        self.topic = topic
        self.config = config
        self.message_handler = message_handler
        self.consumer_stats = consumer_stats
        self.queue = Queue(maxsize=1000)
        self.worker_thread = None
        self.shutdown_event = threading.Event()
        self.stats_lock = threading.Lock()
        self.error_count = 0
        self.success_count = 0
        self.paused = False
        self.pause_time = None
        self.pause_duration = 60  # Start with 1 minute pause
        
    def start(self):
        """Start the partition worker"""
        if self.worker_thread and self.worker_thread.is_alive():
            return
            
        self.worker_thread = threading.Thread(
            target=self._worker_loop,
            name=f"partition-{self.partition_id}-worker",
            daemon=True
        )
        self.worker_thread.start()
        logger.info(f"Started worker for partition {self.partition_id}")
    
    def stop(self):
        """Stop the partition worker"""
        self.shutdown_event.set()
        self.queue.put(None)  # Signal to stop
        
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)
            
        logger.info(f"Stopped worker for partition {self.partition_id}")
    
    def submit_batch(self, batch: MessageBatch) -> bool:
        """Submit a batch for processing"""
        # Check if partition should be resumed
        if self.paused and self._should_resume():
            self._resume_partition()
        
        if self.paused:
            logger.debug(f"Partition {self.partition_id} is paused due to errors")
            return False
            
        try:
            self.queue.put(batch, timeout=5)
            return True
        except Exception as e:
            logger.error(f"Failed to queue batch for partition {self.partition_id}: {e}")
            return False
    
    def _should_resume(self) -> bool:
        """Check if partition should be resumed from pause"""
        if not self.paused or not self.pause_time:
            return False
        
        # Resume after pause duration
        return time.time() - self.pause_time > self.pause_duration
    
    def _resume_partition(self):
        """Resume partition processing"""
        self.paused = False
        self.pause_time = None
        self.error_count = 0  # Reset error count
        logger.info(f"Resuming partition {self.partition_id} after pause")
    
    def _worker_loop(self):
        """Main worker loop"""
        while not self.shutdown_event.is_set():
            try:
                batch = self.queue.get(timeout=1.0)
                if batch is None:  # Shutdown signal
                    break
                    
                self._process_batch(batch)
                
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in partition {self.partition_id} worker: {e}")
                self._handle_worker_error()
    
    def _process_batch(self, batch: MessageBatch):
        """Process a batch of messages"""
        start_time = time.time()
        successful_offsets = []
        batch_errors = 0
        
        for message, offset, timestamp in zip(batch.messages, batch.offsets, batch.timestamps):
            try:
                if self._process_message_with_retry(message):
                    successful_offsets.append(offset)
                    self.success_count += 1
                    with self.stats_lock:
                        self.consumer_stats.messages_processed += 1
                else:
                    logger.warning(f"Failed to process message at offset {offset}")
                    batch_errors += 1
                    with self.stats_lock:
                        self.consumer_stats.messages_failed += 1
                        self.consumer_stats.error_count_by_partition[self.partition_id] += 1
                        
            except Exception as e:
                logger.error(f"Unexpected error processing message at offset {offset}: {e}")
                batch_errors += 1
                with self.stats_lock:
                    self.consumer_stats.messages_failed += 1
                    self.consumer_stats.error_count_by_partition[self.partition_id] += 1
        
        # Update error tracking
        self.error_count += batch_errors
        
        # Check if should pause partition (more reasonable threshold)
        total_processed = self.success_count + self.error_count
        if total_processed > 10:  # Only check after processing some messages
            error_rate = self.error_count / total_processed
            if error_rate > 0.5 and self.error_count >= 5:  # 50% error rate with at least 5 errors
                self._pause_partition()
        
        # Record processing time
        processing_time = time.time() - start_time
        with self.stats_lock:
            self.consumer_stats.processing_times.append(processing_time)
            self.consumer_stats.batches_processed += 1
            self.consumer_stats.last_activity = time.time()
        
        logger.debug(f"Processed batch for partition {self.partition_id}: "
                    f"{len(successful_offsets)}/{len(batch.messages)} successful")
    
    def _process_message_with_retry(self, message: Any) -> bool:
        """Process a message with retry logic"""
        for attempt in range(self.config.max_retries + 1):
            try:
                return self.message_handler(message)
            except Exception as e:
                if attempt < self.config.max_retries:
                    logger.warning(f"Message processing failed (attempt {attempt + 1}): {e}")
                    time.sleep(self.config.retry_backoff_ms / 1000.0)
                else:
                    logger.error(f"Message processing failed after {attempt + 1} attempts: {e}")
                    return False
        return False
    
    def _pause_partition(self):
        """Pause partition processing due to errors"""
        if not self.paused:
            self.paused = True
            self.pause_time = time.time()
            logger.warning(f"Pausing partition {self.partition_id} due to high error rate "
                          f"(errors: {self.error_count}, successes: {self.success_count})")
            # Exponential backoff for pause duration
            self.pause_duration = min(self.pause_duration * 2, 300)  # Max 5 minutes
    
    def _handle_worker_error(self):
        """Handle worker-level errors"""
        self.error_count += 1

class AdvancedKafkaConsumer:
    """Advanced partition-aware Kafka consumer with fault tolerance"""
    
    def __init__(self, config: KafkaConfig, message_handler: Callable[[Any], bool]):
        self.config = config
        self.message_handler = message_handler
        self.consumer = None
        self.admin_client = None
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Partition management
        self.partition_workers: Dict[int, PartitionWorker] = {}
        self.assigned_partitions = set()
        
        # Statistics and monitoring
        self.stats = ConsumerStats()
        self.stats_lock = threading.Lock()
        
        # Health monitoring
        self.health_check_thread = None
        self.last_health_check = time.time()
        
        # Signal handling
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        logger.info(f"Initialized AdvancedKafkaConsumer for topics: {config.topics}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown()
    
    def start(self):
        """Start the consumer"""
        if self.running:
            logger.warning("Consumer is already running")
            return
        
        try:
            self._validate_configuration()
            self.consumer = self._create_consumer()
            self.admin_client = AdminClient(self.config.to_confluent_config())
            
            self.running = True
            self._start_health_monitoring()
            
            logger.info("Starting advanced Kafka consumer...")
            self._consume_loop()
            
        except Exception as e:
            logger.error(f"Failed to start consumer: {e}")
            self.shutdown()
            raise
    
    def shutdown(self):
        """Gracefully shutdown the consumer"""
        if not self.running:
            return
        
        logger.info("Shutting down advanced Kafka consumer...")
        self.running = False
        self.shutdown_event.set()
        
        # Stop all partition workers
        for worker in self.partition_workers.values():
            worker.stop()
        
        # Stop health monitoring
        if self.health_check_thread:
            self.health_check_thread.join(timeout=5)
        
        # Close consumer
        if self.consumer:
            self.consumer.close()
        
        # Final stats
        final_stats = self.get_stats()
        logger.info(f"Final consumer statistics: {final_stats}")
        logger.info("Advanced Kafka consumer shutdown complete")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get consumer statistics"""
        with self.stats_lock:
            return self.stats.get_summary()
    
    def _validate_configuration(self):
        """Validate consumer configuration"""
        if not self.config.topics:
            raise ValueError("No topics specified in configuration")
        
        if not self.config.bootstrap_servers:
            raise ValueError("No bootstrap servers specified")
        
        # Test connection to Kafka
        try:
            admin = AdminClient(self.config.to_confluent_config())
            metadata = admin.list_topics(timeout=10)
            logger.info(f"Successfully connected to Kafka. Available topics: {len(metadata.topics)}")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Kafka: {e}")
    
    def _create_consumer(self) -> Consumer:
        """Create and configure Kafka consumer"""
        consumer_config = self.config.to_confluent_config()
        consumer = Consumer(consumer_config)
        
        # Subscribe with callbacks
        consumer.subscribe(
            self.config.topics,
            on_assign=self._on_partition_assign,
            on_revoke=self._on_partition_revoke
        )
        
        return consumer
    
    def _on_partition_assign(self, consumer, partitions):
        """Handle partition assignment"""
        partition_ids = [p.partition for p in partitions]
        logger.info(f"Partitions assigned: {partition_ids}")
        
        with self.stats_lock:
            self.stats.partitions_assigned = len(partitions)
        
        # Create workers for new partitions
        for partition in partitions:
            partition_id = partition.partition
            topic = partition.topic
            
            if partition_id not in self.partition_workers:
                worker = PartitionWorker(
                    partition_id, topic, self.config, 
                    self.message_handler, self.stats
                )
                worker.start()
                self.partition_workers[partition_id] = worker
                self.assigned_partitions.add(partition_id)
        
        # Use assign instead of automatic assignment for range strategy
        try:
            consumer.assign(partitions)
            logger.info(f"Successfully assigned partitions: {partition_ids}")
        except Exception as e:
            logger.error(f"Failed to assign partitions: {e}")
    
    def _on_partition_revoke(self, consumer, partitions):
        """Handle partition revocation"""
        partition_ids = [p.partition for p in partitions]
        logger.info(f"Partitions revoked: {partition_ids}")
        
        # Stop workers for revoked partitions
        for partition in partitions:
            partition_id = partition.partition
            if partition_id in self.partition_workers:
                self.partition_workers[partition_id].stop()
                del self.partition_workers[partition_id]
                self.assigned_partitions.discard(partition_id)
    
    def _consume_loop(self):
        """Main message consumption loop"""
        batch_messages = defaultdict(list)
        batch_offsets = defaultdict(list)
        batch_timestamps = defaultdict(list)
        last_batch_time = time.time()
        
        while self.running and not self.shutdown_event.is_set():
            try:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # Check for batch timeout
                    if time.time() - last_batch_time > (self.config.batch_timeout_ms / 1000.0):
                        self._flush_batches(batch_messages, batch_offsets, batch_timestamps)
                        last_batch_time = time.time()
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Add message to batch
                partition = msg.partition()
                topic = msg.topic()
                
                batch_messages[partition].append(msg.value())
                batch_offsets[partition].append(msg.offset())
                batch_timestamps[partition].append(msg.timestamp()[1] if msg.timestamp()[0] else int(time.time() * 1000))
                
                # Send batch if size reached
                if len(batch_messages[partition]) >= self.config.batch_size:
                    self._send_batch_to_worker(
                        partition, topic,
                        batch_messages[partition],
                        batch_offsets[partition],
                        batch_timestamps[partition]
                    )
                    batch_messages[partition] = []
                    batch_offsets[partition] = []
                    batch_timestamps[partition] = []
                    last_batch_time = time.time()
                
            except KafkaException as e:
                logger.error(f"Kafka exception in consume loop: {e}")
                time.sleep(1)
            except Exception as e:
                logger.error(f"Unexpected error in consume loop: {e}")
                logger.error(traceback.format_exc())
                time.sleep(1)
        
        # Process final batches
        self._flush_batches(batch_messages, batch_offsets, batch_timestamps)
    
    def _send_batch_to_worker(self, partition: int, topic: str, messages: List, offsets: List, timestamps: List):
        """Send batch to partition worker"""
        if partition in self.partition_workers:
            batch = MessageBatch(partition, topic, messages.copy(), offsets.copy(), timestamps.copy())
            success = self.partition_workers[partition].submit_batch(batch)
            if not success:
                logger.warning(f"Failed to submit batch to worker for partition {partition}")
    
    def _flush_batches(self, batch_messages: Dict, batch_offsets: Dict, batch_timestamps: Dict):
        """Flush remaining batches to workers"""
        for partition in list(batch_messages.keys()):
            if batch_messages[partition]:
                self._send_batch_to_worker(
                    partition, self.config.topics[0],  # Simplified for single topic
                    batch_messages[partition],
                    batch_offsets[partition],
                    batch_timestamps[partition]
                )
                batch_messages[partition] = []
                batch_offsets[partition] = []
                batch_timestamps[partition] = []
    
    def _start_health_monitoring(self):
        """Start health monitoring thread"""
        def health_monitor():
            while self.running:
                try:
                    time.sleep(self.config.health_check_interval)
                    self._perform_health_check()
                except Exception as e:
                    logger.error(f"Health monitoring error: {e}")
        
        self.health_check_thread = threading.Thread(target=health_monitor, daemon=True)
        self.health_check_thread.start()
    
    def _perform_health_check(self):
        """Perform health checks"""
        current_time = time.time()
        
        # Check if consumer is active
        if current_time - self.stats.last_activity > 300:  # 5 minutes
            logger.warning("Consumer appears inactive - no recent activity")
        
        # Log current statistics
        stats = self.get_stats()
        logger.info(f"Health check - Consumer stats: {stats}")
        
        # Check partition worker health
        for partition_id, worker in self.partition_workers.items():
            if not worker.worker_thread.is_alive():
                logger.error(f"Worker for partition {partition_id} has died - attempting restart")
                worker.start()


def sample_message_handler(message: Any) -> bool:
    """Sample message handler"""
    try:
        # Handle different message types
        if isinstance(message, bytes):
            message = message.decode('utf-8')
        
        if isinstance(message, str):
            try:
                data = json.loads(message)
                #logger.info(f"Processed JSON message: {type(data)}")
            except json.JSONDecodeError:
                logger.info(f"Processed text message: {message[:100]}...")
        else:
            logger.info(f"Processed message of type {type(message)}")
        
        # Simulate processing time
        time.sleep(0.01)
        return True
        
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return False


def main():
    """Main function to run the advanced Kafka consumer"""
    # Configuration
    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        group_id=f"advanced-consumer-python-pages_topic",
        topics=["mongo.pages_topic"],  # Change to your topic
        max_workers=4,
        partition_workers=4,
        batch_size=1000,
        batch_timeout_ms=5000,
        max_retries=3,
        error_threshold=100  # Increase threshold to prevent premature pausing
    )
    
    # Create and start consumer
    consumer = AdvancedKafkaConsumer(config, sample_message_handler)
    
    try:
        # Start stats reporting
        def report_stats():
            while consumer.running:
                time.sleep(30)
                stats = consumer.get_stats()
                logger.info(f"=== Consumer Statistics ===\n{json.dumps(stats, indent=2)}")
        
        stats_thread = threading.Thread(target=report_stats, daemon=True)
        stats_thread.start()
        
        # Start consumer
        consumer.start()
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Error running consumer: {e}")
        logger.error(traceback.format_exc())
    finally:
        consumer.shutdown()


if __name__ == "__main__":
    main()
