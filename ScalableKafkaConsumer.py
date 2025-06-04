from kafka import KafkaConsumer
from concurrent.futures import ThreadPoolExecutor
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ScalableKafkaConsumer:

    def __init__(self, topic, bootstrap_servers, group_id, num_workers=4):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.num_workers = num_workers
        self.consumer = None
        self.executor = ThreadPoolExecutor(max_workers=num_workers)
        
    def initialize_consumer(self):
        """Initialize Kafka consumer with proper configurations"""
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',  # or 'earliest'
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_records=100,  # adjust based on your throughput
            max_poll_interval_ms=300000  # 5 minutes
        )
    
    def process_message(self, message):
        """Process a single message - override this in your implementation"""
        try:
            # Your message processing logic here
            logger.info(f"Processing message: {message.value}")
            # Simulate processing time
            time.sleep(0.1)
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def worker(self, message):
        """Worker function that processes a message"""
        if self.process_message(message):
            # If processing is successful, we can optionally manually commit offset
            # self.consumer.commit()
            pass
    
    def start_consuming(self):
        """Start consuming messages from Kafka"""
        self.initialize_consumer()
        logger.info(f"Starting consumer for topic {self.topic} with {self.num_workers} workers")
        
        try:
            for message in self.consumer:
                # Submit message to thread pool for processing
                self.executor.submit(self.worker, message)
        except KeyboardInterrupt:
            logger.info("Stopping consumer due to interrupt")
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Clean shutdown of consumer and executor"""
        logger.info("Shutting down consumer")
        if self.consumer:
            self.consumer.close()
        self.executor.shutdown(wait=True)
        logger.info("Consumer shutdown complete")

if __name__ == "__main__":
    # Configuration
    TOPIC = 'mongo.pages_topic'
    BOOTSTRAP_SERVERS = ['localhost:9092']
    GROUP_ID = 'wikipedia_group'  # Consumer group ID
    NUM_WORKERS = 5  # Adjust based on your needs
    
    # Create and start consumer
    consumer = ScalableKafkaConsumer(
        topic=TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        num_workers=NUM_WORKERS
    )
    
    consumer.start_consuming()
    