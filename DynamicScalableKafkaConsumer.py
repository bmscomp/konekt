import psutil
import time

class DynamicScalableKafkaConsumer(ScalableKafkaConsumer):
    def __init__(self, topic, bootstrap_servers, group_id, min_workers=2, max_workers=16):
        super().__init__(topic, bootstrap_servers, group_id, num_workers=min_workers)
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.last_adjustment = time.time()
    
    def adjust_workers(self):
        """Adjust number of workers based on system load"""
        now = time.time()
        if now - self.last_adjustment < 30:  # Only adjust every 30 seconds
            return
        
        self.last_adjustment = now
        cpu_load = psutil.cpu_percent()
        memory_avail = psutil.virtual_memory().available
        
        current_workers = self.num_workers
        
        # Simple scaling logic - adjust as needed
        if cpu_load < 50 and memory_avail > (2 * 1024 * 1024 * 1024):  # 2GB available
            new_workers = min(current_workers + 2, self.max_workers)
        elif cpu_load > 80 or memory_avail < (1 * 1024 * 1024 * 1024):  # 1GB available
            new_workers = max(current_workers - 2, self.min_workers)
        else:
            return
        
        if new_workers != current_workers:
            logger.info(f"Adjusting workers from {current_workers} to {new_workers}")
            self.num_workers = new_workers
            self.executor._max_workers = new_workers
    
    def start_consuming(self):
        """Start consuming with dynamic worker adjustment"""
        self.initialize_consumer()
        logger.info(f"Starting dynamic consumer for topic {self.topic}")
        
        try:
            for message in self.consumer:
                self.adjust_workers()
                self.executor.submit(self.worker, message)
        except KeyboardInterrupt:
            logger.info("Stopping consumer due to interrupt")
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            self.shutdown()
    