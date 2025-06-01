class PartitionAwareConsumer(ScalableKafkaConsumer):
    def __init__(self, topic, bootstrap_servers, group_id, num_workers_per_partition=2):
        super().__init__(topic, bootstrap_servers, group_id)
        self.num_workers_per_partition = num_workers_per_partition
        self.partition_executors = {}
    
    def initialize_consumer(self):
        super().initialize_consumer()
        # Initialize executors for each partition
        partitions = self.consumer.partitions_for_topic(self.topic)
        for partition in partitions:
            self.partition_executors[partition] = ThreadPoolExecutor(
                max_workers=self.num_workers_per_partition
            )
    
    def start_consuming(self):
        self.initialize_consumer()
        logger.info(f"Starting partition-aware consumer for topic {self.topic}")
        
        try:
            for message in self.consumer:
                partition = message.partition
                self.partition_executors[partition].submit(self.worker, message)
        except KeyboardInterrupt:
            logger.info("Stopping consumer due to interrupt")
        except Exception as e:
            logger.error(f"Error in consumer: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        logger.info("Shutting down partition-aware consumer")
        if self.consumer:
            self.consumer.close()
        for executor in self.partition_executors.values():
            executor.shutdown(wait=True)
        logger.info("Consumer shutdown complete")
 