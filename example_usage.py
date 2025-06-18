#!/usr/bin/env python3
"""
Example usage of the Kafka to SQL Server consumer

This script demonstrates how to use the PartitionAwareKafkaConsumer
with different configurations and scenarios.
"""

import os
import sys
import time
import threading
from kafka_sqlserver_consumer import (
    PartitionAwareKafkaConsumer, 
    KafkaConfig, 
    SQLServerConfig
)

def example_basic_usage():
    """Basic usage example"""
    print("=== Basic Usage Example ===")
    
    # Configure Kafka
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        group_id="example_consumer_group",
        topics=["test-topic"],
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    
    # Configure SQL Server
    sqlserver_config = SQLServerConfig(
        server="localhost",
        port=1433,
        database="kafka_messages",
        username="sa",
        password="YourPassword123!",
        table_name="kafka_messages"
    )
    
    # Create consumer
    consumer = PartitionAwareKafkaConsumer(
        kafka_config=kafka_config,
        sqlserver_config=sqlserver_config,
        max_workers_per_partition=2
    )
    
    # Start consumer (this will run until interrupted)
    try:
        consumer.start()
    except KeyboardInterrupt:
        print("Consumer stopped by user")


def example_multi_topic_usage():
    """Multi-topic usage example"""
    print("=== Multi-Topic Usage Example ===")
    
    # Configure for multiple topics
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        group_id="multi_topic_consumer",
        topics=["orders", "events", "logs"],  # Multiple topics
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=1000  # Higher batch size
    )
    
    sqlserver_config = SQLServerConfig(
        server="localhost",
        port=1433,
        database="kafka_messages",
        username="sa",
        password="YourPassword123!",
        table_name="multi_topic_messages"
    )
    
    consumer = PartitionAwareKafkaConsumer(
        kafka_config=kafka_config,
        sqlserver_config=sqlserver_config,
        max_workers_per_partition=4  # More workers per partition
    )
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        print("Multi-topic consumer stopped")


def example_high_throughput_usage():
    """High throughput configuration example"""
    print("=== High Throughput Usage Example ===")
    
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        group_id="high_throughput_consumer",
        topics=["high_volume_topic"],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        max_poll_records=2000,  # Large batch size
        session_timeout_ms=45000,  # Longer session timeout
        max_poll_interval_ms=600000  # 10 minutes max poll interval
    )
    
    sqlserver_config = SQLServerConfig(
        server="localhost",
        port=1433,
        database="kafka_messages",
        username="sa",
        password="YourPassword123!",
        table_name="high_throughput_messages",
        connection_timeout=60  # Longer connection timeout
    )
    
    consumer = PartitionAwareKafkaConsumer(
        kafka_config=kafka_config,
        sqlserver_config=sqlserver_config,
        max_workers_per_partition=8  # Many workers per partition
    )
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        print("High throughput consumer stopped")


def example_with_monitoring():
    """Example with monitoring and statistics"""
    print("=== Consumer with Monitoring Example ===")
    
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        group_id="monitored_consumer",
        topics=["monitored_topic"],
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    
    sqlserver_config = SQLServerConfig(
        server="localhost",
        port=1433,
        database="kafka_messages",
        username="sa",
        password="YourPassword123!",
        table_name="monitored_messages"
    )
    
    consumer = PartitionAwareKafkaConsumer(
        kafka_config=kafka_config,
        sqlserver_config=sqlserver_config,
        max_workers_per_partition=2
    )
    
    # Start monitoring thread
    def monitor_stats():
        while consumer.running:
            time.sleep(30)  # Report every 30 seconds
            print(f"Messages processed: {consumer.stats['messages_processed']}")
            print(f"Messages failed: {consumer.stats['messages_failed']}")
            print(f"Partition stats: {consumer.stats['partition_stats']}")
            print("-" * 50)
    
    monitor_thread = threading.Thread(target=monitor_stats, daemon=True)
    monitor_thread.start()
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        print("Monitored consumer stopped")


def example_environment_based():
    """Example using environment variables"""
    print("=== Environment-based Configuration Example ===")
    
    # Set environment variables (in practice, these would be set externally)
    os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    os.environ.setdefault("KAFKA_GROUP_ID", "env_consumer")
    os.environ.setdefault("KAFKA_TOPICS", "env_topic")
    os.environ.setdefault("SQLSERVER_HOST", "localhost")
    os.environ.setdefault("SQLSERVER_DATABASE", "kafka_messages")
    os.environ.setdefault("SQLSERVER_USERNAME", "sa")
    os.environ.setdefault("SQLSERVER_PASSWORD", "YourPassword123!")
    
    # Load configuration from environment
    kafka_config = KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        group_id=os.getenv("KAFKA_GROUP_ID"),
        topics=os.getenv("KAFKA_TOPICS").split(","),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        enable_auto_commit=os.getenv("KAFKA_ENABLE_AUTO_COMMIT", "false").lower() == "true"
    )
    
    sqlserver_config = SQLServerConfig(
        server=os.getenv("SQLSERVER_HOST"),
        database=os.getenv("SQLSERVER_DATABASE"),
        username=os.getenv("SQLSERVER_USERNAME"),
        password=os.getenv("SQLSERVER_PASSWORD"),
        table_name=os.getenv("SQLSERVER_TABLE", "kafka_messages")
    )
    
    consumer = PartitionAwareKafkaConsumer(
        kafka_config=kafka_config,
        sqlserver_config=sqlserver_config,
        max_workers_per_partition=int(os.getenv("MAX_WORKERS_PER_PARTITION", "2"))
    )
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        print("Environment-based consumer stopped")


def main():
    """Main function to run examples"""
    if len(sys.argv) < 2:
        print("Usage: python example_usage.py <example_type>")
        print("Available examples:")
        print("  basic - Basic usage example")
        print("  multi - Multi-topic usage example")
        print("  throughput - High throughput configuration")
        print("  monitoring - Consumer with monitoring")
        print("  env - Environment-based configuration")
        return
    
    example_type = sys.argv[1].lower()
    
    if example_type == "basic":
        example_basic_usage()
    elif example_type == "multi":
        example_multi_topic_usage()
    elif example_type == "throughput":
        example_high_throughput_usage()
    elif example_type == "monitoring":
        example_with_monitoring()
    elif example_type == "env":
        example_environment_based()
    else:
        print(f"Unknown example type: {example_type}")
        print("Available examples: basic, multi, throughput, monitoring, env")


if __name__ == "__main__":
    main()
