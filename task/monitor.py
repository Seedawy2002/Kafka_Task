import csv
import time
import subprocess
import os
from kafka.admin import KafkaAdminClient, NewPartitions
from kafka import KafkaConsumer, TopicPartition
import shutil

# Kafka configurations to test
configurations = [
    {"batch_size": 16384, "linger_ms": 0, "compression_type": "none"},
    {"batch_size": 32768, "linger_ms": 5, "compression_type": "gzip"},
    {"batch_size": 65536, "linger_ms": 10, "compression_type": "snappy"},
    {"batch_size": 128000, "linger_ms": 20, "compression_type": "none"},
    {"batch_size": 512000, "linger_ms": 50, "compression_type": "gzip"}
]

# Kafka parameters
TOPIC_NAME = 'image-data7'
BOOTSTRAP_SERVERS = ['localhost:9092']
RESULTS_FILE = "kafka_performance_results.csv"
PRODUCER_METRICS_FILE = "producer_metrics.csv"

# Function to reset test files
def reset_test_files():
    """Reset files and folders for a clean test run."""
    files_to_delete = ["processed_files.txt", "annotation.csv"]
    for file in files_to_delete:
        if os.path.exists(file):
            os.remove(file)
            print(f"Deleted '{file}' for a clean test run.")
        else:
            print(f"No '{file}' found. Starting fresh.")
    folder_path = "output_images"
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        print(f"Deleted folder and all its contents: '{folder_path}'")
    os.makedirs(folder_path, exist_ok=True)

# Initialize CSV file
with open(RESULTS_FILE, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow([
        "Batch Size", "Linger (ms)", "Compression", "Num Partitions",
        "Producer Throughput (msg/sec)", "Consumer Throughput (msg/sec)",
        "Producer Latency (sec/msg)", "Consumer Latency (sec/msg)",
        "Consumer Lag", "Partition Distribution"
    ])

# Function to fetch producer metrics
def get_producer_metrics():
    """Read producer metrics from the CSV file."""
    if not os.path.exists(PRODUCER_METRICS_FILE):
        raise FileNotFoundError(f"{PRODUCER_METRICS_FILE} not found.")
    
    with open(PRODUCER_METRICS_FILE, mode="r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        metrics = list(reader)[-1]  # Get the last row (most recent metrics)
        return float(metrics["Total Processed Images"]), float(metrics["Throughput (images/s)"]), float(metrics["Average Latency (s)"])

# Function to fetch current number of partitions
def get_current_partition_count(topic, bootstrap_servers):
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    partitions = consumer.partitions_for_topic(topic)
    if partitions is None:
        raise ValueError(f"Topic '{topic}' does not exist or is not reachable.")
    return len(partitions)

# Function to update topic partitions
def update_partitions(admin_client, topic, new_partition_count):
    try:
        admin_client.create_partitions(
            topic_partitions={topic: NewPartitions(total_count=new_partition_count)}
        )
        print(f"Partitions for topic '{topic}' increased to {new_partition_count}.")
    except Exception as e:
        print(f"Error updating partitions: {e}")

# Function to get partition distribution
def get_partition_distribution(topic, bootstrap_servers):
    distribution = {}
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    for partition in KafkaConsumer(topic, bootstrap_servers=bootstrap_servers).partitions_for_topic(topic):
        tp = TopicPartition(topic=topic, partition=partition)
        consumer.assign([tp])
        consumer.seek_to_end(tp)
        distribution[f"{topic}-{partition}"] = consumer.position(tp)
    consumer.close()
    return distribution

# Run performance tests for each configuration
admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

try:
    for config in configurations:
        reset_test_files()

        # Get and update partitions
        current_partitions = get_current_partition_count(TOPIC_NAME, BOOTSTRAP_SERVERS)
        new_partition_count = current_partitions + 1
        update_partitions(admin_client, TOPIC_NAME, new_partition_count)

        # Start producer
        producer_process = subprocess.Popen(["python", "producer.py"])
        time.sleep(15)  # Let the producer run for a while
        producer_process.terminate()

        # Get producer metrics
        producer_processed_count, producer_throughput, producer_latency = get_producer_metrics()

        # Start consumer
        consumer_start_time = time.time()
        consumer_process = subprocess.Popen(["python", "consumer.py"])
        time.sleep(15)  # Let the consumer run for a while
        consumer_process.terminate()
        consumer_end_time = time.time()

        # Consumer metrics
        consumer_duration = consumer_end_time - consumer_start_time
        consumer_throughput = producer_processed_count / consumer_duration
        consumer_latency = consumer_duration / producer_processed_count
        consumer_lag = max(0, int(producer_processed_count - (consumer_duration * consumer_throughput)))

        # Partition distribution
        partition_distribution = get_partition_distribution(TOPIC_NAME, BOOTSTRAP_SERVERS)

        # Save results
        with open(RESULTS_FILE, mode="a", newline="") as file:
            writer = csv.writer(file)
            writer.writerow([
                config['batch_size'], config['linger_ms'], config['compression_type'], new_partition_count,
                round(producer_throughput, 2), round(consumer_throughput, 2),
                round(producer_latency, 4), round(consumer_latency, 4),
                consumer_lag, str(partition_distribution)
            ])

except ValueError as e:
    print(f"Error: {e}")
    exit(1)
except KeyboardInterrupt:
    print("\nExecution interrupted by the user. Exiting...")
finally:
    print("Script terminated. All resources have been released.")
