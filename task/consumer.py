import base64
import os
import json
import csv
import time
from kafka import KafkaConsumer

# Kafka configuration
TOPIC_NAME = 'image-data'
BOOTSTRAP_SERVERS = ['localhost:9092']
GROUP_ID = 'image-consumer-group'  # Unique consumer group ID

# Initialize output directory and CSV file
output_directory = "output_images"
os.makedirs(output_directory, exist_ok=True)

csv_file_path = "annotation.csv"
metrics_csv_file = "consumer_metrics.csv"

# Ensure the CSV file for annotations exists with headers
if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["image_path", "annotation"])  # Add headers

# Ensure the CSV file for metrics exists with headers
if not os.path.exists(metrics_csv_file):
    with open(metrics_csv_file, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Total Processed Messages", "Total Time (s)", "Average Latency (s)", "Throughput (messages/s)"])

# Deduplication set
processed_annotations = set()

# Load existing annotations from CSV
if os.path.exists(csv_file_path):
    with open(csv_file_path, mode="r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)  # Skip the header
        for row in reader:
            if row:
                processed_annotations.add(row[1])  # Add annotations to the set

# Kafka Consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    group_id=GROUP_ID,  # Ensures offsets are saved for this group
    enable_auto_commit=True,  # Automatically commit offsets
    auto_offset_reset='earliest',  # Start from the earliest message if no offsets exist
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Save metrics to CSV
def save_metrics_to_csv(processed_count, total_time, avg_latency, throughput):
    """Save consumer performance metrics to a CSV file."""
    with open(metrics_csv_file, mode="a", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([processed_count, f"{total_time:.2f}", f"{avg_latency:.4f}", f"{throughput:.2f}"])

# Process Kafka messages
def process_messages(consumer):
    print("Processing messages...")
    processed_count = 0
    start_time = time.time()

    for message in consumer:
        data = message.value
        annotation = data.get("annotation")
        image_data = data.get("image_file")

        # Skip duplicate annotations
        if annotation in processed_annotations:
            print(f"Skipped duplicate annotation: {annotation}")
            continue

        # Save image to output directory
        if annotation and image_data:
            processed_annotations.add(annotation)
            image_path = os.path.join(output_directory, f"{annotation}.png")
            with open(image_path, "wb") as img_file:
                img_file.write(base64.b64decode(image_data))

            print(f"Processed and saved: {image_path} with annotation: {annotation}")

            # Write to the CSV file
            with open(csv_file_path, mode="a", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow([image_path, annotation])

            # Increment processed message count
            processed_count += 1

        else:
            print(f"Invalid message: {message.value}")
        
        # Metrics calculation after each message batch (you can adjust batch size as needed)
        if processed_count == 192:  # Because I gave Up
            total_time = time.time() - start_time
            avg_latency = total_time / processed_count if processed_count else 0
            throughput = processed_count / total_time if total_time else 0

            print("\n===== Consumer Performance Metrics =====")
            print(f"Total Processed Messages: {processed_count}")
            print(f"Total Time: {total_time:.2f} seconds")
            print(f"Average Latency: {avg_latency:.4f} seconds")
            print(f"Throughput: {throughput:.2f} messages/second")
            print("========================================\n")

            # Save metrics to CSV
            save_metrics_to_csv(processed_count, total_time, avg_latency, throughput)

    # Final metrics for any remaining messages
    total_time = time.time() - start_time
    avg_latency = total_time / processed_count if processed_count else 0
    throughput = processed_count / total_time if total_time else 0

    print("\n===== Final Consumer Performance Metrics =====")
    print(f"Total Processed Messages: {processed_count}")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Average Latency: {avg_latency:.4f} seconds")
    print(f"Throughput: {throughput:.2f} messages/second")
    print("========================================\n")

    # Save final metrics to CSV
    save_metrics_to_csv(processed_count, total_time, avg_latency, throughput)

# Process messages
print("Consumer is waiting for messages...")
try:
    process_messages(consumer)
except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()
