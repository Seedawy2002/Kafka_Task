from kafka import KafkaProducer
import os
import base64
import json
import time
import csv

# Kafka configuration
TOPIC_NAME = 'image-data'
BOOTSTRAP_SERVERS = ['localhost:9092']

# Path to the directory containing the labeled images
image_directory = "Labeled_Images"
processed_files_path = "processed_files.txt"
metrics_csv_file = "producer_metrics.csv"

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')  # Use UTF-8 for JSON
)

# Load processed files
def load_processed_files():
    """Load a list of already processed files."""
    if os.path.exists(processed_files_path):
        with open(processed_files_path, "r", encoding="utf-8") as f:
            return set(f.read().splitlines())
    return set()

processed_files = load_processed_files()

def encode_image(image_path):
    """Encode image to Base64 format."""
    with open(image_path, "rb") as img_file:
        return base64.b64encode(img_file.read()).decode('utf-8')

def mark_as_processed(image_name):
    """Add the image to the processed files list and save it."""
    processed_files.add(image_name)
    with open(processed_files_path, "a", encoding="utf-8") as f:
        f.write(image_name + "\n")

def process_image(image_path):
    """Process and send a single image to Kafka."""
    image_name = os.path.basename(image_path)
    if image_name in processed_files:
        return

    if os.path.isfile(image_path):
        # Serialize the image and prepare the message
        serialized_image = encode_image(image_path)
        annotation = image_name.split('.')[0].strip()
        message = {
            "image_file": serialized_image,
            "annotation": annotation
        }
        # Send message to Kafka
        producer.send(TOPIC_NAME, value=message)
        print(f"Sent message for image: {image_name}")

        # Mark the file as processed
        mark_as_processed(image_name)

# Save producer metrics to CSV
def save_metrics_to_csv(processed_count, total_time, avg_latency, throughput):
    """Save producer performance metrics to a CSV file."""
    file_exists = os.path.exists(metrics_csv_file)
    with open(metrics_csv_file, mode="a", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        if not file_exists:
            writer.writerow(["Total Processed Images", "Total Time (s)", "Average Latency (s)", "Throughput (images/s)"])
        writer.writerow([processed_count, f"{total_time:.2f}", f"{avg_latency:.4f}", f"{throughput:.2f}"])

# Process all images in the folder
def process_existing_images():
    """Process existing images in the directory and measure metrics."""
    print("Processing existing images...")
    start_time = time.time()
    processed_count = 0

    for image_name in os.listdir(image_directory):
        image_path = os.path.join(image_directory, image_name)
        process_image(image_path)
        processed_count += 1

    end_time = time.time()
    total_time = end_time - start_time
    avg_latency = total_time / processed_count if processed_count else 0
    throughput = processed_count / total_time if total_time else 0

    print("\n===== Producer Performance Metrics =====")
    print(f"Total Processed Images: {processed_count}")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Average Latency: {avg_latency:.4f} seconds")
    print(f"Throughput: {throughput:.2f} images/second")
    print("========================================\n")

    # Save metrics to CSV
    save_metrics_to_csv(processed_count, total_time, avg_latency, throughput)

# Main logic to monitor and process images
def monitor_directory():
    """Monitor the directory for new images and process them."""
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

    class ImageHandler(FileSystemEventHandler):
        def on_created(self, event):
            if event.is_directory:
                return
            print(f"New file detected: {event.src_path}")
            process_image(event.src_path)

    observer = Observer()
    event_handler = ImageHandler()
    observer.schedule(event_handler, path=image_directory, recursive=False)

    # Process existing images before starting the observer
    process_existing_images()
    observer.start()

    print("Producer is monitoring for new images...")

    try:
        while True:
            time.sleep(1)  # Keep the script running
    except KeyboardInterrupt:
        print("Producer stopped.")
        observer.stop()
    finally:
        observer.join()
        producer.close()

# Run the producer
if __name__ == "__main__":
    if not os.path.exists(image_directory):
        print(f"Error: Directory '{image_directory}' does not exist.")
    else:
        monitor_directory()
