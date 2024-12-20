# Kafka Image Processing and Performance Monitoring
This project showcases an end-to-end image processing pipeline using Apache Kafka. It includes a producer to send images, a consumer to process them, and a monitoring system to evaluate performance. The system is designed to handle various edge cases and supports deployment using Docker.

## Table of Contents
1. [File Structure](#file-structure)
2. [Prerequisites](#prerequisites)
   - [Docker Installation](#docker-installation)
   - [Requirements](#requirements)
4. [Steps to Run](#steps-to-run)
   - [Run Kafka Producer](#run-kafka-producer)
   - [Run Kafka Consumer](#run-kafka-consumer)
   - [Monitor Kafka Performance](#monitor-kafka-performance)
5. [Kafka Configuration for Performance Testing](#kafka-configuration-for-performance-testing)
6. [Outputs](#outputs)
   - [Annotation File](#1-annotation-file)
   - [Producer Metrics](#2-producer-metrics)
   - [Consumer Metrics](#3-consumer-metrics)
   - [Kafka Performance Results](#4-kafka-performance-results)
7. [Error Handling](#error-handling)
8. [Deliverables](#deliverables)
9. [License](#license)
    
---

## File Structure
```
├── task/                             # Main project folder
│   ├── producer.py                   # Kafka producer script
│   ├── consumer.py                   # Kafka consumer script
│   ├── monitor.py                    # Kafka monitoring script
│   ├── annotation.csv                # CSV file for image annotations
│   ├── consumer_metrics.csv          # Consumer performance metrics
│   ├── producer_metrics.csv          # Producer performance metrics
│   ├── kafka_performance_results.csv # Performance metrics for Kafka
│   ├── processed_files.txt           # Tracks already processed files
│   ├── output_images/                # Folder for processed images (zipped)
│   │   └── ... (processed images saved here)
│   ├── Labeled_Images/               # Input images for processing (zipped)
│   │   └── ... (192 labeled images)
│   └── __init__.py                   # Initialization file
├── docker-compose.yml                # Docker Compose file for Kafka setup
├── Requirements.txt                  # Requirements file
├── README.md                         # Project documentation
├── LICENSE                           # License information
```

---

## Prerequisites

### Docker Installation

1. **Install Docker and Docker Compose**
   - Download and install Docker from [Docker Downloads](https://www.docker.com/).
   - Verify installation:
     ```bash
     docker --version
     docker-compose --version
     ```

2. **Set Up Kafka Services**
   - Use the provided `docker-compose.yml` file to automate Kafka setup:
     ```bash
     docker-compose up --build
     ```
### Requirements

To install and run the project, ensure the following dependencies are available:

- **Python 3.x**
- Libraries:
  - `kafka-python`: For Kafka producer and consumer operations.
  - `watchdog`: To monitor file system events in the producer script.
  - Built-in libraries:
    - `os`: For interacting with the operating system.
    - `time`: To measure execution times.
    - `csv`: For handling CSV files.
    - `json`: For working with JSON data.
    - `base64`: For encoding and decoding image files.
  - Any additional external libraries installed via `requirements.txt`.

Install all required dependencies using:
```bash
pip install -r requirements.txt
```

---

## Steps to Run

### Run Kafka Producer

#### Local
Run the producer script to read labeled images from `Labeled_Images/` and publish them:
```bash
python producer.py
```

#### Docker
Ensure Docker services are running; the producer is included as a service in the `docker-compose.yml`.

---

### Run Kafka Consumer

#### Local
Run the consumer script to process images and save results in `output_images/`:
```bash
python consumer.py
```

#### Docker
The consumer is included as a service in the Docker container.

---

### Monitor Kafka Performance

#### Local
Run the monitoring script to evaluate Kafka performance metrics:
```bash
python monitor.py
```

#### Docker
The monitoring logic is included as a service in Docker.

---
## Kafka Configuration for Performance Testing

The project includes predefined configurations for Kafka producer testing to evaluate different performance scenarios. These configurations adjust key Kafka parameters such as batch size, linger time, and compression type.

#### Testing Configurations

The following configurations are used during performance testing:

| **Batch Size** | **Linger (ms)** | **Compression** |
|----------------|-----------------|-----------------|
| 16384          | 0               | none            |
| 32768          | 5               | gzip            |
| 65536          | 10              | snappy          |
| 128000         | 20              | none            |
| 512000         | 50              | gzip            |

#### **How It Works**

1. **Batch Size**: Defines the maximum size of messages sent in a single batch. Larger batch sizes reduce network overhead but may increase latency.
2. **Linger Time (ms)**: Determines how long the producer waits before sending a batch of messages. Adding a delay allows more messages to be batched together.
3. **Compression**: Specifies the compression algorithm used to reduce the size of messages. Available options:
   - `none`: No compression.
   - `gzip`: Standard compression for smaller message sizes.
   - `snappy`: Fast compression optimized for performance.

---
## Outputs

### 1. Annotation File
`annotation.csv` logs processed image paths and annotations:
```csv
image_path,annotation
output_images/2024-09-10.png,2024-09-10
output_images/2024-09-11.png,2024-09-11
```

### 2. Producer Metrics
`producer_metrics.csv` logs Kafka producer performance metrics:
```csv
Total Processed Images,Total Time (s),Average Latency (s),Throughput (images/s)
192,1.48,0.0077,130.13
```

### 3. Consumer Metrics
`consumer_metrics.csv` logs Kafka consumer performance metrics:
```csv
Total Processed Messages,Total Time (s),Average Latency (s),Throughput (messages/s)
192,2.34,0.0122,82.05
```

### 4. Kafka Performance Results
`kafka_performance_results.csv` logs performance metrics for various configurations:
```csv
Batch Size,Linger (ms),Compression,Num Partitions,Producer Throughput (msg/sec),Consumer Throughput (msg/sec),Producer Latency (sec/msg),Consumer Latency (sec/msg),Consumer Lag,Partition Distribution
16384,0,none,4,128.3,12.78,0.0078,0.0782,0,"{'image-data7-0': 518, 'image-data7-1': 338}"
```
---
## Error Handling

1. **Producer Failure**
   - Automatically retries unsent messages.
   - Logs errors in the console for debugging.

2. **Consumer Failure**
   - Resumes processing from the last committed offset on restart.
   - Handles invalid messages by skipping them and logging errors.

3. **Kafka Downtime**
   - The producer and consumer pause operations until Kafka services are restored.

4. **Unhandled Errors**
   - All errors are logged with detailed stack traces.

---

## Deliverables

1. **Labeled Dataset**
   - `Labeled_Images/`: Contains 192 labeled images.

2. **Producer Code**
   - `producer.py`: Publishes serialized images and annotations to Kafka.

3. **Consumer Code**
   - `consumer.py`: Reads, deserializes, and saves image data locally.

4. **Performance Results**
   - `kafka_performance_results.csv`: Contains performance metrics for tested configurations.
---

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE) file for details.
