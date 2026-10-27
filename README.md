# ✈️ Real-Time Big Data Pipeline: PySpark, HDFS & Structured Streaming with Apache Kafka

🌐 **Idiomas / Languages:** [🇪🇸 Versión en Español](README.es.md) | [🇬🇧 English Version](README.md)


![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%2B-red?logo=apachespark)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Distributed%20Streaming-black?logo=apachekafka)
![HDFS](https://img.shields.io/badge/Hadoop-HDFS-yellow?logo=apachehadoop)
![GCP](https://img.shields.io/badge/Google%20Cloud-Dataproc%20%7C%20GCS-blue?logo=googlecloud)
![Python](https://img.shields.io/badge/PySpark-Batch%20%26%20Streaming-blue?logo=python)

An enterprise-grade, distributed data engineering architecture implementing both **Batch processing** on Hadoop Distributed File System (HDFS) and **Low-latency Real-Time Streaming** with Apache Kafka and Spark Structured Streaming.

---

## 📌 Architecture Design

The system is split into two complementary processing paradigms:

```
[Historical Flights CSV / GCS] ──▶ [PySpark Batch Pipeline] ──▶ [HDFS Storage & Parquet Analytics]
                                                                        │
[Live Flight Telemetry / Kafka Topic] ──▶ [Spark Structured Streaming] ──▶ [Sliding Windows & Real-Time Alerts]
```

1. **Batch Layer (`01_batch_processing_pyspark_hdfs.ipynb`):**
   * Distributed extraction from Google Cloud Storage (GCS) and local clusters.
   * High-throughput data cleaning, column type casting, and partition tuning in PySpark.
   * Persistence of aggregated metrics into HDFS and Parquet for analytical consumption.

2. **Speed / Streaming Layer (`02_structured_streaming_spark_kafka.ipynb`):**
   * Real-time message consumption from Apache Kafka topic.
   * Continuous schema enforcement and streaming transformations (`readStream` / `writeStream`).
   * Windowed aggregations (tumbling/sliding windows) computing real-time flight arrival delays and route bottlenecks.

---

## 🛠️ Key Capabilities & Code Modules

* **Cluster Orchestration:** Compatible with local multinode setups and cloud-managed environments (**Google Cloud Dataproc**).
* **Fault Tolerance & State Management:** Checkpoint directories configured for zero data loss in Kafka streaming offsets.
* **Complex Transformations:**
  * Rolling delay metrics by carrier and route.
  * Windowed watermarking to handle late-arriving telemetry events.

---

## 🚀 Setup & Execution

### Prerequisites
* Java JDK 11+
* Apache Spark 3.x with PySpark
* Apache Kafka (running broker at `localhost:9092` or cloud endpoint)

### 1. Run Batch Analytics
```bash
jupyter notebook 01_batch_processing_pyspark_hdfs.ipynb
```

### 2. Run Real-Time Streaming
Ensure your Kafka broker is producing to the target topic, then launch:
```bash
jupyter notebook 02_structured_streaming_spark_kafka.ipynb
```

---

## 👤 Author
**Julio Andrés Rubio Echeverría**  
*Analytics & Machine Learning Engineer*  
* [LinkedIn](https://www.linkedin.com/in/juliorubio-data/)
* [GitHub](https://github.com/JulioRubio6767)
