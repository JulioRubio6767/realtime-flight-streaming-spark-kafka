# ✈️ Pipeline de Big Data en Tiempo Real: PySpark, HDFS y Streaming Continuo con Apache Kafka

🌐 **Idiomas / Languages:** [🇪🇸 Versión en Español](README.es.md) | [🇬🇧 English Version](README.md)

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%2B-red?logo=apachespark)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Streaming%20Distribuido-black?logo=apachekafka)
![HDFS](https://img.shields.io/badge/Hadoop-HDFS-yellow?logo=apachehadoop)
![GCP](https://img.shields.io/badge/Google%20Cloud-Dataproc%20%7C%20GCS-blue?logo=googlecloud)
![Python](https://img.shields.io/badge/PySpark-Batch%20%26%20Streaming-blue?logo=python)

Arquitectura de ingeniería de datos distribuida para entornos empresariales, implementando tanto **procesamiento por lotes (Batch)** sobre HDFS en la nube como **procesamiento en streaming continuo de baja latencia** con Apache Kafka y Spark Structured Streaming.

---

## 📌 Diseño de la Arquitectura

El sistema implementa dos paradigmas complementarios de procesamiento de datos:

```
[Datos Históricos de Vuelos / GCS] ──▶ [Pipeline Batch en PySpark] ──▶ [Almacenamiento HDFS & Analítica en Parquet]
                                                                                │
[Telemetría en Vivo / Tópico Kafka] ──▶ [Spark Structured Streaming] ──▶ [Ventanas Deslizantes y Alertas en Tiempo Real]
```

1. **Capa Batch (`01_batch_processing_pyspark_hdfs.ipynb`):**
   * Extracción masiva y distribuida desde Google Cloud Storage (GCS) y almacenamiento local.
   * Limpieza de datos a gran escala, ajuste de esquemas y particionado optimizado en PySpark.
   * Persistencia de métricas agregadas en HDFS y formato Parquet para análisis corporativo.

2. **Capa Streaming / Tiempo Real (`02_structured_streaming_spark_kafka.ipynb`):**
   * Ingesta continua de mensajes desde tópicos de Apache Kafka.
   * Aplicación de esquemas tipados y transformaciones en vivo (`readStream` / `writeStream`).
   * Agregaciones temporales basadas en ventanas deslizantes (*sliding windows*) calculando demoras de arribo y congestión de rutas en tiempo real.

---

## 🛠️ Capacidades y Módulos Técnicos

* **Orquestación en Clúster:** Compatible con clústeres locales y entornos administrados en la nube (**Google Cloud Dataproc**).
* **Tolerancia a Fallos y Gestión de Estado:** Configuración de directorios de *checkpoints* para garantizar cero pérdida de datos en los offsets de Kafka.
* **Transformaciones Complejas:**
  * Métricas de demoras rodantes por aerolínea y trayecto.
  * Gestión de marcas de agua (*watermarking*) para manejar eventos de telemetría que llegan con retraso.

---

## 🚀 Configuración y Ejecución

### Requisitos Previos
* Java JDK 11+
* Apache Spark 3.x con soporte para PySpark
* Broker de Apache Kafka activo (local en `localhost:9092` o en la nube)

### 1. Ejecutar Analítica Batch
```bash
jupyter notebook 01_batch_processing_pyspark_hdfs.ipynb
```

### 2. Ejecutar Streaming en Tiempo Real
Asegúrate de que Kafka esté transmitiendo datos al tópico y ejecuta:
```bash
jupyter notebook 02_structured_streaming_spark_kafka.ipynb
```

---

## 👤 Autor
**Julio Andrés Rubio Echeverría**  
*Analytics & Machine Learning Engineer*  
* [LinkedIn](https://www.linkedin.com/in/juliorubio-data/)
* [GitHub](https://github.com/JulioRubio6767)
