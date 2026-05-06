# 🛒 Big Data E-Commerce Case Study

A complete Big Data pipeline for a global e-commerce company collecting **2.8 TB/day** across 4 data sources — built with Apache Kafka, PySpark, HDFS, and visualized via dashboards.

---

## 📌 Problem Statement

A worldwide e-commerce company faces:
- **Slow reporting** — dashboards take hours to refresh
- **No single customer view** — data is siloed across systems
- **Data Scientist struggles** — no clean, accessible data for ML
- **Security concerns** — uncontrolled access to sensitive data

---

## 📊 Data Sources

| Source         | Volume/Day | Format        | Type           |
|----------------|------------|---------------|----------------|
| Weblogs        | 2 TB       | JSON / text   | Unstructured   |
| Transactions   | 500 GB     | CSV / Parquet | Structured     |
| Customer Reviews | 200 GB   | JSON + images | Semi-structured|
| Social Media   | 100 GB     | JSON          | Unstructured   |

**Total: ~2.8 TB/day**

---

## 🏗️ Architecture

```
[Data Sources]
     │
     ▼
[Apache Kafka]  ← Real-time ingestion (4 topics)
     │
     ▼
[HDFS / AWS S3]  ← Raw Data Lake storage
     │
     ▼
[Apache Spark]  ← Batch + stream processing
     │
     ▼
[Data Warehouse (Hive / Redshift)]
     │
     ▼
[Dashboard (Power BI / Superset)]  +  [ML Models]
```

---

## 🛠️ Tech Stack

| Layer        | Technology                        |
|--------------|-----------------------------------|
| Ingestion    | Apache Kafka                      |
| Storage      | Hadoop HDFS / AWS S3              |
| Processing   | Apache Spark (PySpark)            |
| Warehousing  | Apache Hive / AWS Redshift        |
| Orchestration| Apache Airflow                    |
| Security     | Apache Ranger + SSL/TLS           |
| Visualization| Apache Superset / Power BI        |
| ML           | MLflow + Jupyter Notebooks        |

---

## 📁 Project Structure

```
big-data-ecommerce/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│   ├── raw/           ← Raw sample data files
│   └── processed/     ← Cleaned/transformed data
├── src/
│   ├── ingestion/
│   │   └── kafka_producer.py
│   ├── processing/
│   │   └── spark_pipeline.py
│   ├── storage/
│   │   └── hdfs_handler.py
│   └── utils/
│       └── config.py
├── notebooks/
│   └── analysis.ipynb
├── configs/
│   └── config.yaml
├── docs/
│   └── report.pdf
└── scripts/
    └── run_pipeline.sh
```

---

## 🚀 How to Run

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Settings
Edit `configs/config.yaml` with your Kafka broker address and HDFS path.

### 3. Start Kafka Producer (Data Ingestion)
```bash
python src/ingestion/kafka_producer.py
```

### 4. Run Spark Processing Pipeline
```bash
python src/processing/spark_pipeline.py
```

### 5. Or Run Everything at Once
```bash
bash scripts/run_pipeline.sh
```

---

## ✅ Problems Solved

| Problem                  | Solution Implemented                                      |
|--------------------------|-----------------------------------------------------------|
| Slow Reporting           | Spark SQL + pre-aggregated Parquet views                  |
| No Single Customer View  | Customer 360 table joining all 4 sources on `customer_id` |
| Data Scientist Struggles | Jupyter notebook + clean feature store output             |
| Security Concerns        | Role-based access config + encryption settings            |

---

## 👨‍💻 Authors : Subhanil Bhaduri (RA2512039010007) and Anirban Mondal (RA2512039010002)

Submitted as part of Big Data course case study.
