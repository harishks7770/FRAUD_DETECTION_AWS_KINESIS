# 🛡️ Real-Time Financial Fraud Detection Pipeline

A high-throughput, real-time streaming pipeline designed to detect velocity-based financial fraud. A Python ingestion client running on **AWS EC2** generates ~115 transactions per second (~10 Million events/day) and streams them into **AWS Kinesis Data Streams**. **Databricks Structured Streaming** processes the live data using **Spark Watermarking** and **Sliding Windows** to detect rapid multi-transaction fraud attempts, writing real-time alerts to both an in-memory table and a permanent **Delta Lake** table for audit logging.

---


## 🏗️ Architecture & Data Flow

```text
┌─────────────────────────┐
│   AWS EC2 Generator     │  (Synthetic Producer ~115 TPS / ~10M Records/Day)
└────────────┬────────────┘
             │ (Boto3 / Dynamic Batch Ingestion)
             ▼
┌─────────────────────────┐
│   AWS Kinesis Stream    │  (FraudTransactionsStream Partitioned by User ID)
└────────────┬────────────┘
             │ (Kinesis Connector / Structured Streaming)
             ▼
┌─────────────────────────┐
│  Databricks Spark Engine│  (Schema Parsing + Watermarking + Sliding Windows)
└────────────┬────────────┘
             │ (Aggregation: >3 Transactions per 10-Min Window)
             ▼
┌─────────────────────────┼─────────────────────────┐
│                         │                         │
▼                         ▼                         ▼
┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
│ Unity Catalog    │  │ Memory Sink      │  │ Delta Table      │
│ Volume Checkpoint│  │ live_fraud_table │  │ fraud_audit_log  │
└──────────────────┘  └──────────────────┘  └──────────────────┘

## 🛠️ Tech Stack

* **Streaming & Ingestion:** AWS EC2 (Python 3.11, `boto3`, `Faker`) ➔ AWS Kinesis Data Streams
* **Processing & Analytics:** Apache Spark / PySpark (Databricks Structured Streaming, Watermarking, Sliding Windows)
* **Storage & Catalog:** Unity Catalog Volumes ➔ Delta Lake (`fraud_audit_log`)

---
