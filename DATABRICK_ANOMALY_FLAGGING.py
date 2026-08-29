# Databricks notebook source
# DBTITLE 1,Cell 1: Create Volume for Kinesis Stream Checkpoints
%sql
CREATE VOLUME IF NOT EXISTS workspace.default.kinesis_checkpoints;

# COMMAND ----------

# DBTITLE 2,Cell 2: Imports & StructType Schema Definition
from pyspark.sql.functions import col, from_json, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Schema precisely matching the EC2 Python producer payload structure
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True)
])

print("✅ Data schema format locked in.")

# COMMAND ----------

# DBTITLE 3,Cell 3: Live Kinesis Stream Reader Setup
# Establishes connection to Kinesis in ap-south-1 using cluster IAM role authentication
raw_kinesis_stream = spark.readStream \
    .format("kinesis") \
    .option("streamName", "FraudTransactionsStream") \
    .option("region", "ap-south-1") \
    .option("initialPosition", "latest") \
    .load()

# Parse binary stream data into string payload and apply structural JSON schema
parsed_stream = raw_kinesis_stream \
    .selectExpr("CAST(data AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), transaction_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast("timestamp"))

print("📡 Live consumer link established for ap-south-1.")

# COMMAND ----------

# DBTITLE 4,Cell 4: Real-Time Fraud Detection Engine (Sliding Window Aggregation)
fraud_alerts = parsed_stream \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("user_id")
    ) \
    .agg(
        count("transaction_id").alias("transaction_count"),
        sum("amount").alias("total_velocity_spend")
    ) \
    .filter(col("transaction_count") > 3) \
    .select(
        col("user_id"),
        col("window.start").alias("alert_window_start"),
        col("window.end").alias("alert_window_end"),
        col("transaction_count"),
        col("total_velocity_spend")
    )

print("🧠 Fraud detection engine logic compiled successfully.")

# COMMAND ----------

# DBTITLE 5,Cell 5: Continuous Delta Lake Stream Sink
# Writes stream aggregations into a permanent Delta table every 10 seconds
delta_query = fraud_alerts.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/Volumes/workspace/default/kinesis_checkpoints/delta_fraud_audit_v1") \
    .trigger(processingTime="10 seconds") \
    .toTable("workspace.default.fraud_audit_log")

print("💾 Continuous stream anchored to Delta table: workspace.default.fraud_audit_log")

# COMMAND ----------

# DBTITLE 6,Cell 6: Query Processed Fraud Alerts
%sql
SELECT 
    user_id,
    alert_window_start,
    alert_window_end,
    transaction_count,
    ROUND(total_velocity_spend, 2) AS total_spend
FROM workspace.default.fraud_audit_log
ORDER BY alert_window_start DESC, total_velocity_spend DESC;
