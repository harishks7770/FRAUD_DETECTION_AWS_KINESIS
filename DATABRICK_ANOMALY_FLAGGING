%sql
-- Replace 'hive_metastore' with whatever printed out above if different
CREATE VOLUME IF NOT EXISTS workspace.default.kinesis_checkpoints;
-------------------------------------------------------------------------------
from pyspark.sql.functions import col, from_json, expr, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Explicitly maps to the schema coming from your EC2 instance generator
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True)
])

print("✅ Data schema format locked in.")
------------------------------------------------------------------------------
# Configure the live stream reader pointed to North Virginia (us-east-1)
raw_kinesis_stream = spark.readStream \
    .format("kinesis") \
    .option("streamName", "FraudTransactionsStream") \
    .option("region", "<YOUR_REGION>") \
    .option("awsAccessKey", "<YOUR_ACCESS_KEY>") \
    .option("awsSecretKey", "<YOUR_SECRET_KEY>") \
    .option("initialPosition", "trim_horizon") \
    .load()

# Parse out the data segment from the stream metadata envelope
parsed_stream = raw_kinesis_stream \
    .selectExpr("CAST(data AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), transaction_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast("timestamp"))

print("📡 Live consumer link established for us-east-1.")

---------------------------------------------------------------------------------
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

print("🧠 Fraud detection engine ready.")
---------------------------------------------------------------------------------
query = fraud_alerts.writeStream \
    .format("memory") \
    .queryName("live_fraud_table") \
    .outputMode("complete") \
    .option("checkpointLocation", "/Volumes/workspace/default/kinesis_checkpoints/fraud_stream_v1") \
    .trigger(availableNow=True) \
    .start()
----------------------------------------------------------------------------------
# Cell 6: Write the live fraud alerts permanently into a Delta Table
delta_query = fraud_alerts.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/Volumes/workspace/default/kinesis_checkpoints/delta_fraud_audit_v1") \
    .trigger(availableNow=True) \
    .toTable("workspace.default.fraud_audit_log")

print("💾 Stream anchored! Processed alerts are now streaming into a permanent Delta Table.")
-----------------------------------------------------------------------------------
display(spark.sql("SELECT * FROM workspace.default.fraud_audit_log ORDER BY total_velocity_spend DESC"))
-----------------------------------------------------------------------------------
display(spark.sql("SELECT * FROM live_fraud_table ORDER BY total_velocity_spend DESC"))
