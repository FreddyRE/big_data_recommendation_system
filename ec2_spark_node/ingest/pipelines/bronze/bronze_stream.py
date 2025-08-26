from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("bronze-fast").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Direct Kafka to Parquet with minimal processing
raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "172.31.32.202:19092")
    .option("subscribe", "events.raw.v1")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load())

# Minimal transformation - just store raw JSON with basic metadata
simple_bronze = raw.select(
    F.col("topic"),
    F.col("partition").alias("kafka_partition"),
    F.col("offset").alias("kafka_offset"),
    F.col("timestamp").alias("kafka_timestamp"),
    F.cast("value", "string").alias("raw_json"),
    F.current_timestamp().alias("ingest_timestamp")
)

# Direct to parquet without partitioning
query = (simple_bronze.writeStream
    .format("parquet")
    .option("path", "s3a://predict-lake/bronze/fast")
    .option("checkpointLocation", "s3a://predict-lake/_checkpoints/bronze_fast")
    .outputMode("append")
    .trigger(processingTime="15 seconds")  # Faster trigger
    .start())

print("Fast Bronze streaming started")
query.awaitTermination()