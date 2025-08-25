from pyspark.sql import SparkSession, functions as F, types as T

KAFKA_BROKERS = "172.31.32.202:19092"
TOPIC = "events.raw.v1"
BRONZE_PATH = "s3a://predict-lake/bronze/events"
CHECKPOINT = "s3a://predict-lake/_checkpoints/bronze_events_v2"

schema = T.StructType([
    T.StructField("type", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("session_id", T.StringType()),
    T.StructField("ts", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("referrer", T.StringType()),
    T.StructField("ua", T.StringType()),
    T.StructField("action", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("price", T.DoubleType()),
    T.StructField("position", T.IntegerType()),
    T.StructField("timestamp", T.StringType()),
])

print("Creating Spark session...")
spark = SparkSession.builder.appName("bronze-stream-simple").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Attempting to connect to Kafka...")
print(f"Kafka brokers: {KAFKA_BROKERS}")
print(f"Topic: {TOPIC}")

try:
    # Read from Kafka
    raw = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load())

    print("✓ Kafka connection successful!")
    
    # Parse messages
    v = raw.selectExpr(
        "CAST(value AS STRING) AS v",
        "timestamp AS kafka_ts",
        "offset AS kafka_offset",
        "partition AS kafka_partition",
        "topic"
    )

    # Handle timestamp parsing
    ts_str = F.coalesce(F.get_json_object("v", "$.ts"),
                        F.get_json_object("v", "$.timestamp"))
    ts_clean = F.regexp_replace(ts_str, "Z$", "+00:00")
    event_time = F.coalesce(
        F.to_timestamp(ts_clean, "yyyy-MM-dd'\''T'\''HH:mm:ss.SSSXXX"),
        F.to_timestamp(ts_clean, "yyyy-MM-dd'\''T'\''HH:mm:ssXXX"),
        F.col("kafka_ts")
    )

    with_parsed = v.withColumn("p", F.from_json(F.col("v"), schema))

    bronze = (with_parsed
        .withColumn("event_time", event_time)
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("ingest_ts", F.current_timestamp())
        .select(
            "topic", "kafka_partition", "kafka_offset",
            "event_time", "event_date", "ingest_ts",
            F.col("v").alias("raw_json"),
            F.col("p.type").alias("type"),
            F.col("p.user_id").alias("user_id"),
            F.col("p.session_id").alias("session_id"),
            F.col("p.url").alias("url"),
            F.col("p.referrer").alias("referrer"),
            F.col("p.ua").alias("ua"),
            F.col("p.action").alias("action"),
            F.col("p.product_id").alias("product_id"),
            F.col("p.price").alias("price"),
            F.col("p.position").alias("position")
        )
    )

    print("✓ Data transformation setup successful!")
    print("Starting debug console output (30 seconds)...")

    # Debug for 30 seconds first
    debug_query = (bronze.writeStream
                  .format("console")
                  .option("numRows", 5)
                  .option("truncate", False)
                  .trigger(processingTime="5 seconds")
                  .start())

    print("Send some POST requests to your API now!")
    debug_query.awaitTermination(timeout=30)
    debug_query.stop()

    print("Starting production streaming to MinIO...")
    
    # Production streaming
    query = (bronze.writeStream
        .format("parquet")
        .option("path", BRONZE_PATH)
        .option("checkpointLocation", CHECKPOINT)
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start())

    print("✓ Production streaming query started!")
    print(f"Writing to: {BRONZE_PATH}")
    print("Query is running... Press Ctrl+C to stop.")

    query.awaitTermination()

except Exception as e:
    print(f"✗ Error in streaming job: {e}")
    import traceback
    traceback.print_exc()
finally:
    print("Stopping Spark session...")
    spark.stop()
    print("Done.")