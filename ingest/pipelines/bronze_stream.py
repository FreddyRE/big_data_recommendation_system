from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, to_date
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, IntegerType

schema = StructType([
    StructField("type", StringType()),
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("ts", StringType()),
    StructField("url", StringType()),
    StructField("referrer", StringType()),
    StructField("ua", StringType()),
    StructField("action", StringType()),
    StructField("product_id", StringType()),
    StructField("price", DoubleType()),
    StructField("position", IntegerType()),
])

KAFKA_BROKERS = "redpanda:9092"
TOPIC = "events.raw.v1"
BRONZE_PATH = "s3a://predict-lake/bronze/events"
CHECKPOINT = "s3a://predict-lake/_checkpoints/bronze_events"

spark = SparkSession.builder.appName("bronze-stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BROKERS)
      .option("subscribe", TOPIC)
      .option("startingOffsets", "latest")
      .option("maxOffsetsPerTrigger", "200")
      .load())

json_str = df.selectExpr("CAST(value AS STRING) as json")
parsed = json_str.select(from_json(col("json"), schema).alias("p"), col("json").alias("raw"))

bronze = parsed.select(
    col("p.type").alias("type"),
    col("p.user_id").alias("user_id"),
    col("p.session_id").alias("session_id"),
    to_timestamp(col("p.ts")).alias("event_ts"),
    current_timestamp().alias("ingest_ts"),
    col("p.url").alias("url"),
    col("p.referrer").alias("referrer"),
    col("p.ua").alias("ua"),
    col("p.action").alias("action"),
    col("p.product_id").alias("product_id"),
    col("p.price").alias("price"),
    col("p.position").alias("position"),
    to_date(to_timestamp(col("p.ts"))).alias("event_date"),
    col("raw").alias("raw_json")
)

q = (bronze.writeStream
     .format("parquet")
     .option("path", BRONZE_PATH)
     .option("checkpointLocation", CHECKPOINT)
     .partitionBy("event_date")
     .outputMode("append")
     .start())

q.awaitTermination()