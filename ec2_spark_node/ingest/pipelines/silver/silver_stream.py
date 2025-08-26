from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

BRONZE_PATH = "s3a://predict-lake/bronze/events"
SILVER_EVENTS_PATH = "s3a://predict-lake/silver/events"
SILVER_SESSIONS_PATH = "s3a://predict-lake/silver/sessions"
CHECKPOINT_EVENTS = "s3a://predict-lake/_checkpoints/silver_events"
CHECKPOINT_SESSIONS = "s3a://predict-lake/_checkpoints/silver_sessions"

print("Creating Spark session for Silver layer...")
spark = SparkSession.builder.appName("silver-stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("Reading from Bronze layer...")
bronze_events = (spark.readStream
    .format("parquet")
    .option("path", BRONZE_PATH)
    .load())

# Data Quality & Cleaning
print("Applying data quality rules...")
cleaned_events = (bronze_events
    # Remove nulls in critical fields
    .filter(F.col("type").isNotNull())
    .filter(F.col("user_id").isNotNull())
    .filter(F.col("session_id").isNotNull())
    .filter(F.col("event_time").isNotNull())
    
    # Remove  test data
    .filter(~F.col("user_id").rlike("(?i)(test|demo|admin)"))
    
    # Standardize event types
    .withColumn("event_type_clean", 
        F.when(F.col("type") == "interaction", F.col("action"))
        .when(F.col("type") == "pageview", "page_view")
        .otherwise(F.col("type")))
    
    # Add derived fields
    .withColumn("event_hour", F.date_trunc("hour", "event_time"))
    .withColumn("day_of_week", F.dayofweek("event_time"))
    .withColumn("hour_of_day", F.hour("event_time"))
    
    # Price cleaning
    .withColumn("price_clean", 
        F.when((F.col("price") > 0) & (F.col("price") < 10000), F.col("price"))
        .otherwise(None))
)

# Deduplication window
window_dedup = Window.partitionBy("user_id", "session_id", "event_type_clean", "product_id") \
    .orderBy(F.col("event_time").desc())

# Remove duplicates (keep latest)
deduplicated_events = (cleaned_events
    .withColumn("row_number", F.row_number().over(window_dedup))
    .filter(F.col("row_number") == 1)
    .drop("row_number"))

print("Starting Silver events stream...")
events_query = (deduplicated_events
    .writeStream
    .format("parquet")
    .option("path", SILVER_EVENTS_PATH)
    .option("checkpointLocation", CHECKPOINT_EVENTS)
    .partitionBy("event_date")
    .outputMode("append")
    .trigger(processingTime='120 seconds')  # Every 2 minutes
    .start())

print("Silver events streaming started...")
events_query.awaitTermination()