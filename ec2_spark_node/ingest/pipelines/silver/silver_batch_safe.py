from pyspark.sql import SparkSession, functions as F
import sys
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("silver-batch-safe").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Get date
if len(sys.argv) > 1:
    process_date = sys.argv[1]
else:
    yesterday = datetime.now() - timedelta(days=1)
    process_date = yesterday.strftime('%Y-%m-%d')

print(f"Processing Silver batch for date: {process_date}")

try:
    # Read bronze events
    silver_events = (spark.read
        .format("parquet")
        .load("s3a://predict-lake/bronze/events")
        .filter(F.col("event_date") == process_date))

    # Check if data exists
    count = silver_events.count()
    print(f"Found {count} events for {process_date}")
    
    if count == 0:
        print(f"No data found for {process_date}. Exiting.")
        sys.exit(0)
    
    # Process only if we have data
    user_profiles = (silver_events
        .filter(F.col("user_id").isNotNull())
        .groupBy("user_id", "event_date")
        .agg(
            F.count("*").alias("total_events"),
            F.countDistinct("session_id").alias("sessions_count"),
            F.sum(F.when(F.col("action") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
            F.sum(F.when(F.col("action") == "purchase", 1).otherwise(0)).alias("purchases"),
            F.avg("price").alias("avg_price_interest")
        )
        .limit(1000))
    
    print(f"Writing {user_profiles.count()} user profiles...")
    (user_profiles.write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("event_date")
        .save("s3a://predict-lake/silver/user_profiles"))

    print("Silver batch processing completed successfully")
    
except Exception as e:
    print(f"Error processing Silver batch: {str(e)}")
    sys.exit(1)
finally:
    spark.stop()
