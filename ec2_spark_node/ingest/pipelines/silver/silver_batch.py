from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("silver-batch").getOrCreate()

# Read previous day's bronze data
yesterday = F.current_date() - 1
bronze = spark.read.format("delta").load("s3a://predict-lake/bronze/events") \
    .filter(F.col("event_date") == yesterday)

user_profiles = (bronze
    .groupBy("user_id", "event_date")
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("session_id").alias("sessions_count"),
        F.countDistinct("product_id").alias("unique_products_viewed"),
        F.sum(F.when(F.col("action") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
        F.sum(F.when(F.col("action") == "purchase", 1).otherwise(0)).alias("purchases"),
        F.avg("price").alias("avg_price_interest")
    ))

user_profiles.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("event_date") \
    .save("s3a://predict-lake/silver/user_profiles")