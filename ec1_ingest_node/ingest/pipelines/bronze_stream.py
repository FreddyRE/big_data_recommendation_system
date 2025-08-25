from pyspark.sql import SparkSession, functions as F

KAFKA_BROKERS = "172.31.32.202:19092" 
TOPIC = "events_raw"

spark = (SparkSession.builder
         .appName("bronze-stream")
         .getOrCreate())

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BROKERS)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .option("failOnDataLoss", "false")
       .load())

bronze = (raw
          .select(
              F.col("timestamp").alias("ts"),
              F.to_date("timestamp").alias("dt"),
              F.col("topic"),
              F.col("partition"),
              F.col("offset"),
              F.col("key").cast("string").alias("key"),
              F.col("value").cast("string").alias("value")
          ))

q = (bronze.writeStream
     .format("json")
     .option("path", "s3a://predict-lake/bronze/events")  # data root
     .option("checkpointLocation", "s3a://predict-lake/bronze/_checkpoints/events")
     .partitionBy("dt")
     .outputMode("append")
     .trigger(processingTime="10 seconds")
     .start())

q.awaitTermination()