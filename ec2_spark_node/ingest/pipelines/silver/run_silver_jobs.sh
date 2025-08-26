#!/bin/bash

export SPARK_HOME=/opt/spark
export PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

echo "Starting Silver jobs..."

# Run batch job first
$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.delta:delta-core_2.12:2.4.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.hadoop.fs.s3a.endpoint=http://172.31.32.202:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=admin12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  /opt/predictapp/pipelines/silver/silver_batch.py

echo "Silver batch job completed"