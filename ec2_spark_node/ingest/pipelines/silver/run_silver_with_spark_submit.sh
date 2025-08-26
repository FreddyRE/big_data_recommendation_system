#!/bin/bash
set -euo pipefail

export SPARK_HOME=/opt/spark
export PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

echo "Starting silver stream with spark-submit..."

$SPARK_HOME/bin/spark-submit \
  --master local[1] \
  --packages "$PKGS" \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse=s3a://predict-lake/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://172.31.32.202:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=admin12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.sql.streaming.checkpointLocation.deleteOnStop=false \
  --driver-memory 512m \
  --executor-memory 512m \
  /opt/predictapp/pipelines/silver/silver_stream.py