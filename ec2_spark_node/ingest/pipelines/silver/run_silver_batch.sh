#!/bin/bash

export SPARK_HOME=/opt/spark
export PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

DATE=${1:-$(date -d "yesterday" +%Y-%m-%d)}
echo "Running Silver batch job for date: $DATE"

$SPARK_HOME/bin/spark-submit \
  --master local[*] \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.hadoop.fs.s3a.endpoint=http://172.31.32.202:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=admin12345 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --driver-memory 512m \
  --executor-memory 512m \
  /opt/predictapp/pipelines/silver/silver_batch.py $DATE