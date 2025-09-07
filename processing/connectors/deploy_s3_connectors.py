
import requests
import json
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083"

def wait_for_kafka_connect():
    """Wait for Kafka Connect to be ready"""
    max_retries = 30
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            response = requests.get(f"{KAFKA_CONNECT_URL}/")
            if response.status_code == 200:
                logger.info("Kafka Connect is ready!")
                return True
        except requests.exceptions.RequestException:
            pass
        
        retry_count += 1
        logger.info(f"Waiting for Kafka Connect... ({retry_count}/{max_retries})")
        time.sleep(10)
    
    return False

def deploy_connector(connector_name, config):
    """Deploy a single connector"""
    try:
        # Check if connector already exists
        response = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
        if response.status_code == 200:
            logger.info(f"Connector {connector_name} already exists, deleting...")
            requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{connector_name}")
            time.sleep(5)
        
        # Deploy connector
        response = requests.post(
            f"{KAFKA_CONNECT_URL}/connectors",
            headers={"Content-Type": "application/json"},
            data=json.dumps(config)
        )
        
        if response.status_code in [200, 201]:
            logger.info(f"Successfully deployed connector: {connector_name}")
            return True
        else:
            logger.error(f"Failed to deploy connector {connector_name}: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error deploying connector {connector_name}: {str(e)}")
        return False

def main():
    """Deploy S3 sink connectors"""
    
    if not wait_for_kafka_connect():
        logger.error("Kafka Connect not available, exiting...")
        return
    
    # Clickstream events connector
    clickstream_config = {
        "name": "clickstream-s3-sink",
        "config": {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": "1",
            "topics": "clickstream-events",
            "s3.bucket.name": "bronze-layer",
            "s3.part.size": "5242880",
            "flush.size": "1000",
            "rotate.interval.ms": "60000",
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
            "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "partition.duration.ms": "3600000",
            "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
            "timestamp.extractor": "Record",
            "timestamp.field": "timestamp",
            "locale": "en-US",
            "timezone": "UTC",
            
            # MinIO S3 settings
            "store.url": "http://minio:9000",
            "s3.endpoint": "http://minio:9000",
            "s3.access.key.id": "minioadmin",
            "s3.secret.access.key": "minioadmin123",
            "s3.path.style.access.enable": "true",
            "s3.ssl.enabled": "false",

            "aws.access.key.id": "minioadmin",
            "aws.secret.access.key": "minioadmin123",
            "s3.region": "us-east-1",
            "s3.part.size": "5242880",
        }
    }
    
    # User events connector  
    user_events_config = {
        "name": "user-events-s3-sink",
        "config": {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": "1",
            "topics": "user-events",
            "s3.bucket.name": "bronze-layer",
            "s3.part.size": "5242880",
            "flush.size": "1000",
            "rotate.interval.ms": "300000",  # 5 minutes
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
            "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "partition.duration.ms": "3600000",
            "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
            "timestamp.extractor": "Record",
            "timestamp.field": "timestamp",
            "locale": "en-US",
            "timezone": "UTC",
            
            # MinIO S3 settings
            "store.url": "http://minio:9000",
            "s3.endpoint": "http://minio:9000",
            "s3.access.key.id": "minioadmin",
            "s3.secret.access.key": "minioadmin123",
            "s3.path.style.access.enable": "true",
            "s3.ssl.enabled": "false",
            "aws.access.key.id": "minioadmin",
            "aws.secret.access.key": "minioadmin123",
            "s3.region": "us-east-1",
            "s3.part.size": "5242880",
        }
    }
    
    # Product events connector
    product_events_config = {
        "name": "product-events-s3-sink", 
        "config": {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": "1",
            "topics": "product-events",
            "s3.bucket.name": "bronze-layer",
            "s3.part.size": "5242880",
            "flush.size": "1000",
            "rotate.interval.ms": "600000",  # 10 minutes
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
            "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "partition.duration.ms": "3600000",
            "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
            "timestamp.extractor": "Record",
            "timestamp.field": "timestamp",
            "locale": "en-US",
            "timezone": "UTC",
            
            # MinIO S3 settings
            "store.url": "http://minio:9000",
            "s3.endpoint": "http://minio:9000",
            "s3.access.key.id": "minioadmin",
            "s3.secret.access.key": "minioadmin123",
            "s3.path.style.access.enable": "true",
            "s3.ssl.enabled": "false",

            "aws.access.key.id": "minioadmin",
            "aws.secret.access.key": "minioadmin123",
            "s3.region": "us-east-1",
            "s3.part.size": "5242880",
        }
    }
    
    # Deploy connectors
    connectors = [
        ("clickstream-s3-sink", clickstream_config),
        ("user-events-s3-sink", user_events_config),
        ("product-events-s3-sink", product_events_config)
    ]
    
    successful_deployments = 0
    
    for connector_name, config in connectors:
        if deploy_connector(connector_name, config):
            successful_deployments += 1
        time.sleep(2)  # Small delay between deployments
    
    logger.info(f"Deployment complete: {successful_deployments}/{len(connectors)} connectors deployed successfully")
    
    # Check connector status
    time.sleep(10)
    logger.info("Checking connector status...")
    
    try:
        response = requests.get(f"{KAFKA_CONNECT_URL}/connectors")
        if response.status_code == 200:
            connectors_list = response.json()
            logger.info(f"Active connectors: {connectors_list}")
            
            for connector in connectors_list:
                status_response = requests.get(f"{KAFKA_CONNECT_URL}/connectors/{connector}/status")
                if status_response.status_code == 200:
                    status = status_response.json()
                    logger.info(f"Connector {connector} status: {status['connector']['state']}")
        
    except Exception as e:
        logger.error(f"Error checking connector status: {str(e)}")

if __name__ == "__main__":
    main()