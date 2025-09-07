# 1-ingestion/connectors/deploy_s3_connectors.py
import json
import requests
import time
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConnectManager:
    def __init__(self, connect_url: str = "http://localhost:8083"):
        self.connect_url = connect_url
        self.headers = {"Content-Type": "application/json"}
    
    def wait_for_connect(self, max_retries: int = 30):
        """Wait for Kafka Connect to be ready"""
        for i in range(max_retries):
            try:
                response = requests.get(f"{self.connect_url}/connectors")
                if response.status_code == 200:
                    logger.info("Kafka Connect is ready")
                    return True
            except requests.exceptions.ConnectionError:
                logger.info(f"Waiting for Kafka Connect... ({i+1}/{max_retries})")
                time.sleep(10)
        
        raise Exception("Kafka Connect not available")
    
    def create_connector(self, config: dict) -> bool:
        """Create or update a connector"""
        connector_name = config["name"]
        
        try:
            # Check if connector exists
            response = requests.get(f"{self.connect_url}/connectors/{connector_name}")
            
            if response.status_code == 200:
                logger.info(f"Connector {connector_name} exists, updating...")
                response = requests.put(
                    f"{self.connect_url}/connectors/{connector_name}/config",
                    headers=self.headers,
                    json=config["config"]
                )
            else:
                logger.info(f"Creating connector {connector_name}...")
                response = requests.post(
                    f"{self.connect_url}/connectors",
                    headers=self.headers,
                    json=config
                )
            
            if response.status_code in [200, 201]:
                logger.info(f"Connector {connector_name} created/updated successfully")
                return True
            else:
                logger.error(f"Failed to create connector: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error managing connector {connector_name}: {e}")
            return False
    
    def get_connector_status(self, connector_name: str) -> dict:
        """Get connector status"""
        try:
            response = requests.get(f"{self.connect_url}/connectors/{connector_name}/status")
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            logger.error(f"Error getting connector status: {e}")
        return {}

def create_bronze_connectors():
    """Create S3 sink connectors to land data in bronze layer"""
    
    # Base configuration for MinIO (S3-compatible)
    base_config = {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "s3.region": "us-east-1",
        "s3.bucket.name": "bronze-layer",
        "s3.path.style.access.enabled": "true",
        "store.url": "http://minio:9000",
        "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
        "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
        "partition.duration.ms": "900000",  # 15-minute partitions
        "path.format": "YYYY/MM/dd/HH",
        "locale": "US",
        "timezone": "UTC",
        "timestamp.extractor": "Record",
        "timestamp.field": "timestamp",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "storage.class": "io.confluent.connect.s3.storage.S3Storage"
    }
    
    connectors = [
        {
            "name": "clickstream-bronze-sink",
            "config": {
                **base_config,
                "topics": "clickstream-events",
                "tasks.max": "1",
                "flush.size": "100",  # Smaller for testing
                "rotate.interval.ms": "60000",  # 1 minute for testing
                "transforms": "AddPrefix",
                "transforms.AddPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.AddPrefix.regex": "(.*)",
                "transforms.AddPrefix.replacement": "clickstream/$1"
            }
        },
        {
            "name": "user-events-bronze-sink",
            "config": {
                **base_config,
                "topics": "user-events",
                "tasks.max": "1",
                "flush.size": "10",  # Small for testing
                "rotate.interval.ms": "120000",  # 2 minutes for testing
                "transforms": "AddPrefix",
                "transforms.AddPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.AddPrefix.regex": "(.*)",
                "transforms.AddPrefix.replacement": "user-events/$1"
            }
        },
        {
            "name": "product-events-bronze-sink",
            "config": {
                **base_config,
                "topics": "product-events",
                "tasks.max": "1",
                "flush.size": "5",   # Small for testing
                "rotate.interval.ms": "180000",  # 3 minutes for testing
                "transforms": "AddPrefix",
                "transforms.AddPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.AddPrefix.regex": "(.*)",
                "transforms.AddPrefix.replacement": "product-events/$1"
            }
        }
    ]
    
    return connectors

def main():
    """Deploy bronze layer connectors"""
    connect_manager = KafkaConnectManager()
    
    # Wait for Kafka Connect
    logger.info("Waiting for Kafka Connect to be ready...")
    connect_manager.wait_for_connect()
    
    # Create connectors
    connectors = create_bronze_connectors()
    
    success_count = 0
    for connector_config in connectors:
        if connect_manager.create_connector(connector_config):
            success_count += 1
        time.sleep(5)
    
    logger.info(f"Successfully created {success_count}/{len(connectors)} connectors")
    
    # Check status
    logger.info("\nConnector Status:")
    for connector_config in connectors:
        name = connector_config["name"]
        status = connect_manager.get_connector_status(name)
        if status:
            state = status.get('connector', {}).get('state', 'UNKNOWN')
            logger.info(f"{name}: {state}")
        else:
            logger.info(f"{name}: No status available")
    
    logger.info("\n🎯 Data should start flowing to MinIO bronze-layer bucket!")
    logger.info("Check MinIO console: http://localhost:9001")

if __name__ == "__main__":
    main()