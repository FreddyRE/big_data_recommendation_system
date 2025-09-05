import json
import time
import requests
from typing import Dict, Any

class S3ConnectorManager:
    def __init__(self, connect_url="http://localhost:8083"):
        self.connect_url = connect_url
        
    def wait_for_connect(self, max_retries=30):
        """Wait for Kafka Connect to be ready"""
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{self.connect_url}/connectors")
                if response.status_code == 200:
                    print("✅ Kafka Connect is ready!")
                    return True
            except requests.exceptions.ConnectionError:
                pass
            
            print(f"⏳ Waiting for Kafka Connect... (attempt {attempt + 1}/{max_retries})")
            time.sleep(10)
        
        print("❌ Failed to connect to Kafka Connect")
        return False
    
    def create_connector(self, config: Dict[str, Any]):
        """Create or update a connector"""
        name = config['name']
        
        # Check if connector exists
        try:
            response = requests.get(f"{self.connect_url}/connectors/{name}")
            if response.status_code == 200:
                print(f"📝 Updating existing connector: {name}")
                response = requests.put(
                    f"{self.connect_url}/connectors/{name}/config",
                    json=config['config'],
                    headers={'Content-Type': 'application/json'}
                )
            else:
                print(f"🆕 Creating new connector: {name}")
                response = requests.post(
                    f"{self.connect_url}/connectors",
                    json=config,
                    headers={'Content-Type': 'application/json'}
                )
        except Exception as e:
            print(f"❌ Error with connector {name}: {e}")
            return False
        
        if response.status_code in [200, 201]:
            print(f"✅ Connector {name} deployed successfully")
            return True
        else:
            print(f"❌ Failed to deploy connector {name}: {response.text}")
            return False
    
    def get_connector_status(self, name: str):
        """Get connector status"""
        try:
            response = requests.get(f"{self.connect_url}/connectors/{name}/status")
            if response.status_code == 200:
                status = response.json()
                print(f"📊 Connector {name}:")
                print(f"   State: {status['connector']['state']}")
                for i, task in enumerate(status['tasks']):
                    print(f"   Task {i}: {task['state']}")
                return status
        except Exception as e:
            print(f"❌ Error getting status for {name}: {e}")
        return None
    
    def list_connectors(self):
        """List all connectors"""
        try:
            response = requests.get(f"{self.connect_url}/connectors")
            if response.status_code == 200:
                connectors = response.json()
                print(f"📋 Active connectors: {len(connectors)}")
                for connector in connectors:
                    self.get_connector_status(connector)
                return connectors
        except Exception as e:
            print(f"❌ Error listing connectors: {e}")
        return []

def deploy_s3_connectors():
    """Deploy all S3 sink connectors"""
    
    # Connector configurations
    connectors = {
        "bronze_clickstream": {
            "name": "s3-sink-bronze-clickstream",
            "config": {
                "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                "tasks.max": "2",
                "topics": "clickstream",
                
                "s3.bucket.name": "recommendation-bronze",
                "s3.region": "us-west-2",
                "s3.part.size": 5242880,
                "s3.compression.type": "gzip",
                
                "store.url": "http://minio:9000",
                "s3.path.style.access.enabled": "true",
                "aws.access.key.id": "minioadmin",
                "aws.secret.access.key": "minioadmin123",
                
                "storage.class": "io.confluent.connect.s3.storage.S3Storage",
                "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
                
                "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
                "partition.duration.ms": 3600000,
                "path.format": "clickstream/year=YYYY/month=MM/day=dd/hour=HH",
                "locale": "en-US",
                "timezone": "UTC",
                
                "flush.size": 100,
                "rotate.interval.ms": 300000,
                "rotate.schedule.interval.ms": 3600000,
                
                "schema.compatibility": "NONE",
                "store.kafka.headers": "false",
                "store.kafka.keys": "true",
                
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                
                "errors.tolerance": "all",
                "errors.log.enable": "true",
                "errors.log.include.messages": "true"
            }
        },
        
        "bronze_features": {
            "name": "s3-sink-bronze-features",
            "config": {
                "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                "tasks.max": "3",
                "topics": "user-features,product-features,recommendation-signals",
                
                "s3.bucket.name": "recommendation-bronze",
                "s3.region": "us-west-2",
                "s3.part.size": 5242880,
                "s3.compression.type": "gzip",
                
                "store.url": "http://minio:9000",
                "s3.path.style.access.enabled": "true",
                "aws.access.key.id": "minioadmin",
                "aws.secret.access.key": "minioadmin123",
                
                "storage.class": "io.confluent.connect.s3.storage.S3Storage",
                "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
                
                "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
                "partition.duration.ms": 3600000,
                "path.format": "${topic}/year=YYYY/month=MM/day=dd/hour=HH",
                "locale": "en-US",
                "timezone": "UTC",
                
                "flush.size": 50,
                "rotate.interval.ms": 300000,
                "rotate.schedule.interval.ms": 3600000,
                
                "schema.compatibility": "NONE",
                "store.kafka.headers": "false",
                "store.kafka.keys": "true",
                
                "key.converter": "org.apache.kafka.connect.storage.StringConverter", 
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                
                "errors.tolerance": "all",
                "errors.log.enable": "true",
                "errors.log.include.messages": "true"
            }
        }
    }
    
    print("🚀 Starting S3 connector deployment...")
    
    manager = S3ConnectorManager()
    
    # Wait for Kafka Connect to be ready
    if not manager.wait_for_connect():
        return False
    
    # Deploy connectors
    success_count = 0
    for connector_name, config in connectors.items():
        if manager.create_connector(config):
            success_count += 1
    
    print(f"\n📊 Deployment Summary:")
    print(f"   Deployed: {success_count}/{len(connectors)} connectors")
    
    # Show final status
    time.sleep(10)  # Wait for connectors to start
    print(f"\n📋 Final Status:")
    manager.list_connectors()
    
    return success_count == len(connectors)

if __name__ == "__main__":
    deploy_s3_connectors()