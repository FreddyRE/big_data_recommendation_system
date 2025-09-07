"""
Test script for Phase 2a: Core Processing Layer
Tests the minimal bronze-to-silver transformation
"""

import requests
import time
import json
import logging
from minio import Minio
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_minio_connection():
    """Test MinIO connection and bucket access"""
    try:
        client = Minio(
            endpoint='localhost:9000',
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
        # Test bronze layer access
        bronze_objects = list(client.list_objects('bronze-layer', recursive=True))
        logger.info(f"Bronze layer has {len(bronze_objects)} objects")
        
        # Test silver layer access
        try:
            silver_objects = list(client.list_objects('silver-layer', recursive=True))
            logger.info(f"Silver layer has {len(silver_objects)} objects")
        except:
            logger.info("Silver layer is empty or not accessible")
        
        return True
        
    except Exception as e:
        logger.error(f"MinIO connection failed: {str(e)}")
        return False

def test_kafka_connect():
    """Test Kafka Connect status"""
    try:
        response = requests.get("http://localhost:8083/")
        if response.status_code == 200:
            logger.info("Kafka Connect is running")
            
            # Check connectors
            connectors_response = requests.get("http://localhost:8083/connectors")
            if connectors_response.status_code == 200:
                connectors = connectors_response.json()
                logger.info(f"Active connectors: {connectors}")
                
                # Check each connector status
                for connector in connectors:
                    status_response = requests.get(f"http://localhost:8083/connectors/{connector}/status")
                    if status_response.status_code == 200:
                        status = status_response.json()
                        logger.info(f"{connector}: {status['connector']['state']}")
            
            return True
        else:
            logger.error(f"Kafka Connect not responding: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Kafka Connect test failed: {str(e)}")
        return False

def test_processing_pipeline():
    """Test if processing pipeline is working"""
    try:
        # Check if bronze-to-silver container is running and healthy
        # This is a simplified check - in real scenarios you'd check container health
        
        client = Minio(
            endpoint='localhost:9000',
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
        # Check for recent files in bronze layer
        bronze_objects = list(client.list_objects('bronze-layer', prefix='clickstream-events/', recursive=True))
        recent_bronze = [obj for obj in bronze_objects if obj.last_modified and 
                        (datetime.now() - obj.last_modified.replace(tzinfo=None)).total_seconds() < 3600]
        
        logger.info(f"Recent bronze files (last hour): {len(recent_bronze)}")
        
        # Check for processed files in silver layer
        try:
            silver_objects = list(client.list_objects('silver-layer', prefix='clickstream-events/', recursive=True))
            recent_silver = [obj for obj in silver_objects if obj.last_modified and 
                           (datetime.now() - obj.last_modified.replace(tzinfo=None)).total_seconds() < 3600]
            
            logger.info(f"Recent silver files (last hour): {len(recent_silver)}")
            
            if len(recent_silver) > 0:
                logger.info("✅ Processing pipeline is working - silver files being created")
                return True
            else:
                logger.warning("⚠️ No recent silver files found - pipeline may not be processing")
                return False
                
        except Exception as e:
            logger.warning(f"Silver layer check failed: {str(e)}")
            return False
            
    except Exception as e:
        logger.error(f"Processing pipeline test failed: {str(e)}")
        return False

def test_data_flow():
    """Test end-to-end data flow"""
    try:
        client = Minio(
            endpoint='localhost:9000',
            access_key='minioadmin',
            secret_key='minioadmin123',
            secure=False
        )
        
        # Sample the most recent bronze file
        bronze_objects = list(client.list_objects('bronze-layer', prefix='clickstream-events/', recursive=True))
        if not bronze_objects:
            logger.warning("No bronze files found to test")
            return False
        
        # Get the most recent bronze file
        latest_bronze = max(bronze_objects, key=lambda x: x.last_modified)
        logger.info(f"Testing with latest bronze file: {latest_bronze.object_name}")
        
        # Read a sample of the bronze data
        response = client.get_object('bronze-layer', latest_bronze.object_name)
        bronze_content = response.read().decode('utf-8')
        
        # Parse first few lines
        bronze_lines = bronze_content.strip().split('\n')[:5]
        logger.info(f"Bronze file sample ({len(bronze_lines)} lines):")
        
        for i, line in enumerate(bronze_lines):
            try:
                record = json.loads(line)
                logger.info(f"  Line {i+1}: user_id={record.get('user_id')}, event_type={record.get('event_type')}")
            except:
                logger.info(f"  Line {i+1}: Invalid JSON")
        
        # Check if corresponding silver file exists
        silver_objects = list(client.list_objects('silver-layer', prefix='clickstream-events/', recursive=True))
        if silver_objects:
            latest_silver = max(silver_objects, key=lambda x: x.last_modified)
            logger.info(f"Latest silver file: {latest_silver.object_name}")
            
            # Read sample of silver data
            silver_response = client.get_object('silver-layer', latest_silver.object_name)
            silver_content = silver_response.read().decode('utf-8')
            
            silver_lines = silver_content.strip().split('\n')[:3]
            logger.info(f"Silver file sample ({len(silver_lines)} lines):")
            
            for i, line in enumerate(silver_lines):
                try:
                    record = json.loads(line)
                    logger.info(f"  Line {i+1}: user_id={record.get('user_id')}, processed_at={record.get('processed_at')}")
                except:
                    logger.info(f"  Line {i+1}: Invalid JSON")
        
        return True
        
    except Exception as e:
        logger.error(f"Data flow test failed: {str(e)}")
        return False

def generate_test_summary(results):
    """Generate test summary"""
    logger.info("\n" + "="*50)
    logger.info("PHASE 2A TEST SUMMARY")
    logger.info("="*50)
    
    total_tests = len(results)
    passed_tests = sum(results.values())
    
    logger.info(f"Total tests: {total_tests}")
    logger.info(f"Passed: {passed_tests}")
    logger.info(f"Failed: {total_tests - passed_tests}")
    logger.info(f"Success rate: {(passed_tests/total_tests)*100:.1f}%")
    
    logger.info("\nDetailed results:")
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        logger.info(f"  {test_name}: {status}")
    
    if passed_tests == total_tests:
        logger.info("\n🎉 All tests passed! Phase 2a is working correctly.")
        logger.info("\nNext steps:")
        logger.info("1. Monitor the pipeline for a few hours")
        logger.info("2. Check data quality and processing rates")
        logger.info("3. Move to Phase 2b: Add warehouse loading")
    else:
        logger.info(f"\n⚠️ {total_tests - passed_tests} test(s) failed. Please fix issues before proceeding.")
        logger.info("\nTroubleshooting tips:")
        logger.info("1. Check container logs: docker-compose logs bronze-to-silver")
        logger.info("2. Verify Kafka Connect status: curl http://localhost:8083/connectors")
        logger.info("3. Check MinIO browser: http://localhost:9001")
    
    logger.info("="*50)

def main():
    """Run all Phase 2a tests"""
    logger.info("Starting Phase 2a Testing...")
    logger.info("Testing core processing layer functionality")
    
    # Wait a bit for services to be ready
    logger.info("Waiting 30 seconds for services to initialize...")
    time.sleep(30)
    
    # Run tests
    test_results = {}
    
    logger.info("\n1. Testing MinIO Connection...")
    test_results['MinIO Connection'] = test_minio_connection()
    
    logger.info("\n2. Testing Kafka Connect...")
    test_results['Kafka Connect'] = test_kafka_connect()
    
    logger.info("\n3. Testing Processing Pipeline...")
    test_results['Processing Pipeline'] = test_processing_pipeline()
    
    logger.info("\n4. Testing Data Flow...")
    test_results['Data Flow'] = test_data_flow()
    
    # Generate summary
    generate_test_summary(test_results)

if __name__ == "__main__":
    main()