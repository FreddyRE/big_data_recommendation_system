# utils/storage_manager.py - Fixed version with working SafeMinIOManager
import json
import pandas as pd
from minio import Minio
from minio.error import S3Error
from loguru import logger
from typing import List, Dict, Any, Optional
import io
from datetime import datetime

class SafeMinIOManager:
    """
    SAFE MinIO manager - ONLY READS from bronze, ONLY WRITES to silver
    NEVER modifies or deletes anything in bronze layer
    """
    
    def __init__(self, 
                 endpoint: str = "localhost:9000",
                 access_key: str = "minioadmin", 
                 secret_key: str = "minioadmin123"):
        
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False
        )
        
        # Bucket names - NEVER write to bronze!
        self.bronze_bucket = "bronze-layer"  # READ ONLY
        self.silver_bucket = "silver-layer"  # WRITE ONLY
        self.checkpoint_bucket = "checkpoint"  # WRITE ONLY
        
        logger.info("✅ MinIO manager initialized - Bronze=READ-ONLY, Silver=WRITE-ONLY")
    
    def list_bronze_files(self, topic_prefix: str = "") -> List[Dict[str, Any]]:
        """
        SAFELY list files in bronze layer (READ ONLY)
        Returns file metadata without modifying anything
        """
        try:
            logger.info(f"📋 Listing bronze files with prefix: {topic_prefix}")
            
            objects = self.client.list_objects(
                self.bronze_bucket, 
                prefix=topic_prefix, 
                recursive=True
            )
            
            files = []
            for obj in objects:
                file_info = {
                    'key': obj.object_name,
                    'size': obj.size,
                    'last_modified': obj.last_modified,
                    'topic': obj.object_name.split('/')[0] if '/' in obj.object_name else 'unknown'
                }
                files.append(file_info)
            
            logger.info(f"📋 Found {len(files)} files in bronze layer")
            return files
            
        except S3Error as e:
            logger.error(f"❌ Error listing bronze files: {e}")
            return []
    
    def read_bronze_files_safely(self, file_keys: List[str], max_files: int = 5) -> List[Dict]:
        """
        SAFELY read files from bronze layer (READ ONLY)
        Limited to max_files to prevent memory issues
        """
        all_records = []
        
        # Safety limit
        files_to_process = file_keys[:max_files]
        if len(file_keys) > max_files:
            logger.warning(f"⚠️  Limited to {max_files} files (out of {len(file_keys)}) for safety")
        
        for i, key in enumerate(files_to_process):
            try:
                logger.info(f"📖 Reading file {i+1}/{len(files_to_process)}: {key}")
                
                # READ ONLY - never modify bronze
                response = self.client.get_object(self.bronze_bucket, key)
                content = response.read().decode('utf-8')
                
                # Parse JSON lines
                for line_num, line in enumerate(content.strip().split('\n')):
                    if line.strip():
                        try:
                            record = json.loads(line)
                            # Add metadata for tracking
                            record['_source_file'] = key
                            record['_source_line'] = line_num + 1
                            record['_processed_at'] = datetime.utcnow().isoformat()
                            all_records.append(record)
                        except json.JSONDecodeError:
                            logger.warning(f"⚠️  Invalid JSON in {key}, line {line_num + 1}")
                
            except S3Error as e:
                logger.error(f"❌ Error reading {key}: {e}")
            except Exception as e:
                logger.error(f"❌ Unexpected error reading {key}: {e}")
        
        logger.info(f"📖 Successfully read {len(all_records)} records from {len(files_to_process)} files")
        return all_records
    
    def write_to_silver_safely(self, data: pd.DataFrame, table_name: str) -> bool:
        """
        SAFELY write processed data to silver layer
        Creates partitioned parquet files
        """
        try:
            if data.empty:
                logger.warning("⚠️  No data to write to silver layer")
                return False
            
            # Create timestamp for file naming
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            today = datetime.utcnow().strftime("%Y-%m-%d")
            
            # Create partitioned path
            object_name = f"{table_name}/date={today}/{table_name}_{timestamp}.parquet"
            
            logger.info(f"💾 Writing {len(data)} records to silver: {object_name}")
            
            # Convert to parquet bytes
            parquet_buffer = io.BytesIO()
            data.to_parquet(parquet_buffer, engine='pyarrow', index=False)
            parquet_buffer.seek(0)
            
            # Upload to silver bucket (SAFE - only writes to silver)
            self.client.put_object(
                bucket_name=self.silver_bucket,
                object_name=object_name,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            
            logger.success(f"✅ Successfully wrote to silver: {object_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error writing to silver layer: {e}")
            return False
    
    def save_processing_checkpoint(self, checkpoint_data: Dict[str, Any]) -> bool:
        """
        Save processing checkpoint to track what's been processed
        """
        try:
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            checkpoint_key = f"bronze_to_silver_checkpoint_{timestamp}.json"
            
            # Add timestamp to checkpoint
            checkpoint_data['checkpoint_timestamp'] = datetime.utcnow().isoformat()
            checkpoint_data['checkpoint_id'] = checkpoint_key
            
            checkpoint_json = json.dumps(checkpoint_data, indent=2, default=str)
            
            self.client.put_object(
                bucket_name=self.checkpoint_bucket,
                object_name=checkpoint_key,
                data=io.BytesIO(checkpoint_json.encode()),
                length=len(checkpoint_json),
                content_type="application/json"
            )
            
            logger.success(f"✅ Checkpoint saved: {checkpoint_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving checkpoint: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Test MinIO connection safely"""
        try:
            # Test bronze bucket access (READ ONLY)
            bronze_objects = list(self.client.list_objects(self.bronze_bucket))
            logger.success("✅ Bronze bucket connection OK")
            
            # Test silver bucket access
            silver_objects = list(self.client.list_objects(self.silver_bucket))
            logger.success("✅ Silver bucket connection OK")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Connection test failed: {e}")
            return False