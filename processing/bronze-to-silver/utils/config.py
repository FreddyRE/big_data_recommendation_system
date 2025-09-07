import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class ProcessingConfig:
    # MinIO/S3 Configuration
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
    bucket_name: str = os.getenv("BUCKET_NAME", "recommendation-bucket")
    
    # Path Configuration
    bronze_prefix: str = os.getenv("BRONZE_PREFIX", "bronze/")
    silver_prefix: str = os.getenv("SILVER_PREFIX", "silver/")
    
    # Processing Configuration
    batch_size: int = int(os.getenv("PROCESSING_BATCH_SIZE", "1000"))
    data_quality_threshold: float = float(os.getenv("DATA_QUALITY_THRESHOLD", "0.8"))
    retention_days: int = int(os.getenv("RETENTION_DAYS", "30"))
    
    # Output Configuration
    output_format: str = "parquet"
    compression: str = "snappy"
    
    @classmethod
    def from_env(cls) -> 'ProcessingConfig':
        return cls()