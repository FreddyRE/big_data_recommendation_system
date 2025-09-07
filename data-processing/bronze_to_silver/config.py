import os

# MinIO/S3 Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET_NAME = os.getenv("BUCKET_NAME", "recommendation-bucket")

# Processing Configuration
PROCESSING_BATCH_SIZE = int(os.getenv("PROCESSING_BATCH_SIZE", "1000"))
RETENTION_HOURS = int(os.getenv("RETENTION_HOURS", "24"))