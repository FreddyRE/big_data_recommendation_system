import os
import io
import time
import pytest
import boto3
from botocore.config import Config
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"), override=False)

REQUIRED_ENV_VARS = [
    "S3_ENDPOINT_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_DEFAULT_REGION",
    "S3_BUCKET",
    "S3_BUCKET_SILVER",
    "S3_BUCKET_GOLD",
]

def _missing_env():
    return [k for k in REQUIRED_ENV_VARS if not os.getenv(k)]

@pytest.fixture(scope="session")
def s3_client():
    missing = _missing_env()
    if missing:
        pytest.fail(f"Missing required env vars: {', '.join(missing)}")

    endpoint = os.environ["S3_ENDPOINT_URL"]
    region = os.environ["AWS_DEFAULT_REGION"]
    aws_key = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]

    cfg = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        region_name=region,
        config=cfg,
        verify=False if os.getenv("S3_USE_SSL", "false").lower() == "false" else True,
    )
    return s3

def _ensure_bucket(s3, name: str):
    # Create if missing (idempotent)
    try:
        s3.head_bucket(Bucket=name)
        return
    except Exception:
        pass
    s3.create_bucket(Bucket=name)

@pytest.fixture(scope="session", autouse=True)
def ensure_buckets(s3_client):
    bronze = os.environ["S3_BUCKET"]
    silver = os.environ["S3_BUCKET_SILVER"]
    gold   = os.environ["S3_BUCKET_GOLD"]

    for b in (bronze, silver, gold):
        _ensure_bucket(s3_client, b)

    # simple wait in case MinIO needs a beat to list newly created buckets
    time.sleep(0.2)
    yield
