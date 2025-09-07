#!/usr/bin/env python3
"""
test_clickstream.py
End-to-end smoke test for Bronze -> Silver using MinIO/S3, pandas, pyarrow.

Env vars (examples):
  S3_ENDPOINT_URL=http://localhost:9000
  AWS_ACCESS_KEY_ID=minioadmin
  AWS_SECRET_ACCESS_KEY=minioadmin
  AWS_DEFAULT_REGION=us-east-1
  S3_BUCKET=clickstream-bronze
  S3_BUCKET_SILVER=clickstream-silver
  S3_BUCKET_GOLD=clickstream-gold
  # OR single-bucket mode:
  # BUCKET_NAME=clickstream

  S3_USE_SSL=false   # default false for local MinIO
"""

import os
import io
import sys
import json
from datetime import datetime, timezone
from typing import Optional, List, Dict

# optional: load .env if present (won't error if missing)
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"), override=False)
except Exception:
    pass

import boto3
from botocore.config import Config
import pandas as pd


# ------- Config / Env -------

ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
REGION   = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AK       = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SK       = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
USE_SSL  = os.getenv("S3_USE_SSL", "false").lower() == "true"

# Separate-bucket mode (preferred)
S3_BRONZE = os.getenv("S3_BUCKET")          # bronze bucket
S3_SILVER = os.getenv("S3_BUCKET_SILVER")   # silver bucket
S3_GOLD   = os.getenv("S3_BUCKET_GOLD")     # gold bucket

# Single-bucket legacy support
LEGACY_BUCKET = os.getenv("BUCKET_NAME")

# Path prefixes used inside buckets
BRONZE_PREFIX = "bronze/clickstream/"
SILVER_PREFIX = "silver/clickstream_clean/"

# ------- Utilities -------

def s3_client():
    cfg = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        aws_access_key_id=AK,
        aws_secret_access_key=SK,
        region_name=REGION,
        config=cfg,
        verify=USE_SSL,  # False for local http MinIO
    )

def resolve_buckets():
    """
    Decide which buckets to use.
    - If S3_BUCKET[_SILVER/_GOLD] are set, use them.
    - Else use LEGACY_BUCKET for both bronze/silver with different prefixes.
    """
    if S3_BRONZE or S3_SILVER or S3_GOLD:
        bronze = S3_BRONZE or LEGACY_BUCKET
        silver = S3_SILVER or LEGACY_BUCKET
        gold   = S3_GOLD   or LEGACY_BUCKET
    else:
        # pure legacy single-bucket mode
        bronze = LEGACY_BUCKET or "clickstream"
        silver = LEGACY_BUCKET or "clickstream"
        gold   = LEGACY_BUCKET or "clickstream"
    return bronze, silver, gold

def ensure_bucket(client, name: str):
    """Create bucket if it doesn't exist (idempotent)."""
    try:
        client.head_bucket(Bucket=name)
    except Exception:
        client.create_bucket(Bucket=name)

def ensure_required_buckets():
    client = s3_client()
    bronze, silver, gold = resolve_buckets()
    for b in {bronze, silver, gold}:
        ensure_bucket(client, b)
    return client, bronze, silver, gold

def list_objects(client, bucket: str, prefix: str, max_keys: int = 1000) -> List[Dict]:
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=max_keys)
    return resp.get("Contents", [])


# ------- Test Steps -------

def test_minio_connection() -> bool:
    """Connect, ensure buckets, list bronze objects (seed if empty)."""
    print("Testing MinIO connection...")
    try:
        client, bronze, silver, _gold = ensure_required_buckets()

        # show endpoints & buckets actually used (debugging)
        print(f"Endpoint: {ENDPOINT}")
        print(f"Bronze bucket: {bronze}")
        print(f"Silver bucket: {silver}")
        if LEGACY_BUCKET:
            print(f"(Single-bucket mode via BUCKET_NAME={LEGACY_BUCKET})")

        # List bronze files
        objs = list_objects(client, bronze, BRONZE_PREFIX)
        if objs:
            print(f"Found {len(objs)} bronze clickstream files:")
            for obj in objs[:5]:
                print(f"  {obj['Key']}")
        else:
            print("No bronze clickstream files found — seeding a tiny JSONL sample...")
            seed_sample_bronze(client, bronze)
            objs = list_objects(client, bronze, BRONZE_PREFIX)
            if objs:
                print(f"Seeded. Now {len(objs)} file(s) in bronze:")
                for obj in objs[:3]:
                    print(f"  {obj['Key']}")
            else:
                print("Seeding failed (no files).")
                return False

        return True

    except Exception as e:
        print(f"MinIO connection failed: {e}")
        return False

def seed_sample_bronze(client, bronze_bucket: str):
    """Write a tiny JSONL file to bronze to make the flow runnable."""
    records = [
        {
            "user_id": 1,
            "event_type": "page_view",
            "item_id": "A",
            "timestamp": "2025-09-05T12:00:00Z",
            "url": "/home",
        },
        {
            "user_id": 1,
            "event_type": "click",
            "item_id": "A",
            "timestamp": "2025-09-05T12:00:10Z",
            "url": "/product/A",
        },
        {
            "user_id": 2,
            "event_type": "page_view",
            "item_id": "B",
            "timestamp": "2025-09-05T12:01:00Z",
            "url": "/home",
        },
        {
            "user_id": 1,
            "event_type": "purchase",
            "item_id": "A",
            "timestamp": "2025-09-05T12:02:00Z",
            "url": "/checkout",
        },
    ]

    # JSON lines
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")

    key = f"{BRONZE_PREFIX}sample_{int(datetime.now(tz=timezone.utc).timestamp())}.jsonl"
    client.put_object(Bucket=bronze_bucket, Key=key, Body=body, Metadata={
        "seeded": "true",
        "record_count": str(len(records)),
        "seeded_at": datetime.now(tz=timezone.utc).isoformat(),
    })

def read_sample_data() -> Optional[pd.DataFrame]:
    """Read first JSONL from bronze into a DataFrame."""
    print("\nReading sample bronze data...")
    try:
        client, bronze, _silver, _gold = ensure_required_buckets()

        resp = client.list_objects_v2(Bucket=bronze, Prefix=BRONZE_PREFIX, MaxKeys=1)
        if "Contents" not in resp or not resp["Contents"]:
            print("No clickstream files found in bronze (after seed attempt).")
            return None

        file_key = resp["Contents"][0]["Key"]
        print(f"Reading file: {file_key}")

        obj = client.get_object(Bucket=bronze, Key=file_key)
        content = obj["Body"].read().decode("utf-8")

        records = []
        for line in content.strip().splitlines():
            if line.strip():
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"JSON decode error: {e}")

        if not records:
            print("No valid JSON records in file.")
            return None

        df = pd.DataFrame(records)
        print(f"Loaded {len(df)} records")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Sample record:\n{df.iloc[0].to_dict()}")
        return df

    except Exception as e:
        print(f"Error reading bronze data: {e}")
        return None

def basic_transformation(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Apply very basic cleaning & feature engineering."""
    print("\nApplying basic transformations...")

    if df is None or df.empty:
        print("No data to transform")
        return None

    cleaned = df.copy()

    # Robust timestamp parse
    if "timestamp" not in cleaned.columns:
        print("Missing 'timestamp' column.")
        return None

    cleaned["timestamp"] = pd.to_datetime(cleaned["timestamp"], errors="coerce", utc=True)
    cleaned = cleaned.dropna(subset=["timestamp"])

    # Add time features
    cleaned["hour"] = cleaned["timestamp"].dt.hour
    cleaned["day_of_week"] = cleaned["timestamp"].dt.dayofweek
    cleaned["is_weekend"] = cleaned["day_of_week"].isin([5, 6])

    # Event flags (fallback defaults if column missing)
    if "event_type" not in cleaned.columns:
        cleaned["event_type"] = "unknown"

    cleaned["is_purchase"] = cleaned["event_type"].eq("purchase")
    cleaned["is_view"]     = cleaned["event_type"].eq("page_view")

    # Processing metadata
    cleaned["processed_at"] = datetime.now(tz=timezone.utc)

    print(f"Transformation completed. New columns: {cleaned.columns.tolist()}")
    try:
        sample_cols = ["timestamp", "hour", "is_weekend", "is_purchase"]
        print(f"Sample transformed record:\n{cleaned.iloc[0][sample_cols].to_dict()}")
    except Exception:
        pass

    return cleaned

def write_silver_data(df: pd.DataFrame) -> bool:
    """Write transformed DataFrame to silver as Parquet."""
    print("\nWriting to silver layer...")

    if df is None or df.empty:
        print("No data to write")
        return False

    try:
        client, _bronze, silver, _gold = ensure_required_buckets()

        # Parquet to buffer
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, compression="snappy")
        buf.seek(0)

        # Partition by date
        date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        ts = int(datetime.now(tz=timezone.utc).timestamp())
        key = f"{SILVER_PREFIX}date={date_str}/clickstream_{ts}.parquet"

        client.put_object(
            Bucket=silver,
            Key=key,
            Body=buf.getvalue(),
            Metadata={
                "processed_at": datetime.now(tz=timezone.utc).isoformat(),
                "record_count": str(len(df)),
            },
        )

        print(f"Successfully wrote {len(df)} records to s3://{silver}/{key}")
        return True

    except Exception as e:
        print(f"Error writing silver data: {e}")
        return False


# ------- Main -------

def main():
    print("Required packages: pandas, boto3, pyarrow")
    print("Install with: pip install pandas boto3 pyarrow\n")

    print("Testing Bronze to Silver Transformation")
    print("=" * 50)

    if not test_minio_connection():
        print("Cannot proceed without MinIO connection")
        return

    df = read_sample_data()
    if df is None:
        print("Cannot proceed without sample data")
        return

    transformed = basic_transformation(df)
    if transformed is None:
        print("Transformation failed")
        return

    ok = write_silver_data(transformed)
    if ok:
        print("\nTest completed successfully!")
        print("Check your MinIO console for silver layer data")
    else:
        print("\nTest failed during write operation")


if __name__ == "__main__":
    sys.exit(main() or 0)