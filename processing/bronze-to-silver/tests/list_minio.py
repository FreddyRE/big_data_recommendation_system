import os, boto3
from botocore.config import Config

ENDPOINT = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
AK = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
SK = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=AK,
    aws_secret_access_key=SK,
    region_name=REGION,
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    verify=False,  # local http
)

print(f"Endpoint: {ENDPOINT}")
print("Buckets:")
for b in s3.list_buckets().get("Buckets", []):
    print(" -", b["Name"])

def ls(bucket, prefix=""):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1000)
    contents = resp.get("Contents", [])
    print(f"\n{s3.meta.endpoint_url} s3://{bucket}/{prefix} -> {len(contents)} object(s)")
    for o in contents[:20]:  # show up to 20
        print(f"  {o['Key']}  ({o['Size']} bytes)")

# Check both naming schemes
for b in ("clickstream-bronze","clickstream-silver","recommendation-bronze","recommendation-silver"):
    try:
        ls(b)
    except Exception as e:
        print(f"\nCannot list {b}: {e}")