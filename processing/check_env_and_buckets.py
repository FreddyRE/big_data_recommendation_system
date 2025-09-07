import os, boto3
from botocore.config import Config

print("==== ENVIRONMENT ====")
for k in ["S3_ENDPOINT_URL","AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY",
          "AWS_DEFAULT_REGION","S3_BUCKET","S3_BUCKET_SILVER","S3_BUCKET_GOLD"]:
    v = os.getenv(k)
    if not v:
        print(f"{k} = (not set)")
    elif "SECRET" in k:
        print(f"{k} = {'*' * len(v)}")  # mask secrets
    else:
        print(f"{k} = {v}")

print("\n==== CONNECTING TO MINIO ====")
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION","us-east-1"),
    config=Config(signature_version="s3v4", s3={"addressing_style":"path"}),
    verify=os.getenv("S3_USE_SSL","false").lower()=="true"
)

resp = s3.list_buckets()
print("Buckets on server:")
for b in resp.get("Buckets", []):
    print(" -", b["Name"])

for target in ["S3_BUCKET","S3_BUCKET_SILVER","S3_BUCKET_GOLD"]:
    bname = os.getenv(target)
    if not bname: continue
    objs = s3.list_objects_v2(Bucket=bname, MaxKeys=5)
    keys = [o["Key"] for o in objs.get("Contents", [])] if "Contents" in objs else []
    print(f"\n{target} ({bname}) → {len(keys)} objects")
    for k in keys:
        print("   ", k)