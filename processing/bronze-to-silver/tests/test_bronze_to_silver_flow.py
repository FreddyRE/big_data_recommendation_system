import os

def test_buckets_exist(s3_client):
    bronze = os.environ["S3_BUCKET"]
    silver = os.environ["S3_BUCKET_SILVER"]
    gold   = os.environ["S3_BUCKET_GOLD"]

    resp = s3_client.list_buckets()
    names = {b["Name"] for b in resp.get("Buckets", [])}

    assert bronze in names, f"{bronze} bucket should exist"
    assert silver in names, f"{silver} bucket should exist"
    assert gold in names,   f"{gold} bucket should exist"

def test_put_and_get_small_object(s3_client):
    bronze = os.environ["S3_BUCKET"]
    key = "health/hello.txt"
    body = b"hello, minio"

    s3_client.put_object(Bucket=bronze, Key=key, Body=body)
    obj = s3_client.get_object(Bucket=bronze, Key=key)
    data = obj["Body"].read()
    assert data == body
