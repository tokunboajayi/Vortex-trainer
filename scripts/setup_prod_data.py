import os
import boto3
import numpy as np
import io
import asyncio
from botocore.client import Config

# Configuration matching our docker-compose
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "dtrainer-prod"
NUM_SHARDS = 16  # Small number for demo
SHARD_SIZE_MB = 10

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def main():
    s3 = get_s3_client()
    
    # 1. Create Bucket
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Created bucket: {BUCKET_NAME}")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"ℹ️  Bucket {BUCKET_NAME} already exists")

    # 2. Generate and Upload Shards
    print(f"🚀 Generating {NUM_SHARDS} synthetic shards ({SHARD_SIZE_MB}MB each)...")
    
    for i in range(NUM_SHARDS):
        # Generate random bytes to simulate encoded mock data
        # In real life this would be serialized tensors or protobufs
        data = os.urandom(SHARD_SIZE_MB * 1024 * 1024)
        
        key = f"datasets/synthetic-imagenet/shards/shard_{i:06d}.bin"
        
        print(f"  ⬆️ Uploading {key}...", end="\r")
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=data)
        
    print(f"\n✅ Upload complete! Data is ready in s3://{BUCKET_NAME}/datasets/synthetic-imagenet/")

if __name__ == "__main__":
    main()
