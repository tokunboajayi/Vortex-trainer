
import boto3
import numpy as np
import io
import os
from botocore.client import Config

ENDPOINT_URL = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "vortex-prod"
PREFIX = "datasets/synthetic-imagenet/shards/"
NUM_SHARDS = 20
SHARD_SIZE_BYTES = 10 * 1024 * 1024 # 10MB per shard

def generate_shards():
    print(f"Generating {NUM_SHARDS} shards of {SHARD_SIZE_BYTES/1024/1024}MB each...")
    
    s3 = boto3.resource('s3',
                    endpoint_url=ENDPOINT_URL,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

    # Ensure bucket
    if s3.Bucket(BUCKET_NAME) not in s3.buckets.all():
        s3.create_bucket(Bucket=BUCKET_NAME)

    for i in range(NUM_SHARDS):
        # Generate random floats
        # 10MB = 2.5M floats (4 bytes each)
        num_floats = SHARD_SIZE_BYTES // 4
        data = np.random.randn(num_floats).astype(np.float32).tobytes()
        
        key = f"{PREFIX}shard_{i:06d}.bin"
        
        try:
            obj = s3.Object(BUCKET_NAME, key)
            obj.put(Body=data)
            print(f"Uploaded {key} ({len(data)} bytes)")
        except Exception as e:
            print(f"Failed to upload {key}: {e}")

    print("Data generation complete.")

if __name__ == "__main__":
    generate_shards()
