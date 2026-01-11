
import sys
import os
import boto3
import numpy as np
import torch
from botocore.client import Config
from dtrainer.pytorch.dataloader import DTrainerDataLoader
from dtrainer.dataset import Dataset, DatasetConfig

# Configuration for local MinIO
ENDPOINT_URL = "http://localhost:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "dtrainer"
PREFIX = "test_data/"
SHARD_NAME = "shard_000000.bin"

# Set env vars for Rust dtrainer to pick up
os.environ["S3_ENDPOINT"] = ENDPOINT_URL
os.environ["S3_ACCESS_KEY"] = ACCESS_KEY
os.environ["S3_SECRET_KEY"] = SECRET_KEY
os.environ["S3_BUCKET"] = BUCKET_NAME

def setup_test_data():
    """Upload a test shard to MinIO."""
    print("Setting up test data...")
    s3 = boto3.resource('s3',
                    endpoint_url=ENDPOINT_URL,
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY,
                    config=Config(signature_version='s3v4'),
                    region_name='us-east-1')

    # Create dummy shard data (1000 floats)
    data = np.random.randn(1000).astype(np.float32).tobytes()
    
    try:
        # Ensure bucket exists (should be created by minio-init, but check)
        if s3.Bucket(BUCKET_NAME) not in s3.buckets.all():
            s3.create_bucket(Bucket=BUCKET_NAME)
        
        # Upload data
        obj = s3.Object(BUCKET_NAME, f"{PREFIX}{SHARD_NAME}")
        obj.put(Body=data)
        print(f"Uploaded {len(data)} bytes to s3://{BUCKET_NAME}/{PREFIX}{SHARD_NAME}")
        return data
    except Exception as e:
        print(f"Failed to setup test data: {e}")
        sys.exit(1)

def verify_loader():
    """Verify loading from MinIO via Rust."""
    print("Initializing loader...")
    try:
        # Create dataset config pointing to our local MinIO
        config = DatasetConfig(
            name="test_dataset", 
            shard_prefix=PREFIX, 
            num_shards=1,
            shard_pattern=SHARD_NAME.replace("000000", "{:0.6d}") # Just use the exact name or pattern if supported
        )
        # Note: pattern support depends on python_api.rs implementation. 
        # In python_api.rs/Dataset::shard_key using default pattern "shard_{:06d}.bin" if not specified.
        # Our file is shard_000000.bin, so default pattern works if we pass shard_id=0.
        
        dataset = Dataset(id="test_dataset", config=config, assigned_shards=[0])
        
        loader = DTrainerDataLoader(dataset, batch_size=1)
        print("SUCCESS: DTrainerDataLoader instantiated")
        
        print("Iterating loader to fetch data...")
        iterator = iter(loader)
        batch = next(iterator)
        
        if batch is not None and len(batch) > 0:
            print(f"SUCCESS: Got batch of shape {batch.shape}")
            print("VERIFICATION COMPLETE: Real data loaded from MinIO via Rust core!")
        else:
            print("WARNING: Batch was empty or None")
            
    except StopIteration:
        print("FAILURE: Loader yielded no batches (StopIteration)")
    except Exception as e:
        print(f"FAILURE: Exception during loading: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    setup_test_data()
    verify_loader()
