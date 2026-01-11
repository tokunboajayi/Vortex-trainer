"""
Production-grade training script using ResNet50 and Distributed Runtime.
"""
import os
import torch
import torch.nn as nn
import torch.optim as optim
import torchvision.models as models
import vortex
from vortex.pytorch import VortexDataLoader
import time

def setup_logger(worker_id):
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format=f'[Worker {worker_id}] %(asctime)s - %(message)s',
        datefmt='%H:%M:%S'
    )
    return logging.getLogger(__name__)

def main():
    # -------------------------------------------------------------------------
    # 1. Configuration (Production Settings)
    # -------------------------------------------------------------------------
    worker_id = int(os.environ.get("WORKER_ID", "0"))
    logger = setup_logger(worker_id)
    
    config = vortex.RuntimeConfig(
        coordinator_addr=os.environ.get("COORDINATOR_ADDR", "localhost:50051"),
        worker_id=worker_id,
        s3_endpoint=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
        s3_bucket="vortex-prod", 
    )

    # -------------------------------------------------------------------------
    # 2. Runtime Initialization
    # -------------------------------------------------------------------------
    logger.info("Initializing Vortex Runtime...")
    runtime = vortex.init(config)
    
    # ... (Model setup unchanged)
    logger.info("Building ResNet50 model...")
    # Use valid weights parameter instead of pretrained=False (deprecated)
    model = models.resnet50(weights=None) 
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = model.to(device)
    
    optimizer = optim.SGD(model.parameters(), lr=0.01, momentum=0.9)
    criterion = nn.CrossEntropyLoss()

    # -------------------------------------------------------------------------
    # 4. Dataset Registration & Loading
    # -------------------------------------------------------------------------
    logger.info("Registering Synthetic ImageNet dataset...")
    dataset = vortex.register_dataset(
        name="synthetic-imagenet",
        shard_prefix="s3://vortex-prod/datasets/synthetic-imagenet/shards/",
        num_shards=16, 
        shuffle_shards=True,
        seed=2024,
    )

    loader = VortexDataLoader(
        dataset=dataset,
        batch_size=32,       # Standard batch size
        prefetch_factor=2,
        num_workers=2,
    )

    # -------------------------------------------------------------------------
    # 5. Checkpoint Management
    # -------------------------------------------------------------------------
    ckpt_manager = vortex.CheckpointManager(
        job_id="resnet50-prod-run-001",
        checkpoint_interval=50, 
        async_write=True,
    )

    start_epoch, start_step = ckpt_manager.restore_if_exists(
        model=model, 
        optimizer=optimizer,
        loader=loader
    )

    # ... (Training loop unchanged) 

    logger.info("Training Run Complete.")
    vortex.shutdown()

if __name__ == "__main__":
    main()
