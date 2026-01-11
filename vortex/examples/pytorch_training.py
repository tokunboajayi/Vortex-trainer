"""
Example PyTorch training loop with DTrainer.

This example demonstrates:
- Runtime initialization
- Dataset registration
- High-performance data loading
- Async checkpointing
- Epoch synchronization
- Graceful recovery from checkpoints
"""

import os
import torch
import torch.nn as nn
import torch.optim as optim
import dtrainer
from dtrainer.pytorch import DTrainerDataLoader, CheckpointCallback


def create_model():
    """Create a simple model for demonstration."""
    return nn.Sequential(
        nn.Conv2d(3, 64, 3, padding=1),
        nn.ReLU(),
        nn.AdaptiveAvgPool2d(1),
        nn.Flatten(),
        nn.Linear(64, 1000),
    )


def train_step(model, batch, optimizer):
    """Single training step."""
    optimizer.zero_grad()
    output = model(batch)
    # Dummy loss for demonstration
    loss = output.mean()
    loss.backward()
    optimizer.step()
    return loss.item()


def main():
    # 1. Initialize runtime
    config = dtrainer.RuntimeConfig(
        coordinator_addr=os.environ.get("COORDINATOR_ADDR", "localhost:50051"),
        worker_id=int(os.environ.get("WORKER_ID", "0")),
        # Use localhost for local dev, or minio if in docker
        s3_endpoint=os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
        s3_bucket=os.environ.get("S3_BUCKET", "dtrainer"),
    )
    
    # Set env vars for Rust core to pick up (since python_api reads them)
    os.environ["S3_ENDPOINT"] = config.s3_endpoint
    os.environ["S3_BUCKET"] = config.s3_bucket
    os.environ["S3_ACCESS_KEY"] = os.environ.get("S3_ACCESS_KEY", "minioadmin")
    os.environ["S3_SECRET_KEY"] = os.environ.get("S3_SECRET_KEY", "minioadmin")
    
    runtime = dtrainer.init(config)
    print(f"[Worker {config.worker_id}] Runtime initialized")
    
    # 2. Register dataset
    dataset = dtrainer.register_dataset(
        name="test_dataset",
        shard_prefix="test_data/",
        num_shards=20, # We will generate 20 shards
        shuffle_shards=True,
        seed=42,
    )
    print(f"[Worker {config.worker_id}] Dataset registered with {len(dataset)} shards")
    
    # 3. Create data loader with prefetching
    loader = DTrainerDataLoader(
        dataset=dataset,
        batch_size=256,
        prefetch_factor=4,
        num_workers=4,
    )
    
    # 4. Initialize model and optimizer
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = create_model().to(device)
    optimizer = optim.AdamW(model.parameters(), lr=1e-4)
    
    # 5. Setup checkpointing
    ckpt_manager = dtrainer.CheckpointManager(
        job_id="training-run-001",
        checkpoint_interval=1000,
        async_write=True,
        incremental=True,
    )
    
    # 6. Restore from checkpoint if exists
    start_epoch, start_step = ckpt_manager.restore_if_exists(
        model=model,
        optimizer=optimizer,
        loader=loader,
    )
    print(f"[Worker {config.worker_id}] Starting from epoch={start_epoch}, step={start_step}")
    
    # 7. Training loop
    num_epochs = 1000
    global_step = start_step
    import time
    
    for epoch in range(start_epoch, num_epochs):
        model.train()
        epoch_loss = 0.0
        batch_count = 0
        
        for batch in loader:
            # Simulate computation time
            time.sleep(0.05)
            
            batch = batch.to(device)
            
            # Forward/backward pass
            loss = train_step(model, batch, optimizer)
            epoch_loss += loss
            batch_count += 1
            global_step += 1
            
            # Async checkpoint (non-blocking)
            ckpt_manager.maybe_checkpoint(
                step=global_step,
                model=model,
                optimizer=optimizer,
                loader_state=loader.state_dict(),
            )
            
            # Progress logging
            if global_step % 100 == 0:
                print(f"[Worker {config.worker_id}] Step {global_step}, Loss: {loss:.4f}")
        
        # Epoch boundary
        avg_loss = epoch_loss / batch_count if batch_count > 0 else 0
        print(f"[Worker {config.worker_id}] Epoch {epoch} complete, Avg Loss: {avg_loss:.4f}")
        
        # Synchronize with other workers
        runtime.sync_epoch(epoch)
    
    # 8. Final checkpoint
    ckpt_manager.checkpoint(
        step="final",
        model=model,
        optimizer=optimizer,
        force=True,
    )
    
    # 9. Shutdown
    dtrainer.shutdown()
    print(f"[Worker {config.worker_id}] Training complete!")


if __name__ == "__main__":
    main()
