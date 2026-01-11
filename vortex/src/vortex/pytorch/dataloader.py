"""PyTorch DataLoader integration.

Provides a drop-in replacement for torch.utils.data.DataLoader
that uses Vortex's high-performance data loading pipeline.
"""

from typing import Any, Dict, Iterator, Optional
import logging

import torch
from torch.utils.data import IterableDataset

from vortex.dataset import Dataset

logger = logging.getLogger(__name__)


class VortexDataset(IterableDataset):
    """Iterable dataset backed by Vortex's data loading."""
    
    def __init__(self, dataset: Dataset, batch_size: int = 256):
        self.dataset = dataset
        self.batch_size = batch_size
        
    def __iter__(self) -> Iterator[torch.Tensor]:
        """Iterate over batches."""
        # In real implementation, this would:
        # 1. Connect to Rust DataLoader
        # 2. Receive ShardBatch objects
        # 3. Convert to PyTorch tensors
        
        # Placeholder: simulate batch generation
        for shard_id in self.dataset:
            # Would load actual shard data
            for batch_idx in range(10):  # Simulated batches per shard
                yield torch.randn(self.batch_size, 3, 224, 224)



import numpy as np
import vortex_core 

class VortexDataLoader:
    """High-performance DataLoader using Vortex's Rust core.
    
    This is a drop-in replacement for torch.utils.data.DataLoader
    that uses Vortex's async prefetching and backpressure.
    """
    
    def __init__(
        self,
        dataset: Dataset,
        batch_size: int = 256,
        prefetch_factor: int = 4,
        num_workers: int = 4,
    ):
        """Initialize the data loader.
        
        Args:
            dataset: Vortex dataset to load from
            batch_size: Batch size for training
            prefetch_factor: Number of batches to prefetch
            num_workers: Number of async workers (Rust tasks)
        """
        self.dataset = dataset
        self.batch_size = batch_size
        self.prefetch_factor = prefetch_factor
        self.num_workers = num_workers
        self._batches_loaded = 0
        
        # Initialize Rust client
        # Note: In a real app, the client/runtime might be a singleton shared across loaders
        self._client = vortex_core.VortexClient()
        
    def __iter__(self) -> Iterator[torch.Tensor]:
        """Iterate over batches.
        
        Yields:
            torch.Tensor: Batch of training data
        """
        self._batches_loaded = 0
        
        # Create Rust-side dataset from Python metadata
        rust_dataset = self._client.create_dataset(
            self.dataset.config.name,
            self.dataset.config.shard_prefix,
            self.dataset.config.num_shards
        )
        
        # Create Rust loader
        loader = self._client.create_loader(rust_dataset, self.batch_size)
        
        while True:
            # Get raw bytes from Rust
            batch_bytes = loader.next_batch()
            
            if batch_bytes is None:
                break
                
            self._batches_loaded += 1
            
            # Convert raw bytes to Tensor
            try:
                # Example: Interpret bytes as uint8 array
                data_array = np.frombuffer(batch_bytes, dtype=np.uint8)
                yield torch.from_numpy(data_array)
            except Exception as e:
                logger.error(f"Failed to convert batch: {e}")
                # For safety in this test phase, yield a dummy user validation warning
                yield torch.tensor([]) 
    
    def __len__(self) -> int:
        """Estimated number of batches (may not be exact)."""
        # Rough estimate based on shards
        return len(self.dataset) * 10  # This remains an estimate
    
    def state_dict(self) -> Dict[str, Any]:
        """Get loader state for checkpointing.
        
        Returns:
            State dict containing loader position
        """
        return {
            "dataset_id": self.dataset.id,
            "batches_loaded": self._batches_loaded,
            "current_shard_idx": self.dataset._current_idx,
        }
    
    def load_state_dict(self, state: Dict[str, Any]) -> None:
        """Restore loader state from checkpoint.
        
        Args:
            state: State dict from previous checkpoint
        """
        if state["dataset_id"] != self.dataset.id:
            logger.warning(
                f"Dataset ID mismatch: {state['dataset_id']} vs {self.dataset.id}"
            )
            return
        
        self._batches_loaded = state.get("batches_loaded", 0)
        self.dataset._current_idx = state.get("current_shard_idx", 0)
        logger.info(f"Restored loader state: {self._batches_loaded} batches loaded")
