"""Dataset registration and management."""

from dataclasses import dataclass
from typing import List, Optional, Iterator
import logging

from vortex.config import DatasetConfig, ShardAssignment

logger = logging.getLogger(__name__)


@dataclass
class Dataset:
    """A registered dataset with shard assignments.
    
    This class represents a dataset that has been registered with the
    coordinator and has shards assigned to this worker.
    """
    id: str
    config: DatasetConfig
    assigned_shards: List[int]
    _current_idx: int = 0
    
    def __iter__(self) -> Iterator[int]:
        """Iterate over assigned shard IDs."""
        return iter(self.assigned_shards)
    
    def __len__(self) -> int:
        """Number of assigned shards."""
        return len(self.assigned_shards)
    
    def next_shard(self) -> Optional[int]:
        """Get the next shard ID to process."""
        if self._current_idx >= len(self.assigned_shards):
            return None
        shard_id = self.assigned_shards[self._current_idx]
        self._current_idx += 1
        return shard_id
    
    def reset(self) -> None:
        """Reset to the beginning of assigned shards."""
        self._current_idx = 0
    
    def is_exhausted(self) -> bool:
        """Check if all shards have been consumed."""
        return self._current_idx >= len(self.assigned_shards)
    
    def shard_key(self, shard_id: int) -> str:
        """Get the storage key for a shard."""
        pattern = self.config.shard_pattern or "shard_{:06d}.bin"
        filename = pattern.format(shard_id)
        return f"{self.config.shard_prefix}{filename}"


# Global dataset registry
_datasets: dict[str, Dataset] = {}


def register_dataset(
    name: str,
    shard_prefix: str,
    num_shards: int,
    shuffle_shards: bool = True,
    seed: int = 42,
    shard_pattern: Optional[str] = None,
) -> Dataset:
    """Register a dataset with the coordinator.
    
    This function registers a dataset and requests shard assignments
    from the coordinator. The registration is idempotent - calling
    with the same name will return the existing dataset.
    
    Args:
        name: Unique dataset identifier
        shard_prefix: S3 prefix for shards
        num_shards: Total number of shards
        shuffle_shards: Whether to shuffle shards
        seed: Random seed for shuffling
        shard_pattern: Optional filename pattern
        
    Returns:
        Dataset: The registered dataset with shard assignments
    """
    # Check if already registered
    if name in _datasets:
        logger.info(f"Dataset '{name}' already registered, returning existing")
        return _datasets[name]
    
    config = DatasetConfig(
        name=name,
        shard_prefix=shard_prefix,
        num_shards=num_shards,
        shuffle_shards=shuffle_shards,
        seed=seed,
        shard_pattern=shard_pattern,
    )
    
    # In real implementation, this calls into Rust via PyO3
    # to register with coordinator and get shard assignments
    # For now, simulate with placeholder
    logger.info(f"Registering dataset '{name}' with {num_shards} shards")
    
    # Placeholder: would come from coordinator
    assigned_shards = list(range(num_shards))  # Worker 0 gets all in single-worker mode
    
    dataset = Dataset(
        id=name,
        config=config,
        assigned_shards=assigned_shards,
    )
    
    _datasets[name] = dataset
    return dataset


def get_dataset(name: str) -> Optional[Dataset]:
    """Get a registered dataset by name."""
    return _datasets.get(name)


def list_datasets() -> List[str]:
    """List all registered dataset names."""
    return list(_datasets.keys())
