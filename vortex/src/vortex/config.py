"""Configuration dataclasses for Vortex."""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RuntimeConfig:
    """Configuration for the Vortex runtime.
    
    Args:
        coordinator_addr: Address of the coordinator (e.g., "coordinator:50051")
        worker_id: Unique worker identifier (typically from environment)
        s3_endpoint: S3-compatible storage endpoint
        s3_bucket: Storage bucket name
        s3_access_key: Optional S3 access key
        s3_secret_key: Optional S3 secret key
        io_threads: Number of I/O threads
        compute_threads: Number of compute threads
    """
    coordinator_addr: str
    worker_id: int
    s3_endpoint: str = "http://localhost:9000"
    s3_bucket: str = "dtrainer"
    s3_access_key: Optional[str] = None
    s3_secret_key: Optional[str] = None
    io_threads: int = 4
    compute_threads: int = 2


@dataclass
class DatasetConfig:
    """Configuration for dataset registration.
    
    Args:
        name: Unique dataset identifier
        shard_prefix: S3 prefix for shards
        num_shards: Total number of shards
        shuffle_shards: Whether to shuffle shards across workers
        seed: Random seed for shuffling
        shard_pattern: Optional pattern for shard filenames
    """
    name: str
    shard_prefix: str
    num_shards: int
    shuffle_shards: bool = True
    seed: int = 42
    shard_pattern: Optional[str] = None


@dataclass
class CheckpointConfig:
    """Configuration for checkpointing.
    
    Args:
        job_id: Unique job identifier
        checkpoint_interval: Steps between checkpoints
        async_write: Use async (non-blocking) writes
        incremental: Use incremental checkpoints
        verify_writes: Verify writes with read-back
    """
    job_id: str
    checkpoint_interval: int = 1000
    async_write: bool = True
    incremental: bool = True
    verify_writes: bool = False


@dataclass
class ShardAssignment:
    """A shard assigned to a worker.
    
    Args:
        shard_id: Shard identifier
        object_key: S3 object key
        byte_offset: Offset within object
        byte_length: Length to read
        expected_crc32c: Expected checksum
    """
    shard_id: int
    object_key: str
    byte_offset: int = 0
    byte_length: int = 0
    expected_crc32c: Optional[int] = None
