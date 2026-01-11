"""
DTrainer - High-performance distributed training runtime

This package provides a Python API backed by a Rust core for:
- Dataset sharding and loading
- Asynchronous checkpointing
- Worker coordination
- Failure recovery
"""

from vortex.config import RuntimeConfig, DatasetConfig, CheckpointConfig
from vortex.dataset import Dataset, register_dataset
from vortex.checkpoint import CheckpointManager
from vortex.coordinator import WorkerHandle
from vortex.runtime import init, shutdown

__version__ = "0.1.0"

__all__ = [
    # Configuration
    "RuntimeConfig",
    "DatasetConfig",
    "CheckpointConfig",
    # Core API
    "Dataset",
    "register_dataset",
    "CheckpointManager",
    "WorkerHandle",
    # Runtime
    "init",
    "shutdown",
]
