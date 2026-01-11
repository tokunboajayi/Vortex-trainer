# Vortex Python Client

The Python client for the Vortex Distributed Training Engine.

## Installation

```bash
pip install .
```

## Usage

```python
import vortex
from vortex.pytorch import VortexDataLoader

# 1. Configure
config = vortex.RuntimeConfig(
    coordinator_addr="localhost:50051",
    worker_id=0
)

# 2. Initialize
runtime = vortex.init(config)

# 3. Register Dataset
dataset = vortex.register_dataset(
    name="imagenet",
    shard_prefix="s3://vortex-prod/datasets/imagenet/shards/",
    num_shards=1024
)

# 4. Create Loader
loader = VortexDataLoader(dataset, batch_size=32)

# 5. Train
for batch in loader:
    train(batch)
```
