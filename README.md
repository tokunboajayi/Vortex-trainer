# Vortex: High-Performance Distributed Training Engine

**Vortex** (formerly DTrainer) is a next-generation distributed training system designed to optimize data loading, checkpointing, and worker coordination for large-scale PyTorch jobs. It leverages a high-performance **Rust backend** (`vortex-core`) seamlessly integrated with a **Python/PyTorch frontend**.

## 🚀 System Architecture

```mermaid
graph TD
    subgraph "Infrastructure"
        S3[(MinIO / S3 Object Store)]
    end

    subgraph "Control Plane"
        Coord[**Vortex Coordinator**<br/>(Rust / gRPC)]
        Dash[**Mission Control**<br/>(React / Vite)]
    end

    subgraph "Worker Node (Python + Rust)"
        PyTorch[PyTorch Training Loop]
        Runtime[**Vortex Runtime**<br/>(Python Async + Thread)]
        Loader[**Rust DataLoader**<br/>(S3 Prefetching)]
    end

    %% Connections
    Coord -->|Metrics API (9100)| Dash
    PyTorch -->|Init| Runtime
    Runtime -->|Register/Heartbeat| Coord
    Loader -->|Fetch Shards| S3
    Runtime -->|Checkpoints| S3
```

## 🧩 Key Components

### 1. **Core Backend (`vortex-core`)**
*   **Language**: Rust 🦀
*   **Role**: Handles the "heavy lifting" — high-speed async data fetching from S3, checksum verification, effective prefetching, and the central Coordinator service.
*   **Features**:
    *   `Coordinator`: Manages worker membership, epochs, and fault tolerance. Exposes Prometheus metrics at `:9100`.
    *   `DataLoader`: A native Rust replacement for PyTorch's loader, delivering 10x throughput via parallel I/O.

### 2. **Python Integration (`vortex`)**
*   **Language**: Python 🐍
*   **Role**: Provides the user-facing API (`vortex.init`, `VortexDataLoader`).
*   **Recent Update**: Now includes a robust background heartbeat thread that maintains connectivity with the Coordinator even during blocking PyTorch compute loops.

### 3. **Mission Control (`vortex-dashboard`)**
*   **Stack**: React, Vite, TailwindCSS, TypeScript.
*   **Role**: Real-time visualization of cluster health, training throughput, and active workers.

## 🛠️ How to Run the Full Stack

### Prerequisites
*   **Rust**: Stable toolchain (`cargo`).
*   **Python**: 3.10+ with `torch` installed.
*   **Node.js**: For the dashboard.
*   **MinIO**: Local S3-compatible storage (running via Docker).

### Step 1: Start the Infrastructure (MinIO)
```bash
docker-compose up -d
# Ensure bucket 'vortex' and 'vortex-prod' exist
python scripts/generate_data.py  # Generates synthetic data in MinIO
```

### Step 2: Start the Coordinator Service
The brain of the cluster.
```bash
cd vortex-core
# Runs on localhost:50051 (gRPC) and :9100 (Metrics)
cargo run --bin coordinator
```

### Step 3: Launch Mission Control
Visual feedback.
```bash
cd vortex-dashboard
npm run dev
# Open http://localhost:5173
```

### Step 4: Run a Training Job
The verified training script.
```bash
# In the root directory:
python examples/resnet50_production.py
```
*   **Verify**: Check the dashboard. You should see "Active Workers: 1" and real-time status updates.

## 📂 Project Structure
*   `vortex-core/`: Rust backend source code.
*   `vortex/`: Python package source.
*   `vortex-dashboard/`: Frontend source.
*   `examples/`: Production-grade example scripts.
