# Contributing to Vortex

## Development Environment Setup

### Prerequisites
- **Rust**: Latest stable toolchain (`rustup install stable`)
- **Python**: 3.8+
- **Node.js**: 18+ (for Dashboard)
- **Docker**: For MinIO storage backend

### Repository Structure
- `vortex-core/`: Rust backend and Coordinator service.
- `vortex/`: Python client library (PyO3 bindings).
- `vortex-dashboard/`: React-based UI.
- `examples/`: Usage examples.

---

## 🛠️ Developing the Core (Rust)

1. **Build**:
   ```bash
   cd vortex-core
   cargo build
   ```

2. **Run Coordinator**:
   ```bash
   cargo run --bin coordinator
   ```

3. **Run Tests**:
   ```bash
   cargo test
   ```

---

## 🐍 Developing the Python Integration

The Python package is built using `maturin`.

1. **Install Development Dependencies**:
   ```bash
   pip install maturin
   ```

2. **Build & Install (Editable Mode)**:
   This allows you to modify Python files without reinstalling. If you modify Rust code, you must rerun this command.
   ```bash
   cd vortex-core
   maturin develop
   ```

---

## 📊 Developing the Dashboard

1. **Install Dependencies**:
   ```bash
   cd vortex-dashboard
   npm install
   ```

2. **Run Dev Server**:
   ```bash
   npm run dev
   ```

---

## 🧪 Verification

To verify your changes, run the production example:
```bash
# Ensure Coordinator and MinIO are running
python examples/resnet50_production.py
```
