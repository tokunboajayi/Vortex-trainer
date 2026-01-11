//! Checkpoint management
//!
//! Async, incremental checkpointing with atomic commit semantics.

pub mod manager;
pub mod writer;
pub mod reader;
pub mod manifest;
pub mod incremental;

pub use manager::{CheckpointManager, CheckpointConfig};
pub use writer::CheckpointWriter;
pub use reader::CheckpointReader;
pub use manifest::{Manifest, ManifestShard};

