//! Vortex Core - High-performance distributed training runtime
//!
//! This crate provides the Rust core for coordinating:
//! - Dataset sharding and loading
//! - Asynchronous checkpointing
//! - Worker coordination
//! - Failure recovery

pub mod coordinator;
pub mod data;
pub mod checkpoint;
pub mod error;
pub mod protocol;
pub mod runtime;
pub mod storage;
pub mod worker;
pub mod compression;
pub mod metrics;
#[cfg(feature = "python")]
pub mod python_api;

pub use error::DTrainerError;
pub use runtime::DTrainerRuntime;

/// Protocol version for compatibility checking
pub const PROTOCOL_VERSION: u32 = 1;

/// Default heartbeat interval in seconds
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;

/// Default heartbeat timeout in seconds
pub const DEFAULT_HEARTBEAT_TIMEOUT_SECS: u64 = 15;

/// Default prefetch buffer size
pub const DEFAULT_PREFETCH_BUFFER_SIZE: usize = 4;
