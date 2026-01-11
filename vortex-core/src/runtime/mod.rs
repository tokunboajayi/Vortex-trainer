//! Async runtime management for DTrainer
//!
//! Provides dual Tokio runtimes optimized for I/O and compute workloads.

pub mod executor;
pub mod shutdown;

pub use executor::DTrainerRuntime;
pub use shutdown::ShutdownSignal;
