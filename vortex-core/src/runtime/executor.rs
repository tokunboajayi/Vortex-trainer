//! Tokio runtime executor configuration
//!
//! Provides separate I/O and compute runtimes to prevent I/O stalls
//! from blocking CPU-bound work like serialization and checksumming.

use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use crate::error::{DTrainerError, Result};

/// Configuration for the DTrainer runtime
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// Number of threads for I/O operations
    pub io_threads: usize,
    /// Number of threads for compute operations
    pub compute_threads: usize,
    /// Enable tokio-console for debugging
    pub enable_console: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        let cpus = num_cpus::get();
        Self {
            io_threads: cpus.max(4),
            compute_threads: (cpus / 2).max(2),
            enable_console: false,
        }
    }
}

/// Dual-runtime executor for DTrainer
/// 
/// Separates I/O-bound work (S3 reads/writes, gRPC) from compute-bound
/// work (checksums, serialization, compression) to prevent stalls.
pub struct DTrainerRuntime {
    /// High-priority I/O runtime
    io_runtime: Runtime,
    /// CPU-bound work runtime
    compute_runtime: Runtime,
    /// Shared configuration
    config: RuntimeConfig,
}

impl DTrainerRuntime {
    /// Create a new runtime with the given configuration
    pub fn new(config: RuntimeConfig) -> Result<Self> {
        let io_runtime = Builder::new_multi_thread()
            .worker_threads(config.io_threads)
            .thread_name("dtrainer-io")
            .enable_all()
            .build()
            .map_err(|e| DTrainerError::Internal { 
                message: format!("Failed to create I/O runtime: {}", e) 
            })?;

        let compute_runtime = Builder::new_multi_thread()
            .worker_threads(config.compute_threads)
            .thread_name("dtrainer-compute")
            .enable_all()
            .build()
            .map_err(|e| DTrainerError::Internal { 
                message: format!("Failed to create compute runtime: {}", e) 
            })?;

        Ok(Self {
            io_runtime,
            compute_runtime,
            config,
        })
    }

    /// Spawn an I/O-bound task
    pub fn spawn_io<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.io_runtime.spawn(future)
    }

    /// Spawn a compute-bound task
    pub fn spawn_compute<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.compute_runtime.spawn(future)
    }

    /// Run a future on the I/O runtime, blocking until complete
    pub fn block_on_io<F: std::future::Future>(&self, future: F) -> F::Output {
        self.io_runtime.block_on(future)
    }

    /// Get the I/O runtime handle
    pub fn io_handle(&self) -> tokio::runtime::Handle {
        self.io_runtime.handle().clone()
    }

    /// Get the compute runtime handle
    pub fn compute_handle(&self) -> tokio::runtime::Handle {
        self.compute_runtime.handle().clone()
    }

    /// Graceful shutdown of both runtimes
    pub fn shutdown(self) {
        // Shutdown compute first (less critical)
        self.compute_runtime.shutdown_background();
        // Then I/O (may have in-flight writes)
        self.io_runtime.shutdown_timeout(std::time::Duration::from_secs(30));
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_creation() {
        let config = RuntimeConfig::default();
        let runtime = DTrainerRuntime::new(config).unwrap();
        
        let result = runtime.block_on_io(async {
            42
        });
        
        assert_eq!(result, 42);
    }
}
