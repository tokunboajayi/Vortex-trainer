//! Checkpoint manager
//!
//! Orchestrates checkpoint operations across workers.

use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::manifest::Manifest;
use super::reader::CheckpointReader;
use super::writer::{CheckpointWriter, CheckpointCommitter, WriterConfig};
use crate::storage::S3Client;
use crate::error::{DTrainerError, Result};

/// Configuration for checkpoint manager
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// Checkpoint interval in steps
    pub interval_steps: u64,
    /// Enable async (non-blocking) checkpoints
    pub async_checkpoint: bool,
    /// Enable incremental checkpoints
    pub incremental: bool,
    /// Writer configuration
    pub writer: WriterConfig,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval_steps: 1000,
            async_checkpoint: true,
            incremental: true,
            writer: WriterConfig::default(),
        }
    }
}

/// Checkpoint manager for a training job
pub struct CheckpointManager {
    storage: Arc<S3Client>,
    job_id: String,
    config: CheckpointConfig,
    /// Last checkpointed step
    last_checkpoint_step: RwLock<u64>,
    /// Current epoch
    current_epoch: RwLock<u64>,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(storage: Arc<S3Client>, job_id: String, config: CheckpointConfig) -> Self {
        Self {
            storage,
            job_id,
            config,
            last_checkpoint_step: RwLock::new(0),
            current_epoch: RwLock::new(0),
        }
    }

    /// Create a checkpoint writer for a worker
    pub fn create_writer(&self, worker_id: u32) -> CheckpointWriter {
        CheckpointWriter::new(
            self.storage.clone(),
            self.job_id.clone(),
            worker_id,
            self.config.writer.clone(),
        )
    }

    /// Create a checkpoint committer (coordinator only)
    pub fn create_committer(&self) -> CheckpointCommitter {
        CheckpointCommitter::new(self.storage.clone(), self.job_id.clone())
    }

    /// Create a checkpoint reader
    pub fn create_reader(&self) -> CheckpointReader {
        CheckpointReader::new(self.storage.clone(), self.job_id.clone())
    }

    /// Check if checkpoint is due at this step
    pub async fn should_checkpoint(&self, step: u64) -> bool {
        let last = *self.last_checkpoint_step.read().await;
        step >= last + self.config.interval_steps
    }

    /// Record a successful checkpoint
    pub async fn record_checkpoint(&self, step: u64, epoch: u64) {
        *self.last_checkpoint_step.write().await = step;
        *self.current_epoch.write().await = epoch;
        info!("Recorded checkpoint at step {}, epoch {}", step, epoch);
    }

    /// Get the last checkpointed step
    pub async fn last_checkpoint_step(&self) -> u64 {
        *self.last_checkpoint_step.read().await
    }

    /// Get current epoch
    pub async fn current_epoch(&self) -> u64 {
        *self.current_epoch.read().await
    }

    /// Attempt to restore from checkpoint
    pub async fn restore(&self) -> Result<Option<RestoreResult>> {
        let reader = self.create_reader();

        match reader.restore_latest().await? {
            Some((manifest, _shards)) => {
                *self.last_checkpoint_step.write().await = manifest.global_step;
                *self.current_epoch.write().await = manifest.epoch;

                info!(
                    "Restored from epoch {}, step {}",
                    manifest.epoch, manifest.global_step
                );

                Ok(Some(RestoreResult {
                    epoch: manifest.epoch,
                    step: manifest.global_step,
                    manifest,
                }))
            }
            None => {
                info!("No checkpoint found, starting fresh");
                Ok(None)
            }
        }
    }

    /// Get job ID
    pub fn job_id(&self) -> &str {
        &self.job_id
    }
}

/// Result of a checkpoint restore operation
#[derive(Debug)]
pub struct RestoreResult {
    /// Restored epoch
    pub epoch: u64,
    /// Restored step
    pub step: u64,
    /// Full manifest
    pub manifest: Manifest,
}
