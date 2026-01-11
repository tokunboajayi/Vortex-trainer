//! High-throughput data loader
//!
//! Main interface for loading shards and iterating batches.

use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;

use super::backpressure::{BackpressureConfig, BackpressureController};
use super::dataset::Dataset;
use super::prefetcher::{Prefetcher, PrefetcherConfig};
use super::shard::{ShardBatch, ShardSpec};
use crate::storage::S3Client;
use crate::error::{DTrainerError, Result};
use crate::runtime::DTrainerRuntime;

/// Configuration for the data loader
#[derive(Debug, Clone)]
pub struct DataLoaderConfig {
    /// Prefetch configuration
    pub prefetch: PrefetcherConfig,
    /// Backpressure configuration
    pub backpressure: BackpressureConfig,
    /// Channel buffer size
    pub channel_size: usize,
}

impl Default for DataLoaderConfig {
    fn default() -> Self {
        Self {
            prefetch: PrefetcherConfig::default(),
            backpressure: BackpressureConfig::default(),
            channel_size: 16,
        }
    }
}

/// High-throughput async data loader
/// 
/// Provides an async iterator interface for loading batches from shards.
/// Uses prefetching and backpressure to maximize throughput.
pub struct DataLoader {
    /// Channel to receive batches from prefetcher
    receiver: mpsc::Receiver<ShardBatch>,
    /// Backpressure controller
    backpressure: BackpressureController,
    /// Dataset being loaded
    dataset: Arc<Dataset>,
    /// Total batches loaded
    batches_loaded: u64,
    /// Task handle for prefetcher
    prefetch_handle: Option<tokio::task::JoinHandle<()>>,
}

impl DataLoader {
    /// Create a new data loader for a dataset
    pub fn new(
        runtime: &DTrainerRuntime,
        dataset: Arc<Dataset>,
        storage: Arc<S3Client>,
        config: DataLoaderConfig,
    ) -> Self {
        // Create channel for batch delivery
        let (sender, receiver) = mpsc::channel(config.channel_size);

        // Create backpressure controller
        let backpressure = BackpressureController::new(config.backpressure);

        // Build shard specs from dataset
        let shards: Vec<ShardSpec> = dataset
            .assigned_shards
            .iter()
            .map(|&id| ShardSpec::new(id, dataset.shard_key(id)))
            .collect();

        // Create and spawn prefetcher
        let prefetcher = Prefetcher::new(
            sender,
            shards,
            storage,
            backpressure.clone(),
            config.prefetch,
        );

        let prefetch_handle = runtime.spawn_io(prefetcher.run());

        Self {
            receiver,
            backpressure,
            dataset,
            batches_loaded: 0,
            prefetch_handle: Some(prefetch_handle),
        }
    }

    /// Get the next batch
    /// 
    /// This is the hot path, called every training step.
    /// Lock-free and allocation-free in steady state.
    pub async fn next_batch(&mut self) -> Result<ShardBatch> {
        // Backpressure check (fast path: no-op if not under pressure)
        self.backpressure.wait_if_needed().await;

        // Receive from prefetch buffer
        match self.receiver.recv().await {
            Some(batch) => {
                self.backpressure.record_consumption();
                self.batches_loaded += 1;
                Ok(batch)
            }
            None => Err(DTrainerError::DataExhausted),
        }
    }

    /// Try to get a batch without blocking
    pub fn try_next_batch(&mut self) -> Option<ShardBatch> {
        match self.receiver.try_recv() {
            Ok(batch) => {
                self.backpressure.record_consumption();
                self.batches_loaded += 1;
                Some(batch)
            }
            Err(_) => None,
        }
    }

    /// Get total batches loaded
    pub fn batches_loaded(&self) -> u64 {
        self.batches_loaded
    }

    /// Get pending batch count
    pub fn pending_count(&self) -> usize {
        self.backpressure.pending_count()
    }

    /// Get dataset reference
    pub fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    /// Get loader state for checkpointing
    pub fn state_dict(&self) -> DataLoaderState {
        DataLoaderState {
            dataset_id: self.dataset.id.clone(),
            batches_loaded: self.batches_loaded,
            // Note: Full state would include shard position
        }
    }

    /// Shutdown the loader and wait for prefetcher
    pub async fn shutdown(mut self) {
        // Drop receiver to signal prefetcher to stop
        drop(self.receiver);

        // Wait for prefetcher task
        if let Some(handle) = self.prefetch_handle.take() {
            let _ = handle.await;
        }

        debug!("DataLoader shutdown complete, loaded {} batches", self.batches_loaded);
    }
}

/// Serializable state for checkpointing
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataLoaderState {
    pub dataset_id: String,
    pub batches_loaded: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests would go here with mock S3 client
}
