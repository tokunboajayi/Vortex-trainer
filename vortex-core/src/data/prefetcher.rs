//! Async data prefetcher
//!
//! Runs on a separate task, fetching shards ahead of consumption.

use std::collections::VecDeque;
use std::sync::Arc;
use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use tracing::{debug, error, warn};

use super::backpressure::BackpressureController;
use super::shard::{ShardBatch, ShardSpec};
use crate::storage::S3Client;
use crate::error::{DTrainerError, Result};

/// Configuration for the prefetcher
#[derive(Debug, Clone)]
pub struct PrefetcherConfig {
    /// Number of batches to prefetch
    pub prefetch_count: usize,
    /// Batch size in bytes
    pub batch_size: usize,
    /// Number of concurrent fetches
    pub concurrent_fetches: usize,
    /// Retry count for failed fetches
    pub retry_count: u32,
}

impl Default for PrefetcherConfig {
    fn default() -> Self {
        Self {
            prefetch_count: 4,
            batch_size: 1024 * 1024, // 1MB batches
            concurrent_fetches: 4,
            retry_count: 3,
        }
    }
}

/// Async prefetcher that loads shards ahead of consumption
pub struct Prefetcher {
    /// Channel to send batches to the loader
    sender: mpsc::Sender<ShardBatch>,
    /// Queue of shards to fetch
    shard_queue: VecDeque<ShardSpec>,
    /// S3 client for fetching
    storage: Arc<S3Client>,
    /// Backpressure controller
    backpressure: BackpressureController,
    /// Configuration
    config: PrefetcherConfig,
    /// Buffer pool for reuse
    _buffer_pool: BufferPool,
}

impl Prefetcher {
    /// Create a new prefetcher
    pub fn new(
        sender: mpsc::Sender<ShardBatch>,
        shards: Vec<ShardSpec>,
        storage: Arc<S3Client>,
        backpressure: BackpressureController,
        config: PrefetcherConfig,
    ) -> Self {
        Self {
            sender,
            shard_queue: VecDeque::from(shards),
            storage,
            backpressure,
            _buffer_pool: BufferPool::new(config.prefetch_count * 2, config.batch_size),
            config,
        }
    }

    /// Run the prefetcher loop
    pub async fn run(mut self) {
        debug!("Prefetcher starting with {} shards", self.shard_queue.len());

        while let Some(shard) = self.shard_queue.pop_front() {
            // Acquire backpressure permit
            let permit = self.backpressure.acquire().await;

            // Fetch shard with retry
            match self.fetch_shard_with_retry(&shard).await {
                Ok(batches) => {
                    for batch in batches {
                        if self.sender.send(batch).await.is_err() {
                            debug!("Prefetcher: consumer dropped, stopping");
                            return;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to fetch shard {}: {:?}", shard.id, e);
                    // Continue with next shard
                }
            }

            // Permit dropped here, releasing backpressure slot
            drop(permit);
        }

        debug!("Prefetcher completed, all shards processed");
    }

    /// Fetch a shard with retry logic
    async fn fetch_shard_with_retry(&self, shard: &ShardSpec) -> Result<Vec<ShardBatch>> {
        let mut last_error = None;

        for attempt in 0..self.config.retry_count {
            match self.fetch_shard(shard).await {
                Ok(batches) => return Ok(batches),
                Err(e) => {
                    warn!(
                        "Attempt {}/{} failed for shard {}: {:?}",
                        attempt + 1,
                        self.config.retry_count,
                        shard.id,
                        e
                    );
                    last_error = Some(e);

                    // Exponential backoff
                    let delay = std::time::Duration::from_millis(100 * 2u64.pow(attempt));
                    tokio::time::sleep(delay).await;
                }
            }
        }

        Err(last_error.unwrap_or(DTrainerError::Internal {
            message: "Unknown fetch error".into(),
        }))
    }

    /// Fetch a single shard and split into batches
    async fn fetch_shard(&self, shard: &ShardSpec) -> Result<Vec<ShardBatch>> {
        // Fetch from S3
        let data = self.storage.get_object(&shard.key).await?;

        // Verify checksum if expected
        if let Some(expected) = shard.expected_crc32c {
            let actual = crc32c::crc32c(&data);
            if actual != expected {
                return Err(DTrainerError::ChecksumMismatch {
                    key: shard.key.clone(),
                    expected,
                    actual,
                });
            }
        }

        // Split into batches
        let batch_size = self.config.batch_size;
        let total_batches = (data.len() + batch_size - 1) / batch_size;
        let mut batches = Vec::with_capacity(total_batches);

        for (idx, chunk) in data.chunks(batch_size).enumerate() {
            let batch = ShardBatch::new(
                shard.id,
                Bytes::copy_from_slice(chunk),
                (idx * batch_size) as u64,
                idx as u32,
                total_batches as u32,
            );
            batches.push(batch);
        }

        debug!(
            "Fetched shard {} ({} bytes, {} batches)",
            shard.id,
            data.len(),
            batches.len()
        );

        Ok(batches)
    }

    /// Add more shards to the queue
    pub fn add_shards(&mut self, shards: Vec<ShardSpec>) {
        self.shard_queue.extend(shards);
    }
}

/// Simple buffer pool for reusing allocations
pub struct BufferPool {
    buffers: parking_lot::Mutex<Vec<BytesMut>>,
    buffer_size: usize,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(capacity: usize, buffer_size: usize) -> Self {
        let buffers = (0..capacity)
            .map(|_| BytesMut::with_capacity(buffer_size))
            .collect();
        Self {
            buffers: parking_lot::Mutex::new(buffers),
            buffer_size,
        }
    }

    /// Acquire a buffer from the pool
    pub fn acquire(&self) -> BytesMut {
        self.buffers
            .lock()
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(self.buffer_size))
    }

    /// Return a buffer to the pool
    pub fn release(&self, mut buffer: BytesMut) {
        buffer.clear();
        if buffer.capacity() >= self.buffer_size {
            self.buffers.lock().push(buffer);
        }
    }
}
