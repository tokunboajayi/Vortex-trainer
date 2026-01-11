//! Async checkpoint writer
//!
//! Non-blocking checkpoint writes with multipart upload support.

use bytes::Bytes;
use std::sync::Arc;
use tracing::{debug, error, info};

use super::manifest::{Manifest, ManifestShard};
use crate::storage::{S3Client, MultipartUpload};
use crate::error::{DTrainerError, Result};

/// Configuration for checkpoint writer
#[derive(Debug, Clone)]
pub struct WriterConfig {
    /// Use multipart upload for large checkpoints
    pub use_multipart: bool,
    /// Multipart part size in bytes
    pub part_size: usize,
    /// Verify writes with read-back
    pub verify_writes: bool,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            use_multipart: true,
            part_size: 64 * 1024 * 1024, // 64MB
            verify_writes: false,
        }
    }
}

/// Async checkpoint writer for a single worker
pub struct CheckpointWriter {
    storage: Arc<S3Client>,
    job_id: String,
    worker_id: u32,
    config: WriterConfig,
}

impl CheckpointWriter {
    /// Create a new checkpoint writer
    pub fn new(
        storage: Arc<S3Client>,
        job_id: String,
        worker_id: u32,
        config: WriterConfig,
    ) -> Self {
        Self {
            storage,
            job_id,
            worker_id,
            config,
        }
    }

    /// Write worker state to pending location
    /// 
    /// Returns the manifest shard metadata for this write.
    pub async fn write_pending(
        &self,
        epoch: u64,
        data: Bytes,
        state_keys: Vec<String>,
    ) -> Result<ManifestShard> {
        let key = Manifest::pending_shard_key(&self.job_id, epoch, self.worker_id);
        let byte_size = data.len() as u64;
        let crc32c = crc32c::crc32c(&data);

        debug!(
            "Writing pending checkpoint for worker {} at epoch {} ({} bytes)",
            self.worker_id, epoch, byte_size
        );

        // Choose upload method based on size
        let etag = if self.config.use_multipart && data.len() > self.config.part_size {
            self.write_multipart(&key, data.clone()).await?
        } else {
            self.storage.put_object(&key, data.clone()).await?
        };

        // Verify if configured
        if self.config.verify_writes {
            self.verify_write(&key, crc32c).await?;
        }

        let shard = ManifestShard::new(self.worker_id, key, byte_size, crc32c, etag)
            .with_keys(state_keys);

        info!(
            "Worker {} wrote pending checkpoint: {} bytes, crc32c={}",
            self.worker_id, byte_size, crc32c
        );

        Ok(shard)
    }

    /// Write using multipart upload (simplified - uses single put for now)
    async fn write_multipart(&self, key: &str, data: Bytes) -> Result<String> {
        // For large files, we'd use proper multipart upload
        // Simplified implementation uses single PUT
        self.storage.put_object(key, data).await
    }

    /// Verify a write by reading back and checking checksum
    async fn verify_write(&self, key: &str, expected_crc32c: u32) -> Result<()> {
        let data = self.storage.get_object(key).await?;
        let actual_crc32c = crc32c::crc32c(&data);

        if actual_crc32c != expected_crc32c {
            return Err(DTrainerError::ChecksumMismatch {
                key: key.to_string(),
                expected: expected_crc32c,
                actual: actual_crc32c,
            });
        }

        debug!("Verified write for {}", key);
        Ok(())
    }

    /// Get worker ID
    pub fn worker_id(&self) -> u32 {
        self.worker_id
    }
}

/// Coordinator-side commit handler
pub struct CheckpointCommitter {
    storage: Arc<S3Client>,
    job_id: String,
}

impl CheckpointCommitter {
    /// Create a new committer
    pub fn new(storage: Arc<S3Client>, job_id: String) -> Self {
        Self { storage, job_id }
    }

    /// Commit a checkpoint by moving pending shards to committed and writing manifest
    pub async fn commit(
        &self,
        epoch: u64,
        mut manifest: Manifest,
    ) -> Result<String> {
        info!("Committing checkpoint for epoch {}", epoch);

        // 1. Copy pending shards to committed
        let mut committed_shards = Vec::new();
        for shard in &manifest.shards {
            let pending_key = Manifest::pending_shard_key(&self.job_id, epoch, shard.worker_id);
            let committed_key = Manifest::committed_shard_key(&self.job_id, epoch, shard.worker_id);

            // Verify pending shard exists
            if !self.storage.object_exists(&pending_key).await? {
                return Err(DTrainerError::WriteInterrupted {
                    reason: format!("Pending shard missing: {}", pending_key),
                });
            }

            // Copy to committed location
            let etag = self.storage.copy_object(&pending_key, &committed_key).await?;
            
            // Update shard with committed key
            let mut committed_shard = shard.clone();
            committed_shard.object_key = committed_key;
            committed_shard.etag = etag;
            committed_shards.push(committed_shard);
        }

        // 2. Update manifest with committed keys
        manifest.shards = committed_shards;

        // 3. Validate manifest
        manifest.validate()?;

        // 4. Write manifest (atomic commit marker)
        let manifest_key = Manifest::storage_key(&self.job_id, epoch);
        let manifest_json = manifest.to_json()?;
        let etag = self.storage
            .put_object(&manifest_key, Bytes::from(manifest_json))
            .await?;

        info!(
            "Checkpoint committed for epoch {}, manifest etag={}",
            epoch, etag
        );

        // 5. Cleanup pending shards (best effort)
        for shard in &manifest.shards {
            let pending_key = Manifest::pending_shard_key(&self.job_id, epoch, shard.worker_id);
            if let Err(e) = self.storage.delete_object(&pending_key).await {
                debug!("Failed to cleanup pending shard {}: {:?}", pending_key, e);
            }
        }

        // 6. Update latest pointer
        self.update_latest_pointer(epoch).await?;

        Ok(etag)
    }

    /// Update the "latest" pointer to point to this epoch
    async fn update_latest_pointer(&self, epoch: u64) -> Result<()> {
        let latest_key = format!("checkpoints/{}/latest", self.job_id);
        let content = format!("epoch_{:06}", epoch);
        self.storage
            .put_object(&latest_key, Bytes::from(content))
            .await?;
        Ok(())
    }

    /// Abort a checkpoint and cleanup pending shards
    pub async fn abort(&self, epoch: u64, worker_ids: &[u32]) -> Result<()> {
        info!("Aborting checkpoint for epoch {}", epoch);

        for &worker_id in worker_ids {
            let pending_key = Manifest::pending_shard_key(&self.job_id, epoch, worker_id);
            if let Err(e) = self.storage.delete_object(&pending_key).await {
                debug!("Failed to cleanup pending shard {}: {:?}", pending_key, e);
            }
        }

        Ok(())
    }
}
