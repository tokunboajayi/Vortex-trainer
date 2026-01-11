//! Checkpoint reader for restore operations
//!
//! Reads and validates checkpoints from storage.

use bytes::Bytes;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::manifest::{Manifest, ManifestShard};
use crate::storage::S3Client;
use crate::error::{DTrainerError, Result};

/// Checkpoint reader for restore operations
pub struct CheckpointReader {
    storage: Arc<S3Client>,
    job_id: String,
}

impl CheckpointReader {
    /// Create a new checkpoint reader
    pub fn new(storage: Arc<S3Client>, job_id: String) -> Self {
        Self { storage, job_id }
    }

    /// Find the latest complete checkpoint
    pub async fn find_latest(&self) -> Result<Option<u64>> {
        // Try reading the "latest" pointer first
        let latest_key = format!("checkpoints/{}/latest", self.job_id);
        
        match self.storage.get_object(&latest_key).await {
            Ok(data) => {
                let content = String::from_utf8_lossy(&data);
                if let Some(epoch_str) = content.strip_prefix("epoch_") {
                    if let Ok(epoch) = epoch_str.parse::<u64>() {
                        // Verify the manifest exists
                        let manifest_key = Manifest::storage_key(&self.job_id, epoch);
                        if self.storage.object_exists(&manifest_key).await? {
                            return Ok(Some(epoch));
                        }
                    }
                }
            }
            Err(_) => {
                debug!("No 'latest' pointer found, scanning for checkpoints");
            }
        }

        // Fallback: scan for manifests
        self.scan_for_latest().await
    }

    /// Scan storage for the latest manifest
    async fn scan_for_latest(&self) -> Result<Option<u64>> {
        let prefix = format!("checkpoints/{}/epoch_", self.job_id);
        let objects = self.storage.list_objects(&prefix).await?;

        let mut latest_epoch = None;
        for key in objects {
            if key.ends_with("/manifest.json") {
                if let Some(epoch) = self.parse_epoch_from_key(&key) {
                    if latest_epoch.map_or(true, |e| epoch > e) {
                        latest_epoch = Some(epoch);
                    }
                }
            }
        }

        Ok(latest_epoch)
    }

    /// Parse epoch number from manifest key
    fn parse_epoch_from_key(&self, key: &str) -> Option<u64> {
        // Key format: checkpoints/{job_id}/epoch_{epoch:06}/manifest.json
        let parts: Vec<&str> = key.split('/').collect();
        if parts.len() >= 3 {
            let epoch_part = parts[parts.len() - 2];
            if let Some(epoch_str) = epoch_part.strip_prefix("epoch_") {
                return epoch_str.parse().ok();
            }
        }
        None
    }

    /// Load manifest for a specific epoch
    pub async fn load_manifest(&self, epoch: u64) -> Result<Manifest> {
        let manifest_key = Manifest::storage_key(&self.job_id, epoch);
        
        let data = self.storage.get_object(&manifest_key).await.map_err(|_| {
            DTrainerError::NoCheckpointFound {
                job_id: self.job_id.clone(),
            }
        })?;

        let json = String::from_utf8_lossy(&data);
        let manifest = Manifest::from_json(&json)?;
        manifest.validate()?;

        info!(
            "Loaded manifest for epoch {}: {} workers, step {}",
            epoch, manifest.worker_count, manifest.global_step
        );

        Ok(manifest)
    }

    /// Load a specific worker's checkpoint data
    pub async fn load_shard(&self, shard: &ManifestShard) -> Result<Bytes> {
        debug!("Loading shard from {}", shard.object_key);

        let data = self.storage.get_object(&shard.object_key).await?;

        // Verify checksum
        let actual_crc32c = crc32c::crc32c(&data);
        if actual_crc32c != shard.crc32c {
            return Err(DTrainerError::ChecksumMismatch {
                key: shard.object_key.clone(),
                expected: shard.crc32c,
                actual: actual_crc32c,
            });
        }

        // Verify size
        if data.len() as u64 != shard.byte_size {
            return Err(DTrainerError::ManifestInvalid {
                reason: format!(
                    "Size mismatch for {}: expected {}, got {}",
                    shard.object_key, shard.byte_size, data.len()
                ),
            });
        }

        debug!(
            "Loaded shard {} ({} bytes, crc32c verified)",
            shard.worker_id, data.len()
        );

        Ok(data)
    }

    /// Check for partial checkpoints (failed commits)
    pub async fn detect_partial_checkpoint(&self, epoch: u64) -> Result<Option<Vec<u32>>> {
        let pending_prefix = Manifest::pending_prefix(&self.job_id, epoch);
        let pending_objects = self.storage.list_objects(&pending_prefix).await?;

        if pending_objects.is_empty() {
            return Ok(None);
        }

        // Extract worker IDs from pending objects
        let mut pending_workers = Vec::new();
        for key in pending_objects {
            if let Some(worker_id) = self.parse_worker_from_key(&key) {
                pending_workers.push(worker_id);
            }
        }

        if !pending_workers.is_empty() {
            warn!(
                "Detected partial checkpoint at epoch {} with {} pending shards",
                epoch,
                pending_workers.len()
            );
            Ok(Some(pending_workers))
        } else {
            Ok(None)
        }
    }

    /// Parse worker ID from shard key
    fn parse_worker_from_key(&self, key: &str) -> Option<u32> {
        // Key format: .../worker_{id:03}.bin
        let filename = key.split('/').last()?;
        let worker_part = filename.strip_prefix("worker_")?.strip_suffix(".bin")?;
        worker_part.parse().ok()
    }

    /// Restore from the latest checkpoint
    pub async fn restore_latest(&self) -> Result<Option<(Manifest, Vec<(u32, Bytes)>)>> {
        let epoch = match self.find_latest().await? {
            Some(e) => e,
            None => return Ok(None),
        };

        let manifest = self.load_manifest(epoch).await?;
        let mut shards = Vec::with_capacity(manifest.shards.len());

        for shard_meta in &manifest.shards {
            let data = self.load_shard(shard_meta).await?;
            shards.push((shard_meta.worker_id, data));
        }

        info!(
            "Restored checkpoint at epoch {} with {} shards",
            epoch,
            shards.len()
        );

        Ok(Some((manifest, shards)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests would use mock S3
}
