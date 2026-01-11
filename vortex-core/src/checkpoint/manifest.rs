//! Checkpoint manifest file handling
//!
//! The manifest is the atomic commit marker - if it exists and is valid,
//! the checkpoint is complete.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::error::{DTrainerError, Result};

/// Checkpoint manifest - the source of truth for checkpoint state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Manifest version for compatibility
    pub version: u32,
    /// Job identifier
    pub job_id: String,
    /// Training epoch
    pub epoch: u64,
    /// Global step count
    pub global_step: u64,
    /// Timestamp of checkpoint creation
    pub timestamp_utc: DateTime<Utc>,
    /// Total worker count at checkpoint time
    pub worker_count: u32,
    /// Individual shard metadata
    pub shards: Vec<ManifestShard>,
    /// Dataset state for resumption
    pub dataset_state: Option<DatasetState>,
    /// Parent checkpoint for lineage tracking
    pub lineage: Option<Lineage>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Metadata for a single checkpoint shard (per worker)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestShard {
    /// Worker ID that created this shard
    pub worker_id: u32,
    /// Object key in storage
    pub object_key: String,
    /// Size in bytes
    pub byte_size: u64,
    /// CRC32C checksum
    pub crc32c: u32,
    /// State dict keys included (e.g., "model", "optimizer")
    pub state_dict_keys: Vec<String>,
    /// ETag from storage for verification
    pub etag: String,
}

/// Dataset state for resumption
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetState {
    /// Dataset identifier
    pub dataset_id: String,
    /// Shards that were fully consumed
    pub consumed_shards: Vec<u32>,
    /// Current shard being processed
    pub current_shard: u32,
    /// Current position in shard
    pub shard_offset: u64,
    /// Shuffle seed used
    pub shuffle_seed: u64,
}

/// Lineage for checkpoint history
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lineage {
    /// Parent checkpoint epoch
    pub parent_epoch: u64,
    /// Parent manifest ETag for verification
    pub parent_manifest_etag: String,
}

impl Manifest {
    /// Current manifest version
    pub const CURRENT_VERSION: u32 = 2;

    /// Create a new manifest
    pub fn new(job_id: String, epoch: u64, global_step: u64, worker_count: u32) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            job_id,
            epoch,
            global_step,
            timestamp_utc: Utc::now(),
            worker_count,
            shards: Vec::new(),
            dataset_state: None,
            lineage: None,
            metadata: HashMap::new(),
        }
    }

    /// Add a shard to the manifest
    pub fn add_shard(&mut self, shard: ManifestShard) {
        self.shards.push(shard);
    }

    /// Set dataset state
    pub fn with_dataset_state(mut self, state: DatasetState) -> Self {
        self.dataset_state = Some(state);
        self
    }

    /// Set lineage
    pub fn with_lineage(mut self, parent_epoch: u64, parent_etag: String) -> Self {
        self.lineage = Some(Lineage {
            parent_epoch,
            parent_manifest_etag: parent_etag,
        });
        self
    }

    /// Serialize to JSON
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(|e| DTrainerError::ManifestInvalid {
            reason: format!("Serialization failed: {}", e),
        })
    }

    /// Deserialize from JSON
    pub fn from_json(json: &str) -> Result<Self> {
        let manifest: Self = serde_json::from_str(json).map_err(|e| {
            DTrainerError::ManifestInvalid {
                reason: format!("Deserialization failed: {}", e),
            }
        })?;

        // Version check
        if manifest.version > Self::CURRENT_VERSION {
            return Err(DTrainerError::ManifestInvalid {
                reason: format!(
                    "Manifest version {} is newer than supported {}",
                    manifest.version,
                    Self::CURRENT_VERSION
                ),
            });
        }

        Ok(manifest)
    }

    /// Validate manifest consistency
    pub fn validate(&self) -> Result<()> {
        // Check worker count matches shards
        if self.shards.len() != self.worker_count as usize {
            return Err(DTrainerError::ManifestInvalid {
                reason: format!(
                    "Shard count {} doesn't match worker count {}",
                    self.shards.len(),
                    self.worker_count
                ),
            });
        }

        // Check for duplicate worker IDs
        let mut seen_workers = std::collections::HashSet::new();
        for shard in &self.shards {
            if !seen_workers.insert(shard.worker_id) {
                return Err(DTrainerError::ManifestInvalid {
                    reason: format!("Duplicate worker ID: {}", shard.worker_id),
                });
            }
        }

        // Check all shards have valid sizes
        for shard in &self.shards {
            if shard.byte_size == 0 {
                return Err(DTrainerError::ManifestInvalid {
                    reason: format!("Worker {} has zero-size shard", shard.worker_id),
                });
            }
        }

        Ok(())
    }

    /// Get storage key for this manifest
    pub fn storage_key(job_id: &str, epoch: u64) -> String {
        format!("checkpoints/{}/epoch_{:06}/manifest.json", job_id, epoch)
    }

    /// Get the pending prefix for in-flight uploads
    pub fn pending_prefix(job_id: &str, epoch: u64) -> String {
        format!("checkpoints/{}/epoch_{:06}/pending/", job_id, epoch)
    }

    /// Get the committed prefix for finalized shards
    pub fn committed_prefix(job_id: &str, epoch: u64) -> String {
        format!("checkpoints/{}/epoch_{:06}/committed/", job_id, epoch)
    }

    /// Get worker shard key (pending)
    pub fn pending_shard_key(job_id: &str, epoch: u64, worker_id: u32) -> String {
        format!(
            "checkpoints/{}/epoch_{:06}/pending/worker_{:03}.bin",
            job_id, epoch, worker_id
        )
    }

    /// Get worker shard key (committed)
    pub fn committed_shard_key(job_id: &str, epoch: u64, worker_id: u32) -> String {
        format!(
            "checkpoints/{}/epoch_{:06}/committed/worker_{:03}.bin",
            job_id, epoch, worker_id
        )
    }
}

impl ManifestShard {
    /// Create a new manifest shard
    pub fn new(
        worker_id: u32,
        object_key: String,
        byte_size: u64,
        crc32c: u32,
        etag: String,
    ) -> Self {
        Self {
            worker_id,
            object_key,
            byte_size,
            crc32c,
            state_dict_keys: Vec::new(),
            etag,
        }
    }

    /// Set state dict keys
    pub fn with_keys(mut self, keys: Vec<String>) -> Self {
        self.state_dict_keys = keys;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_roundtrip() {
        let mut manifest = Manifest::new("test-job".into(), 42, 42000, 2);
        manifest.add_shard(ManifestShard::new(
            0,
            "checkpoints/test-job/epoch_000042/committed/worker_000.bin".into(),
            1024,
            12345,
            "etag1".into(),
        ));
        manifest.add_shard(ManifestShard::new(
            1,
            "checkpoints/test-job/epoch_000042/committed/worker_001.bin".into(),
            2048,
            67890,
            "etag2".into(),
        ));

        let json = manifest.to_json().unwrap();
        let restored = Manifest::from_json(&json).unwrap();

        assert_eq!(restored.epoch, 42);
        assert_eq!(restored.shards.len(), 2);
        restored.validate().unwrap();
    }
}
