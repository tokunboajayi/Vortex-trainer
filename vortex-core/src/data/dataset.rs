//! Dataset abstraction and registration
//!
//! Provides dataset metadata and configuration for sharded datasets.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Dataset configuration for registration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetConfig {
    /// Unique dataset identifier
    pub name: String,
    /// S3 prefix for shards (e.g., "s3://bucket/dataset/shards/")
    pub shard_prefix: String,
    /// Total number of shards
    pub num_shards: u32,
    /// Whether to shuffle shards across workers
    pub shuffle_shards: bool,
    /// Random seed for shuffling
    pub seed: u64,
    /// Optional shard file pattern (default: "shard_{:06d}.bin")
    pub shard_pattern: Option<String>,
}

/// Dataset metadata stored in object storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetMetadata {
    /// Dataset configuration
    pub config: DatasetConfig,
    /// Creation timestamp
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Total size in bytes (if known)
    pub total_bytes: Option<u64>,
    /// Per-shard metadata
    pub shard_metadata: HashMap<u32, ShardMetadata>,
}

/// Metadata for a single shard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardMetadata {
    /// Shard ID
    pub shard_id: u32,
    /// Object key in storage
    pub object_key: String,
    /// Size in bytes
    pub byte_size: u64,
    /// Number of records (if known)
    pub record_count: Option<u64>,
    /// CRC32C checksum
    pub crc32c: Option<u32>,
}

/// Runtime representation of a dataset
#[derive(Debug, Clone)]
pub struct Dataset {
    /// Unique identifier
    pub id: String,
    /// Dataset metadata
    pub metadata: DatasetMetadata,
    /// Shard assignment for this worker
    pub assigned_shards: Vec<u32>,
    /// Current shard index
    current_shard_idx: usize,
}

impl Dataset {
    /// Create a new dataset from metadata
    pub fn new(id: String, metadata: DatasetMetadata) -> Self {
        Self {
            id,
            metadata,
            assigned_shards: Vec::new(),
            current_shard_idx: 0,
        }
    }

    /// Set the assigned shards for this worker
    pub fn set_assigned_shards(&mut self, shards: Vec<u32>) {
        self.assigned_shards = shards;
        self.current_shard_idx = 0;
    }

    /// Get the next shard to load
    pub fn next_shard(&mut self) -> Option<u32> {
        if self.current_shard_idx >= self.assigned_shards.len() {
            return None;
        }
        let shard = self.assigned_shards[self.current_shard_idx];
        self.current_shard_idx += 1;
        Some(shard)
    }

    /// Reset to the beginning of assigned shards
    pub fn reset(&mut self) {
        self.current_shard_idx = 0;
    }

    /// Check if all shards have been consumed
    pub fn is_exhausted(&self) -> bool {
        self.current_shard_idx >= self.assigned_shards.len()
    }

    /// Get shard object key
    pub fn shard_key(&self, shard_id: u32) -> String {
        let pattern = self.metadata.config.shard_pattern
            .as_deref()
            .unwrap_or("shard_{:06d}.bin");
        
        let filename = pattern.replace("{:06d}", &format!("{:06}", shard_id));
        format!("{}{}", self.metadata.config.shard_prefix, filename)
    }
}

impl DatasetConfig {
    /// Create a new dataset configuration
    pub fn new(name: impl Into<String>, shard_prefix: impl Into<String>, num_shards: u32) -> Self {
        Self {
            name: name.into(),
            shard_prefix: shard_prefix.into(),
            num_shards,
            shuffle_shards: true,
            seed: 42,
            shard_pattern: None,
        }
    }
}
