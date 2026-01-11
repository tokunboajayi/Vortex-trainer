//! Shard abstraction for data loading
//!
//! Zero-copy shard data with checksums and batch handling.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Specification for a shard to be loaded
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardSpec {
    /// Shard identifier
    pub id: u32,
    /// Object key in storage
    pub key: String,
    /// Byte offset within the object (for sub-sharding)
    pub offset: u64,
    /// Byte length to read (0 = entire object)
    pub length: u64,
    /// Expected CRC32C checksum (optional)
    pub expected_crc32c: Option<u32>,
}

/// A loaded shard with its data
#[derive(Debug, Clone)]
pub struct Shard {
    /// Shard specification
    pub spec: ShardSpec,
    /// Raw shard data (zero-copy Bytes)
    pub data: Bytes,
    /// Computed CRC32C checksum
    pub computed_crc32c: u32,
}

impl Shard {
    /// Create a new shard from data
    pub fn new(spec: ShardSpec, data: Bytes) -> Self {
        let computed_crc32c = crc32c::crc32c(&data);
        Self {
            spec,
            data,
            computed_crc32c,
        }
    }

    /// Verify checksum matches expected value
    pub fn verify_checksum(&self) -> bool {
        match self.spec.expected_crc32c {
            Some(expected) => self.computed_crc32c == expected,
            None => true, // No expected checksum
        }
    }

    /// Get shard size in bytes
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// A batch of data from a shard, ready for training
#[derive(Debug)]
pub struct ShardBatch {
    /// Source shard ID
    pub shard_id: u32,
    /// Batch data (zero-copy slice)
    pub data: Bytes,
    /// Offset within the shard
    pub offset: u64,
    /// Checksum of batch data
    pub checksum: u32,
    /// Batch index within shard
    pub batch_idx: u32,
    /// Total batches in shard
    pub total_batches: u32,
}

impl ShardBatch {
    /// Create a new batch from shard data
    pub fn new(
        shard_id: u32,
        data: Bytes,
        offset: u64,
        batch_idx: u32,
        total_batches: u32,
    ) -> Self {
        let checksum = crc32c::crc32c(&data);
        Self {
            shard_id,
            data,
            offset,
            checksum,
            batch_idx,
            total_batches,
        }
    }

    /// Check if this is the last batch in the shard
    pub fn is_last_in_shard(&self) -> bool {
        self.batch_idx + 1 >= self.total_batches
    }
}

impl ShardSpec {
    /// Create a new shard specification
    pub fn new(id: u32, key: impl Into<String>) -> Self {
        Self {
            id,
            key: key.into(),
            offset: 0,
            length: 0,
            expected_crc32c: None,
        }
    }

    /// Set byte range for partial reads
    pub fn with_range(mut self, offset: u64, length: u64) -> Self {
        self.offset = offset;
        self.length = length;
        self
    }

    /// Set expected checksum
    pub fn with_checksum(mut self, crc32c: u32) -> Self {
        self.expected_crc32c = Some(crc32c);
        self
    }
}
