//! Unit tests for manifest handling
//!
//! Tests serialization, validation, and key schema.

use bytes::Bytes;

/// Test manifest structure (standalone test without full crate dependency)
mod manifest_tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct ManifestShard {
        worker_id: u32,
        object_key: String,
        byte_size: u64,
        crc32c: u32,
        etag: String,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct Manifest {
        version: u32,
        job_id: String,
        epoch: u64,
        global_step: u64,
        worker_count: u32,
        shards: Vec<ManifestShard>,
        created_at: String,
    }

    impl Manifest {
        fn new(job_id: &str, epoch: u64, step: u64) -> Self {
            Self {
                version: 1,
                job_id: job_id.into(),
                epoch,
                global_step: step,
                worker_count: 0,
                shards: Vec::new(),
                created_at: "2024-01-01T00:00:00Z".into(),
            }
        }

        fn add_shard(&mut self, worker_id: u32, key: &str, size: u64, crc32c: u32) {
            self.shards.push(ManifestShard {
                worker_id,
                object_key: key.into(),
                byte_size: size,
                crc32c,
                etag: "test-etag".into(),
            });
            self.worker_count = self.shards.len() as u32;
        }

        fn to_json(&self) -> String {
            serde_json::to_string_pretty(self).unwrap()
        }

        fn from_json(json: &str) -> Result<Self, serde_json::Error> {
            serde_json::from_str(json)
        }

        fn validate(&self) -> bool {
            !self.job_id.is_empty() && 
            self.shards.len() == self.worker_count as usize &&
            self.shards.iter().all(|s| s.byte_size > 0)
        }
    }

    #[test]
    fn test_manifest_creation() {
        let manifest = Manifest::new("test-job", 5, 1000);
        assert_eq!(manifest.job_id, "test-job");
        assert_eq!(manifest.epoch, 5);
        assert_eq!(manifest.global_step, 1000);
    }

    #[test]
    fn test_manifest_serialization() {
        let mut manifest = Manifest::new("test-job", 1, 100);
        manifest.add_shard(0, "shard_000.bin", 1024, 12345);
        manifest.add_shard(1, "shard_001.bin", 2048, 67890);

        let json = manifest.to_json();
        let restored = Manifest::from_json(&json).unwrap();

        assert_eq!(manifest.job_id, restored.job_id);
        assert_eq!(manifest.epoch, restored.epoch);
        assert_eq!(manifest.shards.len(), restored.shards.len());
    }

    #[test]
    fn test_manifest_validation() {
        let mut valid = Manifest::new("test-job", 1, 100);
        valid.add_shard(0, "shard.bin", 1024, 12345);
        assert!(valid.validate());

        // Invalid: empty job_id
        let mut invalid = Manifest::new("", 1, 100);
        invalid.add_shard(0, "shard.bin", 1024, 12345);
        assert!(!invalid.validate());
    }

    #[test]
    fn test_key_schema() {
        let job_id = "my-job";
        let epoch = 5u64;
        let worker_id = 3u32;

        // Pending key
        let pending = format!(
            "checkpoints/{}/epoch_{:06}/pending/worker_{:03}.bin",
            job_id, epoch, worker_id
        );
        assert_eq!(pending, "checkpoints/my-job/epoch_000005/pending/worker_003.bin");

        // Committed key
        let committed = format!(
            "checkpoints/{}/epoch_{:06}/committed/worker_{:03}.bin",
            job_id, epoch, worker_id
        );
        assert_eq!(committed, "checkpoints/my-job/epoch_000005/committed/worker_003.bin");

        // Manifest key
        let manifest = format!(
            "checkpoints/{}/epoch_{:06}/manifest.json",
            job_id, epoch
        );
        assert_eq!(manifest, "checkpoints/my-job/epoch_000005/manifest.json");
    }
}
