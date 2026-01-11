//! Incremental checkpoint support
//!
//! Computes deltas between checkpoint versions to reduce write volume.

use bytes::Bytes;
use std::collections::HashMap;

/// Represents a delta between two checkpoint states
#[derive(Debug, Clone)]
pub struct CheckpointDelta {
    /// Changed keys and their new values
    pub changed: HashMap<String, Bytes>,
    /// Deleted keys
    pub deleted: Vec<String>,
    /// Base checkpoint epoch this delta applies to
    pub base_epoch: u64,
}

impl CheckpointDelta {
    /// Create an empty delta
    pub fn new(base_epoch: u64) -> Self {
        Self {
            changed: HashMap::new(),
            deleted: Vec::new(),
            base_epoch,
        }
    }

    /// Add a changed key
    pub fn add_change(&mut self, key: String, value: Bytes) {
        self.changed.insert(key, value);
    }

    /// Add a deleted key
    pub fn add_deletion(&mut self, key: String) {
        self.deleted.push(key);
    }

    /// Check if delta is empty
    pub fn is_empty(&self) -> bool {
        self.changed.is_empty() && self.deleted.is_empty()
    }

    /// Get total size of changes
    pub fn change_size(&self) -> usize {
        self.changed.values().map(|v| v.len()).sum()
    }
}

/// Computes deltas between state versions
pub struct DeltaComputer {
    /// Previous state checksums for comparison
    previous_checksums: HashMap<String, u32>,
}

impl DeltaComputer {
    /// Create a new delta computer
    pub fn new() -> Self {
        Self {
            previous_checksums: HashMap::new(),
        }
    }

    /// Compute delta from previous state
    pub fn compute_delta(
        &mut self,
        base_epoch: u64,
        current_state: &HashMap<String, Bytes>,
    ) -> CheckpointDelta {
        let mut delta = CheckpointDelta::new(base_epoch);

        // Find changed keys
        for (key, value) in current_state {
            let current_checksum = crc32c::crc32c(value);
            
            let changed = match self.previous_checksums.get(key) {
                Some(&prev_checksum) => prev_checksum != current_checksum,
                None => true, // New key
            };

            if changed {
                delta.add_change(key.clone(), value.clone());
            }
        }

        // Find deleted keys
        for key in self.previous_checksums.keys() {
            if !current_state.contains_key(key) {
                delta.add_deletion(key.clone());
            }
        }

        // Update checksums for next comparison
        self.previous_checksums.clear();
        for (key, value) in current_state {
            self.previous_checksums
                .insert(key.clone(), crc32c::crc32c(value));
        }

        delta
    }

    /// Reset state (for full checkpoint)
    pub fn reset(&mut self) {
        self.previous_checksums.clear();
    }
}

impl Default for DeltaComputer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_computation() {
        let mut computer = DeltaComputer::new();

        // First state
        let mut state1 = HashMap::new();
        state1.insert("key1".into(), Bytes::from("value1"));
        state1.insert("key2".into(), Bytes::from("value2"));

        let delta1 = computer.compute_delta(0, &state1);
        assert_eq!(delta1.changed.len(), 2); // Both are new

        // Second state with changes
        let mut state2 = HashMap::new();
        state2.insert("key1".into(), Bytes::from("value1")); // Unchanged
        state2.insert("key2".into(), Bytes::from("value2_modified")); // Changed
        state2.insert("key3".into(), Bytes::from("value3")); // New

        let delta2 = computer.compute_delta(1, &state2);
        assert_eq!(delta2.changed.len(), 2); // key2 and key3
        assert!(delta2.deleted.is_empty());

        // Third state with deletion
        let mut state3 = HashMap::new();
        state3.insert("key1".into(), Bytes::from("value1"));

        let delta3 = computer.compute_delta(2, &state3);
        assert_eq!(delta3.deleted.len(), 2); // key2 and key3 deleted
    }
}
