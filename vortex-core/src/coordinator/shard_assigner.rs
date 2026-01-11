//! Deterministic shard assignment
//!
//! Assigns shards to workers using consistent hashing.

use std::collections::HashMap;
use tracing::debug;

/// Deterministic shard assigner
pub struct ShardAssigner {
    /// Total number of shards
    num_shards: u32,
    /// Shuffle seed
    seed: u64,
}

impl ShardAssigner {
    /// Create a new shard assigner
    pub fn new(num_shards: u32, seed: u64) -> Self {
        Self { num_shards, seed }
    }

    /// Compute shard assignment for all workers
    /// 
    /// Uses deterministic assignment: shard_id % num_workers
    pub fn assign(&self, worker_ids: &[u32]) -> HashMap<u32, Vec<u32>> {
        let num_workers = worker_ids.len();
        if num_workers == 0 {
            return HashMap::new();
        }

        let mut assignments: HashMap<u32, Vec<u32>> = worker_ids
            .iter()
            .map(|&id| (id, Vec::new()))
            .collect();

        // Shuffle shard order if seed is non-zero
        let shard_order = self.shuffled_shard_order();

        // Round-robin assignment
        for (idx, shard_id) in shard_order.iter().enumerate() {
            let worker_idx = idx % num_workers;
            let worker_id = worker_ids[worker_idx];
            assignments.get_mut(&worker_id).unwrap().push(*shard_id);
        }

        debug!(
            "Assigned {} shards to {} workers",
            self.num_shards, num_workers
        );

        assignments
    }

    /// Get shards assigned to a specific worker
    pub fn shards_for_worker(&self, worker_id: u32, num_workers: u32) -> Vec<u32> {
        if num_workers == 0 {
            return Vec::new();
        }

        let shard_order = self.shuffled_shard_order();
        
        shard_order
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| (*idx as u32) % num_workers == worker_id)
            .map(|(_, shard_id)| shard_id)
            .collect()
    }

    /// Generate shuffled shard order based on seed
    fn shuffled_shard_order(&self) -> Vec<u32> {
        let mut order: Vec<u32> = (0..self.num_shards).collect();
        
        if self.seed != 0 {
            // Simple deterministic shuffle using LCG
            let mut rng_state = self.seed;
            for i in (1..order.len()).rev() {
                rng_state = rng_state.wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let j = (rng_state as usize) % (i + 1);
                order.swap(i, j);
            }
        }

        order
    }

    /// Rebalance shards after worker change
    pub fn rebalance(
        &self,
        current_assignments: &HashMap<u32, Vec<u32>>,
        new_worker_ids: &[u32],
    ) -> HashMap<u32, Vec<u32>> {
        // For simplicity, full reassignment
        // In production, minimize shard movement
        self.assign(new_worker_ids)
    }

    /// Check if a shard belongs to a worker
    pub fn shard_owner(&self, shard_id: u32, num_workers: u32) -> u32 {
        if num_workers == 0 {
            return 0;
        }
        
        let shard_order = self.shuffled_shard_order();
        for (idx, &sid) in shard_order.iter().enumerate() {
            if sid == shard_id {
                return (idx as u32) % num_workers;
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_assignment() {
        let assigner = ShardAssigner::new(10, 0);
        let worker_ids = vec![0, 1, 2];
        let assignments = assigner.assign(&worker_ids);

        // Each worker should get some shards
        assert!(!assignments[&0].is_empty());
        assert!(!assignments[&1].is_empty());
        assert!(!assignments[&2].is_empty());

        // Total shards should be 10
        let total: usize = assignments.values().map(|v| v.len()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_deterministic_assignment() {
        let assigner = ShardAssigner::new(100, 42);
        let workers = vec![0, 1, 2, 3];
        
        // Should produce same result every time
        let a1 = assigner.assign(&workers);
        let a2 = assigner.assign(&workers);
        
        assert_eq!(a1, a2);
    }
}
