//! Unit tests for coordinator shard assignment
//!
//! Tests deterministic assignment, rebalancing, and edge cases.

use dtrainer_core::coordinator::ShardAssigner;

#[test]
fn test_basic_assignment() {
    let assigner = ShardAssigner::new(10, 0);
    let workers = vec![0, 1, 2];
    let assignments = assigner.assign(&workers);

    // Each worker should have some shards
    for worker_id in &workers {
        assert!(!assignments[worker_id].is_empty(), "Worker {} has no shards", worker_id);
    }

    // Total shards should match
    let total: usize = assignments.values().map(|v| v.len()).sum();
    assert_eq!(total, 10);
}

#[test]
fn test_deterministic_assignment() {
    let assigner = ShardAssigner::new(100, 42);
    let workers = vec![0, 1, 2, 3];

    let a1 = assigner.assign(&workers);
    let a2 = assigner.assign(&workers);

    assert_eq!(a1, a2, "Assignment should be deterministic");
}

#[test]
fn test_single_worker() {
    let assigner = ShardAssigner::new(50, 0);
    let workers = vec![0];
    let assignments = assigner.assign(&workers);

    assert_eq!(assignments[&0].len(), 50, "Single worker should get all shards");
}

#[test]
fn test_empty_workers() {
    let assigner = ShardAssigner::new(10, 0);
    let workers: Vec<u32> = vec![];
    let assignments = assigner.assign(&workers);

    assert!(assignments.is_empty());
}

#[test]
fn test_more_workers_than_shards() {
    let assigner = ShardAssigner::new(3, 0);
    let workers = vec![0, 1, 2, 3, 4, 5];
    let assignments = assigner.assign(&workers);

    // Some workers will have 0 shards
    let empty_workers: usize = assignments.values().filter(|v| v.is_empty()).count();
    assert!(empty_workers >= 3, "At least 3 workers should have no shards");
}

#[test]
fn test_different_seeds() {
    let assigner1 = ShardAssigner::new(100, 42);
    let assigner2 = ShardAssigner::new(100, 123);
    let workers = vec![0, 1, 2, 3];

    let a1 = assigner1.assign(&workers);
    let a2 = assigner2.assign(&workers);

    // Different seeds should produce different orderings
    assert_ne!(a1[&0], a2[&0], "Different seeds should shuffle differently");
}

#[test]
fn test_shards_for_worker() {
    let assigner = ShardAssigner::new(100, 0);
    let num_workers = 4;

    for worker_id in 0..num_workers {
        let shards = assigner.shards_for_worker(worker_id, num_workers);
        assert!(!shards.is_empty());
        assert!(shards.len() >= 24 && shards.len() <= 26, "Should be ~25 shards");
    }
}

#[test]
fn test_shard_owner() {
    let assigner = ShardAssigner::new(100, 0);
    let num_workers = 4;

    // Each shard should have exactly one owner
    for shard_id in 0..100 {
        let owner = assigner.shard_owner(shard_id, num_workers);
        assert!(owner < num_workers, "Owner should be valid worker ID");
    }
}
