//! Stress tests for coordinator under load
//!
//! Run with: cargo test --release --test stress_coordinator -- --nocapture

use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::mpsc;

/// Simulate worker registration
async fn simulate_registration(worker_id: u32) -> (u32, Duration) {
    let start = Instant::now();
    
    // Simulate registration overhead
    tokio::time::sleep(Duration::from_micros(100)).await;
    
    (worker_id, start.elapsed())
}

/// Simulate heartbeat
async fn simulate_heartbeat(worker_id: u32, step: u64) -> Duration {
    let start = Instant::now();
    
    // Simulate heartbeat processing
    tokio::time::sleep(Duration::from_micros(50)).await;
    
    start.elapsed()
}

/// Test registration scalability
#[tokio::test]
async fn stress_worker_registration() {
    let num_workers = 1000;

    let (tx, mut rx) = mpsc::channel::<(u32, Duration)>(num_workers);

    let start = Instant::now();

    // Spawn registration tasks
    for worker_id in 0..num_workers as u32 {
        let tx = tx.clone();
        tokio::spawn(async move {
            let result = simulate_registration(worker_id).await;
            let _ = tx.send(result).await;
        });
    }
    drop(tx);

    // Collect results
    let mut results = Vec::new();
    while let Some(result) = rx.recv().await {
        results.push(result);
    }

    let total_elapsed = start.elapsed();
    let avg_time: Duration = results.iter().map(|(_, d)| *d).sum::<Duration>() / results.len() as u32;

    println!("Worker registration stress test:");
    println!("  Workers: {}", num_workers);
    println!("  Total elapsed: {:?}", total_elapsed);
    println!("  Avg registration time: {:?}", avg_time);
    println!("  Registrations/sec: {:.2}", num_workers as f64 / total_elapsed.as_secs_f64());

    assert!(results.len() == num_workers, "Not all workers registered");
}

/// Test heartbeat throughput
#[tokio::test]
async fn stress_heartbeat_throughput() {
    let num_workers = 100;
    let heartbeats_per_worker = 100;

    let start = Instant::now();
    let mut total_heartbeats = 0u64;

    let (tx, mut rx) = mpsc::channel::<Duration>(10000);

    // Spawn worker heartbeat loops
    let mut handles = Vec::new();
    for worker_id in 0..num_workers as u32 {
        let tx = tx.clone();
        let handle = tokio::spawn(async move {
            for step in 0..heartbeats_per_worker as u64 {
                let duration = simulate_heartbeat(worker_id, step).await;
                let _ = tx.send(duration).await;
            }
        });
        handles.push(handle);
    }
    drop(tx);

    // Collect results
    let mut heartbeat_times = Vec::new();
    while let Some(duration) = rx.recv().await {
        heartbeat_times.push(duration);
        total_heartbeats += 1;
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let total_elapsed = start.elapsed();
    let avg_time: Duration = heartbeat_times.iter().sum::<Duration>() / heartbeat_times.len() as u32;

    println!("Heartbeat throughput stress test:");
    println!("  Workers: {}", num_workers);
    println!("  Total heartbeats: {}", total_heartbeats);
    println!("  Total elapsed: {:?}", total_elapsed);
    println!("  Avg heartbeat time: {:?}", avg_time);
    println!("  Heartbeats/sec: {:.2}", total_heartbeats as f64 / total_elapsed.as_secs_f64());
}

/// Test shard assignment with many workers
#[tokio::test]
async fn stress_shard_assignment() {
    let num_shards = 10000;
    let num_workers = 1000;

    let start = Instant::now();

    // Simple round-robin assignment simulation
    let mut assignments: HashMap<u32, Vec<u32>> = (0..num_workers)
        .map(|id| (id, Vec::new()))
        .collect();

    for shard_id in 0..num_shards {
        let worker_id = shard_id % num_workers;
        assignments.get_mut(&worker_id).unwrap().push(shard_id);
    }

    let elapsed = start.elapsed();

    // Verify distribution
    let shards_per_worker: Vec<usize> = assignments.values().map(|v| v.len()).collect();
    let min_shards = *shards_per_worker.iter().min().unwrap();
    let max_shards = *shards_per_worker.iter().max().unwrap();

    println!("Shard assignment stress test:");
    println!("  Shards: {}", num_shards);
    println!("  Workers: {}", num_workers);
    println!("  Elapsed: {:?}", elapsed);
    println!("  Min shards/worker: {}", min_shards);
    println!("  Max shards/worker: {}", max_shards);
    println!("  Imbalance: {}", max_shards - min_shards);

    assert!(max_shards - min_shards <= 1, "Shard distribution too imbalanced");
}

/// Test epoch barrier synchronization
#[tokio::test]
async fn stress_epoch_barrier() {
    let num_workers = 100;
    let num_epochs = 10;

    use tokio::sync::Barrier;
    use std::sync::Arc;

    let barrier = Arc::new(Barrier::new(num_workers));
    let start = Instant::now();

    let mut handles = Vec::new();
    for worker_id in 0..num_workers {
        let barrier = barrier.clone();
        let handle = tokio::spawn(async move {
            let mut epoch_times = Vec::new();
            
            for epoch in 0..num_epochs {
                let epoch_start = Instant::now();
                
                // Simulate some work
                tokio::time::sleep(Duration::from_micros((worker_id * 10) as u64)).await;
                
                // Wait at barrier
                barrier.wait().await;
                
                epoch_times.push(epoch_start.elapsed());
            }
            
            (worker_id, epoch_times)
        });
        handles.push(handle);
    }

    // Collect results
    let mut all_times = Vec::new();
    for handle in handles {
        let (worker_id, times) = handle.await.unwrap();
        all_times.push((worker_id, times));
    }

    let total_elapsed = start.elapsed();

    println!("Epoch barrier stress test:");
    println!("  Workers: {}", num_workers);
    println!("  Epochs: {}", num_epochs);
    println!("  Total elapsed: {:?}", total_elapsed);
    println!("  Avg epoch time: {:?}", total_elapsed / num_epochs as u32);
}
