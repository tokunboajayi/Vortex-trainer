//! Stress tests for data loading pipeline
//!
//! Run with: cargo test --release --test stress_data_loader -- --nocapture

use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use bytes::Bytes;

/// Configuration for stress tests
struct StressConfig {
    num_shards: u32,
    shard_size_bytes: usize,
    num_workers: u32,
    duration_secs: u64,
}

impl Default for StressConfig {
    fn default() -> Self {
        Self {
            num_shards: 1000,
            shard_size_bytes: 64 * 1024 * 1024, // 64MB
            num_workers: 8,
            duration_secs: 60,
        }
    }
}

/// Generate synthetic shard data
fn generate_shard(shard_id: u32, size: usize) -> Bytes {
    let mut data = vec![0u8; size];
    // Fill with pattern based on shard_id
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = ((shard_id as usize + i) % 256) as u8;
    }
    Bytes::from(data)
}

/// Test throughput of data generation
#[tokio::test]
async fn stress_data_throughput() {
    let config = StressConfig {
        num_shards: 100,
        shard_size_bytes: 1024 * 1024, // 1MB for faster test
        ..Default::default()
    };

    let start = Instant::now();
    let mut total_bytes = 0usize;

    for shard_id in 0..config.num_shards {
        let data = generate_shard(shard_id, config.shard_size_bytes);
        total_bytes += data.len();
        
        // Simulate checksum verification
        let _ = crc32c::crc32c(&data);
    }

    let elapsed = start.elapsed();
    let throughput_mbps = (total_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();

    println!("Data throughput test:");
    println!("  Shards processed: {}", config.num_shards);
    println!("  Total bytes: {} MB", total_bytes / 1024 / 1024);
    println!("  Elapsed: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", throughput_mbps);

    assert!(throughput_mbps > 100.0, "Throughput below 100 MB/s");
}

/// Test concurrent shard processing
#[tokio::test]
async fn stress_concurrent_shards() {
    let num_workers = 4;
    let shards_per_worker = 25;
    let shard_size = 512 * 1024; // 512KB

    let (tx, mut rx) = mpsc::channel::<(u32, Duration)>(100);

    let start = Instant::now();

    // Spawn workers
    let mut handles = Vec::new();
    for worker_id in 0..num_workers {
        let tx = tx.clone();
        let handle = tokio::spawn(async move {
            for i in 0..shards_per_worker {
                let shard_id = worker_id * shards_per_worker + i;
                let shard_start = Instant::now();
                
                let data = generate_shard(shard_id, shard_size);
                let _ = crc32c::crc32c(&data);
                
                let _ = tx.send((shard_id, shard_start.elapsed())).await;
            }
        });
        handles.push(handle);
    }
    drop(tx);

    // Collect results
    let mut shard_times = Vec::new();
    while let Some((shard_id, duration)) = rx.recv().await {
        shard_times.push((shard_id, duration));
    }

    // Wait for all workers
    for handle in handles {
        handle.await.unwrap();
    }

    let total_elapsed = start.elapsed();
    let total_shards = num_workers * shards_per_worker;
    let avg_shard_time: Duration = shard_times.iter().map(|(_, d)| *d).sum::<Duration>() / shard_times.len() as u32;

    println!("Concurrent shard test:");
    println!("  Workers: {}", num_workers);
    println!("  Total shards: {}", total_shards);
    println!("  Total elapsed: {:?}", total_elapsed);
    println!("  Avg shard time: {:?}", avg_shard_time);
    println!("  Shards/sec: {:.2}", total_shards as f64 / total_elapsed.as_secs_f64());
}

/// Test memory pressure under load
#[tokio::test]
async fn stress_memory_pressure() {
    let num_batches = 50;
    let batch_size = 10 * 1024 * 1024; // 10MB per batch
    
    let mut batches: Vec<Bytes> = Vec::with_capacity(num_batches);
    
    let start = Instant::now();
    
    for i in 0..num_batches {
        let batch = generate_shard(i as u32, batch_size);
        batches.push(batch);
        
        // Simulate processing - keep only last 10 batches
        if batches.len() > 10 {
            batches.remove(0);
        }
    }

    let elapsed = start.elapsed();
    
    println!("Memory pressure test:");
    println!("  Batches processed: {}", num_batches);
    println!("  Elapsed: {:?}", elapsed);
    println!("  Final buffer size: {} batches", batches.len());
}
