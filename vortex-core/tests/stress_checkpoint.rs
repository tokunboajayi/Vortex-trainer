//! Stress tests for checkpointing system
//!
//! Run with: cargo test --release --test stress_checkpoint -- --nocapture

use std::time::{Duration, Instant};
use std::collections::HashMap;
use bytes::Bytes;
use tokio::sync::mpsc;

/// Generate checkpoint data for a worker
fn generate_checkpoint_data(worker_id: u32, size: usize) -> Bytes {
    let mut data = vec![0u8; size];
    // Pattern to verify integrity
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = ((worker_id as usize * 7 + i * 13) % 256) as u8;
    }
    Bytes::from(data)
}

/// Verify checkpoint data integrity
fn verify_checkpoint_data(worker_id: u32, data: &Bytes) -> bool {
    for (i, &byte) in data.iter().enumerate() {
        let expected = ((worker_id as usize * 7 + i * 13) % 256) as u8;
        if byte != expected {
            return false;
        }
    }
    true
}

/// Test checkpoint serialization throughput
#[tokio::test]
async fn stress_checkpoint_serialization() {
    let num_workers = 8;
    let checkpoint_size = 100 * 1024 * 1024; // 100MB per worker

    let start = Instant::now();
    let mut total_bytes = 0usize;
    let mut checksums = Vec::new();

    for worker_id in 0..num_workers {
        let data = generate_checkpoint_data(worker_id, checkpoint_size);
        let checksum = crc32c::crc32c(&data);
        checksums.push(checksum);
        total_bytes += data.len();
    }

    let elapsed = start.elapsed();
    let throughput_mbps = (total_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();

    println!("Checkpoint serialization test:");
    println!("  Workers: {}", num_workers);
    println!("  Total bytes: {} MB", total_bytes / 1024 / 1024);
    println!("  Elapsed: {:?}", elapsed);
    println!("  Throughput: {:.2} MB/s", throughput_mbps);

    assert!(throughput_mbps > 500.0, "Serialization throughput too low");
}

/// Test concurrent checkpoint writes
#[tokio::test]
async fn stress_concurrent_checkpoint_writes() {
    let num_workers = 16;
    let checkpoint_size = 10 * 1024 * 1024; // 10MB per worker

    let (tx, mut rx) = mpsc::channel::<(u32, Duration, u32)>(100);

    let start = Instant::now();

    // Simulate concurrent checkpoint writes
    let mut handles = Vec::new();
    for worker_id in 0..num_workers {
        let tx = tx.clone();
        let handle = tokio::spawn(async move {
            let write_start = Instant::now();
            
            // Generate and checksum data
            let data = generate_checkpoint_data(worker_id, checkpoint_size);
            let checksum = crc32c::crc32c(&data);
            
            // Simulate write delay (would be network I/O in real impl)
            tokio::time::sleep(Duration::from_millis(10)).await;
            
            let _ = tx.send((worker_id, write_start.elapsed(), checksum)).await;
        });
        handles.push(handle);
    }
    drop(tx);

    // Collect results
    let mut results = HashMap::new();
    while let Some((worker_id, duration, checksum)) = rx.recv().await {
        results.insert(worker_id, (duration, checksum));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let total_elapsed = start.elapsed();
    let avg_write_time: Duration = results.values().map(|(d, _)| *d).sum::<Duration>() / results.len() as u32;

    println!("Concurrent checkpoint writes test:");
    println!("  Workers: {}", num_workers);
    println!("  Total elapsed: {:?}", total_elapsed);
    println!("  Avg write time: {:?}", avg_write_time);
    println!("  All checksums unique: {}", results.values().map(|(_, c)| *c).collect::<std::collections::HashSet<_>>().len() == num_workers as usize);
}

/// Test checkpoint verification
#[tokio::test]
async fn stress_checkpoint_verification() {
    let num_workers = 32;
    let checkpoint_size = 1024 * 1024; // 1MB per worker

    let start = Instant::now();
    let mut verify_count = 0u32;

    for worker_id in 0..num_workers {
        let data = generate_checkpoint_data(worker_id, checkpoint_size);
        
        // Verify
        if verify_checkpoint_data(worker_id, &data) {
            verify_count += 1;
        }
        
        // Compute checksum
        let _ = crc32c::crc32c(&data);
    }

    let elapsed = start.elapsed();

    println!("Checkpoint verification test:");
    println!("  Workers: {}", num_workers);
    println!("  Verified: {}/{}", verify_count, num_workers);
    println!("  Elapsed: {:?}", elapsed);

    assert_eq!(verify_count, num_workers, "Some checkpoints failed verification");
}

/// Test checkpoint commit simulation
#[tokio::test]
async fn stress_checkpoint_commit_flow() {
    let num_workers = 8;
    let epochs = 10;
    let checkpoint_size = 5 * 1024 * 1024; // 5MB per worker

    let start = Instant::now();
    let mut epoch_times = Vec::new();

    for epoch in 0..epochs {
        let epoch_start = Instant::now();
        
        // Phase 1: All workers write pending
        let mut pending_shards = Vec::new();
        for worker_id in 0..num_workers {
            let data = generate_checkpoint_data(worker_id, checkpoint_size);
            let checksum = crc32c::crc32c(&data);
            pending_shards.push((worker_id, checksum, data.len()));
        }

        // Phase 2: Coordinator commits (copy pending -> committed)
        let mut committed_shards = Vec::new();
        for (worker_id, checksum, size) in pending_shards {
            // Simulate copy operation
            committed_shards.push((worker_id, checksum, size));
        }

        // Phase 3: Write manifest
        let manifest_data = format!(
            r#"{{"epoch":{},"workers":{},"shards":{}}}"#,
            epoch, num_workers, committed_shards.len()
        );
        let _ = crc32c::crc32c(manifest_data.as_bytes());

        epoch_times.push(epoch_start.elapsed());
    }

    let total_elapsed = start.elapsed();
    let avg_epoch_time: Duration = epoch_times.iter().sum::<Duration>() / epochs as u32;

    println!("Checkpoint commit flow test:");
    println!("  Epochs: {}", epochs);
    println!("  Workers per epoch: {}", num_workers);
    println!("  Total elapsed: {:?}", total_elapsed);
    println!("  Avg epoch time: {:?}", avg_epoch_time);
}
