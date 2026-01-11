//! Epoch and step synchronization
//!
//! Tracks training progress across workers.

use std::collections::HashMap;
use tokio::sync::{RwLock, Notify};
use std::sync::Arc;
use tracing::{debug, info};

/// Tracks epoch and step progress across workers
pub struct EpochTracker {
    /// Current epoch
    current_epoch: RwLock<u64>,
    /// Current global step
    current_step: RwLock<u64>,
    /// Per-worker step progress
    worker_steps: RwLock<HashMap<u32, u64>>,
    /// Barrier for epoch sync
    epoch_barrier: Arc<EpochBarrier>,
}

impl EpochTracker {
    /// Create a new epoch tracker
    pub fn new() -> Self {
        Self {
            current_epoch: RwLock::new(0),
            current_step: RwLock::new(0),
            worker_steps: RwLock::new(HashMap::new()),
            epoch_barrier: Arc::new(EpochBarrier::new()),
        }
    }

    /// Initialize from restored state
    pub async fn restore(&self, epoch: u64, step: u64) {
        *self.current_epoch.write().await = epoch;
        *self.current_step.write().await = step;
        info!("Epoch tracker restored to epoch={}, step={}", epoch, step);
    }

    /// Get current epoch
    pub async fn epoch(&self) -> u64 {
        *self.current_epoch.read().await
    }

    /// Get current step
    pub async fn step(&self) -> u64 {
        *self.current_step.read().await
    }

    /// Report worker step progress
    pub async fn report_step(&self, worker_id: u32, step: u64) {
        self.worker_steps.write().await.insert(worker_id, step);
        
        // Update global step to minimum across workers
        let workers = self.worker_steps.read().await;
        if let Some(&min_step) = workers.values().min() {
            let mut global = self.current_step.write().await;
            if min_step > *global {
                *global = min_step;
                debug!("Global step advanced to {}", min_step);
            }
        }
    }

    /// Advance to next epoch
    pub async fn advance_epoch(&self) -> u64 {
        let mut epoch = self.current_epoch.write().await;
        *epoch += 1;
        info!("Advanced to epoch {}", *epoch);
        *epoch
    }

    /// Wait at epoch barrier
    pub async fn barrier_wait(&self, worker_id: u32, worker_count: usize) {
        self.epoch_barrier.wait(worker_id, worker_count).await;
    }

    /// Get minimum step across all workers
    pub async fn min_step(&self) -> Option<u64> {
        self.worker_steps.read().await.values().min().copied()
    }

    /// Remove worker from tracking
    pub async fn remove_worker(&self, worker_id: u32) {
        self.worker_steps.write().await.remove(&worker_id);
    }
}

impl Default for EpochTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Epoch barrier for synchronizing workers
pub struct EpochBarrier {
    arrived: RwLock<Vec<u32>>,
    notify: Notify,
}

impl EpochBarrier {
    /// Create a new barrier
    pub fn new() -> Self {
        Self {
            arrived: RwLock::new(Vec::new()),
            notify: Notify::new(),
        }
    }

    /// Wait at the barrier
    pub async fn wait(&self, worker_id: u32, expected_count: usize) {
        {
            let mut arrived = self.arrived.write().await;
            if !arrived.contains(&worker_id) {
                arrived.push(worker_id);
            }
            
            if arrived.len() >= expected_count {
                // All workers arrived, release everyone
                self.notify.notify_waiters();
                arrived.clear();
                return;
            }
        }

        // Wait for all workers
        self.notify.notified().await;
    }

    /// Reset the barrier
    pub async fn reset(&self) {
        self.arrived.write().await.clear();
    }
}

impl Default for EpochBarrier {
    fn default() -> Self {
        Self::new()
    }
}
