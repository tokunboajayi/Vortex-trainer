//! Worker state machine
//!
//! Tracks local worker state and transitions.

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

/// Worker status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerPhase {
    /// Initializing
    Initializing,
    /// Registered with coordinator
    Registered,
    /// Loading data
    Loading,
    /// Training
    Training,
    /// Writing checkpoint
    Checkpointing,
    /// Idle between epochs
    EpochBoundary,
    /// Shutting down
    ShuttingDown,
    /// Failed
    Failed,
}

/// Local worker state
pub struct WorkerState {
    /// Worker ID assigned by coordinator
    worker_id: RwLock<Option<u32>>,
    /// Current phase
    phase: RwLock<WorkerPhase>,
    /// Current epoch
    epoch: AtomicU64,
    /// Current step
    step: AtomicU64,
    /// Assigned shards
    assigned_shards: RwLock<Vec<u32>>,
    /// Fencing token from coordinator
    fencing_token: AtomicU64,
}

impl WorkerState {
    /// Create new worker state
    pub fn new() -> Self {
        Self {
            worker_id: RwLock::new(None),
            phase: RwLock::new(WorkerPhase::Initializing),
            epoch: AtomicU64::new(0),
            step: AtomicU64::new(0),
            assigned_shards: RwLock::new(Vec::new()),
            fencing_token: AtomicU64::new(0),
        }
    }

    /// Set worker ID after registration
    pub async fn set_worker_id(&self, id: u32) {
        *self.worker_id.write().await = Some(id);
        *self.phase.write().await = WorkerPhase::Registered;
    }

    /// Get worker ID
    pub async fn worker_id(&self) -> Option<u32> {
        *self.worker_id.read().await
    }

    /// Get current phase
    pub async fn phase(&self) -> WorkerPhase {
        *self.phase.read().await
    }

    /// Set phase
    pub async fn set_phase(&self, phase: WorkerPhase) {
        *self.phase.write().await = phase;
    }

    /// Get current epoch
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Set epoch
    pub fn set_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::Relaxed);
    }

    /// Get current step
    pub fn step(&self) -> u64 {
        self.step.load(Ordering::Relaxed)
    }

    /// Increment step
    pub fn increment_step(&self) -> u64 {
        self.step.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Set step
    pub fn set_step(&self, step: u64) {
        self.step.store(step, Ordering::Relaxed);
    }

    /// Update assigned shards
    pub async fn set_shards(&self, shards: Vec<u32>) {
        *self.assigned_shards.write().await = shards;
    }

    /// Get assigned shards
    pub async fn shards(&self) -> Vec<u32> {
        self.assigned_shards.read().await.clone()
    }

    /// Update fencing token
    pub fn set_fencing_token(&self, token: u64) {
        self.fencing_token.store(token, Ordering::Relaxed);
    }

    /// Get fencing token
    pub fn fencing_token(&self) -> u64 {
        self.fencing_token.load(Ordering::Relaxed)
    }

    /// Check if fencing token is valid
    pub fn validate_fencing_token(&self, token: u64) -> bool {
        self.fencing_token.load(Ordering::Relaxed) <= token
    }
}

impl Default for WorkerState {
    fn default() -> Self {
        Self::new()
    }
}
