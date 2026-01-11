//! Worker membership management
//!
//! Tracks worker registration, heartbeats, and status.

use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::error::{DTrainerError, Result};

/// Worker status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    /// Worker is registered and healthy
    Active,
    /// Worker missed heartbeats, possibly failed
    Suspect,
    /// Worker confirmed failed
    Failed,
    /// Worker is checkpointing
    Checkpointing,
    /// Worker gracefully deregistered
    Deregistered,
}

/// Information about a registered worker
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    /// Worker ID (0-indexed)
    pub worker_id: u32,
    /// Unique worker identifier string
    pub worker_uuid: String,
    /// Hostname
    pub hostname: String,
    /// Port
    pub port: u16,
    /// Current status
    pub status: WorkerStatus,
    /// Last heartbeat time
    pub last_heartbeat: Instant,
    /// Current step
    pub current_step: u64,
    /// Assigned shards
    pub assigned_shards: Vec<u32>,
    /// Worker capabilities
    pub capabilities: HashMap<String, String>,
}

/// Configuration for membership manager
#[derive(Debug, Clone)]
pub struct MembershipConfig {
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
    /// Heartbeat timeout (mark as suspect)
    pub heartbeat_timeout: Duration,
    /// Failure timeout (mark as failed)
    pub failure_timeout: Duration,
    /// Minimum workers required
    pub min_workers: u32,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            heartbeat_timeout: Duration::from_secs(15),
            failure_timeout: Duration::from_secs(30),
            min_workers: 1,
        }
    }
}

/// Manages worker membership and health
pub struct MembershipManager {
    workers: RwLock<HashMap<u32, WorkerInfo>>,
    config: MembershipConfig,
    next_worker_id: RwLock<u32>,
}

impl MembershipManager {
    /// Create a new membership manager
    pub fn new(config: MembershipConfig) -> Self {
        Self {
            workers: RwLock::new(HashMap::new()),
            config,
            next_worker_id: RwLock::new(0),
        }
    }

    /// Register a new worker
    pub async fn register(
        &self,
        worker_uuid: String,
        hostname: String,
        port: u16,
        capabilities: HashMap<String, String>,
    ) -> Result<u32> {
        let worker_id = {
            let mut next_id = self.next_worker_id.write().await;
            let id = *next_id;
            *next_id += 1;
            id
        };

        let worker = WorkerInfo {
            worker_id,
            worker_uuid: worker_uuid.clone(),
            hostname: hostname.clone(),
            port,
            status: WorkerStatus::Active,
            last_heartbeat: Instant::now(),
            current_step: 0,
            assigned_shards: Vec::new(),
            capabilities,
        };

        self.workers.write().await.insert(worker_id, worker);

        info!(
            "Registered worker {} ({}:{}) as worker_id={}",
            worker_uuid, hostname, port, worker_id
        );

        Ok(worker_id)
    }

    /// Process a heartbeat from a worker
    pub async fn heartbeat(&self, worker_id: u32, current_step: u64) -> Result<()> {
        let mut workers = self.workers.write().await;
        
        let worker = workers.get_mut(&worker_id).ok_or_else(|| {
            DTrainerError::WorkerNotRegistered { worker_id }
        })?;

        worker.last_heartbeat = Instant::now();
        worker.current_step = current_step;

        // Recover from suspect status
        if worker.status == WorkerStatus::Suspect {
            worker.status = WorkerStatus::Active;
            info!("Worker {} recovered from suspect status", worker_id);
        }

        debug!("Heartbeat from worker {}, step={}", worker_id, current_step);
        Ok(())
    }

    /// Deregister a worker
    pub async fn deregister(&self, worker_id: u32) -> Result<()> {
        let mut workers = self.workers.write().await;
        
        if let Some(worker) = workers.get_mut(&worker_id) {
            worker.status = WorkerStatus::Deregistered;
            info!("Worker {} deregistered", worker_id);
            Ok(())
        } else {
            Err(DTrainerError::WorkerNotRegistered { worker_id })
        }
    }

    /// Check for failed workers and update status
    pub async fn check_health(&self) -> Vec<u32> {
        let now = Instant::now();
        let mut failed_workers = Vec::new();

        let mut workers = self.workers.write().await;
        for (id, worker) in workers.iter_mut() {
            if worker.status == WorkerStatus::Deregistered 
                || worker.status == WorkerStatus::Failed {
                continue;
            }

            let elapsed = now.duration_since(worker.last_heartbeat);

            if elapsed > self.config.failure_timeout {
                if worker.status != WorkerStatus::Failed {
                    warn!("Worker {} marked as failed (timeout)", id);
                    worker.status = WorkerStatus::Failed;
                    failed_workers.push(*id);
                }
            } else if elapsed > self.config.heartbeat_timeout {
                if worker.status == WorkerStatus::Active {
                    warn!("Worker {} marked as suspect", id);
                    worker.status = WorkerStatus::Suspect;
                }
            }
        }

        failed_workers
    }

    /// Get active worker count
    pub async fn active_count(&self) -> usize {
        self.workers
            .read()
            .await
            .values()
            .filter(|w| w.status == WorkerStatus::Active)
            .count()
    }

    /// Get all active worker IDs
    pub async fn active_worker_ids(&self) -> Vec<u32> {
        self.workers
            .read()
            .await
            .values()
            .filter(|w| w.status == WorkerStatus::Active)
            .map(|w| w.worker_id)
            .collect()
    }

    /// Get worker info
    pub async fn get_worker(&self, worker_id: u32) -> Option<WorkerInfo> {
        self.workers.read().await.get(&worker_id).cloned()
    }

    /// Update worker shard assignment
    pub async fn set_shards(&self, worker_id: u32, shards: Vec<u32>) -> Result<()> {
        let mut workers = self.workers.write().await;
        let worker = workers.get_mut(&worker_id).ok_or_else(|| {
            DTrainerError::WorkerNotRegistered { worker_id }
        })?;
        worker.assigned_shards = shards;
        Ok(())
    }

    /// Set worker status to checkpointing
    pub async fn set_checkpointing(&self, worker_id: u32) -> Result<()> {
        let mut workers = self.workers.write().await;
        let worker = workers.get_mut(&worker_id).ok_or_else(|| {
            DTrainerError::WorkerNotRegistered { worker_id }
        })?;
        worker.status = WorkerStatus::Checkpointing;
        Ok(())
    }
}
