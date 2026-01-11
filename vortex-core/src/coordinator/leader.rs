//! Coordinator leader logic
//!
//! Main coordinator implementation orchestrating workers.

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{Duration, interval};
use tracing::{debug, info, error, warn};

use super::membership::{MembershipManager, MembershipConfig};
use super::epoch::EpochTracker;
use super::shard_assigner::ShardAssigner;
use crate::checkpoint::{CheckpointManager, CheckpointConfig};
use crate::checkpoint::manifest::Manifest;
use crate::storage::S3Client;
use crate::error::{DTrainerError, Result};
use tonic::{Request, Response, Status};
use crate::protocol::coordinator_server::Coordinator as CoordinatorTrait;
use crate::protocol::*;

/// Configuration for the coordinator
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Membership configuration
    pub membership: MembershipConfig,
    /// Checkpoint configuration
    pub checkpoint: CheckpointConfig,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Minimum workers to start
    pub min_workers: u32,
    /// Job identifier
    pub job_id: String,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            membership: MembershipConfig::default(),
            checkpoint: CheckpointConfig::default(),
            health_check_interval: Duration::from_secs(5),
            min_workers: 1,
            job_id: "default-job".into(),
        }
    }
}

/// Coordinator state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoordinatorState {
    /// Waiting for minimum workers
    WaitingForWorkers,
    /// Training in progress
    Running,
    /// Checkpoint in progress
    Checkpointing,
    /// Shutting down
    ShuttingDown,
}

/// Active checkpoint tracking
struct ActiveCheckpoint {
    epoch: u64,
    step: u64,
    pending_workers: Vec<u32>,
    completed_workers: Vec<u32>,
}

/// Main coordinator
pub struct Coordinator {
    config: CoordinatorConfig,
    membership: Arc<MembershipManager>,
    epoch_tracker: Arc<EpochTracker>,
    shard_assigner: Arc<RwLock<Option<ShardAssigner>>>,
    checkpoint_manager: Arc<CheckpointManager>,
    state: RwLock<CoordinatorState>,
    active_checkpoint: RwLock<Option<ActiveCheckpoint>>,
    fencing_token: RwLock<u64>,
}

impl Coordinator {
    /// Create a new coordinator
    pub async fn new(storage: Arc<S3Client>, config: CoordinatorConfig) -> Result<Self> {
        let membership = Arc::new(MembershipManager::new(config.membership.clone()));
        let epoch_tracker = Arc::new(EpochTracker::new());
        let checkpoint_manager = Arc::new(CheckpointManager::new(
            storage,
            config.job_id.clone(),
            config.checkpoint.clone(),
        ));

        Ok(Self {
            config,
            membership,
            epoch_tracker,
            shard_assigner: Arc::new(RwLock::new(None)),
            checkpoint_manager,
            state: RwLock::new(CoordinatorState::WaitingForWorkers),
            active_checkpoint: RwLock::new(None),
            fencing_token: RwLock::new(0),
        })
    }

    /// Start the coordinator background tasks
    pub async fn start(&self) {
        info!("Coordinator starting for job {}", self.config.job_id);
        
        // Try to restore from checkpoint
        if let Ok(Some(result)) = self.checkpoint_manager.restore().await {
            self.epoch_tracker.restore(result.epoch, result.step).await;
            info!("Restored from epoch {}", result.epoch);
        }

        // Start health check loop
        let membership = self.membership.clone();
        let health_interval = self.config.health_check_interval;
        tokio::spawn(async move {
            let mut interval = interval(health_interval);
            loop {
                interval.tick().await;
                let failed = membership.check_health().await;
                for worker_id in failed {
                    warn!("Worker {} failed health check", worker_id);
                }
            }
        });
    }

    /// Register a worker
    pub async fn register_worker(
        &self,
        worker_uuid: String,
        hostname: String,
        port: u16,
        capabilities: std::collections::HashMap<String, String>,
    ) -> Result<(u32, u64, u64, Vec<(u32, String)>)> {
        let worker_id = self.membership
            .register(worker_uuid, hostname, port, capabilities)
            .await?;

        let epoch = self.epoch_tracker.epoch().await;
        let step = self.epoch_tracker.step().await;

        // Assign shards if we have an assigner
        let shards = if let Some(assigner) = self.shard_assigner.read().await.as_ref() {
            let num_workers = self.membership.active_count().await as u32;
            assigner.shards_for_worker(worker_id, num_workers)
        } else {
            Vec::new()
        };

        // Update membership with shard assignment
        self.membership.set_shards(worker_id, shards.clone()).await?;

        // Format shard assignments with keys for response
        let shard_assignments: Vec<(u32, String)> = shards
            .iter()
            .map(|&id| (id, format!("shard_{:06}.bin", id)))
            .collect();

        // Check if we have enough workers to start
        self.check_worker_count().await;

        Ok((worker_id, epoch, step, shard_assignments))
    }

    /// Process heartbeat
    pub async fn heartbeat(&self, worker_id: u32, step: u64) -> Result<()> {
        self.membership.heartbeat(worker_id, step).await?;
        self.epoch_tracker.report_step(worker_id, step).await;
        Ok(())
    }

    /// Begin a checkpoint
    pub async fn begin_checkpoint(&self, step: u64) -> Result<u64> {
        let mut state = self.state.write().await;
        if *state == CoordinatorState::Checkpointing {
            return Err(DTrainerError::CheckpointInProgress {
                epoch: self.epoch_tracker.epoch().await,
            });
        }

        let epoch = self.epoch_tracker.advance_epoch().await;
        *state = CoordinatorState::Checkpointing;

        let worker_ids = self.membership.active_worker_ids().await;
        *self.active_checkpoint.write().await = Some(ActiveCheckpoint {
            epoch,
            step,
            pending_workers: worker_ids.clone(),
            completed_workers: Vec::new(),
        });

        info!("Beginning checkpoint at epoch {}, step {}", epoch, step);
        Ok(epoch)
    }

    /// Report checkpoint shard completion
    pub async fn complete_checkpoint_shard(&self, worker_id: u32, _epoch: u64) -> Result<bool> {
        let mut ckpt = self.active_checkpoint.write().await;
        let ckpt = ckpt.as_mut().ok_or_else(|| DTrainerError::Internal {
            message: "No active checkpoint".into(),
        })?;

        ckpt.pending_workers.retain(|&id| id != worker_id);
        ckpt.completed_workers.push(worker_id);

        let all_complete = ckpt.pending_workers.is_empty();
        
        debug!(
            "Checkpoint shard complete for worker {}, remaining: {}",
            worker_id,
            ckpt.pending_workers.len()
        );

        Ok(all_complete)
    }

    /// Finalize checkpoint
    pub async fn finalize_checkpoint(&self, epoch: u64, manifest: Manifest) -> Result<()> {
        let committer = self.checkpoint_manager.create_committer();
        committer.commit(epoch, manifest).await?;

        let step = self.active_checkpoint.read().await
            .as_ref()
            .map(|c| c.step)
            .unwrap_or(0);

        self.checkpoint_manager.record_checkpoint(step, epoch).await;

        *self.active_checkpoint.write().await = None;
        *self.state.write().await = CoordinatorState::Running;

        info!("Checkpoint finalized at epoch {}", epoch);
        Ok(())
    }

    /// Handle worker failure during checkpoint
    pub async fn handle_checkpoint_failure(&self, worker_id: u32) -> Result<()> {
        let mut ckpt = self.active_checkpoint.write().await;
        if let Some(ref mut ckpt) = *ckpt {
            ckpt.pending_workers.retain(|&id| id != worker_id);
            
            // Abort if too few workers remain
            let remaining = ckpt.completed_workers.len() + ckpt.pending_workers.len();
            if remaining < self.config.min_workers as usize {
                error!("Cannot complete checkpoint, aborting");
                let committer = self.checkpoint_manager.create_committer();
                committer.abort(ckpt.epoch, &ckpt.completed_workers).await?;
                
                *self.state.write().await = CoordinatorState::Running;
            }
        }
        Ok(())
    }

    /// Initialize shard assigner
    pub async fn init_shard_assigner(&self, num_shards: u32, seed: u64) {
        *self.shard_assigner.write().await = Some(ShardAssigner::new(num_shards, seed));
        info!("Initialized shard assigner with {} shards", num_shards);
    }

    /// Check worker count and update state
    async fn check_worker_count(&self) {
        let count = self.membership.active_count().await;
        let mut state = self.state.write().await;
        
        if *state == CoordinatorState::WaitingForWorkers 
            && count >= self.config.min_workers as usize 
        {
            *state = CoordinatorState::Running;
            info!("Minimum workers reached ({}), starting training", count);
        }
    }

    /// Get coordinator state
    pub async fn state(&self) -> CoordinatorState {
        *self.state.read().await
    }

    /// Get fencing token
    pub async fn fencing_token(&self) -> u64 {
        *self.fencing_token.read().await
    }

    /// Increment fencing token (for leader election)
    pub async fn increment_fencing_token(&self) -> u64 {
        let mut token = self.fencing_token.write().await;
        *token += 1;
        *token
    }
}

#[tonic::async_trait]
impl CoordinatorTrait for Coordinator {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> std::result::Result<Response<RegisterWorkerResponse>, Status> {
        let req = request.into_inner();
        let (id, epoch, step, shards) = self.register_worker(
            req.worker_uuid,
            req.hostname,
            req.port as u16,
            req.capabilities,
        ).await.map_err(|e| Status::internal(e.to_string()))?;

        // Convert shards to proto type
        let initial_shards = shards.into_iter().map(|(sid, key)| ShardAssignment {
            shard_id: sid,
            object_key: key,
            byte_offset: 0,
            byte_length: 0,
            expected_crc32c: 0,
        }).collect();

        Ok(Response::new(RegisterWorkerResponse {
            worker_id: id,
            current_epoch: epoch,
            current_step: step,
            initial_shards,
            fencing_token: self.fencing_token().await,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        
        // Update global metrics
        crate::metrics::standard::ACTIVE_WORKERS.set(self.membership.active_count().await as i64);
        crate::metrics::standard::CURRENT_EPOCH.set(self.epoch_tracker.epoch().await as i64);

        // Process heartbeat logic
        self.heartbeat(req.worker_id, req.current_step)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Basic metric aggregation (for MVP dashboard)
        if let Some(metrics) = req.metrics {
             // For now, just logging or we could aggregate if we track per-worker state
             // standard::BYTES_READ.inc_by(metrics.bytes_read); 
             // Implementing correct counter aggregation requires state tracking (deltas), skipped for quick MVP fix on Active Workers
        }

        Ok(Response::new(HeartbeatResponse {
            acknowledged: true,
            reassignments: vec![],
            checkpoint_trigger: None,
            fencing_token: self.fencing_token().await,
        }))
    }

    async fn deregister_worker(
        &self,
        request: Request<DeregisterWorkerRequest>,
    ) -> std::result::Result<Response<DeregisterWorkerResponse>, Status> {
        let req = request.into_inner();
        self.membership.deregister(req.worker_id).await;
        Ok(Response::new(DeregisterWorkerResponse { success: true }))
    }

    async fn get_shard_assignment(
        &self,
        request: Request<GetShardAssignmentRequest>,
    ) -> std::result::Result<Response<GetShardAssignmentResponse>, Status> {
        Ok(Response::new(GetShardAssignmentResponse { shards: vec![] }))
    }

    async fn report_shard_complete(
        &self,
        request: Request<ReportShardCompleteRequest>,
    ) -> std::result::Result<Response<ReportShardCompleteResponse>, Status> {
        Ok(Response::new(ReportShardCompleteResponse { next_shard: None }))
    }

    async fn sync_epoch(
        &self,
        request: Request<SyncEpochRequest>,
    ) -> std::result::Result<Response<SyncEpochResponse>, Status> {
        let req = request.into_inner();
        self.epoch_tracker.report_step(req.worker_id, req.step).await;
        
        Ok(Response::new(SyncEpochResponse {
            proceed: true,
            global_step: self.epoch_tracker.step().await,
        }))
    }

    async fn barrier_wait(
        &self,
        request: Request<BarrierWaitRequest>,
    ) -> std::result::Result<Response<BarrierWaitResponse>, Status> {
        Ok(Response::new(BarrierWaitResponse { released: true }))
    }

    async fn begin_checkpoint(
        &self,
        request: Request<BeginCheckpointRequest>,
    ) -> std::result::Result<Response<BeginCheckpointResponse>, Status> {
        let req = request.into_inner();
        let epoch = self.begin_checkpoint(req.step)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
            
        Ok(Response::new(BeginCheckpointResponse {
            epoch,
            pending_key_prefix: format!("checkpoints/{}/epoch_{}/pending", self.config.job_id, epoch),
        }))
    }

    async fn commit_checkpoint_shard(
        &self,
        request: Request<CommitCheckpointShardRequest>,
    ) -> std::result::Result<Response<CommitCheckpointShardResponse>, Status> {
        let req = request.into_inner();
        let all_complete = self.complete_checkpoint_shard(req.worker_id, req.epoch)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
            
        Ok(Response::new(CommitCheckpointShardResponse { all_shards_complete: all_complete }))
    }

    async fn finalize_checkpoint(
        &self,
        request: Request<FinalizeCheckpointRequest>,
    ) -> std::result::Result<Response<FinalizeCheckpointResponse>, Status> {
        let req = request.into_inner();
        let worker_count = self.membership.active_count().await as u32;
        let manifest = Manifest::new(
            self.config.job_id.clone(),
            req.epoch,
            self.epoch_tracker.step().await,
            worker_count
        );

        self.finalize_checkpoint(req.epoch, manifest)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
            
        Ok(Response::new(FinalizeCheckpointResponse {
            success: true,
            manifest_etag: "etag-placeholder".into(),
        }))
    }
}
