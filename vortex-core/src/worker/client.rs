//! Coordinator client for workers
//!
//! gRPC client wrapper for worker-coordinator communication.

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::state::{WorkerState, WorkerPhase};
use crate::error::{DTrainerError, Result};

/// Configuration for coordinator client
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Coordinator address
    pub coordinator_addr: String,
    /// Connection timeout
    pub connect_timeout: Duration,
    /// Request timeout
    pub request_timeout: Duration,
    /// Heartbeat interval
    pub heartbeat_interval: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            coordinator_addr: "http://localhost:50051".into(),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(5),
        }
    }
}

use crate::protocol::coordinator_client::CoordinatorClient as GrpcClient;
use crate::protocol::*;
use tonic::transport::Channel;

/// Coordinator client for workers
pub struct CoordinatorClient {
    config: ClientConfig,
    state: Arc<WorkerState>,
    client: RwLock<Option<GrpcClient<Channel>>>,
}

impl CoordinatorClient {
    /// Create a new coordinator client
    pub fn new(config: ClientConfig, state: Arc<WorkerState>) -> Self {
        Self {
            config,
            state,
            client: RwLock::new(None),
        }
    }

    /// Connect to coordinator
    pub async fn connect(config: ClientConfig, state: Arc<WorkerState>) -> Result<Self> {
        let mut client = Self::new(config.clone(), state);
        client.reconnect().await?;
        Ok(client)
    }

    /// Establish connection
    async fn reconnect(&mut self) -> Result<()> {
        info!("Connecting to coordinator at {}", self.config.coordinator_addr);
        match GrpcClient::connect(self.config.coordinator_addr.clone()).await {
            Ok(grpc) => {
                *self.client.write().await = Some(grpc);
                Ok(())
            }
            Err(e) => Err(DTrainerError::ConnectionFailed {
                endpoint: self.config.coordinator_addr.clone(),
                reason: e.to_string(),
            }),
        }
    }

    /// Register with coordinator
    pub async fn register(&self, req: RegisterWorkerRequest) -> Result<RegisterWorkerResponse> {
        let mut guard = self.client.write().await;
        if let Some(client) = guard.as_mut() {
            let response = client.register_worker(req).await
                .map_err(|e| DTrainerError::Internal { message: e.to_string() })?;
            
            let inner = response.into_inner();
            self.state.set_worker_id(inner.worker_id).await;
            Ok(inner)
        } else {
            Err(DTrainerError::ConnectionFailed {
                endpoint: self.config.coordinator_addr.clone(),
                reason: "Not connected".into(),
            })
        }
    }

    /// Send heartbeat
    pub async fn send_heartbeat(&self, worker_id: u32, step: u64) -> Result<HeartbeatResponse> {
        let mut guard = self.client.write().await;
        if let Some(client) = guard.as_mut() {
            let req = HeartbeatRequest {
                worker_id,
                current_step: step,
                status: WorkerStatus::Training as i32,
                metrics: None,
                fencing_token: self.state.fencing_token(),
            };
            
            let response = client.heartbeat(req).await
                .map_err(|e| DTrainerError::Internal { message: e.to_string() })?;
            Ok(response.into_inner())
        } else {
            Err(DTrainerError::ConnectionFailed {
                endpoint: self.config.coordinator_addr.clone(),
                reason: "Not connected".into(),
            })
        }
    }
}

