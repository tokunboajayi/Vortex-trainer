//! Worker service binary

use vortex_core::storage::{S3Client, S3Config};
use vortex_core::worker::{CoordinatorClient, WorkerState};
use vortex_core::worker::client::ClientConfig;
use vortex_core::protocol::RegisterWorkerRequest;
use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;
use tracing::{info, error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("Starting Vortex Worker");

    // Load configuration
    let s3_config = S3Config {
        endpoint: std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into()),
        bucket: std::env::var("S3_BUCKET").unwrap_or_else(|_| "vortex".into()),
        region: std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").or(std::env::var("S3_ACCESS_KEY")).ok(),
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").or(std::env::var("S3_SECRET_KEY")).ok(),
        ..Default::default()
    };

    let coordinator_addr = std::env::var("COORDINATOR_ADDR").unwrap_or_else(|_| "http://localhost:50051".into());
    let worker_uuid = std::env::var("WORKER_ID")
        .unwrap_or_else(|_| uuid::Uuid::new_v4().to_string());

    info!("Connecting to storage at {}", s3_config.endpoint);
    let _storage = match S3Client::new(s3_config).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            error!("Failed to connect to storage: {}", e);
            return Err(e.into());
        }
    };

    info!("Connecting to coordinator at {}", coordinator_addr);
    
    // Create state and config
    let state = Arc::new(WorkerState::new());
    let client_config = ClientConfig {
        coordinator_addr,
        ..Default::default()
    };

    // Connect to coordinator (reconnect logic handled internally if needed, but connect waits for first connection)
    let client = CoordinatorClient::connect(client_config, state.clone()).await?;
    
    // Register worker
    let req = RegisterWorkerRequest {
        worker_uuid: worker_uuid.clone(),
        hostname: worker_uuid, // Just using UUID as hostname for now
        port: 0,
        capabilities: HashMap::new(),
        protocol_version: 1,
    };

    let response = client.register(req).await?;
    info!("Registered with coordinator, assigned ID: {}", response.worker_id);

    // Initial sync
    state.set_worker_id(response.worker_id).await;
    state.set_epoch(response.current_epoch);
    state.set_step(response.current_step);

    // Start heartbeat loop
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        if let Err(e) = client.send_heartbeat(response.worker_id, state.step()).await {
            error!("Heartbeat failed: {}", e);
        }
    }
}
