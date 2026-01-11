//! Coordinator service binary

use vortex_core::storage::{S3Client, S3Config};
use vortex_core::coordinator::{Coordinator, CoordinatorConfig};
use vortex_core::protocol::coordinator_server::CoordinatorServer;
use tonic::transport::Server;
use std::sync::Arc;
use tracing::{info, error, warn};
use axum::{Router, routing::get};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    info!("Starting Vortex Coordinator");

    // Load configuration from environment
    let s3_config = S3Config {
        endpoint: std::env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".into()),
        bucket: std::env::var("S3_BUCKET").unwrap_or_else(|_| "vortex".into()),
        region: std::env::var("S3_REGION").unwrap_or_else(|_| "us-east-1".into()),
        access_key_id: std::env::var("AWS_ACCESS_KEY_ID").or(std::env::var("S3_ACCESS_KEY")).ok(),
        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY").or(std::env::var("S3_SECRET_KEY")).ok(),
        ..Default::default()
    };

    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:50051".into());
    let addr = bind_addr.parse()?;

    info!("Connecting to storage at {}", s3_config.endpoint);
    let storage = match S3Client::new(s3_config).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            error!("Failed to connect to storage: {}", e);
            return Err(e.into());
        }
    };

    let config = CoordinatorConfig {
        job_id: std::env::var("JOB_ID").unwrap_or_else(|_| "default-job".into()),
        ..Default::default()
    };

    let coordinator = Coordinator::new(storage, config).await?;
    
    // Start background tasks
    coordinator.start().await;

    // Start Metrics API Server
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(|| async { vortex_core::metrics::gather_system_metrics() }))
            .layer(CorsLayer::permissive());

        let addr = SocketAddr::from(([0, 0, 0, 0], 9100));
        info!("Metrics API listening on {}", addr);

        if let Err(e) = axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app).await {
            warn!("Metrics server error: {}", e);
        }
    });

    info!("Coordinator listening on {}", addr);

    // Start gRPC server
    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
