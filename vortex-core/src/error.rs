//! Error types for DTrainer
//!
//! Comprehensive error taxonomy covering storage, coordination,
//! checkpoint, and protocol errors.

use thiserror::Error;

/// Primary error type for all DTrainer operations
#[derive(Debug, Error)]
pub enum DTrainerError {
    // ========== Storage Errors ==========
    
    /// S3 operation failed
    #[error("S3 operation failed: {message}")]
    StorageError { message: String },
    
    /// Object not found in storage
    #[error("Object not found: {key}")]
    ObjectNotFound { key: String },
    
    /// Stale listing detected (eventual consistency issue)
    #[error("Stale listing: expected version {expected}, got {actual}")]
    StaleList { expected: u64, actual: u64 },
    
    /// Checksum mismatch on read
    #[error("Checksum mismatch for {key}: expected {expected}, got {actual}")]
    ChecksumMismatch { key: String, expected: u32, actual: u32 },
    
    /// Multipart upload failed
    #[error("Multipart upload failed for {key}: {reason}")]
    MultipartUploadFailed { key: String, reason: String },

    // ========== Coordination Errors ==========
    
    /// Worker heartbeat timeout
    #[error("Worker {worker_id} failed heartbeat after {missed_count} attempts")]
    HeartbeatTimeout { worker_id: u32, missed_count: u32 },
    
    /// Epoch mismatch between coordinator and worker
    #[error("Epoch mismatch: coordinator at {coordinator}, worker at {worker}")]
    EpochMismatch { coordinator: u64, worker: u64 },
    
    /// Shard already claimed by another worker
    #[error("Shard {shard_id} already claimed by worker {owner}")]
    ShardConflict { shard_id: u32, owner: u32 },
    
    /// Worker not registered
    #[error("Worker {worker_id} not registered")]
    WorkerNotRegistered { worker_id: u32 },
    
    /// Fencing token is stale
    #[error("Stale fencing token: coordinator has {current}, received {received}")]
    StaleFencingToken { current: u64, received: u64 },

    // ========== Checkpoint Errors ==========
    
    /// Manifest validation failed
    #[error("Manifest validation failed: {reason}")]
    ManifestInvalid { reason: String },
    
    /// Partial checkpoint detected
    #[error("Partial checkpoint at epoch {epoch}, missing workers: {missing:?}")]
    PartialCheckpoint { epoch: u64, missing: Vec<u32> },
    
    /// Checkpoint write was interrupted
    #[error("Checkpoint write interrupted: {reason}")]
    WriteInterrupted { reason: String },
    
    /// No checkpoint found for restore
    #[error("No checkpoint found for job {job_id}")]
    NoCheckpointFound { job_id: String },
    
    /// Checkpoint already in progress
    #[error("Checkpoint already in progress at epoch {epoch}")]
    CheckpointInProgress { epoch: u64 },

    // ========== Protocol Errors ==========
    
    /// Protocol version mismatch
    #[error("Protocol version mismatch: server={server}, client={client}")]
    VersionMismatch { server: u32, client: u32 },
    
    /// Invalid message format
    #[error("Invalid message: {reason}")]
    InvalidMessage { reason: String },
    
    /// Connection failed
    #[error("Connection to {endpoint} failed: {reason}")]
    ConnectionFailed { endpoint: String, reason: String },

    // ========== Data Errors ==========
    
    /// Dataset not found
    #[error("Dataset not found: {dataset_id}")]
    DatasetNotFound { dataset_id: String },
    
    /// Data exhausted (end of epoch)
    #[error("Data exhausted for current epoch")]
    DataExhausted,
    
    /// Invalid shard specification
    #[error("Invalid shard spec: {reason}")]
    InvalidShardSpec { reason: String },

    // ========== Runtime Errors ==========
    
    /// Runtime not initialized
    #[error("Runtime not initialized")]
    RuntimeNotInitialized,
    
    /// Shutdown in progress
    #[error("Shutdown in progress")]
    ShutdownInProgress,
    
    /// Internal error
    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl DTrainerError {
    /// Returns true if this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            DTrainerError::StorageError { .. }
                | DTrainerError::ConnectionFailed { .. }
                | DTrainerError::HeartbeatTimeout { .. }
        )
    }

    /// Returns true if this error indicates data corruption
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            DTrainerError::ChecksumMismatch { .. }
                | DTrainerError::ManifestInvalid { .. }
        )
    }
}

/// Result type alias for DTrainer operations
pub type Result<T> = std::result::Result<T, DTrainerError>;
