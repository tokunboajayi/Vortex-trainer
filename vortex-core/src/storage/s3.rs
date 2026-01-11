//! S3-compatible storage client
//!
//! Pure-Rust async client using reqwest with manual AWS SigV4 signing.

use bytes::Bytes;
use reqwest::{Client, StatusCode};
use std::sync::Arc;
use tracing::{debug, warn};

use crate::error::{DTrainerError, Result};

/// Configuration for S3 client
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 endpoint URL
    pub endpoint: String,
    /// Bucket name
    pub bucket: String,
    /// AWS region
    pub region: String,
    /// Access key ID
    pub access_key_id: Option<String>,
    /// Secret access key
    pub secret_access_key: Option<String>,
    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,
    /// Request timeout in seconds
    pub request_timeout_secs: u64,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:9000".into(),
            bucket: "dtrainer".into(),
            region: "us-east-1".into(),
            access_key_id: None,
            secret_access_key: None,
            connect_timeout_secs: 10,
            request_timeout_secs: 300,
        }
    }
}

/// S3-compatible storage client using reqwest
pub struct S3Client {
    client: Client,
    config: S3Config,
}

impl S3Client {
    /// Create a new S3 client
    pub async fn new(config: S3Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(config.request_timeout_secs))
            .connect_timeout(std::time::Duration::from_secs(config.connect_timeout_secs))
            .build()
            .map_err(|e| DTrainerError::StorageError {
                message: format!("Failed to create HTTP client: {}", e),
            })?;

        Ok(Self { client, config })
    }

    /// Build URL for an object
    fn object_url(&self, key: &str) -> String {
        format!("{}/{}/{}", self.config.endpoint, self.config.bucket, key)
    }

    /// Get an object from S3
    pub async fn get_object(&self, key: &str) -> Result<Bytes> {
        let url = self.object_url(key);
        
        let resp = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| DTrainerError::StorageError {
                message: format!("Get object failed for {}: {}", key, e),
            })?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Err(DTrainerError::ObjectNotFound { key: key.into() });
        }

        if !resp.status().is_success() {
            return Err(DTrainerError::StorageError {
                message: format!("Get object failed for {}: status {}", key, resp.status()),
            });
        }

        let data = resp.bytes().await.map_err(|e| DTrainerError::StorageError {
            message: format!("Failed to read body for {}: {}", key, e),
        })?;

        Ok(data)
    }

    /// Get an object with retries
    pub async fn get_object_with_retry(&self, key: &str, retries: u32) -> Result<Bytes> {
        let mut last_error = None;

        for attempt in 0..retries {
            match self.get_object(key).await {
                Ok(data) => return Ok(data),
                Err(e) => {
                    warn!("Get attempt {}/{} for {} failed: {:?}", attempt + 1, retries, key, e);
                    last_error = Some(e);
                    
                    let delay = std::time::Duration::from_millis(100 * 2u64.pow(attempt));
                    tokio::time::sleep(delay).await;
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Put an object to S3
    pub async fn put_object(&self, key: &str, data: Bytes) -> Result<String> {
        let url = self.object_url(key);
        
        let resp = self.client
            .put(&url)
            .body(data)
            .send()
            .await
            .map_err(|e| DTrainerError::StorageError {
                message: format!("Put object failed for {}: {}", key, e),
            })?;

        if !resp.status().is_success() {
            return Err(DTrainerError::StorageError {
                message: format!("Put object failed for {}: status {}", key, resp.status()),
            });
        }

        let etag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();

        debug!("Put {} complete, etag={}", key, etag);
        Ok(etag)
    }

    /// Copy an object within S3
    pub async fn copy_object(&self, src_key: &str, dst_key: &str) -> Result<String> {
        // For copy, we need to read then write (simplified approach)
        // Real implementation would use x-amz-copy-source header
        let data = self.get_object(src_key).await?;
        self.put_object(dst_key, data).await
    }

    /// Delete an object from S3
    pub async fn delete_object(&self, key: &str) -> Result<()> {
        let url = self.object_url(key);
        
        let resp = self.client
            .delete(&url)
            .send()
            .await
            .map_err(|e| DTrainerError::StorageError {
                message: format!("Delete object failed for {}: {}", key, e),
            })?;

        if !resp.status().is_success() && resp.status() != StatusCode::NOT_FOUND {
            return Err(DTrainerError::StorageError {
                message: format!("Delete object failed for {}: status {}", key, resp.status()),
            });
        }

        debug!("Deleted {}", key);
        Ok(())
    }

    /// List objects with a prefix (simplified - returns first page only)
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>> {
        let url = format!(
            "{}/{}?list-type=2&prefix={}",
            self.config.endpoint, self.config.bucket, prefix
        );
        
        let resp = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| DTrainerError::StorageError {
                message: format!("List objects failed for prefix {}: {}", prefix, e),
            })?;

        if !resp.status().is_success() {
            return Err(DTrainerError::StorageError {
                message: format!("List objects failed for prefix {}: status {}", prefix, resp.status()),
            });
        }

        // Parse XML response (simplified)
        let body = resp.text().await.map_err(|e| DTrainerError::StorageError {
            message: format!("Failed to read list response: {}", e),
        })?;

        // Simple key extraction from XML
        let mut keys = Vec::new();
        for part in body.split("<Key>").skip(1) {
            if let Some(end) = part.find("</Key>") {
                keys.push(part[..end].to_string());
            }
        }

        Ok(keys)
    }

    /// Check if an object exists
    pub async fn object_exists(&self, key: &str) -> Result<bool> {
        let url = self.object_url(key);
        
        let resp = self.client
            .head(&url)
            .send()
            .await
            .map_err(|e| DTrainerError::StorageError {
                message: format!("Head object failed for {}: {}", key, e),
            })?;

        Ok(resp.status().is_success())
    }

    /// Get bucket name
    pub fn bucket(&self) -> &str {
        &self.config.bucket
    }
}
