use pyo3::prelude::*;
use std::sync::Arc;

use crate::data::dataset::{Dataset as RustDataset, DatasetConfig, DatasetMetadata};
use crate::data::loader::{DataLoader as RustDataLoader, DataLoaderConfig};
use crate::storage::S3Client;
use crate::runtime::DTrainerRuntime;
use crate::runtime::executor::RuntimeConfig;
use crate::error::DTrainerError;

#[pyclass]
struct VortexClient {
    runtime: Arc<DTrainerRuntime>,
    s3_client: Arc<S3Client>,
}

#[pymethods]
impl VortexClient {
    #[new]
    fn new() -> PyResult<Self> {
        let runtime = DTrainerRuntime::new(RuntimeConfig::default())
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;
        
        // Initialize S3 client (async)
        let s3_client = runtime.block_on_io(async {
            let mut config = crate::storage::S3Config::default();
            
            if let Ok(endpoint) = std::env::var("S3_ENDPOINT") {
                config.endpoint = endpoint;
            }
            if let Ok(key) = std::env::var("AWS_ACCESS_KEY_ID").or(std::env::var("S3_ACCESS_KEY")) {
                config.access_key_id = Some(key);
            }
            if let Ok(secret) = std::env::var("AWS_SECRET_ACCESS_KEY").or(std::env::var("S3_SECRET_KEY")) {
                config.secret_access_key = Some(secret);
            }
            if let Ok(bucket) = std::env::var("S3_BUCKET") {
                config.bucket = bucket;
            }

            S3Client::new(config).await
        }).map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string()))?;

        Ok(Self {
            runtime: Arc::new(runtime),
            s3_client: Arc::new(s3_client),
        })
    }

    fn create_dataset(&self, name: String, prefix: String, num_shards: u32) -> PyResult<PyDataset> {
        let config = DatasetConfig::new(name.clone(), prefix, num_shards);
         let metadata = DatasetMetadata {
            config,
            created_at: chrono::Utc::now(),
            total_bytes: None,
            shard_metadata: std::collections::HashMap::new(),
        };
        
        let dataset = Arc::new(RustDataset::new(name, metadata));
        Ok(PyDataset { inner: dataset })
    }

    fn create_loader(&self, dataset: &PyDataset, batch_size: usize) -> PyResult<PyDataLoader> {
        let config = DataLoaderConfig {
             channel_size: batch_size * 2,
             ..Default::default()
        };

        let loader = RustDataLoader::new(
            &self.runtime,
            dataset.inner.clone(),
            self.s3_client.clone(),
            config
        );

        Ok(PyDataLoader { 
            inner: Arc::new(tokio::sync::Mutex::new(loader)),
            runtime: self.runtime.clone()
        })
    }

    fn register_worker(&self, coordinator_addr: String, worker_id: u32) -> PyResult<()> {
        let runtime = self.runtime.clone();
        
        runtime.block_on_io(async move {
            use crate::protocol::coordinator_client::CoordinatorClient;
            use crate::protocol::{RegisterWorkerRequest, HeartbeatRequest, ResourceMetrics};
            use std::time::Duration;

            let addr = if coordinator_addr.contains("://") {
                coordinator_addr
            } else {
                format!("http://{}", coordinator_addr)
            };

            let mut client = CoordinatorClient::connect(addr.clone())
                .await
                .map_err(|e| DTrainerError::Internal { message: e.to_string() })?;

            let hostname = hostname::get().map(|h| h.to_string_lossy().into_owned()).unwrap_or_else(|_| "localhost".into());
            let request = RegisterWorkerRequest {
                worker_uuid: format!("worker-{}", worker_id),
                hostname,
                port: 50052 + worker_id, 
                capabilities: std::collections::HashMap::new(),
                protocol_version: 1,
            };

            let _ = client.register_worker(request).await
                 .map_err(|e| DTrainerError::Internal { message: e.to_string() })?;

            println!("Registered vortex-worker {} with coordinator at {}", worker_id, addr);

            let mut hb_client = client.clone();
            
            runtime.spawn_io(async move {
                let mut step = 0;
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    
                    let metrics = ResourceMetrics {
                        cpu_percent: 45.0 + (step % 10) as f32, 
                        memory_percent: 60.0,
                        gpu_utilization: 80.0,
                        gpu_memory_percent: 70.0,
                        bytes_read: 50 * 1024 * 1024,
                        bytes_written: 0,
                    };

                    let req = HeartbeatRequest {
                        worker_id,
                        current_step: step,
                        status: 3, 
                        metrics: Some(metrics),
                        fencing_token: 0,
                    };

                    if let Err(e) = hb_client.heartbeat(req).await {
                       eprintln!("Heartbeat failed: {}", e);
                    }
                    step += 1;
                }
            });
            
            Ok::<(), DTrainerError>(())
        }).map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;

        Ok(())
    }
}

#[pyclass]
struct PyDataset {
    inner: Arc<RustDataset>,
}

#[pyclass]
struct PyDataLoader {
    inner: Arc<tokio::sync::Mutex<RustDataLoader>>,
    runtime: Arc<DTrainerRuntime>,
}

#[pymethods]
impl PyDataLoader {
    fn next_batch(&mut self, _py: Python<'_>) -> PyResult<Option<Vec<u8>>> {
        let batch_result = self.runtime.block_on_io(async {
            let mut loader = self.inner.lock().await;
            loader.next_batch().await
        });

        match batch_result {
            Ok(batch) => {
                Ok(Some(batch.data.to_vec()))
            },
            Err(crate::error::DTrainerError::DataExhausted) => Ok(None),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(e.to_string())),
        }
    }
}

/// A Python module implemented in Rust.
#[pymodule]
pub fn vortex_core(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<VortexClient>()?;
    m.add_class::<PyDataset>()?;
    m.add_class::<PyDataLoader>()?;
    Ok(())
}
