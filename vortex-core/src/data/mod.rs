//! Data loading pipeline
//!
//! High-throughput async data loading with prefetching and backpressure.

pub mod dataset;
pub mod shard;
pub mod loader;
pub mod prefetcher;
pub mod backpressure;

pub use dataset::{Dataset, DatasetConfig, DatasetMetadata};
pub use shard::{Shard, ShardSpec, ShardBatch};
pub use loader::DataLoader;
pub use prefetcher::Prefetcher;
pub use backpressure::BackpressureController;
