//! S3-compatible storage client
//!
//! Provides async object storage operations with retry and connection pooling.

pub mod s3;
pub mod buffer_pool;
pub mod multipart;

pub use s3::{S3Client, S3Config};
pub use buffer_pool::ByteBufferPool;
pub use multipart::MultipartUpload;
