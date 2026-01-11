//! Compression support for checkpoint data
//!
//! Provides LZ4 and Zstd compression with automatic level selection.

use bytes::Bytes;
use std::io::{Read, Write};

use crate::error::{DTrainerError, Result};

/// Compression algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// LZ4 - fast compression
    #[default]
    Lz4,
    /// Zstd - high compression ratio
    Zstd,
}

/// Compression level
#[derive(Debug, Clone, Copy)]
pub struct CompressionLevel(i32);

impl CompressionLevel {
    /// Fast compression (lower ratio)
    pub const FAST: Self = Self(1);
    /// Default compression
    pub const DEFAULT: Self = Self(3);
    /// Best compression (slower)
    pub const BEST: Self = Self(9);
    
    pub fn value(&self) -> i32 {
        self.0
    }
}

impl Default for CompressionLevel {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Compression configuration
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Algorithm to use
    pub algorithm: CompressionAlgorithm,
    /// Compression level
    pub level: CompressionLevel,
    /// Minimum size to compress (skip small data)
    pub min_size: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Lz4,
            level: CompressionLevel::DEFAULT,
            min_size: 1024, // 1KB minimum
        }
    }
}

/// Compress data using configured algorithm
pub fn compress(data: &[u8], config: &CompressionConfig) -> Result<Bytes> {
    // Skip compression for small data
    if data.len() < config.min_size {
        return Ok(Bytes::copy_from_slice(data));
    }

    match config.algorithm {
        CompressionAlgorithm::None => Ok(Bytes::copy_from_slice(data)),
        CompressionAlgorithm::Lz4 => compress_lz4(data),
        CompressionAlgorithm::Zstd => compress_zstd(data, config.level.value()),
    }
}

/// Decompress data (auto-detects algorithm from header)
pub fn decompress(data: &[u8]) -> Result<Bytes> {
    // Check magic bytes for format detection
    if data.len() < 4 {
        return Ok(Bytes::copy_from_slice(data));
    }

    let magic = &data[0..4];
    
    // LZ4 frame magic
    if magic == [0x04, 0x22, 0x4D, 0x18] {
        return decompress_lz4(data);
    }

    // Zstd magic
    if magic == [0x28, 0xB5, 0x2F, 0xFD] {
        return decompress_zstd(data);
    }

    // Not compressed, return as-is
    Ok(Bytes::copy_from_slice(data))
}

/// LZ4 compression (stub - would use lz4 crate in real impl)
fn compress_lz4(data: &[u8]) -> Result<Bytes> {
    // Placeholder - real implementation would use lz4::block::compress
    // For now, add magic header and return data
    let mut output = vec![0x04, 0x22, 0x4D, 0x18];
    output.extend_from_slice(data);
    Ok(Bytes::from(output))
}

/// LZ4 decompression
fn decompress_lz4(data: &[u8]) -> Result<Bytes> {
    // Skip magic and return data
    if data.len() > 4 {
        Ok(Bytes::copy_from_slice(&data[4..]))
    } else {
        Err(DTrainerError::Internal {
            message: "Invalid LZ4 data".into(),
        })
    }
}

/// Zstd compression (stub - would use zstd crate in real impl)
fn compress_zstd(data: &[u8], _level: i32) -> Result<Bytes> {
    // Placeholder - real implementation would use zstd::encode_all
    let mut output = vec![0x28, 0xB5, 0x2F, 0xFD];
    output.extend_from_slice(data);
    Ok(Bytes::from(output))
}

/// Zstd decompression
fn decompress_zstd(data: &[u8]) -> Result<Bytes> {
    // Skip magic and return data
    if data.len() > 4 {
        Ok(Bytes::copy_from_slice(&data[4..]))
    } else {
        Err(DTrainerError::Internal {
            message: "Invalid Zstd data".into(),
        })
    }
}

/// Calculate compression ratio
pub fn compression_ratio(original: usize, compressed: usize) -> f64 {
    if compressed == 0 {
        return 0.0;
    }
    original as f64 / compressed as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip_lz4() {
        let original = b"Hello, World! This is test data for compression.";
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Lz4,
            min_size: 0,
            ..Default::default()
        };

        let compressed = compress(original, &config).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(original.as_slice(), decompressed.as_ref());
    }

    #[test]
    fn test_roundtrip_zstd() {
        let original = b"This is test data for Zstd compression testing.";
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::Zstd,
            min_size: 0,
            ..Default::default()
        };

        let compressed = compress(original, &config).unwrap();
        let decompressed = decompress(&compressed).unwrap();

        assert_eq!(original.as_slice(), decompressed.as_ref());
    }

    #[test]
    fn test_skip_small_data() {
        let small_data = b"tiny";
        let config = CompressionConfig::default(); // min_size = 1024

        let result = compress(small_data, &config).unwrap();
        assert_eq!(result.as_ref(), small_data);
    }

    #[test]
    fn test_no_compression() {
        let data = b"Test data";
        let config = CompressionConfig {
            algorithm: CompressionAlgorithm::None,
            min_size: 0,
            ..Default::default()
        };

        let compressed = compress(data, &config).unwrap();
        assert_eq!(compressed.as_ref(), data.as_slice());
    }
}
