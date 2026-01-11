//! Reusable byte buffer pool
//!
//! Reduces allocations by reusing byte buffers across operations.

use bytes::BytesMut;
use parking_lot::Mutex;
use std::sync::Arc;

/// Thread-safe byte buffer pool
pub struct ByteBufferPool {
    buffers: Mutex<Vec<BytesMut>>,
    buffer_capacity: usize,
    max_buffers: usize,
}

impl ByteBufferPool {
    /// Create a new buffer pool
    pub fn new(initial_count: usize, buffer_capacity: usize) -> Arc<Self> {
        let buffers = (0..initial_count)
            .map(|_| BytesMut::with_capacity(buffer_capacity))
            .collect();
        
        Arc::new(Self {
            buffers: Mutex::new(buffers),
            buffer_capacity,
            max_buffers: initial_count * 2,
        })
    }

    /// Acquire a buffer from the pool
    pub fn acquire(&self) -> BytesMut {
        self.buffers
            .lock()
            .pop()
            .unwrap_or_else(|| BytesMut::with_capacity(self.buffer_capacity))
    }

    /// Return a buffer to the pool
    pub fn release(&self, mut buffer: BytesMut) {
        buffer.clear();
        
        let mut buffers = self.buffers.lock();
        if buffers.len() < self.max_buffers && buffer.capacity() >= self.buffer_capacity {
            buffers.push(buffer);
        }
        // Otherwise, buffer is dropped
    }

    /// Get current pool size
    pub fn available(&self) -> usize {
        self.buffers.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool() {
        let pool = ByteBufferPool::new(2, 1024);
        
        // Acquire all buffers
        let b1 = pool.acquire();
        let b2 = pool.acquire();
        assert_eq!(pool.available(), 0);
        
        // Acquire one more (creates new)
        let b3 = pool.acquire();
        assert_eq!(pool.available(), 0);
        
        // Release buffers
        pool.release(b1);
        pool.release(b2);
        pool.release(b3);
        
        // Should have 3 now (under max of 4)
        assert_eq!(pool.available(), 3);
    }
}
