//! Backpressure control for data loading
//!
//! Prevents memory exhaustion by controlling prefetch rate based on
//! consumption speed.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::{Duration, Instant};

/// Configuration for backpressure control
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum number of prefetched batches
    pub max_pending: usize,
    /// Target consumption rate (batches/sec)
    pub target_rate: f64,
    /// Minimum delay between prefetches when under pressure
    pub min_delay: Duration,
    /// Maximum delay between prefetches when under pressure
    pub max_delay: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_pending: 8,
            target_rate: 100.0,
            min_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(100),
        }
    }
}

/// Backpressure controller using adaptive rate limiting
#[derive(Clone)]
pub struct BackpressureController {
    /// Semaphore to limit pending batches
    semaphore: Arc<Semaphore>,
    /// Configuration
    config: BackpressureConfig,
    /// Batches consumed counter
    consumed: Arc<AtomicU64>,
    /// Last consumption time
    last_consumption: Arc<AtomicU64>,
    /// Current pending count
    pending: Arc<AtomicUsize>,
}

impl BackpressureController {
    /// Create a new backpressure controller
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_pending)),
            config,
            consumed: Arc::new(AtomicU64::new(0)),
            last_consumption: Arc::new(AtomicU64::new(0)),
            pending: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Wait if backpressure is active (called by consumer)
    /// This is non-blocking in the fast path.
    pub async fn wait_if_needed(&self) {
        // Fast path: if we have capacity, no waiting
        if self.pending.load(Ordering::Relaxed) < self.config.max_pending / 2 {
            return;
        }

        // Slow path: adaptive delay based on pressure
        let pressure = self.calculate_pressure();
        if pressure > 0.5 {
            let delay = self.calculate_delay(pressure);
            tokio::time::sleep(delay).await;
        }
    }

    /// Acquire a slot for prefetching (called by prefetcher)
    /// Returns a permit that must be held while the batch is pending.
    pub async fn acquire(&self) -> BackpressurePermit {
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();
        self.pending.fetch_add(1, Ordering::Relaxed);
        BackpressurePermit {
            _permit: permit,
            pending: self.pending.clone(),
        }
    }

    /// Try to acquire without blocking
    pub fn try_acquire(&self) -> Option<BackpressurePermit> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                self.pending.fetch_add(1, Ordering::Relaxed);
                Some(BackpressurePermit {
                    _permit: permit,
                    pending: self.pending.clone(),
                })
            }
            Err(_) => None,
        }
    }

    /// Record a batch consumption
    pub fn record_consumption(&self) {
        self.consumed.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now().elapsed().as_nanos() as u64;
        self.last_consumption.store(now, Ordering::Relaxed);
    }

    /// Get current pending count
    pub fn pending_count(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    /// Calculate current pressure (0.0 = no pressure, 1.0 = full)
    fn calculate_pressure(&self) -> f64 {
        let pending = self.pending.load(Ordering::Relaxed) as f64;
        let max = self.config.max_pending as f64;
        (pending / max).min(1.0)
    }

    /// Calculate adaptive delay based on pressure
    fn calculate_delay(&self, pressure: f64) -> Duration {
        let min = self.config.min_delay.as_nanos() as f64;
        let max = self.config.max_delay.as_nanos() as f64;
        let nanos = min + (max - min) * (pressure * pressure); // Quadratic ramp
        Duration::from_nanos(nanos as u64)
    }
}

/// Permit that tracks a pending batch
pub struct BackpressurePermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    pending: Arc<AtomicUsize>,
}

impl Drop for BackpressurePermit {
    fn drop(&mut self) {
        self.pending.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_backpressure_acquire() {
        let config = BackpressureConfig {
            max_pending: 2,
            ..Default::default()
        };
        let controller = BackpressureController::new(config);

        // Acquire two permits
        let p1 = controller.acquire().await;
        let p2 = controller.acquire().await;
        assert_eq!(controller.pending_count(), 2);

        // Third acquire should block, so try_acquire should fail
        assert!(controller.try_acquire().is_none());

        // Drop one permit
        drop(p1);
        assert_eq!(controller.pending_count(), 1);

        // Now we can acquire
        let _p3 = controller.acquire().await;
        assert_eq!(controller.pending_count(), 2);
    }
}
