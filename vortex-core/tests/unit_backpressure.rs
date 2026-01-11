//! Unit tests for backpressure controller

use std::time::Duration;
use tokio::sync::Semaphore;
use std::sync::Arc;

/// Backpressure configuration for testing
struct BackpressureConfig {
    max_permits: usize,
    base_delay_ms: u64,
    max_delay_ms: u64,
}

/// Simplified backpressure controller for testing
struct BackpressureController {
    semaphore: Arc<Semaphore>,
    config: BackpressureConfig,
    current_level: std::sync::atomic::AtomicU32,
}

impl BackpressureController {
    fn new(config: BackpressureConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_permits)),
            config,
            current_level: std::sync::atomic::AtomicU32::new(0),
        }
    }

    fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    fn try_acquire(&self) -> bool {
        self.semaphore.try_acquire().is_ok()
    }

    async fn acquire(&self) {
        let _ = self.semaphore.acquire().await;
    }

    fn compute_delay(&self, level: u32) -> Duration {
        let delay_ms = self.config.base_delay_ms * (level as u64).pow(2);
        Duration::from_millis(delay_ms.min(self.config.max_delay_ms))
    }

    fn increase_pressure(&self) {
        self.current_level.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn decrease_pressure(&self) {
        let current = self.current_level.load(std::sync::atomic::Ordering::Relaxed);
        if current > 0 {
            self.current_level.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn pressure_level(&self) -> u32 {
        self.current_level.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[test]
fn test_initial_state() {
    let controller = BackpressureController::new(BackpressureConfig {
        max_permits: 10,
        base_delay_ms: 10,
        max_delay_ms: 1000,
    });

    assert_eq!(controller.available_permits(), 10);
    assert_eq!(controller.pressure_level(), 0);
}

#[test]
fn test_permit_acquisition() {
    let controller = BackpressureController::new(BackpressureConfig {
        max_permits: 3,
        base_delay_ms: 10,
        max_delay_ms: 1000,
    });

    // Acquire all permits
    assert!(controller.try_acquire());
    assert!(controller.try_acquire());
    assert!(controller.try_acquire());
    
    // No more permits
    assert!(!controller.try_acquire());
}

#[test]
fn test_delay_computation() {
    let controller = BackpressureController::new(BackpressureConfig {
        max_permits: 10,
        base_delay_ms: 10,
        max_delay_ms: 1000,
    });

    // Quadratic delay
    assert_eq!(controller.compute_delay(0), Duration::from_millis(0));
    assert_eq!(controller.compute_delay(1), Duration::from_millis(10));
    assert_eq!(controller.compute_delay(2), Duration::from_millis(40));
    assert_eq!(controller.compute_delay(3), Duration::from_millis(90));
    
    // Should cap at max
    assert_eq!(controller.compute_delay(100), Duration::from_millis(1000));
}

#[test]
fn test_pressure_levels() {
    let controller = BackpressureController::new(BackpressureConfig {
        max_permits: 10,
        base_delay_ms: 10,
        max_delay_ms: 1000,
    });

    assert_eq!(controller.pressure_level(), 0);
    
    controller.increase_pressure();
    assert_eq!(controller.pressure_level(), 1);
    
    controller.increase_pressure();
    controller.increase_pressure();
    assert_eq!(controller.pressure_level(), 3);
    
    controller.decrease_pressure();
    assert_eq!(controller.pressure_level(), 2);
}

#[tokio::test]
async fn test_async_acquire() {
    let controller = BackpressureController::new(BackpressureConfig {
        max_permits: 2,
        base_delay_ms: 10,
        max_delay_ms: 1000,
    });

    // Acquire permits
    controller.acquire().await;
    controller.acquire().await;

    assert_eq!(controller.available_permits(), 0);
}
