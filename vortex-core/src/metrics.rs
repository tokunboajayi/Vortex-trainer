//! Prometheus metrics for monitoring
//!
//! Provides counters, gauges, and histograms for observability.

use std::sync::atomic::{AtomicU64, AtomicI64, Ordering};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use parking_lot::RwLock;

/// Counter metric (monotonically increasing)
pub struct Counter {
    value: AtomicU64,
    name: String,
    help: String,
}

impl Counter {
    /// Create a new counter
    pub fn new(name: &str, help: &str) -> Self {
        Self {
            value: AtomicU64::new(0),
            name: name.into(),
            help: help.into(),
        }
    }

    /// Increment by 1
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment by delta
    pub fn inc_by(&self, delta: u64) {
        self.value.fetch_add(delta, Ordering::Relaxed);
    }

    /// Get current value
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Format as Prometheus metric
    pub fn to_prometheus(&self) -> String {
        format!(
            "# HELP {} {}\n# TYPE {} counter\n{} {}\n",
            self.name, self.help, self.name, self.name, self.get()
        )
    }
}

/// Gauge metric (can go up or down)
pub struct Gauge {
    value: AtomicI64,
    name: String,
    help: String,
}

impl Gauge {
    /// Create a new gauge
    pub fn new(name: &str, help: &str) -> Self {
        Self {
            value: AtomicI64::new(0),
            name: name.into(),
            help: help.into(),
        }
    }

    /// Set value
    pub fn set(&self, val: i64) {
        self.value.store(val, Ordering::Relaxed);
    }

    /// Increment by 1
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement by 1
    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current value
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Format as Prometheus metric
    pub fn to_prometheus(&self) -> String {
        format!(
            "# HELP {} {}\n# TYPE {} gauge\n{} {}\n",
            self.name, self.help, self.name, self.name, self.get()
        )
    }
}

/// Histogram for latency measurements
pub struct Histogram {
    buckets: Vec<(f64, AtomicU64)>,
    sum: AtomicU64,
    count: AtomicU64,
    name: String,
    help: String,
}

impl Histogram {
    /// Create with default buckets
    pub fn new(name: &str, help: &str) -> Self {
        Self::with_buckets(
            name,
            help,
            vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
        )
    }

    /// Create with custom buckets
    pub fn with_buckets(name: &str, help: &str, bounds: Vec<f64>) -> Self {
        let buckets = bounds
            .into_iter()
            .map(|b| (b, AtomicU64::new(0)))
            .collect();
        
        Self {
            buckets,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            name: name.into(),
            help: help.into(),
        }
    }

    /// Observe a value
    pub fn observe(&self, value: f64) {
        // Increment count
        self.count.fetch_add(1, Ordering::Relaxed);
        
        // Add to sum (as u64 micros for precision)
        let micros = (value * 1_000_000.0) as u64;
        self.sum.fetch_add(micros, Ordering::Relaxed);

        // Increment appropriate buckets
        for (bound, count) in &self.buckets {
            if value <= *bound {
                count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Time a closure and record the duration
    pub fn time<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        self.observe(start.elapsed().as_secs_f64());
        result
    }

    /// Format as Prometheus metric
    pub fn to_prometheus(&self) -> String {
        let mut output = format!(
            "# HELP {} {}\n# TYPE {} histogram\n",
            self.name, self.help, self.name
        );

        for (bound, count) in &self.buckets {
            output.push_str(&format!(
                "{}_bucket{{le=\"{}\"}} {}\n",
                self.name, bound, count.load(Ordering::Relaxed)
            ));
        }

        let sum_secs = self.sum.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        output.push_str(&format!("{}_sum {}\n", self.name, sum_secs));
        output.push_str(&format!(
            "{}_count {}\n",
            self.name,
            self.count.load(Ordering::Relaxed)
        ));

        output
    }
}

/// Metrics registry
pub struct MetricsRegistry {
    counters: RwLock<HashMap<String, Counter>>,
    gauges: RwLock<HashMap<String, Gauge>>,
    histograms: RwLock<HashMap<String, Histogram>>,
}

impl MetricsRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }

    /// Register a counter
    pub fn counter(&self, name: &str, help: &str) {
        let mut counters = self.counters.write();
        if !counters.contains_key(name) {
            counters.insert(name.into(), Counter::new(name, help));
        }
    }

    /// Get a counter
    pub fn get_counter(&self, name: &str) -> Option<&Counter> {
        // This is unsafe in real code - would need Arc or similar
        None
    }

    /// Export all metrics in Prometheus format
    pub fn export(&self) -> String {
        let mut output = String::new();

        for counter in self.counters.read().values() {
            output.push_str(&counter.to_prometheus());
        }

        for gauge in self.gauges.read().values() {
            output.push_str(&gauge.to_prometheus());
        }

        for histogram in self.histograms.read().values() {
            output.push_str(&histogram.to_prometheus());
        }

        output
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Standard DTrainer metrics
pub mod standard {
    use super::*;
    use std::sync::LazyLock;

    /// Global metrics
    pub static SHARDS_LOADED: LazyLock<Counter> = LazyLock::new(|| {
        Counter::new("vortex_shards_loaded_total", "Total number of shards loaded")
    });

    pub static BYTES_READ: LazyLock<Counter> = LazyLock::new(|| {
        Counter::new("vortex_bytes_read_total", "Total bytes read from storage")
    });

    pub static CHECKPOINTS_WRITTEN: LazyLock<Counter> = LazyLock::new(|| {
        Counter::new("vortex_checkpoints_written_total", "Total checkpoints written")
    });

    pub static ACTIVE_WORKERS: LazyLock<Gauge> = LazyLock::new(|| {
        Gauge::new("vortex_active_workers", "Number of active workers")
    });

    pub static CURRENT_EPOCH: LazyLock<Gauge> = LazyLock::new(|| {
        Gauge::new("vortex_current_epoch", "Current training epoch")
    });

    pub static SHARD_LOAD_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
        Histogram::new("vortex_shard_load_duration_seconds", "Shard load latency")
    });

    pub static CHECKPOINT_DURATION: LazyLock<Histogram> = LazyLock::new(|| {
        Histogram::new("vortex_checkpoint_duration_seconds", "Checkpoint write latency")
    });
}

/// Helper to gather all standard metrics
pub fn gather_system_metrics() -> String {
    let mut output = String::new();
    
    // Counters
    output.push_str(&standard::SHARDS_LOADED.to_prometheus());
    output.push_str(&standard::BYTES_READ.to_prometheus());
    output.push_str(&standard::CHECKPOINTS_WRITTEN.to_prometheus());
    
    // Gauges
    output.push_str(&standard::ACTIVE_WORKERS.to_prometheus());
    output.push_str(&standard::CURRENT_EPOCH.to_prometheus());
    
    // Histograms
    output.push_str(&standard::SHARD_LOAD_DURATION.to_prometheus());
    output.push_str(&standard::CHECKPOINT_DURATION.to_prometheus());
    
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = Counter::new("test_counter", "Test counter");
        assert_eq!(counter.get(), 0);
        
        counter.inc();
        assert_eq!(counter.get(), 1);
        
        counter.inc_by(5);
        assert_eq!(counter.get(), 6);
    }

    #[test]
    fn test_gauge() {
        let gauge = Gauge::new("test_gauge", "Test gauge");
        assert_eq!(gauge.get(), 0);
        
        gauge.set(10);
        assert_eq!(gauge.get(), 10);
        
        gauge.dec();
        assert_eq!(gauge.get(), 9);
    }

    #[test]
    fn test_histogram() {
        let histogram = Histogram::new("test_histogram", "Test histogram");
        
        histogram.observe(0.001);
        histogram.observe(0.01);
        histogram.observe(0.1);
        
        let prometheus = histogram.to_prometheus();
        assert!(prometheus.contains("test_histogram_count 3"));
    }
}
