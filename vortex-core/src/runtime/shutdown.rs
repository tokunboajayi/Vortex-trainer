//! Graceful shutdown handling
//!
//! Provides broadcast-based shutdown signaling for coordinated cleanup.

use std::sync::Arc;
use tokio::sync::broadcast;

/// Shutdown signal broadcaster
/// 
/// Use subscribe() to get a receiver, then clone the signal for distribution.
pub struct ShutdownSignal {
    sender: Arc<broadcast::Sender<()>>,
}

impl ShutdownSignal {
    /// Create a new shutdown signal
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(1);
        Self {
            sender: Arc::new(sender),
        }
    }

    /// Trigger shutdown
    pub fn shutdown(&self) {
        let _ = self.sender.send(());
    }

    /// Create a new receiver for this signal
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.sender.subscribe()
    }

    /// Check if any receivers exist
    pub fn has_receivers(&self) -> bool {
        self.sender.receiver_count() > 0
    }
}

impl Clone for ShutdownSignal {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

impl Default for ShutdownSignal {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shutdown_signal() {
        let signal = ShutdownSignal::new();
        let mut receiver = signal.subscribe();
        
        // Spawn task waiting for shutdown
        let handle = tokio::spawn(async move {
            let _ = receiver.recv().await;
            42
        });
        
        // Trigger shutdown
        signal.shutdown();
        
        // Task should complete
        let result = handle.await.unwrap();
        assert_eq!(result, 42);
    }
}
