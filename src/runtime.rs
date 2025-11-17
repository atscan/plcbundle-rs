// Runtime module - shutdown coordination for server and background tasks
use tokio::signal;
use tokio::sync::watch;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Lightweight coordination for bundle operations shutdown and background tasks
/// Used by both server and sync continuous mode for graceful shutdown handling
#[derive(Clone)]
pub struct BundleRuntime {
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    fatal_error: Arc<AtomicBool>,
}

impl BundleRuntime {
    /// Create a new bundle runtime coordinator
    pub fn new() -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            shutdown_tx,
            shutdown_rx,
            fatal_error: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Get a receiver to watch for shutdown signals
    /// Clone this and pass it to background tasks
    pub fn shutdown_signal(&self) -> watch::Receiver<bool> {
        self.shutdown_rx.clone()
    }

    /// Get a sender to trigger shutdown programmatically
    /// Use this when you need to pass the sender to other components
    pub fn shutdown_sender(&self) -> watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    /// Trigger a programmatic shutdown
    /// Call this from background tasks when they encounter fatal errors
    pub fn trigger_shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Trigger shutdown due to a fatal error
    /// This marks the shutdown as fatal, which will cause tasks to be aborted immediately
    pub fn trigger_fatal_shutdown(&self) {
        self.fatal_error.store(true, Ordering::Relaxed);
        self.trigger_shutdown();
    }

    /// Check if shutdown was triggered by a fatal error
    pub fn is_fatal_shutdown(&self) -> bool {
        self.fatal_error.load(Ordering::Relaxed)
    }

    /// Create a unified shutdown future that responds to both Ctrl+C and programmatic shutdown
    /// Use this with axum's `with_graceful_shutdown()`
    pub fn create_shutdown_future(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut shutdown_rx = self.shutdown_rx.clone();

        async move {
            tokio::select! {
                _ = signal::ctrl_c() => {
                    eprintln!("\n⚠️  Shutdown signal (Ctrl+C) received...");
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        eprintln!("\n⚠️  Shutdown triggered by background task...");
                    }
                }
            }
        }
    }
}

impl Default for BundleRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_programmatic_shutdown() {
        let runtime = BundleRuntime::new();
        let mut rx = runtime.shutdown_signal();

        // Spawn task to trigger shutdown
        let rt_clone = runtime.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            rt_clone.trigger_shutdown();
        });

        // Wait for shutdown signal
        rx.changed().await.unwrap();
        assert!(*rx.borrow());
    }

    #[tokio::test]
    async fn test_shutdown_signal_cloning() {
        let runtime = BundleRuntime::new();
        let mut rx1 = runtime.shutdown_signal();
        let mut rx2 = runtime.shutdown_signal();

        runtime.trigger_shutdown();

        // Both receivers should see the change
        rx1.changed().await.unwrap();
        rx2.changed().await.unwrap();

        assert!(*rx1.borrow());
        assert!(*rx2.borrow());
    }

    #[tokio::test]
    async fn test_fatal_shutdown() {
        let runtime = BundleRuntime::new();
        let mut rx = runtime.shutdown_signal();

        // Initially not a fatal shutdown
        assert!(!runtime.is_fatal_shutdown());

        // Trigger fatal shutdown
        runtime.trigger_fatal_shutdown();

        // Should be marked as fatal
        assert!(runtime.is_fatal_shutdown());

        // Shutdown signal should be set
        rx.changed().await.unwrap();
        assert!(*rx.borrow());
    }
}

