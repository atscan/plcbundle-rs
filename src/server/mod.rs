// HTTP Server for serving bundle data
// This module provides an HTTP API for accessing bundle data, similar to the Go implementation

#[cfg(feature = "server")]
mod config;
#[cfg(feature = "server")]
mod handlers;
#[cfg(feature = "server")]
mod websocket;

#[cfg(feature = "server")]
use crate::manager::BundleManager;
#[cfg(feature = "server")]
use axum::Router;
#[cfg(feature = "server")]
use std::sync::Arc;
#[cfg(feature = "server")]
use std::time::Instant;

#[cfg(feature = "server")]
pub use config::ServerConfig;

#[cfg(feature = "server")]
pub struct Server {
    manager: Arc<BundleManager>,
    config: ServerConfig,
    start_time: Instant,
}

#[cfg(feature = "server")]
impl Server {
    pub fn new(manager: Arc<BundleManager>, config: ServerConfig) -> Self {
        Self {
            manager,
            config,
            start_time: Instant::now(),
        }
    }

    pub fn router(&self) -> Router {
        handlers::create_router(
            Arc::clone(&self.manager),
            self.config.clone(),
            self.start_time,
        )
    }

    pub fn start_time(&self) -> Instant {
        self.start_time
    }
}

#[cfg(feature = "server")]
pub use handlers::ServerState;
