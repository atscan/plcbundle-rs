// HTTP handlers module

#[cfg(feature = "server")]
mod handle_bundle;
#[cfg(feature = "server")]
mod handle_debug;
#[cfg(feature = "server")]
mod handle_did;
#[cfg(feature = "server")]
mod handle_root;
#[cfg(feature = "server")]
mod handle_status;

#[cfg(feature = "server")]
use crate::server::config::ServerConfig;
#[cfg(feature = "server")]
use crate::manager::BundleManager;
#[cfg(feature = "server")]
use std::sync::Arc;
#[cfg(feature = "server")]
use std::time::Instant;

#[derive(Clone)]
pub struct ServerState {
    pub manager: Arc<BundleManager>,
    pub config: ServerConfig,
    pub start_time: Instant,
}

#[cfg(feature = "server")]
pub use handle_bundle::*;
#[cfg(feature = "server")]
pub use handle_debug::*;
#[cfg(feature = "server")]
pub use handle_did::*;
#[cfg(feature = "server")]
pub use handle_root::*;
#[cfg(feature = "server")]
pub use handle_status::*;

