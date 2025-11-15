// Server configuration

#[derive(Clone)]
pub struct ServerConfig {
    pub sync_mode: bool,
    pub sync_interval_seconds: u64,
    pub enable_websocket: bool,
    pub enable_resolver: bool,
    pub version: String,
}
