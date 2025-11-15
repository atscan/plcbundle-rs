// Logging utilities for CLI
use std::io::Write;

/// Initialize logger for CLI
pub fn init_logger(verbose: bool, quiet: bool) {
    let default_level = if quiet {
        log::LevelFilter::Error
    } else if verbose {
        log::LevelFilter::Info // Verbose = Info level for app, Debug for dependencies via filter
    } else {
        log::LevelFilter::Warn
    };

    let mut builder = env_logger::Builder::from_default_env();

    builder.filter_level(default_level).format(|buf, record| {
        // For verbose/debug output, include level prefix
        if record.level() <= log::Level::Debug {
            writeln!(buf, "[{}] {}", record.level(), record.args())
        } else {
            // For info and above, just the message
            writeln!(buf, "{}", record.args())
        }
    });

    // When verbose, only show Debug logs from our own crate, not dependencies
    if verbose {
        builder.filter_module("plcbundle", log::LevelFilter::Debug);
        // Suppress noisy debug logs from dependencies
        builder.filter_module("reqwest", log::LevelFilter::Info);
        builder.filter_module("hyper", log::LevelFilter::Info);
        builder.filter_module("h2", log::LevelFilter::Info);
        builder.filter_module("tokio", log::LevelFilter::Info);
    }

    // Check if RUST_LOG environment variable is set for override
    // If user sets RUST_LOG=debug, they get debug from everything
    if std::env::var("RUST_LOG").is_ok() {
        // RUST_LOG is set, let env_logger handle it completely
        return env_logger::init();
    }

    builder.init();
}
