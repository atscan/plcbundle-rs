// Logging utilities for CLI
use std::io::Write;

/// Initialize logger for CLI
pub fn init_logger(verbose: bool, quiet: bool) {
    let log_level = if quiet {
        log::LevelFilter::Error
    } else if verbose {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    env_logger::Builder::from_default_env()
        .filter_level(log_level)
        .format(|buf, record| {
            // For verbose/debug output, include level prefix
            if record.level() <= log::Level::Debug {
                writeln!(
                    buf,
                    "[{}] {}",
                    record.level(),
                    record.args()
                )
            } else {
                // For info and above, just the message
                writeln!(buf, "{}", record.args())
            }
        })
        .init();
}

