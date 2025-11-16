// Shared formatting helpers used across CLI/server/library components.

use chrono::Duration as ChronoDuration;
use std::time::Duration as StdDuration;

/// Format a byte count as a human-readable string (e.g. "1.23 MB").
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];

    let mut size = bytes as f64;
    let mut unit_idx = 0usize;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[unit_idx])
    } else {
        format!("{:.2} {}", size, UNITS[unit_idx])
    }
}

/// Format a byte count in compact ls/df style (e.g. "1.5K", "2.3M", "1.2G").
/// Similar to `ls -h` or `df -h` output format.
pub fn format_bytes_compact(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["", "K", "M", "G", "T"];

    if bytes == 0 {
        return "0".to_string();
    }

    let mut size = bytes as f64;
    let mut unit_idx = 0usize;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{}", bytes)
    } else {
        // Use 1 decimal place, but remove trailing zero
        let formatted = format!("{:.1}", size);
        let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
        format!("{}{}", trimmed, UNITS[unit_idx])
    }
}

/// Format an integer with thousands separators (e.g. 12_345 -> "12,345").
pub fn format_number<T>(value: T) -> String
where
    T: std::fmt::Display,
{
    let s = value.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (idx, ch) in s.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

/// Format a chrono duration using verbose units (e.g. "2d 3h 10m 5s").
pub fn format_duration_verbose(duration: ChronoDuration) -> String {
    let seconds = duration.num_seconds();
    let sign = if seconds < 0 { "-" } else { "" };
    let seconds = seconds.abs();

    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let secs = seconds % 60;

    let body = if days > 0 {
        format!("{}d {}h {}m {}s", days, hours, minutes, secs)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    };

    format!("{sign}{body}")
}

/// Format a duration using compact units (e.g. "5s", "3m", "4h", "2d").
pub fn format_duration_compact(duration: ChronoDuration) -> String {
    let seconds = duration.num_seconds();
    let sign = if seconds < 0 { "-" } else { "" };
    let seconds = seconds.abs();

    let body = if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3_600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3_600)
    } else if seconds < 31_536_000 {
        format!("{}d", seconds / 86_400)
    } else {
        format!("{}y", seconds / 31_536_000)
    };

    format!("{sign}{body}")
}

/// Convenience wrapper for formatting std::time::Duration using compact units.
pub fn format_std_duration(duration: StdDuration) -> String {
    match ChronoDuration::from_std(duration) {
        Ok(d) => format_duration_compact(d),
        Err(_) => format!("{}s", duration.as_secs()),
    }
}

pub fn format_std_duration_ms(duration: StdDuration) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    if ms < 100.0 {
        format!("{:.3}ms", ms)
    } else if ms < 1000.0 {
        format!("{:.0}ms", ms)
    } else {
        format!("{:.0}ms", ms)
    }
}

/// Format a bytes-per-second rate as a human-readable string (e.g. "1.23 MB/sec").
/// Takes bytes per second as a floating point number.
pub fn format_bytes_per_sec(bytes_per_sec: f64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];

    let mut size = bytes_per_sec;
    let mut unit_idx = 0usize;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{:.1} {}/sec", bytes_per_sec, UNITS[unit_idx])
    } else {
        format!("{:.1} {}/sec", size, UNITS[unit_idx])
    }
}
