//! Formatting helpers for bytes, durations, rates, and numbers used across CLI/server/library components
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

// Internal helpers for duration formatting

/// Format seconds as verbose units (e.g. "2d 3h 10m 5s").
fn format_seconds_verbose(seconds: u64) -> String {
    let days = seconds / 86_400;
    let hours = (seconds % 86_400) / 3_600;
    let minutes = (seconds % 3_600) / 60;
    let secs = seconds % 60;

    if days > 0 {
        format!("{}d {}h {}m {}s", days, hours, minutes, secs)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}

/// Format seconds as compact units (e.g. "5s", "3m", "4h", "2d").
fn format_seconds_compact(seconds: u64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3_600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3_600)
    } else if seconds < 31_536_000 {
        format!("{}d", seconds / 86_400)
    } else {
        format!("{}y", seconds / 31_536_000)
    }
}

// Public duration formatting functions

/// Format a chrono duration using verbose units (e.g. "2d 3h 10m 5s").
pub fn format_duration_verbose(duration: ChronoDuration) -> String {
    let seconds = duration.num_seconds();
    let sign = if seconds < 0 { "-" } else { "" };
    let abs_seconds = seconds.unsigned_abs();
    format!("{}{}", sign, format_seconds_verbose(abs_seconds))
}

/// Format a duration using compact units (e.g. "5s", "3m", "4h", "2d").
pub fn format_duration_compact(duration: ChronoDuration) -> String {
    let seconds = duration.num_seconds();
    let sign = if seconds < 0 { "-" } else { "" };
    let abs_seconds = seconds.unsigned_abs();
    format!("{}{}", sign, format_seconds_compact(abs_seconds))
}

/// Format a std::time::Duration using compact units (e.g. "5s", "3m", "4h", "2d").
pub fn format_std_duration(duration: StdDuration) -> String {
    format_seconds_compact(duration.as_secs())
}

/// Format a std::time::Duration using verbose units (e.g. "2d 3h 10m 5s").
pub fn format_std_duration_verbose(duration: StdDuration) -> String {
    format_seconds_verbose(duration.as_secs())
}

/// Format a duration in milliseconds (e.g. "123ms", "1.234ms").
pub fn format_std_duration_ms(duration: StdDuration) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    if ms < 100.0 {
        format!("{:.3}ms", ms)
    } else {
        format!("{:.0}ms", ms)
    }
}

/// Format a duration with auto-scaling units (μs/ms for < 1s, then s/m/h for longer).
pub fn format_std_duration_auto(duration: StdDuration) -> String {
    let secs = duration.as_secs_f64();
    if secs < 0.001 {
        format!("{:.0}μs", secs * 1_000_000.0)
    } else if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.1}s", secs)
    } else {
        // Use HumanDuration for longer durations (handles m, h, etc.)
        use indicatif::HumanDuration;
        HumanDuration(duration).to_string()
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration as ChronoDuration;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
    }

    #[test]
    fn test_format_bytes_compact() {
        assert_eq!(format_bytes_compact(0), "0");
        assert_eq!(format_bytes_compact(512), "512");
        assert_eq!(format_bytes_compact(1024), "1K");
        assert_eq!(format_bytes_compact(1536), "1.5K");
        assert_eq!(format_bytes_compact(1024 * 1024), "1M");
        assert_eq!(format_bytes_compact(1024 * 1024 * 1024), "1G");
        assert_eq!(format_bytes_compact(1536 * 1024), "1.5M");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(123), "123");
        assert_eq!(format_number(1234), "1,234");
        assert_eq!(format_number(12345), "12,345");
        assert_eq!(format_number(123456), "123,456");
        assert_eq!(format_number(1234567), "1,234,567");
        assert_eq!(format_number(12345678), "12,345,678");
    }

    #[test]
    fn test_format_std_duration() {
        assert_eq!(format_std_duration(StdDuration::from_secs(0)), "0s");
        assert_eq!(format_std_duration(StdDuration::from_secs(30)), "30s");
        assert_eq!(format_std_duration(StdDuration::from_secs(60)), "1m");
        assert_eq!(format_std_duration(StdDuration::from_secs(3600)), "1h");
        assert_eq!(format_std_duration(StdDuration::from_secs(86400)), "1d");
        assert_eq!(format_std_duration(StdDuration::from_secs(31536000)), "1y");
    }

    #[test]
    fn test_format_std_duration_verbose() {
        assert_eq!(format_std_duration_verbose(StdDuration::from_secs(0)), "0s");
        assert_eq!(format_std_duration_verbose(StdDuration::from_secs(5)), "5s");
        assert_eq!(format_std_duration_verbose(StdDuration::from_secs(65)), "1m 5s");
        assert_eq!(format_std_duration_verbose(StdDuration::from_secs(3665)), "1h 1m 5s");
        assert_eq!(format_std_duration_verbose(StdDuration::from_secs(90065)), "1d 1h 1m 5s");
    }

    #[test]
    fn test_format_std_duration_ms() {
        assert_eq!(format_std_duration_ms(StdDuration::from_millis(0)), "0.000ms");
        assert_eq!(format_std_duration_ms(StdDuration::from_millis(50)), "50.000ms");
        assert_eq!(format_std_duration_ms(StdDuration::from_millis(100)), "100ms");
        assert_eq!(format_std_duration_ms(StdDuration::from_millis(1234)), "1234ms");
    }

    #[test]
    fn test_format_duration_verbose() {
        assert_eq!(format_duration_verbose(ChronoDuration::seconds(0)), "0s");
        assert_eq!(format_duration_verbose(ChronoDuration::seconds(65)), "1m 5s");
        assert_eq!(format_duration_verbose(ChronoDuration::seconds(-65)), "-1m 5s");
    }

    #[test]
    fn test_format_duration_compact() {
        assert_eq!(format_duration_compact(ChronoDuration::seconds(0)), "0s");
        assert_eq!(format_duration_compact(ChronoDuration::seconds(30)), "30s");
        assert_eq!(format_duration_compact(ChronoDuration::seconds(60)), "1m");
        assert_eq!(format_duration_compact(ChronoDuration::seconds(-60)), "-1m");
    }

    #[test]
    fn test_format_bytes_per_sec() {
        assert_eq!(format_bytes_per_sec(0.0), "0.0 B/sec");
        assert_eq!(format_bytes_per_sec(512.0), "512.0 B/sec");
        assert_eq!(format_bytes_per_sec(1024.0), "1.0 KB/sec");
        assert_eq!(format_bytes_per_sec(1536.0), "1.5 KB/sec");
        assert_eq!(format_bytes_per_sec(1024.0 * 1024.0), "1.0 MB/sec");
    }
}
