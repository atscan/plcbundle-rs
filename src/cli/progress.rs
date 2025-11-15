use std::io::{self, IsTerminal, Write};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Progress bar for displaying operation progress
pub struct ProgressBar {
    total: usize,
    current: Arc<Mutex<usize>>,
    current_bytes: Arc<Mutex<u64>>,
    start_time: Instant,
    last_print: Arc<Mutex<Instant>>,
    show_bytes: bool,
    width: usize,
    is_tty: bool,
    last_line_print: Arc<Mutex<Instant>>,
}

impl ProgressBar {
    /// Create a new progress bar
    pub fn new(total: usize) -> Self {
        let is_tty = io::stderr().is_terminal();
        Self {
            total,
            current: Arc::new(Mutex::new(0)),
            current_bytes: Arc::new(Mutex::new(0)),
            start_time: Instant::now(),
            last_print: Arc::new(Mutex::new(Instant::now())),
            last_line_print: Arc::new(Mutex::new(Instant::now())),
            show_bytes: false,
            width: 40,
            is_tty,
        }
    }

    /// Create a progress bar with byte tracking
    pub fn with_bytes(total: usize, _total_bytes: u64) -> Self {
        let is_tty = io::stderr().is_terminal();
        Self {
            total,
            current: Arc::new(Mutex::new(0)),
            current_bytes: Arc::new(Mutex::new(0)),
            start_time: Instant::now(),
            last_print: Arc::new(Mutex::new(Instant::now())),
            last_line_print: Arc::new(Mutex::new(Instant::now())),
            show_bytes: true,
            width: 40,
            is_tty,
        }
    }

    /// Create a progress bar with auto-estimated bytes
    #[allow(dead_code)]
    pub fn with_bytes_auto(total: usize, _avg_bytes_per_item: u64) -> Self {
        let is_tty = io::stderr().is_terminal();
        Self {
            total,
            current: Arc::new(Mutex::new(0)),
            current_bytes: Arc::new(Mutex::new(0)),
            start_time: Instant::now(),
            last_print: Arc::new(Mutex::new(Instant::now())),
            last_line_print: Arc::new(Mutex::new(Instant::now())),
            show_bytes: true,
            width: 40,
            is_tty,
        }
    }

    /// Set current progress
    pub fn set(&self, current: usize) {
        let mut cur = self.current.lock().unwrap();
        *cur = current;
        drop(cur);
        self.print();
    }

    /// Set progress with exact byte tracking
    pub fn set_with_bytes(&self, current: usize, bytes: u64) {
        let mut cur = self.current.lock().unwrap();
        *cur = current;
        drop(cur);
        let mut bytes_guard = self.current_bytes.lock().unwrap();
        *bytes_guard = bytes;
        drop(bytes_guard);
        self.print();
    }

    /// Add bytes to current progress
    #[allow(dead_code)]
    pub fn add_bytes(&self, increment: usize, bytes: u64) {
        let mut cur = self.current.lock().unwrap();
        *cur += increment;
        drop(cur);
        let mut bytes_guard = self.current_bytes.lock().unwrap();
        *bytes_guard += bytes;
        drop(bytes_guard);
        self.print();
    }

    /// Finish the progress bar
    pub fn finish(&self) {
        let mut cur = self.current.lock().unwrap();
        *cur = self.total;
        drop(cur);
        self.print();
        eprintln!();
    }

    fn print(&self) {
        let current = *self.current.lock().unwrap();
        let current_bytes = *self.current_bytes.lock().unwrap();

        if self.is_tty {
            self.print_bar(current, current_bytes);
        } else {
            self.print_line(current, current_bytes);
        }
    }

    fn print_bar(&self, current: usize, current_bytes: u64) {
        let mut last_print = self.last_print.lock().unwrap();

        // Throttle updates (max 10 per second)
        if last_print.elapsed() < Duration::from_millis(100) && current < self.total {
            return;
        }
        *last_print = Instant::now();
        drop(last_print);

        let percent = if self.total > 0 {
            (current as f64 / self.total as f64) * 100.0
        } else {
            0.0
        };

        let filled = if self.total > 0 {
            let calc = (self.width as f64 * current as f64) / self.total as f64;
            (calc as usize).min(self.width)
        } else {
            0
        };

        let bar = "█".repeat(filled) + &"░".repeat(self.width - filled);

        let elapsed = self.start_time.elapsed();
        let speed = if elapsed.as_secs_f64() > 0.0 {
            current as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        let remaining = self.total.saturating_sub(current);
        let eta = if speed > 0.0 && remaining > 0 {
            Duration::from_secs_f64(remaining as f64 / speed)
        } else {
            Duration::ZERO
        };

        let is_complete = current >= self.total;

        if self.show_bytes && current_bytes > 0 {
            let mb_processed = current_bytes as f64 / 1_000_000.0;
            let mb_per_sec = if elapsed.as_secs_f64() > 0.0 {
                mb_processed / elapsed.as_secs_f64()
            } else {
                0.0
            };

            if is_complete {
                eprint!(
                    "\r  [{}] {:6.2}% | {}/{} | {:.1}/s | {:.1} MB/s | Done    ",
                    bar, percent, current, self.total, speed, mb_per_sec
                );
            } else {
                eprint!(
                    "\r  [{}] {:6.2}% | {}/{} | {:.1}/s | {:.1} MB/s | ETA: {} ",
                    bar,
                    percent,
                    current,
                    self.total,
                    speed,
                    mb_per_sec,
                    format_eta(eta)
                );
            }
        } else {
            if is_complete {
                eprint!(
                    "\r  [{}] {:6.2}% | {}/{} | {:.1}/s | Done    ",
                    bar, percent, current, self.total, speed
                );
            } else {
                eprint!(
                    "\r  [{}] {:6.2}% | {}/{} | {:.1}/s | ETA: {} ",
                    bar,
                    percent,
                    current,
                    self.total,
                    speed,
                    format_eta(eta)
                );
            }
        }
        let _ = io::stderr().flush();
    }

    fn print_line(&self, current: usize, current_bytes: u64) {
        let mut last_line_print = self.last_line_print.lock().unwrap();

        // Throttle line updates (max 1 per second for logging, but always show completion)
        let is_complete = current >= self.total;
        if !is_complete && last_line_print.elapsed() < Duration::from_secs(1) {
            return;
        }
        *last_line_print = Instant::now();
        drop(last_line_print);

        let percent = if self.total > 0 {
            (current as f64 / self.total as f64) * 100.0
        } else {
            0.0
        };

        let elapsed = self.start_time.elapsed();
        let speed = if elapsed.as_secs_f64() > 0.0 {
            current as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        if self.show_bytes && current_bytes > 0 {
            let mb_processed = current_bytes as f64 / 1_000_000.0;
            let mb_per_sec = if elapsed.as_secs_f64() > 0.0 {
                mb_processed / elapsed.as_secs_f64()
            } else {
                0.0
            };
            eprintln!(
                "Progress: {:.1}% ({}/{} | {:.1}/s | {:.1} MB/s)",
                percent, current, self.total, speed, mb_per_sec
            );
        } else {
            eprintln!(
                "Progress: {:.1}% ({}/{} | {:.1}/s)",
                percent, current, self.total, speed
            );
        }
    }
}

fn format_eta(duration: Duration) -> String {
    if duration.is_zero() {
        return "0s".to_string();
    }

    let secs = duration.as_secs();
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        let mins = secs / 60;
        let secs_remainder = secs % 60;
        format!("{}m {}s", mins, secs_remainder)
    } else {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    }
}
