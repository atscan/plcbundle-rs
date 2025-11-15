use indicatif::{ProgressBar as IndicatifProgressBar, ProgressStyle};
use std::sync::Arc;
use std::sync::Mutex;

/// Progress bar wrapper around indicatif for displaying operation progress
pub struct ProgressBar {
    pb: IndicatifProgressBar,
    show_bytes: bool,
    current_bytes: Arc<Mutex<u64>>,
    total_bytes: u64,
}

impl ProgressBar {
    /// Create a new progress bar
    pub fn new(total: usize) -> Self {
        let pb = IndicatifProgressBar::new(total as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} | {per_sec:.1}/s | {msg} | ETA: {eta}")
                .unwrap()
                .progress_chars("█▓▒░ "),
        );
        
        Self {
            pb,
            show_bytes: false,
            current_bytes: Arc::new(Mutex::new(0)),
            total_bytes: 0,
        }
    }

    /// Create a progress bar with byte tracking
    pub fn with_bytes(total: usize, total_bytes: u64) -> Self {
        let pb = IndicatifProgressBar::new(total as u64);
        
        // Custom template that shows MB/s calculated from our tracked bytes
        // We'll calculate MB/s manually and include it in the message
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} | {msg} | ETA: {eta}")
                .unwrap()
                .progress_chars("█▓▒░ "),
        );
        
        Self {
            pb,
            show_bytes: true,
            current_bytes: Arc::new(Mutex::new(0)),
            total_bytes,
        }
    }

    /// Set current progress
    pub fn set(&self, current: usize) {
        self.pb.set_position(current as u64);
    }

    /// Set progress with exact byte tracking
    pub fn set_with_bytes(&self, current: usize, bytes: u64) {
        self.pb.set_position(current as u64);
        let mut bytes_guard = self.current_bytes.lock().unwrap();
        *bytes_guard = bytes;
        let current_msg = self.pb.message().to_string();
        drop(bytes_guard);
        
        // Update message to include MB/s, preserving user message if present
        let elapsed = self.pb.elapsed().as_secs_f64();
        let bytes_guard = self.current_bytes.lock().unwrap();
        let bytes = *bytes_guard;
        drop(bytes_guard);
        
        let mb_per_sec = if elapsed > 0.0 {
            (bytes as f64 / 1_000_000.0) / elapsed
        } else {
            0.0
        };
        
        // Extract user message (everything after " | " if present)
        let user_msg = if let Some(pos) = current_msg.find(" | ") {
            &current_msg[pos + 3..]
        } else {
            ""
        };
        
        let new_msg = if user_msg.is_empty() {
            format!("{:.1} MB/s", mb_per_sec)
        } else {
            format!("{:.1} MB/s | {}", mb_per_sec, user_msg)
        };
        self.pb.set_message(new_msg);
    }

    /// Add bytes to current progress
    #[allow(dead_code)]
    pub fn add_bytes(&self, increment: usize, bytes: u64) {
        self.pb.inc(increment as u64);
        let mut bytes_guard = self.current_bytes.lock().unwrap();
        *bytes_guard += bytes;
        let current_msg = self.pb.message().to_string();
        drop(bytes_guard);
        
        // Update message to include MB/s, preserving user message if present
        let elapsed = self.pb.elapsed().as_secs_f64();
        let bytes_guard = self.current_bytes.lock().unwrap();
        let bytes = *bytes_guard;
        drop(bytes_guard);
        
        let mb_per_sec = if elapsed > 0.0 {
            (bytes as f64 / 1_000_000.0) / elapsed
        } else {
            0.0
        };
        
        // Extract user message (everything after " | " if present)
        let user_msg = if let Some(pos) = current_msg.find(" | ") {
            &current_msg[pos + 3..]
        } else {
            ""
        };
        
        let new_msg = if user_msg.is_empty() {
            format!("{:.1} MB/s", mb_per_sec)
        } else {
            format!("{:.1} MB/s | {}", mb_per_sec, user_msg)
        };
        self.pb.set_message(new_msg);
    }


    pub fn set_message<S: Into<String>>(&self, msg: S) {
        let msg_str: String = msg.into();
        if self.show_bytes {
            // If showing bytes, prepend MB/s to the message
            let elapsed = self.pb.elapsed().as_secs_f64();
            let bytes_guard = self.current_bytes.lock().unwrap();
            let bytes = *bytes_guard;
            drop(bytes_guard);
            
            let mb_per_sec = if elapsed > 0.0 {
                (bytes as f64 / 1_000_000.0) / elapsed
            } else {
                0.0
            };
            
            let new_msg = if msg_str.is_empty() {
                format!("{:.1} MB/s", mb_per_sec)
            } else {
                format!("{:.1} MB/s | {}", mb_per_sec, msg_str)
            };
            self.pb.set_message(new_msg);
        } else {
            self.pb.set_message(msg_str);
        }
    }

    /// Finish the progress bar
    pub fn finish(&self) {
        self.pb.finish();
    }
}
