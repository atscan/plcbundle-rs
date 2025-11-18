use plcbundle::format::format_bytes_per_sec;
use indicatif::{ProgressBar as IndicatifProgressBar, ProgressStyle};
use std::sync::Arc;
use std::sync::Mutex;

/// Progress bar wrapper around indicatif for displaying operation progress
pub struct ProgressBar {
    pb: IndicatifProgressBar,
    show_bytes: bool,
    current_bytes: Arc<Mutex<u64>>,
}

impl ProgressBar {
    /// Create a new progress bar
    /// This is used for Stage 2 (shard consolidation) where per_sec isn't meaningful
    /// and we don't want indicatif to color things red based on slow progress detection
    pub fn new(total: usize) -> Self {
        let pb = IndicatifProgressBar::new(total as u64);
        // Remove {per_sec} and {msg} from template since they're not meaningful for shard consolidation
        // and indicatif may color it red when it detects slow progress
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:.cyan}/{len:.cyan} | ETA: {eta}")
                .unwrap()
                .progress_chars("█▓▒░ "),
        );
        
        Self {
            pb,
            show_bytes: false,
            current_bytes: Arc::new(Mutex::new(0)),
        }
    }

    /// Create a progress bar with byte tracking
    pub fn with_bytes(total: usize, _total_bytes: u64) -> Self {
        let pb = IndicatifProgressBar::new(total as u64);
        
        // Custom template that shows data rate calculated from our tracked bytes
        // We'll calculate bytes/sec manually and include it in the message
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
        
        // Update message to include data rate, preserving user message if present
        let elapsed = self.pb.elapsed().as_secs_f64();
        let bytes_guard = self.current_bytes.lock().unwrap();
        let bytes = *bytes_guard;
        drop(bytes_guard);
        
        let bytes_per_sec = if elapsed > 0.0 {
            bytes as f64 / elapsed
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
            format_bytes_per_sec(bytes_per_sec)
        } else {
            format!("{} | {}", format_bytes_per_sec(bytes_per_sec), user_msg)
        };
        self.pb.set_message(new_msg);
    }

    pub fn set_message<S: Into<String>>(&self, msg: S) {
        let msg_str: String = msg.into();
        if self.show_bytes {
            // If showing bytes, prepend data rate to the message
            let elapsed = self.pb.elapsed().as_secs_f64();
            let bytes_guard = self.current_bytes.lock().unwrap();
            let bytes = *bytes_guard;
            drop(bytes_guard);
            
            let bytes_per_sec = if elapsed > 0.0 {
                bytes as f64 / elapsed
            } else {
                0.0
            };
            
            let new_msg = if msg_str.is_empty() {
                format_bytes_per_sec(bytes_per_sec)
            } else {
                format!("{} | {}", format_bytes_per_sec(bytes_per_sec), msg_str)
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

/// Manages a two-stage progress bar for DID index building
/// Stage 1: Processing bundles (with byte tracking)
/// Stage 2: Consolidating shards (simple count)
pub struct TwoStageProgress {
    stage1_progress: Arc<Mutex<Option<ProgressBar>>>,
    stage2_progress: Arc<Mutex<Option<ProgressBar>>>,
    stage2_started: Arc<Mutex<bool>>,
    stage1_finished: Arc<Mutex<bool>>,
    interrupted: Arc<std::sync::atomic::AtomicBool>,
    last_bundle: u32,
}

impl TwoStageProgress {
    /// Create a new two-stage progress bar
    /// 
    /// # Arguments
    /// * `last_bundle` - Total number of bundles to process in stage 1
    /// * `total_bytes` - Total uncompressed bytes to process in stage 1
    pub fn new(last_bundle: u32, total_bytes: u64) -> Self {
        use std::sync::atomic::AtomicBool;
        
        Self {
            stage1_progress: Arc::new(Mutex::new(Some(ProgressBar::with_bytes(last_bundle as usize, total_bytes)))),
            stage2_progress: Arc::new(Mutex::new(None)),
            stage2_started: Arc::new(Mutex::new(false)),
            stage1_finished: Arc::new(Mutex::new(false)),
            interrupted: Arc::new(AtomicBool::new(false)),
            last_bundle,
        }
    }

    /// Get the interrupted flag (for passing to build_did_index)
    pub fn interrupted(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.interrupted.clone()
    }

    /// Create a callback function for build_did_index (signature: Fn(u32, u32, u64, u64))
    pub fn callback_for_build_did_index(&self) -> impl Fn(u32, u32, u64, u64) + Send + Sync + 'static {
        let progress = self.clone_for_callback();
        move |current, total, bytes_processed, _total_bytes| {
            progress.update_progress(current, total, bytes_processed);
        }
    }

    /// Clone the necessary parts for use in callbacks
    fn clone_for_callback(&self) -> TwoStageProgressCallback {
        TwoStageProgressCallback {
            stage1_progress: self.stage1_progress.clone(),
            stage2_progress: self.stage2_progress.clone(),
            stage2_started: self.stage2_started.clone(),
            stage1_finished: self.stage1_finished.clone(),
            interrupted: self.interrupted.clone(),
            last_bundle: self.last_bundle,
        }
    }
}

impl TwoStageProgress {
    /// Finish any remaining progress bars (call this after build completes)
    pub fn finish(&self) {
        if let Some(pb) = self.stage1_progress.lock().unwrap().take() {
            pb.finish();
        }
        if let Some(pb) = self.stage2_progress.lock().unwrap().take() {
            pb.finish();
        }
    }
}

/// Internal struct for callback closures (avoids lifetime issues)
struct TwoStageProgressCallback {
    stage1_progress: Arc<Mutex<Option<ProgressBar>>>,
    stage2_progress: Arc<Mutex<Option<ProgressBar>>>,
    stage2_started: Arc<Mutex<bool>>,
    stage1_finished: Arc<Mutex<bool>>,
    interrupted: Arc<std::sync::atomic::AtomicBool>,
    last_bundle: u32,
}

impl TwoStageProgressCallback {
    fn update_progress(&self, current: u32, total: u32, bytes_processed: u64) {
        use std::sync::atomic::Ordering;
        
        // Stop updating progress bar if interrupted
        if self.interrupted.load(Ordering::Relaxed) {
            return;
        }
        
        // Detect stage change: if total changes from bundle count to shard count (256), we're in stage 2
        let is_stage_2 = total == 256 && current <= 256;
        
        if is_stage_2 {
            // Check if this is the first time we're entering stage 2
            let mut started = self.stage2_started.lock().unwrap();
            if !*started {
                *started = true;
                drop(started);
                
                // Finish Stage 1 progress bar if not already finished
                // (did_index.rs already printed empty line + Stage 2 header before this callback)
                let mut finished = self.stage1_finished.lock().unwrap();
                if !*finished {
                    *finished = true;
                    drop(finished);
                    if let Some(pb) = self.stage1_progress.lock().unwrap().take() {
                        pb.finish();
                    }
                    // Add extra newline after Stage 1 (did_index.rs already printed one before Stage 2 header)
                    eprintln!();
                }
                
                // Create new simple progress bar for Stage 2 (256 shards, no byte tracking)
                let mut stage2_pb = self.stage2_progress.lock().unwrap();
                *stage2_pb = Some(ProgressBar::new(256));
            }
            
            // Update Stage 2 progress bar (pos/len already shows the count, no message needed)
            let mut stage2_pb_guard = self.stage2_progress.lock().unwrap();
            if let Some(ref pb) = *stage2_pb_guard {
                pb.set(current as usize);
                // Don't set message - progress bar template will show pos/len without extra message
                
                // Finish progress bar when stage 2 completes (256/256)
                if current == 256
                    && let Some(pb) = stage2_pb_guard.take() {
                        pb.finish();
                }
            }
        } else {
            // Stage 1: use byte tracking, no stage message in progress bar
            // Check if Stage 1 is complete (current == total and total == last_bundle)
            if current == total && total == self.last_bundle {
                let mut finished = self.stage1_finished.lock().unwrap();
                if !*finished {
                    *finished = true;
                    drop(finished);
                    // Finish Stage 1 progress bar and add extra newline
                    if let Some(pb) = self.stage1_progress.lock().unwrap().take() {
                        pb.finish();
                    }
                    eprintln!();
                }
            } else if let Some(ref pb) = *self.stage1_progress.lock().unwrap() {
                pb.set_with_bytes(current as usize, bytes_processed);
                // Don't set stage message - just show bytes/sec
            }
        }
    }
}
