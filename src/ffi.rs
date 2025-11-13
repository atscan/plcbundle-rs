use crate::*;
use crate::processor::Processor;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::PathBuf;
use std::sync::Arc;

pub struct COutputHandler {
    callback: extern "C" fn(*const c_char) -> i32,
}

impl OutputHandler for COutputHandler {
    fn write_batch(&self, batch: &str) -> anyhow::Result<()> {
        let c_str = CString::new(batch)?;
        let result = (self.callback)(c_str.as_ptr());
        if result != 0 {
            anyhow::bail!("Callback returned error: {}", result);
        }
        Ok(())
    }
}

#[repr(C)]
pub struct CStats {
    pub operations: usize,
    pub matches: usize,
    pub total_bytes: u64,
    pub matched_bytes: u64,
}

impl From<Stats> for CStats {
    fn from(s: Stats) -> Self {
        CStats {
            operations: s.operations,
            matches: s.matches,
            total_bytes: s.total_bytes,
            matched_bytes: s.matched_bytes,
        }
    }
}

#[repr(C)]
pub struct BundleqProcessor {
    processor: Box<Processor>,
}

/// Create a new bundle processor
/// Returns null on error
#[no_mangle]
pub extern "C" fn bundleq_new(
    bundle_dir: *const c_char,
    query: *const c_char,
    simple_mode: bool,
    num_threads: usize,
    batch_size: usize,
) -> *mut BundleqProcessor {
    let bundle_dir = unsafe {
        if bundle_dir.is_null() {
            return std::ptr::null_mut();
        }
        match CStr::from_ptr(bundle_dir).to_str() {
            Ok(s) => PathBuf::from(s),
            Err(_) => return std::ptr::null_mut(),
        }
    };

    let query = unsafe {
        if query.is_null() {
            return std::ptr::null_mut();
        }
        match CStr::from_ptr(query).to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        }
    };

    let query_mode = if simple_mode {
        QueryMode::Simple
    } else {
        QueryMode::JmesPath
    };

    let options = OptionsBuilder::new()
        .directory(bundle_dir)
        .query(query)
        .query_mode(query_mode)
        .num_threads(num_threads)
        .batch_size(batch_size)
        .build();

    match Processor::new(options) {
        Ok(processor) => Box::into_raw(Box::new(BundleqProcessor {
            processor: Box::new(processor),
        })),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Parse bundle range specification
/// Returns array of bundle numbers and sets out_len
/// Caller must free the returned pointer with bundleq_free_bundle_list
#[no_mangle]
pub extern "C" fn bundleq_parse_bundles(
    spec: *const c_char,
    max_bundle: u32,
    out_len: *mut usize,
) -> *mut u32 {
    let spec = unsafe {
        if spec.is_null() {
            return std::ptr::null_mut();
        }
        match CStr::from_ptr(spec).to_str() {
            Ok(s) => s,
            Err(_) => return std::ptr::null_mut(),
        }
    };

    match parse_bundle_range(spec, max_bundle) {
        Ok(bundles) => {
            unsafe { *out_len = bundles.len() };
            let mut boxed = bundles.into_boxed_slice();
            let ptr = boxed.as_mut_ptr();
            std::mem::forget(boxed);
            ptr
        }
        Err(_) => std::ptr::null_mut(),
    }
}

/// Free bundle list
#[no_mangle]
pub extern "C" fn bundleq_free_bundle_list(ptr: *mut u32, len: usize) {
    if !ptr.is_null() {
        unsafe {
            let _ = Box::from_raw(std::slice::from_raw_parts_mut(ptr, len));
        }
    }
}

/// Process bundles
/// callback receives batches of output lines
/// Returns 0 on success, -1 on error
#[no_mangle]
pub extern "C" fn bundleq_process(
    processor: *mut BundleqProcessor,
    bundle_numbers: *const u32,
    bundle_count: usize,
    callback: extern "C" fn(*const c_char) -> i32,
    out_stats: *mut CStats,
) -> i32 {
    if processor.is_null() || bundle_numbers.is_null() {
        return -1;
    }

    let processor = unsafe { &*processor };
    let bundles = unsafe { std::slice::from_raw_parts(bundle_numbers, bundle_count) };

    let handler = Arc::new(COutputHandler { callback });

    match processor.processor.process(bundles, handler, None::<fn(usize, &Stats)>) {
        Ok(stats) => {
            if !out_stats.is_null() {
                unsafe { *out_stats = stats.into() };
            }
            0
        }
        Err(_) => -1,
    }
}

/// Load index and return last bundle number
/// Returns 0 on error
#[no_mangle]
pub extern "C" fn bundleq_get_last_bundle(bundle_dir: *const c_char) -> u32 {
    let bundle_dir = unsafe {
        if bundle_dir.is_null() {
            return 0;
        }
        match CStr::from_ptr(bundle_dir).to_str() {
            Ok(s) => PathBuf::from(s),
            Err(_) => return 0,
        }
    };

    match Index::load(&bundle_dir) {
        Ok(index) => index.last_bundle,
        Err(_) => 0,
    }
}

/// Free the processor
#[no_mangle]
pub extern "C" fn bundleq_free(processor: *mut BundleqProcessor) {
    if !processor.is_null() {
        unsafe {
            let _ = Box::from_raw(processor);
        }
    }
}

/// Get last error message
#[no_mangle]
pub extern "C" fn bundleq_last_error() -> *const c_char {
    // You could implement thread-local error storage here
    std::ptr::null()
}
