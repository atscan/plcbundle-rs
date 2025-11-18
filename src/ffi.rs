//! C-compatible FFI surface exposing BundleManager and related types for integration from other languages
use crate::constants;
use crate::manager::*;
use crate::operations::*;
use sonic_rs::JsonValueTrait;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::path::PathBuf;
use std::sync::Arc;

// ============================================================================
// C-compatible types
// ============================================================================

#[repr(C)]
pub struct CBundleManager {
    manager: Arc<BundleManager>,
}

#[repr(C)]
pub struct CLoadOptions {
    pub verify_hash: bool,
    pub decompress: bool,
    pub cache: bool,
}

#[repr(C)]
pub struct CLoadResult {
    pub success: bool,
    pub operation_count: u32,
    pub size_bytes: u64,
    pub error_msg: *mut c_char,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct COperation {
    pub did: *mut c_char,
    pub op_type: *mut c_char,
    pub cid: *mut c_char,
    pub nullified: bool,
    pub created_at: *mut c_char,
    pub json: *mut c_char,
}

#[repr(C)]
pub struct COperationRequest {
    pub bundle_num: u32,
    pub operation_idx: usize,
}

#[repr(C)]
pub struct CVerifyResult {
    pub valid: bool,
    pub hash_match: bool,
    pub compression_ok: bool,
    pub error_msg: *mut c_char,
}

#[repr(C)]
pub struct CBundleInfo {
    pub bundle_number: u32,
    pub operation_count: u32,
    pub did_count: u32,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub start_time: *mut c_char,
    pub end_time: *mut c_char,
    pub hash: *mut c_char,
}

#[repr(C)]
pub struct CManagerStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub bytes_read: u64,
    pub operations_processed: u64,
}

#[repr(C)]
pub struct CDIDIndexStats {
    pub total_dids: usize,
    pub total_operations: usize,
    pub index_size_bytes: usize,
}

#[repr(C)]
pub struct CRebuildStats {
    pub bundles_processed: u32,
    pub operations_indexed: u64,
    pub unique_dids: usize,
    pub duration_ms: u64,
}

#[repr(C)]
pub struct CExportSpec {
    pub bundle_start: u32,
    pub bundle_end: u32,
    pub export_all: bool,
    pub format: u8,                     // 0=jsonl, 1=json, 2=csv
    pub count_limit: u64,               // 0 = no limit
    pub after_timestamp: *const c_char, // NULL = no filter
    pub did_filter: *const c_char,      // NULL = no filter
    pub op_type_filter: *const c_char,  // NULL = no filter
}

#[repr(C)]
pub struct CExportStats {
    pub records_written: u64,
    pub bytes_written: u64,
}

// ============================================================================
// BundleManager lifecycle
// ============================================================================

/// # Safety
///
/// The caller must ensure `bundle_dir` is a valid, NUL-terminated C string
/// pointer. The returned pointer is owned by the caller and must be freed
/// with `bundle_manager_free` when no longer needed. Passing a null or
/// invalid pointer is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_new(bundle_dir: *const c_char) -> *mut CBundleManager {
    let bundle_dir = unsafe {
        if bundle_dir.is_null() {
            return std::ptr::null_mut();
        }
        match CStr::from_ptr(bundle_dir).to_str() {
            Ok(s) => PathBuf::from(s),
            Err(_) => return std::ptr::null_mut(),
        }
    };

    match BundleManager::new(bundle_dir, ()) {
        Ok(manager) => Box::into_raw(Box::new(CBundleManager {
            manager: Arc::new(manager),
        })),
        Err(_) => std::ptr::null_mut(),
    }
}

/// # Safety
///
/// The caller must ensure `manager` is a pointer previously returned by
/// `bundle_manager_new` and not already freed. Passing invalid or dangling
/// pointers is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_free(manager: *mut CBundleManager) {
    if !manager.is_null() {
        unsafe {
            let _ = Box::from_raw(manager);
        }
    }
}

// ============================================================================
// Smart Loading
// ============================================================================

/// # Safety
///
/// The `manager` pointer must be valid. `options` may be NULL to use defaults.
/// `out_result` must be a valid writable pointer to a `CLoadResult` struct.
/// Strings passed to this API must be NUL-terminated. Violating these
/// requirements is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_load_bundle(
    manager: *const CBundleManager,
    bundle_num: u32,
    options: *const CLoadOptions,
    out_result: *mut CLoadResult,
) -> i32 {
    if manager.is_null() || out_result.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };

    let load_opts = if options.is_null() {
        LoadOptions::default()
    } else {
        let opts = unsafe { &*options };
        LoadOptions {
            cache: opts.cache,
            decompress: opts.decompress,
            filter: None,
            limit: None,
        }
    };

    match manager.manager.load_bundle(bundle_num, load_opts) {
        Ok(result) => {
            unsafe {
                (*out_result).success = true;
                (*out_result).operation_count = result.operations.len() as u32;
                (*out_result).size_bytes = 0; // TODO: calculate actual size
                (*out_result).error_msg = std::ptr::null_mut();
            }
            0
        }
        Err(e) => {
            let err_msg = CString::new(e.to_string()).unwrap();
            unsafe {
                (*out_result).success = false;
                (*out_result).operation_count = 0;
                (*out_result).size_bytes = 0;
                (*out_result).error_msg = err_msg.into_raw();
            }
            -1
        }
    }
}

// ============================================================================
// Batch Operations
// ============================================================================

/// # Safety
///
/// The `manager` pointer must be valid. `requests` must point to `count`
/// valid `COperationRequest` items. `out_operations` and `out_count` must
/// be valid writable pointers. Returned `COperation` arrays must be freed
/// by the caller using `bundle_manager_free_operations` if applicable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_get_operations_batch(
    manager: *const CBundleManager,
    requests: *const COperationRequest,
    count: usize,
    out_operations: *mut *mut COperation,
    out_count: *mut usize,
) -> i32 {
    if manager.is_null() || requests.is_null() || out_operations.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };
    let requests_slice = unsafe { std::slice::from_raw_parts(requests, count) };

    let mut op_requests = Vec::new();
    for req in requests_slice {
        op_requests.push(OperationRequest {
            bundle: req.bundle_num,
            index: Some(req.operation_idx),
            filter: None,
        });
    }

    match manager.manager.get_operations_batch(op_requests) {
        Ok(operations) => {
            let mut c_ops = Vec::new();
            for op in operations {
                c_ops.push(operation_to_c(op));
            }

            unsafe {
                *out_count = c_ops.len();
                *out_operations = c_ops.as_mut_ptr();
            }
            std::mem::forget(c_ops);
            0
        }
        Err(_) => -1,
    }
}

// ============================================================================
// DID Operations
// ============================================================================

/// # Safety
///
/// The `manager` pointer must be valid. `did` must be a valid NUL-terminated
/// C string. `out_operations` and `out_count` must be valid writable pointers
/// to receive results. Returned memory ownership rules are documented in the
/// API and must be followed by the caller.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_get_did_operations(
    manager: *const CBundleManager,
    did: *const c_char,
    out_operations: *mut *mut COperation,
    out_count: *mut usize,
) -> i32 {
    if manager.is_null() || did.is_null() || out_operations.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };
    let did = unsafe {
        match CStr::from_ptr(did).to_str() {
            Ok(s) => s,
            Err(_) => return -1,
        }
    };

    match manager.manager.get_did_operations(did, false, false) {
        Ok(result) => {
            let mut c_ops = Vec::new();
            for op in result.operations {
                c_ops.push(operation_to_c(op));
            }

            unsafe {
                *out_count = c_ops.len();
                *out_operations = c_ops.as_mut_ptr();
            }
            std::mem::forget(c_ops);
            0
        }
        Err(_) => -1,
    }
}

/// # Safety
///
/// `manager` must be valid. `dids` must point to `did_count` valid
/// NUL-terminated C string pointers. `callback` will be called from this
/// function and must be a valid function pointer. The caller must ensure the
/// callback and its environment remain valid for the duration of the call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_batch_resolve_dids(
    manager: *const CBundleManager,
    dids: *const *const c_char,
    did_count: usize,
    callback: extern "C" fn(*const c_char, *const COperation, usize),
) -> i32 {
    if manager.is_null() || dids.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };
    let dids_slice = unsafe { std::slice::from_raw_parts(dids, did_count) };

    let mut did_strings = Vec::new();
    for did_ptr in dids_slice {
        let did = unsafe {
            match CStr::from_ptr(*did_ptr).to_str() {
                Ok(s) => s.to_string(),
                Err(_) => return -1,
            }
        };
        did_strings.push(did);
    }

    match manager.manager.batch_resolve_dids(did_strings) {
        Ok(results) => {
            for (did, operations) in results {
                let c_did = CString::new(did).unwrap();
                let c_ops: Vec<COperation> = operations.into_iter().map(operation_to_c).collect();

                callback(c_did.as_ptr(), c_ops.as_ptr(), c_ops.len());

                for op in c_ops {
                    free_c_operation(op);
                }
            }
            0
        }
        Err(_) => -1,
    }
}

// ============================================================================
// Query
// ============================================================================

/// # Safety
///
/// `manager` must be valid. `query_str` must be a valid NUL-terminated C
/// string. `out_operations` and `out_count` must be valid writable pointers.
/// The caller is responsible for freeing any returned memory according to the
/// API's ownership rules.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_query(
    manager: *const CBundleManager,
    query_str: *const c_char,
    bundle_start: u32,
    bundle_end: u32,
    out_operations: *mut *mut COperation,
    out_count: *mut usize,
) -> i32 {
    if manager.is_null() || query_str.is_null() || out_operations.is_null() || out_count.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };
    let query = unsafe {
        match CStr::from_ptr(query_str).to_str() {
            Ok(s) => s.to_string(),
            Err(_) => return -1,
        }
    };

    // Determine bundle range
    let (start, end) = if bundle_start == 0 && bundle_end == 0 {
        // Query all bundles if not specified (like Rust CLI)
        (1, manager.manager.get_last_bundle())
    } else if bundle_end == 0 {
        (bundle_start, bundle_start)
    } else {
        (bundle_start, bundle_end)
    };

    // Simple query: search through operations for matching fields
    let mut c_ops = Vec::new();
    for bundle_num in start..=end {
        let result = manager
            .manager
            .load_bundle(bundle_num, LoadOptions::default());
        if let Ok(load_result) = result {
            for op in load_result.operations {
                // Simple string matching in operation JSON
                let op_json = op.operation.to_string();
                if op_json.contains(&query) || op.did.contains(&query) {
                    c_ops.push(operation_to_c(op));
                }
            }
        }
    }

    unsafe {
        *out_count = c_ops.len();
        *out_operations = c_ops.as_mut_ptr();
    }
    std::mem::forget(c_ops);
    0
}

// ============================================================================
// Verification
// ============================================================================

/// # Safety
///
/// `manager` must be a valid pointer. `out_result` must be a valid writable
/// pointer to `CVerifyResult`. Passing invalid or null pointers is undefined
/// behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_verify_bundle(
    manager: *const CBundleManager,
    bundle_num: u32,
    check_hash: bool,
    _check_chain: bool,
    out_result: *mut CVerifyResult,
) -> i32 {
    if manager.is_null() || out_result.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };

    let spec = VerifySpec {
        check_hash,
        check_content_hash: check_hash,
        check_operations: true,
        fast: false,
    };

    match manager.manager.verify_bundle(bundle_num, spec) {
        Ok(result) => {
            unsafe {
                (*out_result).valid = result.valid;
                (*out_result).hash_match = result.valid; // TODO: return actual hash match result
                (*out_result).compression_ok = true; // TODO: return actual compression result
                (*out_result).error_msg = std::ptr::null_mut();
            }
            0
        }
        Err(e) => {
            let err_msg = CString::new(e.to_string()).unwrap();
            unsafe {
                (*out_result).valid = false;
                (*out_result).hash_match = false;
                (*out_result).compression_ok = false;
                (*out_result).error_msg = err_msg.into_raw();
            }
            -1
        }
    }
}

/// # Safety
///
/// `manager` must be a valid pointer. Calling this function with invalid
/// pointers is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_verify_chain(
    manager: *const CBundleManager,
    start_bundle: u32,
    end_bundle: u32,
) -> i32 {
    if manager.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };

    let spec = ChainVerifySpec {
        start_bundle,
        end_bundle: Some(end_bundle),
        check_parent_links: true,
    };

    match manager.manager.verify_chain(spec) {
        Ok(result) => {
            if result.valid {
                0
            } else {
                -1
            }
        }
        Err(_) => -1,
    }
}

// ============================================================================
// Info
// ============================================================================

/// # Safety
///
/// `manager` must be valid. `out_info` must be a valid writable pointer to
/// `CBundleInfo`. Any returned string pointers must be freed by the caller as
/// documented by the API.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_get_bundle_info(
    manager: *const CBundleManager,
    bundle_num: u32,
    include_operations: bool,
    _include_dids: bool,
    out_info: *mut CBundleInfo,
) -> i32 {
    if manager.is_null() || out_info.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };

    let flags = InfoFlags {
        include_operations,
        include_size_info: true,
    };

    match manager.manager.get_bundle_info(bundle_num, flags) {
        Ok(info) => {
            unsafe {
                (*out_info).bundle_number = info.metadata.bundle_number;
                (*out_info).operation_count = info.metadata.operation_count;
                (*out_info).did_count = info.metadata.did_count;
                (*out_info).compressed_size = info.metadata.compressed_size;
                (*out_info).uncompressed_size = info.metadata.uncompressed_size;
                (*out_info).start_time = CString::new(info.metadata.start_time).unwrap().into_raw();
                (*out_info).end_time = CString::new(info.metadata.end_time).unwrap().into_raw();
                (*out_info).hash = CString::new(info.metadata.hash).unwrap().into_raw();
            }
            0
        }
        Err(_) => -1,
    }
}

// ============================================================================
// Cache Management
// ============================================================================

/// # Safety
///
/// `manager` must be valid. `bundle_nums` must point to `count` valid u32
/// values. Passing invalid pointers is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_prefetch_bundles(
    manager: *const CBundleManager,
    bundle_nums: *const u32,
    count: usize,
) -> i32 {
    if manager.is_null() || bundle_nums.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };
    let bundles = unsafe { std::slice::from_raw_parts(bundle_nums, count) };

    match manager.manager.prefetch_bundles(bundles.to_vec()) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

/// # Safety
///
/// `manager` must be valid. Passing invalid pointers is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_warm_up(
    manager: *const CBundleManager,
    strategy: u8,
    start_bundle: u32,
    end_bundle: u32,
) -> i32 {
    if manager.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };

    let warm_strategy = match strategy {
        0 => WarmUpStrategy::Recent(10), // Default count for recent
        1 => WarmUpStrategy::All,
        2 => WarmUpStrategy::Range(start_bundle, end_bundle),
        _ => return -1,
    };

    let spec = WarmUpSpec {
        strategy: warm_strategy,
    };

    match manager.manager.warm_up(spec) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

/// # Safety
///
/// `manager` must be a valid pointer previously returned by
/// `bundle_manager_new`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_clear_caches(manager: *const CBundleManager) -> i32 {
    if manager.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };
    manager.manager.clear_caches();
    0
}

// ============================================================================
// DID Index
// ============================================================================

/// # Safety
///
/// `manager` must be valid. `out_stats` must be a valid writable pointer if
/// provided. The caller must ensure `progress_callback` is a valid function
/// pointer if passed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_rebuild_did_index(
    manager: *const CBundleManager,
    progress_callback: Option<extern "C" fn(u32, u32, u64, u64)>,
    out_stats: *mut CRebuildStats,
) -> i32 {
    if manager.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };

    let callback = progress_callback.map(|cb| {
        Box::new(
            move |current: u32, total: u32, bytes_processed: u64, total_bytes: u64| {
                cb(current, total, bytes_processed, total_bytes);
            },
        ) as Box<dyn Fn(u32, u32, u64, u64) + Send + Sync>
    });

    // Use default flush interval for FFI
    match manager
        .manager
        .build_did_index(constants::DID_INDEX_FLUSH_INTERVAL, callback, None, None)
    {
        Ok(stats) => {
            if !out_stats.is_null() {
                unsafe {
                    (*out_stats).bundles_processed = stats.bundles_processed;
                    (*out_stats).operations_indexed = stats.operations_indexed;
                    (*out_stats).unique_dids = 0; // TODO: track unique DIDs
                    (*out_stats).duration_ms = 0; // TODO: track duration
                }
            }
            0
        }
        Err(_) => -1,
    }
}

/// # Safety
///
/// `manager` must be valid. `out_stats` must be a valid writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_get_did_index_stats(
    manager: *const CBundleManager,
    out_stats: *mut CDIDIndexStats,
) -> i32 {
    if manager.is_null() || out_stats.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };

    let stats = manager.manager.get_did_index_stats();
    unsafe {
        (*out_stats).total_dids = stats
            .get("total_dids")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;
        (*out_stats).total_operations = stats
            .get("total_entries")
            .and_then(|v| v.as_i64())
            .unwrap_or(0) as usize;
        (*out_stats).index_size_bytes = 0; // TODO: track actual index size
    }
    0
}

// ============================================================================
// Observability
// ============================================================================

/// # Safety
///
/// `manager` must be valid. `out_stats` must be a valid writable pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_get_stats(
    manager: *const CBundleManager,
    out_stats: *mut CManagerStats,
) -> i32 {
    if manager.is_null() || out_stats.is_null() {
        return -1;
    }

    let manager = unsafe { &*manager };

    let stats = manager.manager.get_stats();
    unsafe {
        (*out_stats).cache_hits = stats.cache_hits;
        (*out_stats).cache_misses = stats.cache_misses;
        (*out_stats).bytes_read = stats.bundles_loaded; // TODO: track actual bytes read
        (*out_stats).operations_processed = stats.operations_read;
    }
    0
}

// ============================================================================
// Helper functions
// ============================================================================

fn operation_to_c(op: Operation) -> COperation {
    // Extract operation type from the operation object
    let op_type = op
        .operation
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    COperation {
        did: CString::new(op.did).unwrap().into_raw(),
        op_type: CString::new(op_type).unwrap().into_raw(),
        cid: op
            .cid
            .map(|s| CString::new(s).unwrap().into_raw())
            .unwrap_or(std::ptr::null_mut()),
        nullified: op.nullified,
        created_at: CString::new(op.created_at).unwrap().into_raw(),
        json: CString::new(op.operation.to_string()).unwrap().into_raw(),
    }
}

fn free_c_operation(op: COperation) {
    unsafe {
        if !op.did.is_null() {
            let _ = CString::from_raw(op.did);
        }
        if !op.op_type.is_null() {
            let _ = CString::from_raw(op.op_type);
        }
        if !op.cid.is_null() {
            let _ = CString::from_raw(op.cid);
        }
        if !op.created_at.is_null() {
            let _ = CString::from_raw(op.created_at);
        }
        if !op.json.is_null() {
            let _ = CString::from_raw(op.json);
        }
    }
}

/// # Safety
///
/// `s` must be a pointer previously returned by this API that is safe to
/// free. Passing invalid pointers is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_free_string(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            let _ = CString::from_raw(s);
        }
    }
}

/// # Safety
///
/// `op` must be a pointer previously returned by this API and safe to free.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_free_operation(op: *mut COperation) {
    if !op.is_null() {
        unsafe {
            let op_val = *op;
            free_c_operation(op_val);
        }
    }
}

/// # Safety
///
/// `ops` must point to an array of `count` `COperation` previously returned
/// by this API and safe to free.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_free_operations(ops: *mut COperation, count: usize) {
    if !ops.is_null() {
        unsafe {
            let ops_slice = std::slice::from_raw_parts(ops, count);
            for op in ops_slice {
                free_c_operation(*op);
            }
            let _ = Vec::from_raw_parts(ops, count, count);
        }
    }
}

// ============================================================================
// Export
// ============================================================================

/// Callback type for streaming export
/// Returns 0 to continue, non-zero to stop
pub type ExportCallback =
    extern "C" fn(data: *const c_char, len: usize, user_data: *mut std::ffi::c_void) -> i32;

/// # Safety
///
/// `manager` must be valid. `spec` must point to a valid `CExportSpec` and
/// `callback` must be a valid function pointer. `out_stats` must be a valid
/// writable pointer if provided.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn bundle_manager_export(
    manager: *const CBundleManager,
    spec: *const CExportSpec,
    callback: ExportCallback,
    user_data: *mut std::ffi::c_void,
    out_stats: *mut CExportStats,
) -> i32 {
    if manager.is_null() || spec.is_null() || callback as usize == 0 {
        return -1;
    }

    let manager = unsafe { &*manager };
    let spec = unsafe { &*spec };

    use crate::index::Index;
    use std::fs::File;
    use std::io::BufRead;

    // Get directory path
    let dir_path = manager.manager.directory().clone();
    // Load index
    let index = match Index::load(&dir_path) {
        Ok(idx) => idx,
        Err(_) => return -1,
    };

    // Determine bundle range
    let bundle_numbers: Vec<u32> = if spec.export_all {
        (1..=index.last_bundle).collect()
    } else if spec.bundle_end > 0 && spec.bundle_end >= spec.bundle_start {
        (spec.bundle_start..=spec.bundle_end).collect()
    } else {
        vec![spec.bundle_start]
    };

    // Filter bundles by timestamp if specified
    let bundle_numbers: Vec<u32> = if !spec.after_timestamp.is_null() {
        let after_ts = unsafe {
            match CStr::from_ptr(spec.after_timestamp).to_str() {
                Ok(s) => s.to_string(),
                Err(_) => return -1,
            }
        };
        bundle_numbers
            .into_iter()
            .filter(|num| match index.get_bundle(*num) {
                Some(meta) => meta.end_time >= after_ts,
                None => true,
            })
            .collect()
    } else {
        bundle_numbers
    };

    let mut exported_count = 0u64;
    let mut bytes_written = 0u64;
    let mut output_buffer = Vec::with_capacity(1024 * 1024);

    // Determine if we need parsing
    let needs_parsing = !spec.after_timestamp.is_null()
        || !spec.did_filter.is_null()
        || !spec.op_type_filter.is_null()
        || spec.format == 1
        || spec.format == 2; // JSON or CSV

    // Process bundles
    for bundle_num in bundle_numbers {
        if spec.count_limit > 0 && exported_count >= spec.count_limit {
            break;
        }

        let bundle_path = constants::bundle_path(&dir_path, bundle_num);
        if !bundle_path.exists() {
            continue;
        }

        let file = match File::open(&bundle_path) {
            Ok(f) => f,
            Err(_) => continue,
        };

        let decoder = match zstd::Decoder::new(file) {
            Ok(d) => d,
            Err(_) => continue,
        };

        let reader = std::io::BufReader::with_capacity(1024 * 1024, decoder);

        if !needs_parsing {
            // Fast path: no parsing
            for line in reader.lines() {
                if spec.count_limit > 0 && exported_count >= spec.count_limit {
                    break;
                }

                let line = match line {
                    Ok(l) => l,
                    Err(_) => continue,
                };

                if line.is_empty() {
                    continue;
                }

                output_buffer.extend_from_slice(line.as_bytes());
                output_buffer.push(b'\n');
                exported_count += 1;
                bytes_written += line.len() as u64 + 1;

                // Flush buffer when large
                if output_buffer.len() >= 1024 * 1024 {
                    let result = callback(
                        output_buffer.as_ptr() as *const c_char,
                        output_buffer.len(),
                        user_data,
                    );
                    if result != 0 {
                        break;
                    }
                    output_buffer.clear();
                }
            }
        } else {
            // Slow path: parse and filter
            use sonic_rs::JsonValueTrait;

            let after_ts = if !spec.after_timestamp.is_null() {
                Some(unsafe {
                    CStr::from_ptr(spec.after_timestamp)
                        .to_str()
                        .unwrap()
                        .to_string()
                })
            } else {
                None
            };

            let did_filter = if !spec.did_filter.is_null() {
                Some(unsafe {
                    CStr::from_ptr(spec.did_filter)
                        .to_str()
                        .unwrap()
                        .to_string()
                })
            } else {
                None
            };

            let op_type_filter = if !spec.op_type_filter.is_null() {
                Some(unsafe {
                    CStr::from_ptr(spec.op_type_filter)
                        .to_str()
                        .unwrap()
                        .to_string()
                })
            } else {
                None
            };

            for line in reader.lines() {
                if spec.count_limit > 0 && exported_count >= spec.count_limit {
                    break;
                }

                let line = match line {
                    Ok(l) => l,
                    Err(_) => continue,
                };

                if line.is_empty() {
                    continue;
                }

                // Parse JSON
                let data: sonic_rs::Value = match sonic_rs::from_str(&line) {
                    Ok(d) => d,
                    Err(_) => continue,
                };

                // Apply filters
                if let Some(ref after_ts) = after_ts {
                    if let Some(created_at) =
                        data.get("createdAt").or_else(|| data.get("created_at"))
                    {
                        if let Some(ts_str) = created_at.as_str() {
                            if ts_str < after_ts.as_str() {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                if let Some(ref did_filter) = did_filter {
                    if let Some(did_val) = data.get("did") {
                        if let Some(did_str) = did_val.as_str() {
                            if did_str != did_filter {
                                continue;
                            }
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                if let Some(ref op_type_filter) = op_type_filter {
                    if let Some(op_val) = data.get("operation") {
                        let matches = if op_val.is_str() {
                            op_val.as_str().unwrap() == op_type_filter
                        } else if op_val.is_object() {
                            if let Some(typ_val) = op_val.get("type") {
                                typ_val.is_str() && typ_val.as_str().unwrap() == op_type_filter
                            } else {
                                false
                            }
                        } else {
                            false
                        };
                        if !matches {
                            continue;
                        }
                    } else {
                        continue;
                    }
                }

                // Format
                let formatted = match spec.format {
                    0 => line,                                                  // JSONL - use original
                    1 => sonic_rs::to_string_pretty(&data).unwrap_or_default(), // JSON
                    2 => {
                        let did = data.get("did").and_then(|v| v.as_str()).unwrap_or("");
                        let op = data
                            .get("operation")
                            .map(|v| sonic_rs::to_string(v).unwrap_or_default())
                            .unwrap_or_default();
                        let created_at = data
                            .get("createdAt")
                            .or_else(|| data.get("created_at"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        let nullified = data
                            .get("nullified")
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);
                        format!("{},{},{},{}", did, op, created_at, nullified)
                    }
                    _ => line,
                };

                output_buffer.extend_from_slice(formatted.as_bytes());
                output_buffer.push(b'\n');
                exported_count += 1;
                bytes_written += formatted.len() as u64 + 1;

                // Flush buffer when large
                if output_buffer.len() >= 1024 * 1024 {
                    let result = callback(
                        output_buffer.as_ptr() as *const c_char,
                        output_buffer.len(),
                        user_data,
                    );
                    if result != 0 {
                        break;
                    }
                    output_buffer.clear();
                }
            }
        }
    }

    // Flush remaining buffer
    if !output_buffer.is_empty() {
        callback(
            output_buffer.as_ptr() as *const c_char,
            output_buffer.len(),
            user_data,
        );
    }

    if !out_stats.is_null() {
        unsafe {
            (*out_stats).records_written = exported_count;
            (*out_stats).bytes_written = bytes_written;
        }
    }

    0
}
