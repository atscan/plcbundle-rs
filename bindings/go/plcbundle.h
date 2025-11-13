#ifndef PLCBUNDLE_H
#define PLCBUNDLE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Opaque types
// ============================================================================

typedef void* CBundleManager;

// ============================================================================
// Options and results
// ============================================================================

typedef struct {
    bool verify_hash;
    bool decompress;
    bool cache;
} CLoadOptions;

typedef struct {
    bool success;
    uint32_t operation_count;
    uint64_t size_bytes;
    char* error_msg;  // Must be freed with bundle_manager_free_string
} CLoadResult;

typedef struct {
    char* did;         // Must be freed
    char* op_type;     // Must be freed
    char* cid;         // Must be freed
    bool nullified;
    char* created_at;  // Must be freed
    char* json;        // Must be freed
} COperation;

typedef struct {
    uint32_t bundle_num;
    size_t operation_idx;
} COperationRequest;

typedef struct {
    bool valid;
    bool hash_match;
    bool compression_ok;
    char* error_msg;  // Must be freed with bundle_manager_free_string
} CVerifyResult;

typedef struct {
    uint32_t bundle_number;
    uint32_t operation_count;
    uint32_t did_count;
    uint64_t compressed_size;
    uint64_t uncompressed_size;
    char* start_time;  // Must be freed
    char* end_time;    // Must be freed
    char* hash;        // Must be freed
} CBundleInfo;

typedef struct {
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t bytes_read;
    uint64_t operations_processed;
} CManagerStats;

typedef struct {
    size_t total_dids;
    size_t total_operations;
    size_t index_size_bytes;
} CDIDIndexStats;

typedef struct {
    uint32_t bundles_processed;
    uint64_t operations_indexed;
    size_t unique_dids;
    uint64_t duration_ms;
} CRebuildStats;

typedef struct {
    uint32_t bundle_start;
    uint32_t bundle_end;
    bool export_all;
    uint8_t format;  // 0=jsonl, 1=json, 2=csv
    uint64_t count_limit;  // 0 = no limit
    const char* after_timestamp;  // NULL = no filter
    const char* did_filter;  // NULL = no filter
    const char* op_type_filter;  // NULL = no filter
} CExportSpec;

typedef struct {
    uint64_t records_written;
    uint64_t bytes_written;
} CExportStats;

// Warm-up strategies
#define WARMUP_RECENT 0
#define WARMUP_ALL 1
#define WARMUP_RANGE 2

// ============================================================================
// Lifecycle
// ============================================================================

/**
 * Create a new bundle manager.
 * Returns NULL on error.
 * Must be freed with bundle_manager_free().
 */
CBundleManager bundle_manager_new(const char* bundle_dir);

/**
 * Free the bundle manager and all associated resources.
 */
void bundle_manager_free(CBundleManager manager);

// ============================================================================
// Smart Loading
// ============================================================================

/**
 * Load a bundle with options.
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_load_bundle(
    CBundleManager manager,
    uint32_t bundle_num,
    const CLoadOptions* options,  // NULL for defaults
    CLoadResult* out_result
);

// ============================================================================
// Batch Operations
// ============================================================================

/**
 * Get multiple operations in a single call.
 * out_operations must be freed with bundle_manager_free_operations().
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_get_operations_batch(
    CBundleManager manager,
    const COperationRequest* requests,
    size_t count,
    COperation** out_operations,
    size_t* out_count
);

// ============================================================================
// DID Operations
// ============================================================================

/**
 * Get all operations for a DID.
 * out_operations must be freed with bundle_manager_free_operations().
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_get_did_operations(
    CBundleManager manager,
    const char* did,
    COperation** out_operations,
    size_t* out_count
);

/**
 * Batch resolve multiple DIDs.
 * Callback is called for each DID with its operations.
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_batch_resolve_dids(
    CBundleManager manager,
    const char** dids,
    size_t did_count,
    void (*callback)(const char* did, const COperation* ops, size_t count)
);

// ============================================================================
// Query
// ============================================================================

/**
 * Query operations across bundles.
 *
 * @param manager The bundle manager
 * @param query_str Query string (e.g., "did" or "type")
 * @param bundle_start Start bundle number (0 for all bundles)
 * @param bundle_end End bundle number (0 for all bundles)
 * @param out_operations Output pointer to receive operations array
 * @param out_count Output pointer to receive operation count
 * @return 0 on success, -1 on error
 */
int bundle_manager_query(
    CBundleManager manager,
    const char* query_str,
    uint32_t bundle_start,
    uint32_t bundle_end,
    COperation** out_operations,
    size_t* out_count
);

// ============================================================================
// Verification
// ============================================================================

/**
 * Verify a single bundle.
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_verify_bundle(
    CBundleManager manager,
    uint32_t bundle_num,
    bool check_hash,
    bool check_chain,
    CVerifyResult* out_result
);

/**
 * Verify chain continuity between bundles.
 * Returns 0 if chain is valid, -1 otherwise.
 */
int bundle_manager_verify_chain(
    CBundleManager manager,
    uint32_t start_bundle,
    uint32_t end_bundle
);

// ============================================================================
// Info
// ============================================================================

/**
 * Get detailed bundle information.
 * String fields in out_info must be freed with bundle_manager_free_string().
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_get_bundle_info(
    CBundleManager manager,
    uint32_t bundle_num,
    bool include_operations,
    bool include_dids,
    CBundleInfo* out_info
);

// ============================================================================
// Cache Management
// ============================================================================

/**
 * Prefetch bundles into cache.
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_prefetch_bundles(
    CBundleManager manager,
    const uint32_t* bundle_nums,
    size_t count
);

/**
 * Warm up cache with specified strategy.
 * strategy: WARMUP_RECENT, WARMUP_ALL, or WARMUP_RANGE
 * For WARMUP_RANGE, specify start_bundle and end_bundle.
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_warm_up(
    CBundleManager manager,
    uint8_t strategy,
    uint32_t start_bundle,
    uint32_t end_bundle
);

/**
 * Clear all caches.
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_clear_caches(CBundleManager manager);

// ============================================================================
// DID Index
// ============================================================================

/**
 * Rebuild the DID index.
 * progress_callback(bundle_num, operations_processed) is called periodically.
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_rebuild_did_index(
    CBundleManager manager,
    void (*progress_callback)(uint32_t, uint64_t),  // NULL for no progress
    CRebuildStats* out_stats  // NULL if stats not needed
);

/**
 * Get DID index statistics.
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_get_did_index_stats(
    CBundleManager manager,
    CDIDIndexStats* out_stats
);

// ============================================================================
// Observability
// ============================================================================

/**
 * Get manager statistics.
 * Returns 0 on success, -1 on error.
 */
int bundle_manager_get_stats(
    CBundleManager manager,
    CManagerStats* out_stats
);

// ============================================================================
// Export
// ============================================================================

/**
 * Export operations with streaming callback.
 * callback(data, len, user_data) is called for each batch of data.
 * Returns 0 to continue, non-zero to stop.
 * Returns 0 on success, -1 on error.
 */
typedef int (*ExportCallback)(const char* data, size_t len, void* user_data);

int bundle_manager_export(
    CBundleManager manager,
    const CExportSpec* spec,
    ExportCallback callback,
    void* user_data,
    CExportStats* out_stats  // NULL if stats not needed
);

// ============================================================================
// Memory Management
// ============================================================================

/**
 * Free a string returned by the library.
 */
void bundle_manager_free_string(char* s);

/**
 * Free a single operation.
 */
void bundle_manager_free_operation(COperation* op);

/**
 * Free an array of operations.
 */
void bundle_manager_free_operations(COperation* ops, size_t count);

#ifdef __cplusplus
}
#endif

#endif // PLCBUNDLE_H