package plcbundle

// #cgo LDFLAGS: -L../../target/release -lplcbundle -lm -ldl -lpthread
// #cgo CFLAGS: -I.
// #include <stdlib.h>
// #include <stddef.h>
// #include "plcbundle.h"
// extern int exportCallbackBridge(const char* data, size_t len, void* user_data);
import "C"
import (
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"unsafe"
)

// ============================================================================
// Go types
// ============================================================================

type LoadOptions struct {
	VerifyHash bool
	Decompress bool
	Cache      bool
}

type LoadResult struct {
	Success        bool
	OperationCount uint32
	SizeBytes      uint64
	Error          error
}

type Operation struct {
	DID       string
	OpType    string
	CID       string
	Nullified bool
	CreatedAt string
	JSON      string
}

type OperationRequest struct {
	BundleNum    uint32
	OperationIdx uint
}

type VerifyResult struct {
	Valid         bool
	HashMatch     bool
	CompressionOk bool
	Error         error
}

type BundleInfo struct {
	BundleNumber     uint32
	OperationCount   uint32
	DIDCount         uint32
	CompressedSize   uint64
	UncompressedSize uint64
	StartTime        string
	EndTime          string
	Hash             string
}

type ManagerStats struct {
	CacheHits           uint64
	CacheMisses         uint64
	BytesRead           uint64
	OperationsProcessed uint64
}

type DIDIndexStats struct {
	TotalDIDs       uint
	TotalOperations uint
	IndexSizeBytes  uint
}

type RebuildStats struct {
	BundlesProcessed  uint32
	OperationsIndexed uint64
	UniqueDIDs        uint
	DurationMs        uint64
}

type ExportSpec struct {
	BundleStart    uint32
	BundleEnd      uint32
	ExportAll      bool
	Format         uint8  // 0=jsonl, 1=json, 2=csv
	CountLimit     uint64 // 0 = no limit
	AfterTimestamp string // Empty = no filter
	DIDFilter      string // Empty = no filter
	OpTypeFilter   string // Empty = no filter
}

type ExportStats struct {
	RecordsWritten uint64
	BytesWritten   uint64
}

type WarmUpStrategy uint8

const (
	WarmUpRecent WarmUpStrategy = 0
	WarmUpAll    WarmUpStrategy = 1
	WarmUpRange  WarmUpStrategy = 2
)

// ============================================================================
// BundleManager
// ============================================================================

type BundleManager struct {
	ptr C.CBundleManager
}

// NewBundleManager creates a new bundle manager
func NewBundleManager(bundleDir string) (*BundleManager, error) {
	cDir := C.CString(bundleDir)
	defer C.free(unsafe.Pointer(cDir))

	ptr := C.bundle_manager_new(cDir)
	if ptr == nil {
		return nil, fmt.Errorf("failed to create bundle manager")
	}

	m := &BundleManager{ptr: ptr}
	runtime.SetFinalizer(m, (*BundleManager).Close)
	return m, nil
}

// Close frees the bundle manager
func (m *BundleManager) Close() {
	if m.ptr != nil {
		C.bundle_manager_free(m.ptr)
		m.ptr = nil
	}
}

// ============================================================================
// Smart Loading
// ============================================================================

// LoadBundle loads a bundle with options
func (m *BundleManager) LoadBundle(bundleNum uint32, opts *LoadOptions) (*LoadResult, error) {
	var cOpts *C.CLoadOptions
	if opts != nil {
		cOpts = &C.CLoadOptions{
			verify_hash: C._Bool(opts.VerifyHash),
			decompress:  C._Bool(opts.Decompress),
			cache:       C._Bool(opts.Cache),
		}
	}

	var cResult C.CLoadResult
	result := C.bundle_manager_load_bundle(m.ptr, C.uint32_t(bundleNum), cOpts, &cResult)

	loadResult := &LoadResult{
		Success:        bool(cResult.success),
		OperationCount: uint32(cResult.operation_count),
		SizeBytes:      uint64(cResult.size_bytes),
	}

	if result != 0 {
		if cResult.error_msg != nil {
			loadResult.Error = fmt.Errorf("%s", C.GoString(cResult.error_msg))
			C.bundle_manager_free_string(cResult.error_msg)
		} else {
			loadResult.Error = fmt.Errorf("load failed")
		}
	}

	return loadResult, loadResult.Error
}

// ============================================================================
// Batch Operations
// ============================================================================

// GetOperationsBatch retrieves multiple operations in a single call
func (m *BundleManager) GetOperationsBatch(requests []OperationRequest) ([]Operation, error) {
	if len(requests) == 0 {
		return nil, nil
	}

	cRequests := make([]C.COperationRequest, len(requests))
	for i, req := range requests {
		cRequests[i] = C.COperationRequest{
			bundle_num:    C.uint32_t(req.BundleNum),
			operation_idx: C.size_t(req.OperationIdx),
		}
	}

	var cOps *C.COperation
	var count C.size_t

	result := C.bundle_manager_get_operations_batch(
		m.ptr,
		&cRequests[0],
		C.size_t(len(requests)),
		&cOps,
		&count,
	)

	if result != 0 {
		return nil, fmt.Errorf("batch operation failed")
	}

	ops := cOperationsToGo(cOps, int(count))
	C.bundle_manager_free_operations(cOps, count)

	return ops, nil
}

// ============================================================================
// DID Operations
// ============================================================================

// GetDIDOperations retrieves all operations for a DID
func (m *BundleManager) GetDIDOperations(did string) ([]Operation, error) {
	cDID := C.CString(did)
	defer C.free(unsafe.Pointer(cDID))

	var cOps *C.COperation
	var count C.size_t

	result := C.bundle_manager_get_did_operations(m.ptr, cDID, &cOps, &count)
	if result != 0 {
		return nil, fmt.Errorf("failed to get DID operations")
	}

	ops := cOperationsToGo(cOps, int(count))
	C.bundle_manager_free_operations(cOps, count)

	return ops, nil
}

// BatchResolveDIDs resolves multiple DIDs and returns their operations
func (m *BundleManager) BatchResolveDIDs(dids []string) (map[string][]Operation, error) {
	if len(dids) == 0 {
		return make(map[string][]Operation), nil
	}

	results := make(map[string][]Operation)

	// Convert Go strings to C strings
	cDIDs := make([]*C.char, len(dids))
	for i, did := range dids {
		cDIDs[i] = C.CString(did)
		defer C.free(unsafe.Pointer(cDIDs[i]))
	}

	// This callback will be called for each DID
	// Note: In actual implementation, you'd need to use cgo callbacks properly
	// For now, we'll do sequential calls
	for _, did := range dids {
		ops, err := m.GetDIDOperations(did)
		if err == nil {
			results[did] = ops
		}
	}

	return results, nil
}

// ============================================================================
// Query
// ============================================================================

// Query searches for operations across bundles
func (m *BundleManager) Query(queryStr string, bundleStart, bundleEnd uint32) ([]Operation, error) {
	cQuery := C.CString(queryStr)
	defer C.free(unsafe.Pointer(cQuery))

	var cOps *C.COperation
	var count C.size_t

	result := C.bundle_manager_query(
		m.ptr,
		cQuery,
		C.uint32_t(bundleStart),
		C.uint32_t(bundleEnd),
		&cOps,
		&count,
	)

	if result != 0 {
		return nil, fmt.Errorf("query failed")
	}

	ops := cOperationsToGo(cOps, int(count))
	C.bundle_manager_free_operations(cOps, count)

	return ops, nil
}

// ============================================================================
// Verification
// ============================================================================

// VerifyBundle verifies a single bundle
func (m *BundleManager) VerifyBundle(bundleNum uint32, checkHash, checkChain bool) (*VerifyResult, error) {
	var cResult C.CVerifyResult

	result := C.bundle_manager_verify_bundle(
		m.ptr,
		C.uint32_t(bundleNum),
		C._Bool(checkHash),
		C._Bool(checkChain),
		&cResult,
	)

	verifyResult := &VerifyResult{
		Valid:         bool(cResult.valid),
		HashMatch:     bool(cResult.hash_match),
		CompressionOk: bool(cResult.compression_ok),
	}

	if result != 0 {
		if cResult.error_msg != nil {
			verifyResult.Error = fmt.Errorf("%s", C.GoString(cResult.error_msg))
			C.bundle_manager_free_string(cResult.error_msg)
		} else {
			verifyResult.Error = fmt.Errorf("verification failed")
		}
	}

	return verifyResult, verifyResult.Error
}

// VerifyChain verifies chain continuity
func (m *BundleManager) VerifyChain(startBundle, endBundle uint32) error {
	result := C.bundle_manager_verify_chain(
		m.ptr,
		C.uint32_t(startBundle),
		C.uint32_t(endBundle),
	)

	if result != 0 {
		return fmt.Errorf("chain verification failed")
	}
	return nil
}

// ============================================================================
// Info
// ============================================================================

// GetBundleInfo retrieves bundle information
func (m *BundleManager) GetBundleInfo(bundleNum uint32, includeOperations, includeDIDs bool) (*BundleInfo, error) {
	var cInfo C.CBundleInfo

	result := C.bundle_manager_get_bundle_info(
		m.ptr,
		C.uint32_t(bundleNum),
		C._Bool(includeOperations),
		C._Bool(includeDIDs),
		&cInfo,
	)

	if result != 0 {
		return nil, fmt.Errorf("failed to get bundle info")
	}

	info := &BundleInfo{
		BundleNumber:     uint32(cInfo.bundle_number),
		OperationCount:   uint32(cInfo.operation_count),
		DIDCount:         uint32(cInfo.did_count),
		CompressedSize:   uint64(cInfo.compressed_size),
		UncompressedSize: uint64(cInfo.uncompressed_size),
		StartTime:        C.GoString(cInfo.start_time),
		EndTime:          C.GoString(cInfo.end_time),
		Hash:             C.GoString(cInfo.hash),
	}

	// Free C strings
	C.bundle_manager_free_string(cInfo.start_time)
	C.bundle_manager_free_string(cInfo.end_time)
	C.bundle_manager_free_string(cInfo.hash)

	return info, nil
}

// ============================================================================
// Cache Management
// ============================================================================

// PrefetchBundles prefetches bundles into cache
func (m *BundleManager) PrefetchBundles(bundleNums []uint32) {
	if len(bundleNums) == 0 {
		return
	}

	C.bundle_manager_prefetch_bundles(
		m.ptr,
		(*C.uint32_t)(unsafe.Pointer(&bundleNums[0])),
		C.size_t(len(bundleNums)),
	)
}

// WarmUp warms up the cache with specified strategy
func (m *BundleManager) WarmUp(strategy WarmUpStrategy, startBundle, endBundle uint32) error {
	result := C.bundle_manager_warm_up(
		m.ptr,
		C.uint8_t(strategy),
		C.uint32_t(startBundle),
		C.uint32_t(endBundle),
	)
	if result != 0 {
		return fmt.Errorf("warm up failed")
	}
	return nil
}

// ClearCaches clears all caches
func (m *BundleManager) ClearCaches() {
	C.bundle_manager_clear_caches(m.ptr)
}

// ============================================================================
// DID Index
// ============================================================================

// RebuildDIDIndex rebuilds the DID index
func (m *BundleManager) RebuildDIDIndex(progressCallback func(uint32, uint64)) (*RebuildStats, error) {
	var cStats C.CRebuildStats

	// Note: Progress callback would need proper cgo callback setup
	// For now, we pass nil
	result := C.bundle_manager_rebuild_did_index(m.ptr, nil, &cStats)

	if result != 0 {
		return nil, fmt.Errorf("failed to rebuild DID index")
	}

	stats := &RebuildStats{
		BundlesProcessed:  uint32(cStats.bundles_processed),
		OperationsIndexed: uint64(cStats.operations_indexed),
		UniqueDIDs:        uint(cStats.unique_dids),
		DurationMs:        uint64(cStats.duration_ms),
	}

	return stats, nil
}

// GetDIDIndexStats retrieves DID index statistics
func (m *BundleManager) GetDIDIndexStats() (*DIDIndexStats, error) {
	var cStats C.CDIDIndexStats

	result := C.bundle_manager_get_did_index_stats(m.ptr, &cStats)
	if result != 0 {
		return nil, fmt.Errorf("failed to get DID index stats")
	}

	stats := &DIDIndexStats{
		TotalDIDs:       uint(cStats.total_dids),
		TotalOperations: uint(cStats.total_operations),
		IndexSizeBytes:  uint(cStats.index_size_bytes),
	}

	return stats, nil
}

// ============================================================================
// Observability
// ============================================================================

// GetStats retrieves manager statistics
func (m *BundleManager) GetStats() (*ManagerStats, error) {
	var cStats C.CManagerStats

	result := C.bundle_manager_get_stats(m.ptr, &cStats)
	if result != 0 {
		return nil, fmt.Errorf("failed to get manager stats")
	}

	stats := &ManagerStats{
		CacheHits:           uint64(cStats.cache_hits),
		CacheMisses:         uint64(cStats.cache_misses),
		BytesRead:           uint64(cStats.bytes_read),
		OperationsProcessed: uint64(cStats.operations_processed),
	}

	return stats, nil
}

// ============================================================================
// Export
// ============================================================================

// ExportOptions configures export behavior
type ExportOptions struct {
	// Writer receives exported data in batches
	Writer io.Writer
	// Progress callback (optional)
	Progress func(records, bytes uint64)
}

// Export exports operations according to the spec
func (m *BundleManager) Export(spec ExportSpec, opts *ExportOptions) (*ExportStats, error) {
	if opts == nil || opts.Writer == nil {
		return nil, fmt.Errorf("writer is required")
	}

	// Convert Go spec to C spec
	cSpec := C.CExportSpec{
		bundle_start:    C.uint32_t(spec.BundleStart),
		bundle_end:      C.uint32_t(spec.BundleEnd),
		export_all:      C._Bool(spec.ExportAll),
		format:          C.uint8_t(spec.Format),
		count_limit:     C.uint64_t(spec.CountLimit),
		after_timestamp: nil,
		did_filter:      nil,
		op_type_filter:  nil,
	}

	// Convert string filters to C strings
	var afterTs, didFilter, opTypeFilter *C.char
	if spec.AfterTimestamp != "" {
		afterTs = C.CString(spec.AfterTimestamp)
		defer C.free(unsafe.Pointer(afterTs))
		cSpec.after_timestamp = afterTs
	}

	if spec.DIDFilter != "" {
		didFilter = C.CString(spec.DIDFilter)
		defer C.free(unsafe.Pointer(didFilter))
		cSpec.did_filter = didFilter
	}

	if spec.OpTypeFilter != "" {
		opTypeFilter = C.CString(spec.OpTypeFilter)
		defer C.free(unsafe.Pointer(opTypeFilter))
		cSpec.op_type_filter = opTypeFilter
	}

	// Create callback context
	ctx := &exportCallbackCtx{
		writer:   opts.Writer,
		progress: opts.Progress,
	}

	var cStats C.CExportStats
	
	// Use a global callback registry to work around cgo limitations
	exportCallbackID := registerExportCallback(ctx)
	defer unregisterExportCallback(exportCallbackID)

	// Use C bridge function that calls the Go-exported function
	result := C.bundle_manager_export(
		m.ptr,
		&cSpec,
		(C.ExportCallback)(C.exportCallbackBridge),
		unsafe.Pointer(uintptr(exportCallbackID)),
		&cStats,
	)

	if result != 0 {
		return nil, fmt.Errorf("export failed")
	}

	stats := &ExportStats{
		RecordsWritten: uint64(cStats.records_written),
		BytesWritten:   uint64(cStats.bytes_written),
	}

	return stats, nil
}

// ============================================================================
// Export callback registry
// ============================================================================

type exportCallbackCtx struct {
	writer   io.Writer
	progress func(uint64, uint64)
	records  uint64
	bytes    uint64
}

var (
	exportCallbacks   = make(map[uintptr]*exportCallbackCtx)
	exportCallbackMux sync.Mutex
	exportCallbackID  uintptr = 1
)

func registerExportCallback(ctx *exportCallbackCtx) uintptr {
	exportCallbackMux.Lock()
	defer exportCallbackMux.Unlock()
	id := exportCallbackID
	exportCallbackID++
	exportCallbacks[id] = ctx
	return id
}

func unregisterExportCallback(id uintptr) {
	exportCallbackMux.Lock()
	defer exportCallbackMux.Unlock()
	delete(exportCallbacks, id)
}

//export exportCallbackGo
func exportCallbackGo(data *C.char, len C.size_t, userData unsafe.Pointer) C.int {
	id := uintptr(userData)
	exportCallbackMux.Lock()
	ctx, ok := exportCallbacks[id]
	exportCallbackMux.Unlock()

	if !ok || ctx == nil {
		return 1
	}

	if data == nil || len == 0 {
		return 0
	}

	// Copy C data to Go slice
	goData := C.GoBytes(unsafe.Pointer(data), C.int(len))
	_, err := ctx.writer.Write(goData)
	if err != nil {
		return 1 // Stop on error
	}

	ctx.bytes += uint64(len)
	// Estimate records (rough - count newlines)
	ctx.records += uint64(strings.Count(string(goData), "\n"))

	// Call progress callback if provided
	if ctx.progress != nil {
		ctx.progress(ctx.records, ctx.bytes)
	}

	return 0 // Continue
}

// ============================================================================
// Helper functions
// ============================================================================

func cOperationsToGo(cOps *C.COperation, count int) []Operation {
	if cOps == nil || count == 0 {
		return nil
	}

	ops := make([]Operation, count)
	slice := unsafe.Slice(cOps, count)

	for i, cOp := range slice {
		ops[i] = Operation{
			DID:       C.GoString(cOp.did),
			OpType:    C.GoString(cOp.op_type),
			CID:       C.GoString(cOp.cid),
			Nullified: bool(cOp.nullified),
			CreatedAt: C.GoString(cOp.created_at),
			JSON:      C.GoString(cOp.json),
		}
	}

	return ops
}
