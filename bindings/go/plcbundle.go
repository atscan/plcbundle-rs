package plcbundle

// #cgo LDFLAGS: -L../../target/release -lbundleq -lm -ldl -lpthread
// #include <stdlib.h>
// #include "callback.h"
//
// typedef struct {
//     size_t operations;
//     size_t matches;
//     unsigned long long total_bytes;
//     unsigned long long matched_bytes;
// } CStats;
//
// typedef void* BundleqProcessor;
//
// extern BundleqProcessor bundleq_new(const char* bundle_dir, const char* query,
//                                      _Bool simple_mode, size_t num_threads, size_t batch_size);
// extern unsigned int* bundleq_parse_bundles(const char* spec, unsigned int max_bundle, size_t* out_len);
// extern void bundleq_free_bundle_list(unsigned int* ptr, size_t len);
// extern int bundleq_process(BundleqProcessor processor, const unsigned int* bundles, size_t count,
//                             int (*callback)(const char*), CStats* out_stats);
// extern unsigned int bundleq_get_last_bundle(const char* bundle_dir);
// extern void bundleq_free(BundleqProcessor processor);
import "C"
import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"
)

type Stats struct {
	Operations   uint64
	Matches      uint64
	TotalBytes   uint64
	MatchedBytes uint64
}

type Processor struct {
	ptr      C.BundleqProcessor
	callback OutputCallback
	mu       sync.Mutex
}

type OutputCallback func(batch string) error

var (
	processorRegistry   = make(map[uintptr]*Processor)
	processorRegistryMu sync.Mutex
)

//export goCallback
func goCallback(batch *C.char) C.int {
	str := C.GoString(batch)

	processorRegistryMu.Lock()
	var callback OutputCallback
	for _, p := range processorRegistry {
		p.mu.Lock()
		if p.callback != nil {
			callback = p.callback
			p.mu.Unlock()
			break
		}
		p.mu.Unlock()
	}
	processorRegistryMu.Unlock()

	if callback != nil {
		if err := callback(str); err != nil {
			return -1
		}
	}
	return 0
}

// NewProcessor creates a new bundle processor
func NewProcessor(bundleDir, query string, simpleMode bool, numThreads, batchSize int) (*Processor, error) {
	cBundleDir := C.CString(bundleDir)
	defer C.free(unsafe.Pointer(cBundleDir))

	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	if numThreads == 0 {
		numThreads = runtime.NumCPU()
	}

	ptr := C.bundleq_new(cBundleDir, cQuery, C._Bool(simpleMode), C.size_t(numThreads), C.size_t(batchSize))
	if ptr == nil {
		return nil, fmt.Errorf("failed to create processor")
	}

	p := &Processor{ptr: ptr}

	processorRegistryMu.Lock()
	processorRegistry[uintptr(ptr)] = p
	processorRegistryMu.Unlock()

	runtime.SetFinalizer(p, (*Processor).Close)
	return p, nil
}

// ParseBundles parses bundle range specification
func ParseBundles(spec string, maxBundle uint32) ([]uint32, error) {
	cSpec := C.CString(spec)
	defer C.free(unsafe.Pointer(cSpec))

	var outLen C.size_t
	ptr := C.bundleq_parse_bundles(cSpec, C.uint(maxBundle), &outLen)
	if ptr == nil {
		return nil, fmt.Errorf("failed to parse bundle range")
	}
	defer C.bundleq_free_bundle_list(ptr, outLen)

	bundles := make([]uint32, int(outLen))
	slice := unsafe.Slice(ptr, int(outLen))
	for i, v := range slice {
		bundles[i] = uint32(v)
	}

	return bundles, nil
}

// GetLastBundle returns the last bundle number from the index
func GetLastBundle(bundleDir string) (uint32, error) {
	cDir := C.CString(bundleDir)
	defer C.free(unsafe.Pointer(cDir))

	lastBundle := C.bundleq_get_last_bundle(cDir)
	if lastBundle == 0 {
		return 0, fmt.Errorf("failed to load index")
	}

	return uint32(lastBundle), nil
}

// Process processes the bundles
func (p *Processor) Process(bundles []uint32, callback OutputCallback) (Stats, error) {
	if len(bundles) == 0 {
		return Stats{}, fmt.Errorf("no bundles to process")
	}

	p.mu.Lock()
	p.callback = callback
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.callback = nil
		p.mu.Unlock()
	}()

	var cStats C.CStats

	// Use the helper function from callback.c
	result := C.bundleq_process(
		p.ptr,
		(*C.uint)(unsafe.Pointer(&bundles[0])),
		C.size_t(len(bundles)),
		C.get_go_callback_ptr(),
		&cStats,
	)

	if result != 0 {
		return Stats{}, fmt.Errorf("processing failed")
	}

	return Stats{
		Operations:   uint64(cStats.operations),
		Matches:      uint64(cStats.matches),
		TotalBytes:   uint64(cStats.total_bytes),
		MatchedBytes: uint64(cStats.matched_bytes),
	}, nil
}

// Close frees the processor
func (p *Processor) Close() {
	if p.ptr != nil {
		processorRegistryMu.Lock()
		delete(processorRegistry, uintptr(p.ptr))
		processorRegistryMu.Unlock()

		C.bundleq_free(p.ptr)
		p.ptr = nil
	}
}
