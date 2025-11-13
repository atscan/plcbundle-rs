# PLC Bundle Go Bindings

High-performance Go bindings for the PLC Bundle Rust library.

## Installation

```bash
# Build the Rust library first
cd ../..
cargo build --release
```

## Quick Start

```go
import "plcbundle"

manager, err := plcbundle.NewBundleManager("./bundles")
if err != nil {
    log.Fatal(err)
}
defer manager.Close()
```

## API Reference

### BundleManager Methods

- `NewBundleManager(bundleDir string) (*BundleManager, error)` - Create a new manager
- `Close()` - Free resources

**Loading:**
- `LoadBundle(bundleNum uint32, opts *LoadOptions) (*LoadResult, error)`
- `GetOperationsBatch(requests []OperationRequest) ([]Operation, error)`

**DID Operations:**
- `GetDIDOperations(did string) ([]Operation, error)`
- `BatchResolveDIDs(dids []string) (map[string][]Operation, error)`

**Query:**
- `Query(queryStr string, bundleStart, bundleEnd uint32) ([]Operation, error)`

**Export:**
- `Export(spec ExportSpec, opts *ExportOptions) (*ExportStats, error)`

**Verification:**
- `VerifyBundle(bundleNum uint32, checkHash, checkChain bool) (*VerifyResult, error)`
- `VerifyChain(startBundle, endBundle uint32) error`

**Information:**
- `GetBundleInfo(bundleNum uint32, includeOperations, includeDIDs bool) (*BundleInfo, error)`

**Cache Management:**
- `PrefetchBundles(bundleNums []uint32)`
- `WarmUp(strategy WarmUpStrategy, startBundle, endBundle uint32) error`
- `ClearCaches()`

**DID Index:**
- `RebuildDIDIndex(progressCallback func(uint32, uint64)) (*RebuildStats, error)`
- `GetDIDIndexStats() (*DIDIndexStats, error)`

**Observability:**
- `GetStats() (*ManagerStats, error)`

## Types

**Options & Results:**
- `LoadOptions` - Bundle loading options
- `LoadResult` - Load operation result
- `ExportSpec` - Export configuration
- `ExportOptions` - Export options (writer, progress callback)
- `ExportStats` - Export statistics
- `VerifyResult` - Verification result

**Data:**
- `Operation` - Operation data (DID, OpType, CID, JSON, etc.)
- `OperationRequest` - Request for specific operation
- `BundleInfo` - Bundle metadata

**Statistics:**
- `ManagerStats` - Manager performance stats
- `DIDIndexStats` - DID index statistics
- `RebuildStats` - Index rebuild statistics

**Constants:**
- `WarmUpStrategy` - WarmUpRecent, WarmUpAll, WarmUpRange

## Examples

**Export:**
```go
spec := plcbundle.ExportSpec{
    BundleStart: 1,
    BundleEnd:   100,
    Format:      0, // 0=jsonl, 1=json, 2=csv
}
opts := &plcbundle.ExportOptions{Writer: os.Stdout}
stats, err := manager.Export(spec, opts)
```

**Query:**
```go
ops, err := manager.Query("@", 1, 100) // All operations from bundles 1-100
```

**DID Lookup:**
```go
ops, err := manager.GetDIDOperations("did:plc:example")
```
