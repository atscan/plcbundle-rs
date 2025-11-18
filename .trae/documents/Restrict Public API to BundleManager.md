## Goal
Expose only the `BundleManager` API (and the minimal inputs/outputs it requires) to library users, hiding all other modules and helpers.

## Strategy
- Turn crate root into a narrow facade that re-exports `BundleManager` and its associated types.
- Make all non-essential modules private (`mod` or `pub(crate)`) instead of `pub mod`.
- Keep only the re-exports required by `BundleManager` method signatures.
- Gate CLI/server/FFI pieces behind features so they are not part of the default public surface.

## Current Surface (reference)
- `BundleManager` is defined at `src/manager.rs:83` and implemented starting `src/manager.rs:204`.
- Crate root re-exports a large set starting at `src/lib.rs:73`.
- Many modules are public at `src/lib.rs:33–55`.

## Changes in `src/lib.rs`
- Module visibility:
  - Change all `pub mod ...` to `pub(crate) mod ...` (or `mod ...`) except `manager`.
  - Examples: `bundle_format`, `cache`, `constants`, `did_index`, `format`, `handle_resolver`, `index`, `iterators`, `mempool`, `operations`, `options`, `plc_client`, `processor`, `remote`, `resolver`, `runtime`, `sync`, `verification`, `server` become crate-private.
- Re-export trimming:
  - Remove broad re-exports at `src/lib.rs:57–93`.
  - Keep only items needed by `BundleManager` signatures:
    - Constructors/options: `ManagerOptions`, `IntoManagerOptions`.
    - Query/export: `QuerySpec`, `QueryMode`, `ExportSpec`, `ExportIterator`, `QueryIterator`, `RangeIterator`.
    - Loading: `LoadOptions`, `LoadResult`.
    - Operations: `Operation`, `OperationRequest`, `OperationWithLocation`, `OperationResult`.
    - DID: `ResolveResult`, `DIDIndexStats`, `DIDLookupStats`, `DIDLookupTimings`, `DIDOperationsResult` (if present; otherwise keep the return struct used in `get_did_operations()` at `src/manager.rs:510–586`).
    - Verification: `VerifySpec`, `VerifyResult`, `ChainVerifySpec`, `ChainVerifyResult`.
    - Info/Stats: `InfoFlags`, `BundleInfo`, `ManagerStats`, `SizeInfo`.
    - Rollback: `RollbackSpec`, `RollbackPlan`, `RollbackResult`.
    - Warm-up: `WarmUpSpec`, `WarmUpStrategy`.
  - Re-export from `manager` when possible, otherwise re-export specific types from their modules without exposing the whole module.
- Optional prelude:
  - Add `pub mod prelude` (facade-only) that re-exports the curated set for `use plcbundle::prelude::*;` consumers.

## Changes in internal modules
- Where an internal type is only used by `BundleManager`, make it `pub(crate)` and reference via `crate::module::Type` from `manager`.
- Avoid leaking helpers (functions and structs) by keeping them `pub(crate)` or private.
- If a `BundleManager` method requires a type from another module, prefer re-exporting that type at the crate root rather than making the module public.

## Feature gating
- Hide CLI/server by default:
  - Ensure `server` remains behind `#[cfg(feature = "server")]` and is not re-exported.
  - Keep `ffi` off by default; expose only via `feature = "ffi"` for `cdylib` consumers.
- Confirm `[lib] crate-type = ["cdylib", "rlib"]` remains, but do not publicly expose `ffi` unless the feature is enabled.

## Docs and examples
- Update crate-level docs to show only `use plcbundle::{BundleManager, ManagerOptions, QuerySpec, BundleRange, QueryMode};` (already present in `src/lib.rs:11–25`).
- Ensure rustdoc for internal modules is hidden or marked as crate-private.
- Optionally add `#![deny(missing_docs)]` and document the curated public types.

## Validation
- Build and run rustdoc to verify only the curated items appear in the public API.
- Compile with `--all-features` to ensure feature-gated code compiles.
- Run existing tests; add a test that `use plcbundle::*;` only imports the intended items.

## Result
Library users see a minimal surface:
- `use plcbundle::{BundleManager, ManagerOptions, QuerySpec, BundleRange, QueryMode, ...}`
- Everything else is crate-private, accessible only via `BundleManager` methods.

Confirm, and I will implement these visibility and re-export adjustments in `src/lib.rs` and tighten module visibilities accordingly.