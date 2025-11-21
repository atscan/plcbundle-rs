# Memory Profiling with `dhat`

This project integrates the [`dhat`](https://docs.rs/dhat/latest/dhat/) heap profiler to analyze memory allocations across CLI commands. It works on macOS/Linux/Windows and writes a JSON profile you can open in DHAT’s HTML viewer.

## Overview

- Uses `dhat` as the global allocator to track allocations.
- Starts the profiler at process startup and stops on exit.
- Saves a profile file `dhat-heap.json` in the current working directory.
- Best used in release mode with line-level debug info for clear backtraces.

## Prerequisites

- Rust toolchain installed.
- This repository already wires `dhat`:
  - Global allocator and profiler start: `src/cli/mod.rs:6-8`, `src/cli/mod.rs:154-155`
  - Feature flag and release debug info: `Cargo.toml:76-79`, `Cargo.toml:82-84`

## Enable Profiling

Build and run with the profiling feature enabled:

```bash
cargo build --release --features dhat-heap
cargo run --release --features dhat-heap -- <COMMAND> [ARGS]
```

Examples:

```bash
# Show repository status (quiet run)
cargo run --release --features dhat-heap -- status --quiet

# List bundles
cargo run --release --features dhat-heap -- ls
```

Notes:

- Use release builds (`--release`) for realistic performance and readable backtraces.
- Avoid commands that immediately exit (`--help`, `--version`) if you want a profile; they may not exercise useful code paths.

## Output

After the command finishes, `dhat` prints a summary to stderr and writes `dhat-heap.json` in the current directory. For example:

```
dhat: Total:     433,983 bytes in 1,186 blocks
dhat: At t-gmax: 314,288 bytes in 986 blocks
dhat: At t-end:  806 bytes in 7 blocks
dhat: The data has been saved to dhat-heap.json, and is viewable with dhat/dh_view.html
```

## Viewing the Profile

Open DHAT’s viewer (`dh_view.html`) and load `dhat-heap.json`:

- Option 1: Clone the viewer
  - `git clone https://github.com/nnethercote/dhat-rs`
  - Open `dhat-rs/dhat/dh_view.html` in your browser
  - Use the “Load” button to select `dhat-heap.json`

- Option 2: Use the copy from crate docs
  - Refer to `dhat` crate documentation for viewer location and usage: https://docs.rs/dhat/latest/dhat/

The viewer provides allocation hot spots, call stacks, counts, sizes, and lifetimes.

## Customization

If you need to customize the output file name or backtrace trimming, switch from `new_heap()` to the builder:

```rust
// Replace the default initialization in main with:
let _profiler = dhat::Profiler::builder()
    .file_name(format!("heap-{}.json", std::process::id()))
    .trim_backtraces(Some(16))
    .build();
```

Backtrace trimming defaults to a small depth to keep profiles readable and fast. Increase the frame count if you need deeper stacks, noting this increases overhead and file size.

## Disabling Profiling

- Omit the feature flag: run without `--features dhat-heap`.
- The instrumentation is guarded by `#[cfg(feature = "dhat-heap")]`, so the binary runs with the normal allocator when the feature is disabled.

## Tips

- Profile realistic workloads (e.g., `sync`, `verify`, `server`) to see meaningful allocation behavior.
- Consider ignoring generated profiles if you don’t want them in version control:
  - Add `dhat-*.json` to `.gitignore`.
- `dhat` adds overhead; keep it off for normal runs.

## References

- Crate docs: https://docs.rs/dhat/latest/dhat/
- Profiler builder: https://docs.rs/dhat/latest/dhat/struct.ProfilerBuilder.html
- Repository integration points:
  - Global allocator: `src/cli/mod.rs:6-8`
  - Profiler start: `src/cli/mod.rs:154-155`
  - Feature and release profile: `Cargo.toml:76-79`, `Cargo.toml:82-84`