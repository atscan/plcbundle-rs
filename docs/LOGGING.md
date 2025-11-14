# Logging System

The CLI uses a unified logging system based on the `log` crate + `env_logger`.

## Usage

### In Command Files

```rust
// Info messages (shown in normal mode)
log::info!("Processing {} bundles", count);

// Debug messages (shown only with -v/--verbose)
log::debug!("📦 Index: v{} ({})", version, origin);

// Warnings (shown in normal mode)
log::warn!("Index is behind (has bundle {}, repo has {})", last, current);

// Errors (always shown, even with --quiet)
log::error!("DID index does not exist");
```

### Log Levels

| Level | Flag | When Shown | Use For |
|-------|------|------------|---------|
| `ERROR` | Always | All modes | Critical errors |
| `WARN` | `--quiet` excluded | Normal, Verbose | Warnings that need attention |
| `INFO` | Default | Normal, Verbose | Standard progress/status messages |
| `DEBUG` | `-v, --verbose` | Verbose only | Detailed diagnostics, timing info |
| `TRACE` | Not used | - | Reserved for future use |

### Command-Line Flags

```bash
# Normal mode (INFO level)
plcbundle-rs did resolve did:plc:...

# Verbose mode (DEBUG level) - shows all timing and diagnostics
plcbundle-rs did resolve did:plc:... -v

# Quiet mode (ERROR only) - only JSON output and critical errors
plcbundle-rs did resolve did:plc:... --quiet
```

## Output Guidelines

### Use `log::info!()` for:
- Operation start/completion messages
- Progress summaries
- Final statistics
- User-facing status updates

### Use `log::debug!()` for:
- Detailed timing information
- Internal state information
- Diagnostic messages
- Performance metrics

### Use `log::error!()` for:
- Operation failures
- Missing prerequisites
- Invalid input

### Use `log::warn!()` for:
- Non-critical issues
- Deprecation notices
- Performance warnings

### Use `println!()` for:
- Actual data output (JSON, formatted results)
- Structured output that should ALWAYS appear

### Use `eprint!()` / `eprintln!()` for:
- Progress bars (dynamic updates like `\r`)
- Live counters

## Examples

### Good ✅
```rust
// Verbose diagnostics
log::debug!("Shard {:02x} loaded, size: {} bytes", shard_num, size);

// Normal progress
log::info!("Building DID index...");
log::info!("✅ Complete in {:?}", elapsed);

// Actual output
println!("{}", json);
```

### Bad ❌
```rust
// Don't use eprintln! for static messages
eprintln!("Processing bundles...");  // Use log::info! instead

// Don't use println! for diagnostics
println!("DEBUG: shard loaded");  // Use log::debug! instead

// Don't use log::info! for JSON output
log::info!("{}", json);  // Use println! instead
```

## Environment Variable

The logger respects `RUST_LOG` environment variable:

```bash
# Override log level
RUST_LOG=trace plcbundle-rs did resolve did:plc:...

# Filter by module
RUST_LOG=plcbundle::did_index=debug plcbundle-rs did resolve did:plc:...
```

## Implementation

The logger is initialized in `main()` based on CLI flags:

```rust
fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Initialize logger
    commands::logger::init_logger(cli.verbose, cli.quiet);
    
    // ... rest of main
}
```

Logger configuration in `src/bin/commands/logger.rs`:
- **Quiet mode**: Only ERROR level
- **Normal mode**: INFO level and above
- **Verbose mode**: DEBUG level and above

Format:
- DEBUG/ERROR: Include `[LEVEL]` prefix
- INFO/WARN: Clean output without prefix

