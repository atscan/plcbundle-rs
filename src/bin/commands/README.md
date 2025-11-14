# Commands Module Structure

This directory contains the modular organization of CLI command implementations for `plcbundle-rs`.

## Directory Structure

```
src/bin/commands/
├── mod.rs          # Module declarations
├── utils.rs        # Shared utility functions ✅ COMPLETE
├── op.rs           # Operation inspection commands ✅ COMPLETE
├── export.rs       # Export command ✅ COMPLETE
├── query.rs        # Query command ✅ COMPLETE
├── info.rs         # Info command (TODO: extract from main.rs)
├── verify.rs       # Verify command (TODO: extract from main.rs)
└── stats.rs        # Stats command (TODO: extract from main.rs)
```

## Completed Modules

### ✅ `utils.rs` - Shared utility functions
  - `format_number()` - Format numbers with thousand separators
  - `format_bytes()` - Format bytes in human-readable format
  - `parse_bundle_spec()` - Parse bundle specification strings
  - `parse_bundle_range()` - Parse bundle ranges (e.g., "1-10,15,20-25")

### ✅ `op.rs` - Operation inspection commands (~170 lines)
  - `cmd_op_get()` - Get operation as JSON
  - `cmd_op_show()` - Show operation (human-readable)
  - `cmd_op_find()` - Find operation by CID
  - `parse_op_position()` - Parse global position or bundle+position

### ✅ `export.rs` - Export command (~300 lines)
  - `cmd_export()` - Export operations with filtering and formatting
  - Supports JSONL, JSON, CSV, Parquet formats
  - Fast path optimization for simple pass-through
  - Filters: --after, --did, --op-type, --count

### ✅ `query.rs` - Query command (~170 lines)
  - `cmd_query()` - Query operations with JMESPath or simple path
  - `StdoutHandler` - Output handler for streaming results
  - Auto-detection of query mode
  - Progress reporting and statistics

## TODO

Extract remaining commands from `main.rs` into their respective modules:
- `info.rs` - Extract `cmd_info()` and `print_bundle_info()`
- `verify.rs` - Extract `cmd_verify()`
- `stats.rs` - Extract `cmd_stats()`

## Benefits

1. **Better Organization**: Each command has its own module
2. **Easier Maintenance**: Changes to one command don't affect others
3. **Clearer Dependencies**: Shared utilities are explicitly in `utils.rs`
4. **Incremental Refactoring**: Can move commands one at a time
5. **Reduced Main File Size**: Main file reduced from 1130 to ~490 lines (57% reduction!)
6. **Modular Testing**: Each command can be tested independently
7. **Parallel Development**: Multiple developers can work on different commands

## Usage Pattern

Commands use the module functions like this:

```rust
// In main.rs
match cli.command {
    Commands::Op { command } => {
        match command {
            OpCommands::Get { dir, bundle, position } => {
                commands::op::cmd_op_get(dir, bundle, position, cli.quiet)?;
            }
            // ...
        }
    }
}
```

Utility functions are accessed as:

```rust
commands::utils::format_number(count)
commands::utils::format_bytes(size)
commands::utils::parse_bundle_spec(spec, max_bundle)?
```

