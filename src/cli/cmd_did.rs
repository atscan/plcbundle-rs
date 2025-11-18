// DID Resolution and Query commands
use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueHint};
use plcbundle::BundleManager;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};
use std::path::PathBuf;

#[derive(Args)]
#[command(
    about = "DID operations and queries",
    long_about = "Query and analyze DIDs stored in the bundle repository. These commands provide
comprehensive DID functionality including resolution to W3C DID documents, operation
history lookup, and cryptographic validation of DID operation chains.

All commands require a DID index to be built for optimal performance. The index
enables fast O(1) lookups by mapping DIDs to their bundle locations. Use 'index build'
to create the index if it doesn't exist.

The 'resolve' subcommand converts a DID (or handle) into a complete W3C DID document
by following the operation chain and applying all operations. The 'log' subcommand
shows all operations for a DID in chronological order. The 'audit' subcommand performs
cryptographic validation of the entire operation chain, detecting forks and verifying
signatures.

These commands are essential for DID-based applications, identity verification,
and understanding how DIDs evolve over time through their operation history.",
    help_template = crate::clap_help!(
        examples: "  # Resolve DID to current document\n  \
                   {bin} did resolve did:plc:524tuhdhh3m7li5gycdn6boe\n\n  \
                   # Show DID operation log\n  \
                   {bin} did log did:plc:524tuhdhh3m7li5gycdn6boe\n\n  \
                   # Show complete audit log\n  \
                   {bin} did history did:plc:524tuhdhh3m7li5gycdn6boe\n\n  \
                   # Validate DID chain\n  \
                   {bin} did validate did:plc:524tuhdhh3m7li5gycdn6boe"
    )
)]
pub struct DidCommand {
    #[command(subcommand)]
    pub command: DIDCommands,
}

#[derive(Args)]
#[command(
    about = "Resolve handle to DID",
    long_about = "Resolve an AT Protocol handle (e.g., example.bsky.social) to its
corresponding DID using a handle resolver service.

Handles are human-readable identifiers that map to DIDs, which are the
cryptographic identifiers used in the PLC directory. This command queries
the handle resolver to perform the lookup and displays the resulting DID.

By default uses the quickdid.smokesignal.tools resolver, but you can specify
a custom resolver URL with --handle-resolver if needed. This is useful for
testing with different resolver implementations or when the default resolver
is unavailable."
)]
pub struct HandleCommand {
    /// Handle to resolve (e.g., tree.fail)
    pub handle: String,

    /// Handle resolver URL (defaults to quickdid.smokesignal.tools)
    #[arg(long, value_hint = ValueHint::Url)]
    pub handle_resolver: Option<String>,
}

#[derive(Subcommand)]
pub enum DIDCommands {
    /// Resolve DID to current W3C DID document
    ///
    /// By default, pretty-prints with colors when outputting to a terminal.
    /// Use --raw to force raw JSON output (useful for piping).
    /// Use -q/--query to extract a value using JMESPath.
    #[command(
        alias = "doc",
        alias = "document",
        help_template = crate::clap_help!(
            examples: "  # Resolve DID to full document\n  \
                       {bin} did resolve did:plc:524tuhdhh3m7li5gycdn6boe\n\n  \
                       # Query with JMESPath\n  \
                       {bin} did resolve did:plc:524tuhdhh3m7li5gycdn6boe -q 'id'\n  \
                       {bin} did resolve did:plc:524tuhdhh3m7li5gycdn6boe -q 'verificationMethod[0].id'\n  \
                       {bin} did resolve did:plc:524tuhdhh3m7li5gycdn6boe -q 'service[].id'\n\n  \
                       # Force raw JSON output\n  \
                       {bin} did resolve did:plc:524tuhdhh3m7li5gycdn6boe --raw"
        )
    )]
    Resolve {
        /// DID or handle to resolve
        #[arg(value_name = "DID")]
        did: Option<String>,

        /// Handle resolver URL (e.g., https://quickdid.smokesignal.tools)
        #[arg(long, value_hint = ValueHint::Url)]
        handle_resolver: Option<String>,

        /// Query DID document JSON using JMESPath expression
        ///
        /// Extracts a value from the DID document using JMESPath.
        /// Examples: 'id', 'verificationMethod[0].id', 'service[].id'
        #[arg(short = 'q', long = "query")]
        query: Option<String>,

        /// Force raw JSON output (no pretty printing, no colors)
        ///
        /// By default, output is pretty-printed with colors when writing to a terminal.
        /// Use this flag to force raw JSON output, useful for piping to other tools.
        #[arg(long = "raw")]
        raw: bool,

        /// Compare DID document with remote PLC directory
        ///
        /// Fetches the DID document from the remote PLC directory and compares it
        /// with the document resolved from local bundles. Shows differences if any.
        /// If URL is not provided, uses the repository origin from the local index.
        #[arg(long, value_name = "URL", num_args = 0..=1, value_hint = ValueHint::Url)]
        compare: Option<Option<String>>,
    },

    /// Show DID operation log
    #[command(alias = "lookup", alias = "find", alias = "get", alias = "history")]
    Log {
        /// DID to show log for
        did: String,

        /// Output as JSON
        #[arg(long)]
        json: bool,

        /// Output format: bundle,position,global,status,cid,created_at,nullified
        ///
        /// Available fields:
        ///   - bundle: bundle number
        ///   - position: position within bundle
        ///   - global/global_pos: global position (bundle * 10000 + position)
        ///   - status: operation status (✓ or ✗)
        ///   - cid: operation CID
        ///   - created_at/created/date/time: creation timestamp
        ///   - nullified: nullified flag
        #[arg(long, default_value = "bundle,position,global,status,cid,created_at")]
        format: String,

        /// Omit header row
        #[arg(long)]
        no_header: bool,

        /// Field separator (default: tab)
        #[arg(long, default_value = "\t")]
        separator: String,

        /// Reverse order (show newest first)
        #[arg(long)]
        reverse: bool,
    },

    /// Process multiple DIDs from file or stdin (TODO)
    Batch {
        /// Action: lookup, resolve, export
        #[arg(long, default_value = "lookup")]
        action: String,

        /// Number of parallel threads (0 = auto-detect)
        #[arg(short = 'j', long, default_value = "0")]
        threads: usize,

        /// Output file
        #[arg(short, long, value_hint = ValueHint::FilePath)]
        output: Option<PathBuf>,

        /// Read from stdin
        #[arg(long)]
        stdin: bool,
    },

    /// Audit DID operation chain from local bundles
    #[command(alias = "validate")]
    Audit {
        /// DID to audit (e.g., did:plc:ewvi7nxzyoun6zhxrhs64oiz)
        did: String,

        /// Show verbose output including all operations
        #[arg(short, long)]
        verbose: bool,

        /// Only show summary (no operation details)
        #[arg(short, long)]
        quiet: bool,

        /// Show fork visualization
        #[arg(long)]
        fork_viz: bool,
    },
}

pub fn run_did(cmd: DidCommand, dir: PathBuf, global_verbose: bool) -> Result<()> {
    match cmd.command {
        DIDCommands::Resolve {
            did,
            handle_resolver,
            query,
            raw,
            compare,
        } => {
            cmd_did_resolve(dir, did, handle_resolver, global_verbose, query, raw, compare)?;
        }
        DIDCommands::Log { did, json, format, no_header, separator, reverse } => {
            cmd_did_lookup(dir, did, global_verbose, json, format, no_header, separator, reverse)?;
        }
        DIDCommands::Batch {
            action,
            threads,
            output,
            stdin,
        } => {
            cmd_did_batch(dir, action, threads, output, stdin)?;
        }
        DIDCommands::Audit {
            did,
            verbose,
            quiet,
            fork_viz,
        } => {
            cmd_did_validate(dir, did, verbose, quiet, fork_viz)?;
        }
    }
    Ok(())
}

pub fn run_handle(cmd: HandleCommand, dir: PathBuf) -> Result<()> {
    cmd_did_handle(dir, cmd.handle, cmd.handle_resolver)?;
    Ok(())
}

// DID RESOLVE - Convert DID to W3C DID Document

pub fn cmd_did_resolve(
    dir: PathBuf,
    input: Option<String>,
    handle_resolver_url: Option<String>,
    verbose: bool,
    query: Option<String>,
    raw: bool,
    compare: Option<Option<String>>,
) -> Result<()> {
    let input = input.ok_or_else(|| anyhow::anyhow!("DID or handle is required"))?;
    use plcbundle::constants;

    // Use default resolver if none provided
    let resolver_url =
        handle_resolver_url.or_else(|| Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string()));

    // Initialize manager with handle resolver and preload mempool (for resolve command)
    let options = plcbundle::ManagerOptions {
        handle_resolver_url: resolver_url,
        preload_mempool: true,
        verbose: false,
    };
    let manager = BundleManager::new(dir, options)?;

    // Resolve handle to DID if needed
    let (did, handle_resolve_time) = manager.resolve_handle_or_did(&input)?;

    if handle_resolve_time > 0 {
        log::info!(
            "Resolved handle: {} → {} (in {}ms)",
            input,
            did,
            handle_resolve_time
        );
    } else {
        log::info!("Resolving DID: {}", did);
    }

    // Get resolve result with stats
    let result = manager.resolve_did(&did)?;

    // If compare is enabled, fetch remote document and compare
    if let Some(maybe_url) = compare {
        use tokio::runtime::Runtime;
        use plcbundle::plc_client::PLCClient;
        use std::time::Instant;

        // Use provided URL, or use repository origin, or fall back to default
        let plc_url = match maybe_url {
            Some(url) if !url.is_empty() => {
                if verbose {
                    log::info!("Using provided PLC directory URL: {}", url);
                }
                url
            },
            _ => {
                // Get origin from local repository index
                let local_index = manager.get_index();
                let origin = &local_index.origin;
                
                // If origin is "local" or empty, use default PLC directory
                let url = if origin == "local" || origin.is_empty() {
                    if verbose {
                        log::info!("Origin is '{}', using default PLC directory: {}", origin, constants::DEFAULT_PLC_DIRECTORY_URL);
                    }
                    constants::DEFAULT_PLC_DIRECTORY_URL.to_string()
                } else {
                    if verbose {
                        log::info!("Using repository origin as PLC directory: {}", origin);
                    }
                    origin.clone()
                };
                url
            }
        };

        eprintln!("🔍 Comparing with remote PLC directory...");
        
        if verbose {
            log::info!("Target PLC directory: {}", plc_url);
            log::info!("DID to fetch: {}", did);
        }
        
        let fetch_start = Instant::now();
        let rt = Runtime::new().context("Failed to create tokio runtime")?;
        let (remote_doc, remote_json_raw): (plcbundle::DIDDocument, String) = rt.block_on(async {
            let client = PLCClient::new(&plc_url)
                .context("Failed to create PLC client")?;
            if verbose {
                log::info!("Created PLC client, fetching DID document...");
            }
            // Fetch both the parsed document and raw JSON for accurate comparison
            let raw_json = client.fetch_did_document_raw(&did).await?;
            let parsed_doc = client.fetch_did_document(&did).await?;
            Ok::<(plcbundle::DIDDocument, String), anyhow::Error>((parsed_doc, raw_json))
        })
        .context("Failed to fetch DID document from remote PLC directory")?;
        let fetch_duration = fetch_start.elapsed();

        if verbose {
            log::info!("Fetched DID document in {:?}", fetch_duration);
        }

        eprintln!("✅ Fetched remote document from {}", plc_url);
        eprintln!();

        // Compare documents and return early (don't show full document)
        compare_did_documents(&result.document, &remote_doc, &remote_json_raw, &did, &plc_url, fetch_duration)?;
        return Ok(());
    }

    // Get DID index for shard calculation (only for PLC DIDs)
    if did.starts_with("did:plc:") {
        let identifier = &did[8..]; // Strip "did:plc:" prefix
        let shard_num = calculate_shard_for_display(identifier);
        log::debug!(
            "DID {} -> identifier '{}' -> shard {:02x}",
            did,
            identifier,
            shard_num
        );
    }

    if let Some(stats) = &result.shard_stats {
        log::debug!(
            "Shard {:02x} loaded, size: {} bytes",
            result.shard_num,
            stats.shard_size
        );

        let reduction = if stats.total_entries > 0 {
            ((stats.total_entries - stats.prefix_narrowed_to) as f64 / stats.total_entries as f64)
                * 100.0
        } else {
            0.0
        };

        log::debug!(
            "Prefix index narrowed search: {} entries → {} entries ({:.1}% reduction)",
            stats.total_entries,
            stats.prefix_narrowed_to,
            reduction
        );
        log::debug!(
            "Binary search found {} locations after {} attempts",
            stats.locations_found,
            stats.binary_search_attempts
        );
    }

    if verbose {
        // Convert handle resolution time to Duration
        let handle_resolve_duration = std::time::Duration::from_millis(handle_resolve_time);

        if handle_resolve_time > 0 {
            log::info!("Handle resolution: {:?}", handle_resolve_duration);
        }

        // Show mempool lookup time
        log::info!("Mempool check: {:?}", result.mempool_time);

        // Show detailed index lookup timings if available
        if let Some(ref timings) = result.lookup_timings {
            log::info!("");
            log::info!("Index Lookup Breakdown:");
            log::info!("  Extract ID:    {:?}", timings.extract_identifier);
            log::info!("  Calc shard:    {:?}", timings.calculate_shard);
            log::info!(
                "  Load shard:    {:?} ({})",
                timings.load_shard,
                if timings.cache_hit {
                    "cache hit"
                } else {
                    "cache miss"
                }
            );

            // Search breakdown
            log::info!("  Search:");
            if let Some(ref base_time) = timings.base_search_time {
                log::info!("    Base shard:  {:?}", base_time);
            }
            if !timings.delta_segment_times.is_empty() {
                let total_delta_time: std::time::Duration = timings
                    .delta_segment_times
                    .iter()
                    .map(|(_, time)| *time)
                    .sum();
                log::info!(
                    "    Delta segs:  {:?} ({} segment{})",
                    total_delta_time,
                    timings.delta_segment_times.len(),
                    if timings.delta_segment_times.len() == 1 {
                        ""
                    } else {
                        "s"
                    }
                );

                // Show individual delta segments if there are multiple or if verbose
                if timings.delta_segment_times.len() > 1 || verbose {
                    for (seg_name, seg_time) in &timings.delta_segment_times {
                        log::info!("      - {}: {:?}", seg_name, seg_time);
                    }
                }
            }
            if timings.merge_time.as_nanos() > 0 {
                log::info!("    Merge/sort:  {:?}", timings.merge_time);
            }
            log::info!("    Search total: {:?}", timings.search);
            log::info!("  Index total:   {:?}", result.index_time);
        } else {
            log::info!("Index: {:?}", result.index_time);
        }

        log::info!("Load operation: {:?}", result.load_time);

        // Calculate true total including handle resolution
        // Note: result.total_time already includes mempool_time + index_time + load_time
        let true_total = handle_resolve_duration + result.total_time;
        let did_resolve_time = result.mempool_time + result.index_time + result.load_time;
        log::info!(
            "Total:          {:?} (handle: {:?} + did: {:?})",
            true_total,
            handle_resolve_duration,
            did_resolve_time
        );
        log::info!(
            "  DID resolve breakdown: mempool: {:?} + index: {:?} + load: {:?}",
            result.mempool_time,
            result.index_time,
            result.load_time
        );

        // Calculate global position and display source
        if result.bundle_number == 0 {
            // Operation from mempool
            let index = manager.get_index();
            let global_pos = plcbundle::mempool_position_to_global(index.last_bundle, result.position);
            log::info!(
                "Source: mempool position {} (global: {})\n",
                result.position,
                global_pos
            );
        } else {
            // Operation from bundle
            let global_pos = plcbundle::bundle_position_to_global(result.bundle_number, result.position);
            log::info!(
                "Source: bundle {}, position {} (global: {})\n",
                result.bundle_number,
                result.position,
                global_pos
            );
        }
    }

    // Convert document to JSON string
    let document_json = sonic_rs::to_string(&result.document)?;

    // If query is provided, apply JMESPath query
    let output_json = if let Some(query_expr) = query {
        // Compile JMESPath expression
        let expr = jmespath::compile(&query_expr)
            .map_err(|e| anyhow::anyhow!("Failed to compile JMESPath query '{}': {}", query_expr, e))?;

        // Execute query
        let data = jmespath::Variable::from_json(&document_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse DID document JSON: {}", e))?;

        let result = expr.search(&data)
            .map_err(|e| anyhow::anyhow!("JMESPath query failed: {}", e))?;

        if result.is_null() {
            anyhow::bail!("Query '{}' returned null (field not found)", query_expr);
        }

        // Convert result to JSON string
        // Note: jmespath uses serde_json internally, so we use serde_json here (not bundle/operation data)
        if result.is_string() {
            result.as_string().unwrap().to_string()
        } else {
            serde_json::to_string(&*result)
                .map_err(|e| anyhow::anyhow!("Failed to serialize query result: {}", e))?
        }
    } else {
        document_json
    };

    // Output document (always to stdout)
    // Determine if we should pretty print:
    // - Pretty print if stdout is a TTY (interactive terminal) and --raw is not set
    // - Use raw output if --raw is set or if output is piped (not a TTY)
    #[cfg(feature = "cli")]
    let should_pretty = !raw && super::utils::is_stdout_tty();
    #[cfg(not(feature = "cli"))]
    let should_pretty = !raw;

    if should_pretty {
        // Try to parse and pretty print the result
        match sonic_rs::from_str::<sonic_rs::Value>(&output_json) {
            Ok(parsed) => {
                let pretty_json = sonic_rs::to_string_pretty(&parsed)?;
                #[cfg(feature = "cli")]
                {
                    println!("{}", super::utils::colorize_json(&pretty_json));
                }
                #[cfg(not(feature = "cli"))]
                {
                    println!("{}", pretty_json);
                }
            }
            Err(_) => {
                // If it's not valid JSON (e.g., a string result), just output as-is
                println!("{}", output_json);
            }
        }
    } else {
        // Raw JSON output (compact, no colors)
        println!("{}", output_json);
    }

    Ok(())
}

/// Compare two DID documents and show differences
fn compare_did_documents(
    local: &plcbundle::DIDDocument,
    _remote: &plcbundle::DIDDocument,
    remote_json_raw: &str,
    _did: &str,
    remote_url: &str,
    fetch_duration: std::time::Duration,
) -> Result<()> {
    use sha2::{Digest, Sha256};

    eprintln!("📊 Document Comparison");
    eprintln!("═══════════════════════");
    
    // Construct the full URL that was fetched
    let full_url = format!("{}/{}", remote_url.trim_end_matches('/'), _did);
    eprintln!("   Remote URL: {}", full_url);
    eprintln!("   Fetch time: {:?}", fetch_duration);
    eprintln!();

    // Serialize local document (respects skip_serializing_if attributes)
    let local_json = serde_json::to_string(local)?;

    // Normalize both JSON strings by parsing and re-serializing with consistent formatting
    // This ensures we compare equivalent JSON structures, handling:
    // - Key ordering differences
    // - Whitespace differences
    // - Empty array representation (preserved from raw remote JSON)
    let local_normalized = normalize_json_for_comparison(&local_json)?;
    let remote_normalized = normalize_json_for_comparison(remote_json_raw)?;

    // Calculate SHA256 hashes of normalized JSON
    let mut local_hasher = Sha256::new();
    local_hasher.update(local_normalized.as_bytes());
    let local_hash = local_hasher.finalize();
    let local_hash_hex = format!("{:x}", local_hash);

    let mut remote_hasher = Sha256::new();
    remote_hasher.update(remote_normalized.as_bytes());
    let remote_hash = remote_hasher.finalize();
    let remote_hash_hex = format!("{:x}", remote_hash);

    eprintln!("   Local hash:  {}", local_hash_hex);
    eprintln!("   Remote hash: {}", remote_hash_hex);
    eprintln!();

    // Compare hashes
    if local_hash == remote_hash {
        eprintln!("✅ Documents are identical (SHA256 hashes match)");
        eprintln!();
        return Ok(());
    }

    eprintln!("⚠️  Documents differ (SHA256 hashes do not match)");
    eprintln!();

    #[cfg(feature = "similar")]
    {
        use similar::{ChangeTag, TextDiff};

        // Convert normalized JSON to pretty format for diff display
        let local_json_pretty = json_to_pretty(&local_normalized)?;
        let remote_json_pretty = json_to_pretty(&remote_normalized)?;

        // Use similar to compute and display colored diff
        let diff = TextDiff::from_lines(&local_json_pretty, &remote_json_pretty);

        eprintln!("Diff (Local → Remote):");
        eprintln!("──────────────────────");

        for (idx, group) in diff.grouped_ops(3).iter().enumerate() {
            if idx > 0 {
                eprintln!("...");
            }
            for op in group {
                for change in diff.iter_changes(op) {
                    use super::utils::colors;
                    let (sign, style) = match change.tag() {
                        ChangeTag::Delete => ("-", colors::RED),
                        ChangeTag::Insert => ("+", colors::GREEN),
                        ChangeTag::Equal => (" ", colors::DIM),
                    };
                    // Color the whole line
                    eprint!("{}{}{}{}", style, sign, change.value(), colors::RESET);
                }
            }
        }

        eprintln!();
        use super::utils::colors;
        eprintln!("Legend: {}Local{} {}Remote{}", colors::RED, colors::RESET, colors::GREEN, colors::RESET);
        eprintln!();
    }

    #[cfg(not(feature = "similar"))]
    {
        eprintln!("💡 Tip: Enable 'similar' feature for colored diff output");
        eprintln!();
    }

    Ok(())
}

/// Normalize JSON string for comparison by parsing and re-serializing
/// This ensures consistent representation for hash comparison
fn normalize_json_for_comparison(json: &str) -> Result<String> {
    // Parse JSON using sonic_rs (faster than serde_json)
    let value: sonic_rs::Value = sonic_rs::from_str(json)
        .map_err(|e| anyhow::anyhow!("Failed to parse JSON for normalization: {}", e))?;
    
    // Re-serialize with consistent formatting (compact, no whitespace)
    // This normalizes key ordering and whitespace differences
    let normalized = sonic_rs::to_string(&value)
        .map_err(|e| anyhow::anyhow!("Failed to serialize normalized JSON: {}", e))?;
    
    Ok(normalized)
}

/// Convert JSON string to pretty-printed format for display
fn json_to_pretty(json: &str) -> Result<String> {
    let value: sonic_rs::Value = sonic_rs::from_str(json)
        .map_err(|e| anyhow::anyhow!("Failed to parse JSON for pretty printing: {}", e))?;
    
    let pretty = sonic_rs::to_string_pretty(&value)
        .map_err(|e| anyhow::anyhow!("Failed to serialize pretty JSON: {}", e))?;
    
    Ok(pretty)
}

fn calculate_shard_for_display(identifier: &str) -> u8 {
    use fnv::FnvHasher;
    use std::hash::Hasher;

    let mut hasher = FnvHasher::default();
    hasher.write(identifier.as_bytes());
    let hash = hasher.finish() as u32;
    (hash % 256) as u8
}

// DID HANDLE - Resolve handle to DID

pub fn cmd_did_handle(
    dir: PathBuf,
    handle: String,
    handle_resolver_url: Option<String>,
) -> Result<()> {
    use plcbundle::constants;

    // Use default resolver if none provided
    let resolver_url =
        handle_resolver_url.or_else(|| Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string()));

    // Initialize manager with handle resolver and preload mempool (for resolve command)
    let options = plcbundle::ManagerOptions {
        handle_resolver_url: resolver_url,
        preload_mempool: true,
        verbose: false,
    };
    let manager = BundleManager::new(dir, options)?;

    // Resolve handle to DID
    let (did, resolve_time) = manager.resolve_handle_or_did(&handle)?;

    if resolve_time > 0 {
        println!("{}", did);
    } else {
        // If it was already a DID, just print it
        println!("{}", did);
    }

    Ok(())
}

// DID LOOKUP - Find all operations for a DID

pub fn cmd_did_lookup(
    dir: PathBuf,
    input: String,
    verbose: bool,
    json: bool,
    format: String,
    no_header: bool,
    separator: String,
    reverse: bool,
) -> Result<()> {
    use plcbundle::constants;
    use std::time::Instant;

    // Use default resolver if none provided (same pattern as cmd_did_resolve)
    let resolver_url = Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string());

    // Initialize manager with handle resolver and preload mempool (for resolve command)
    let options = plcbundle::ManagerOptions {
        handle_resolver_url: resolver_url,
        preload_mempool: true,
        verbose: false,
    };
    let manager = BundleManager::new(dir, options)?;

    // Resolve handle to DID if needed
    let (did, handle_resolve_time) = manager.resolve_handle_or_did(&input)?;

    if handle_resolve_time > 0 {
        log::info!(
            "Resolved handle: {} → {} (in {}ms)",
            input,
            did,
            handle_resolve_time
        );
    } else {
        log::info!("Looking up DID: {}", did);
    }

    // Get DID index for shard calculation (only for PLC DIDs)
    if did.starts_with("did:plc:") {
        let identifier = &did[8..]; // Strip "did:plc:" prefix
        let shard_num = calculate_shard_for_display(identifier);
        log::debug!(
            "DID {} -> identifier '{}' -> shard {:02x}",
            did,
            identifier,
            shard_num
        );
    }

    let total_start = Instant::now();

    // Lookup operations with locations and stats (for verbose mode)
    let result = manager.get_did_operations(&did, true, verbose)?;
    let ops_with_loc = result.operations_with_locations.unwrap_or_default();
    let shard_stats = result.stats.unwrap_or_default();
    let shard_num = result.shard_num.unwrap_or(0);
    let lookup_timings = result.lookup_timings.unwrap_or_default();
    let load_time = result.load_time.unwrap_or_default();

    let index_time = lookup_timings.extract_identifier
        + lookup_timings.calculate_shard
        + lookup_timings.load_shard
        + lookup_timings.search;
    let lookup_elapsed = index_time + load_time;

    // Show shard stats if available
    if verbose && shard_stats.total_entries > 0 {
        log::debug!(
            "Shard {:02x} loaded, size: {} bytes",
            shard_num,
            shard_stats.shard_size
        );

        let reduction = if shard_stats.total_entries > 0 {
            ((shard_stats.total_entries - shard_stats.prefix_narrowed_to) as f64
                / shard_stats.total_entries as f64)
                * 100.0
        } else {
            0.0
        };

        log::debug!(
            "Prefix index narrowed search: {} entries → {} entries ({:.1}% reduction)",
            shard_stats.total_entries,
            shard_stats.prefix_narrowed_to,
            reduction
        );
        log::debug!(
            "Binary search found {} locations after {} attempts",
            shard_stats.locations_found,
            shard_stats.binary_search_attempts
        );
    }

    let total_elapsed = total_start.elapsed();

    // Show verbose timing breakdown
    if verbose {
        // Convert handle resolution time to Duration
        let handle_resolve_duration = std::time::Duration::from_millis(handle_resolve_time);

        if handle_resolve_time > 0 {
            log::info!("Handle resolution: {:?}", handle_resolve_duration);
        }

        // Show detailed index lookup timings
        log::info!("Index Lookup Breakdown:");
        log::info!("  Extract ID:    {:?}", lookup_timings.extract_identifier);
        log::info!("  Calc shard:    {:?}", lookup_timings.calculate_shard);
        log::info!(
            "  Load shard:    {:?} ({})",
            lookup_timings.load_shard,
            if lookup_timings.cache_hit {
                "cache hit"
            } else {
                "cache miss"
            }
        );

        // Search breakdown
        log::info!("  Search:");
        if let Some(ref base_time) = lookup_timings.base_search_time {
            log::info!("    Base shard:  {:?}", base_time);
        }
        if !lookup_timings.delta_segment_times.is_empty() {
            let total_delta_time: std::time::Duration = lookup_timings
                .delta_segment_times
                .iter()
                .map(|(_, time)| *time)
                .sum();
            log::info!(
                "    Delta segs:  {:?} ({} segment{})",
                total_delta_time,
                lookup_timings.delta_segment_times.len(),
                if lookup_timings.delta_segment_times.len() == 1 {
                    ""
                } else {
                    "s"
                }
            );

            // Show individual delta segments if there are multiple
            if lookup_timings.delta_segment_times.len() > 1 {
                for (seg_name, seg_time) in &lookup_timings.delta_segment_times {
                    log::info!("      - {}: {:?}", seg_name, seg_time);
                }
            }
        }
        if lookup_timings.merge_time.as_nanos() > 0 {
            log::info!("    Merge/sort:  {:?}", lookup_timings.merge_time);
        }
        log::info!("    Search total: {:?}", lookup_timings.search);
        log::info!("  Index total:   {:?}", index_time);
        log::info!(
            "  Load operations: {:?} ({} operations)",
            load_time,
            ops_with_loc.len()
        );
        // Calculate true total including handle resolution
        let true_total = handle_resolve_duration + total_elapsed;
        log::info!(
            "Total:          {:?} (handle: {:?} + lookup: {:?})",
            true_total,
            handle_resolve_duration,
            total_elapsed
        );
        log::info!("");
    }

    // Separate bundled and mempool operations (mempool ops have bundle=0)
    let mut bundled_ops = Vec::new();
    let mut mempool_ops = Vec::new();
    for owl in ops_with_loc.iter() {
        if owl.bundle != 0 {
            bundled_ops.push(owl.clone());
        } else {
            mempool_ops.push(&owl.operation);
        }
    }

    if bundled_ops.is_empty() && mempool_ops.is_empty() {
        if json {
            println!("{{\"found\": false, \"operations\": []}}");
        } else {
            println!("DID not found (searched in {:?})", total_elapsed);
        }
        return Ok(());
    }

    if json {
        return output_lookup_json(
            &did,
            &bundled_ops,
            &mempool_ops,
            total_elapsed,
            lookup_elapsed,
            reverse,
        );
    }

    display_lookup_results(
        &did,
        &bundled_ops,
        &mempool_ops,
        total_elapsed,
        lookup_elapsed,
        verbose,
        &format,
        no_header,
        &separator,
        reverse,
    )
}

fn output_lookup_json(
    did: &str,
    bundled_ops: &[plcbundle::OperationWithLocation],
    mempool_ops: &[&plcbundle::Operation],
    total_elapsed: std::time::Duration,
    lookup_elapsed: std::time::Duration,
    reverse: bool,
) -> Result<()> {
    use serde_json::json;

    let mut bundled = Vec::new();
    for owl in bundled_ops {
        bundled.push(json!({
            "bundle": owl.bundle,
            "position": owl.position,
            "cid": owl.operation.cid,
            "nullified": owl.nullified,
            "created_at": owl.operation.created_at,
        }));
    }

    let mut mempool = Vec::new();
    for op in mempool_ops {
        mempool.push(json!({
            "cid": op.cid,
            "nullified": op.nullified,
            "created_at": op.created_at,
        }));
    }

    if reverse {
        bundled.reverse();
        mempool.reverse();
    }

    let output = json!({
        "found": true,
        "did": did,
        "timing": {
            "total_ms": total_elapsed.as_millis(),
            "lookup_ms": lookup_elapsed.as_millis(),
        },
        "bundled": bundled,
        "mempool": mempool,
    });

    println!("{}", sonic_rs::to_string_pretty(&output)?);
    Ok(())
}

fn display_lookup_results(
    did: &str,
    bundled_ops: &[plcbundle::OperationWithLocation],
    mempool_ops: &[&plcbundle::Operation],
    _total_elapsed: std::time::Duration,
    _lookup_elapsed: std::time::Duration,
    verbose: bool,
    format: &str,
    no_header: bool,
    separator: &str,
    reverse: bool,
) -> Result<()> {
    let nullified_count = bundled_ops.iter().filter(|owl| owl.nullified).count();
    let total_ops = bundled_ops.len() + mempool_ops.len();
    let active_ops = bundled_ops.len() - nullified_count + mempool_ops.len();

    // Parse format string
    let fields = parse_format_string(format);

    // Print summary header (unless no_header is set, but we always show the DID summary)
    if !no_header {
        println!("DID: {} ({} ops, {} active)", did, total_ops, active_ops);
    }

    if fields.is_empty() {
        return Ok(());
    }

    // Calculate column widths for alignment
    let mut column_widths = vec![0; fields.len()];
    
    // Check header widths
    for (i, field) in fields.iter().enumerate() {
        let header = get_lookup_field_header(field);
        column_widths[i] = column_widths[i].max(header.len());
    }
    
    // Check data widths
    for owl in bundled_ops.iter() {
        for (i, field) in fields.iter().enumerate() {
            let value = get_lookup_field_value(owl, None, field);
            column_widths[i] = column_widths[i].max(value.len());
        }
    }
    
    for op in mempool_ops.iter() {
        for (i, field) in fields.iter().enumerate() {
            let value = get_lookup_field_value_mempool(op, field);
            column_widths[i] = column_widths[i].max(value.len());
        }
    }

    // Print column header (unless disabled)
    if !no_header {
        let headers: Vec<String> = fields.iter()
            .enumerate()
            .map(|(i, f)| {
                let header = get_lookup_field_header(f);
                if separator == "\t" {
                    // For tabs, pad with spaces for alignment
                    format!("{:<width$}", header, width = column_widths[i])
                } else {
                    header
                }
            })
            .collect();
        println!("{}", headers.join(separator));
    }

    // Show operations - one per line
    let bundled_iter: Box<dyn Iterator<Item = &plcbundle::OperationWithLocation>> = if reverse {
        Box::new(bundled_ops.iter().rev())
    } else {
        Box::new(bundled_ops.iter())
    };

    for owl in bundled_iter {
        let values: Vec<String> = fields.iter()
            .enumerate()
            .map(|(i, f)| {
                let value = get_lookup_field_value(owl, None, f);
                if separator == "\t" {
                    // For tabs, pad with spaces for alignment
                    format!("{:<width$}", value, width = column_widths[i])
                } else {
                    value
                }
            })
            .collect();
        println!("{}", values.join(separator));
        
        if verbose && !owl.nullified {
            let op_val = &owl.operation.operation;
            if let Some(op_type) = op_val.get("type").and_then(|v| v.as_str()) {
                eprintln!("      type: {}", op_type);
            }
            if let Some(handle) = op_val.get("handle").and_then(|v| v.as_str()) {
                eprintln!("      handle: {}", handle);
            } else {
                if let Some(aka) = op_val.get("alsoKnownAs").and_then(|v| v.as_array()) {
                    if let Some(aka_str) = aka.first().and_then(|v| v.as_str()) {
                        let handle = aka_str.strip_prefix("at://").unwrap_or(aka_str);
                        eprintln!("      handle: {}", handle);
                    }
                }
            }
        }
    }

    // Show mempool operations
    let mempool_iter: Box<dyn Iterator<Item = &&plcbundle::Operation>> = if reverse {
        Box::new(mempool_ops.iter().rev())
    } else {
        Box::new(mempool_ops.iter())
    };

    for op in mempool_iter {
        let values: Vec<String> = fields.iter()
            .enumerate()
            .map(|(i, f)| {
                let value = get_lookup_field_value_mempool(op, f);
                if separator == "\t" {
                    // For tabs, pad with spaces for alignment
                    format!("{:<width$}", value, width = column_widths[i])
                } else {
                    value
                }
            })
            .collect();
        println!("{}", values.join(separator));
    }

    Ok(())
}

fn parse_format_string(format: &str) -> Vec<String> {
    format
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn get_lookup_field_header(field: &str) -> String {
    match field {
        "bundle" => "bundle",
        "position" | "pos" => "position",
        "global" | "global_pos" => "global",
        "status" => "status",
        "cid" => "cid",
        "created_at" | "created" | "date" | "time" => "created_at",
        "nullified" => "nullified",
        "date_short" => "date",
        "timestamp" | "unix" => "timestamp",
        _ => field,
    }
    .to_string()
}

fn get_lookup_field_value(
    owl: &plcbundle::OperationWithLocation,
    _mempool: Option<&plcbundle::Operation>,
    field: &str,
) -> String {
    match field {
        "bundle" => format!("{}", owl.bundle),
        "position" | "pos" => format!("{:04}", owl.position),
        "global" | "global_pos" => {
            let global_pos = plcbundle::bundle_position_to_global(owl.bundle, owl.position);
            format!("{}", global_pos)
        },
        "status" => {
            if owl.nullified {
                "✗".to_string()
            } else {
                "✓".to_string()
            }
        }
        "cid" => {
            owl.operation.cid.as_ref()
                .map(|c| c.clone())
                .unwrap_or_else(|| "".to_string())
        }
        "created_at" | "created" | "date" | "time" => owl.operation.created_at.clone(),
        "nullified" => {
            if owl.nullified {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        "date_short" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&owl.operation.created_at) {
                dt.format("%Y-%m-%d").to_string()
            } else {
                owl.operation.created_at.clone()
            }
        }
        "timestamp" | "unix" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&owl.operation.created_at) {
                format!("{}", dt.timestamp())
            } else {
                "0".to_string()
            }
        }
        _ => String::new(),
    }
}

fn get_lookup_field_value_mempool(op: &plcbundle::Operation, field: &str) -> String {
    match field {
        "bundle" => "mempool".to_string(),
        "position" | "pos" => "".to_string(),
        "global" | "global_pos" => "".to_string(),
        "status" => "✓".to_string(),
        "cid" => {
            op.cid.as_ref()
                .map(|c| c.clone())
                .unwrap_or_else(|| "".to_string())
        }
        "created_at" | "created" | "date" | "time" => op.created_at.clone(),
        "nullified" => "false".to_string(),
        "date_short" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&op.created_at) {
                dt.format("%Y-%m-%d").to_string()
            } else {
                op.created_at.clone()
            }
        }
        "timestamp" | "unix" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&op.created_at) {
                format!("{}", dt.timestamp())
            } else {
                "0".to_string()
            }
        }
        _ => String::new(),
    }
}


// DID BATCH - Process multiple DIDs (TODO)

pub fn cmd_did_batch(
    _dir: PathBuf,
    _action: String,
    _threads: usize,
    _output: Option<PathBuf>,
    _from_stdin: bool,
) -> Result<()> {
    log::error!("`did batch` not yet implemented");
    Ok(())
}

// DID VALIDATE - Validate DID audit log from plc.directory
//
// This implementation is based on the atproto-plc library examples:
// https://docs.rs/atproto-plc/0.2.0/atproto_plc/index.html
// Library author: ngerakines
// License: MIT OR Apache-2.0

use atproto_plc::{Operation, OperationChainValidator, PlcState, VerifyingKey};
use serde::Deserialize;
use std::collections::HashMap;

/// Audit log response from plc.directory
#[derive(Debug, Deserialize, Clone)]
struct AuditLogEntry {
    /// The DID this operation is for
    #[allow(dead_code)]
    did: String,

    /// The operation itself
    operation: Operation,

    /// CID of this operation
    cid: String,

    /// Timestamp when this operation was created
    #[serde(rename = "createdAt")]
    created_at: String,

    /// Nullified flag (if this operation was invalidated)
    #[serde(default)]
    nullified: bool,
}

/// Represents a fork point in the operation chain
#[derive(Debug, Clone)]
struct ForkPoint {
    /// The prev CID that all operations in this fork reference
    prev_cid: String,

    /// Competing operations at this fork
    operations: Vec<ForkOperation>,

    /// The winning operation (after fork resolution)
    #[allow(dead_code)]
    winner_cid: String,
}

/// An operation that's part of a fork
#[derive(Debug, Clone)]
struct ForkOperation {
    cid: String,
    #[allow(dead_code)]
    operation: Operation,
    timestamp: chrono::DateTime<chrono::Utc>,
    signing_key_index: Option<usize>,
    signing_key: Option<String>,
    is_winner: bool,
    rejection_reason: Option<String>,
}

pub fn cmd_did_validate(
    dir: PathBuf,
    did_input: String,
    verbose: bool,
    quiet: bool,
    fork_viz: bool,
) -> Result<()> {
    use plcbundle::constants;

    // Initialize manager
    let resolver_url = Some(constants::DEFAULT_HANDLE_RESOLVER_URL.to_string());
    let options = plcbundle::ManagerOptions {
        handle_resolver_url: resolver_url,
        preload_mempool: false,
        verbose: false,
    };
    let manager = BundleManager::new(dir, options)?;

    // Resolve handle to DID if needed (same pattern as other did commands)
    let (did_str, handle_resolve_time) = manager.resolve_handle_or_did(&did_input)?;

    if handle_resolve_time > 0 {
        log::info!(
            "Resolved handle: {} → {} (in {}ms)",
            did_input,
            did_str,
            handle_resolve_time
        );
    } else {
        log::info!("Validating DID: {}", did_str);
    }

    // Validate DID format for atproto_plc library (must be did:plc)
    if !did_str.starts_with("did:plc:") {
        eprintln!("❌ Error: Validation only supports did:plc identifiers");
        eprintln!("   Got: {}", did_str);
        return Err(anyhow::anyhow!("Only did:plc identifiers can be validated"));
    }

    if !quiet {
        println!("🔍 Fetching audit log for: {}", did_str);
        println!("   Source: local bundles");
        println!();
    }

    // Fetch operations from local bundles and mempool
    let result = manager.get_did_operations(&did_str, true, false)?;
    let ops_with_loc = result.operations_with_locations.unwrap_or_default();

    if ops_with_loc.is_empty() {
        eprintln!("❌ Error: No operations found for DID: {}", did_str);
        return Err(anyhow::anyhow!("No operations found"));
    }

    // Convert to audit log format
    let audit_log = convert_to_audit_log(ops_with_loc)?;

    if audit_log.is_empty() {
        eprintln!("❌ Error: No operations found in audit log");
        return Err(anyhow::anyhow!("No operations found"));
    }

    if !quiet {
        println!("📊 Audit Log Summary:");
        println!("   Total operations: {}", audit_log.len());
        println!("   Genesis operation: {}", audit_log[0].cid);
        println!("   Latest operation: {}", audit_log.last().unwrap().cid);
        println!();
    }

    // If fork visualization is requested, show that instead
    if fork_viz {
        return visualize_forks(&audit_log, &did_str, verbose);
    }

    // Display operations if verbose
    if verbose {
        println!("📋 Operations:");
        for (i, entry) in audit_log.iter().enumerate() {
            let status = if entry.nullified { "❌ NULLIFIED" } else { "✅" };
            println!(
                "   [{}] {} {} - {}",
                i, status, entry.cid, entry.created_at
            );
            if entry.operation.is_genesis() {
                println!("       Type: Genesis (creates the DID)");
            } else {
                println!("       Type: Update");
            }
            if let Some(prev) = entry.operation.prev() {
                println!("       Previous: {}", prev);
            }
        }
        println!();
    }

    // Detect forks and build canonical chain
    if !quiet {
        println!("🔐 Analyzing operation chain...");
        println!();
    }

    // Detect fork points and nullified operations
    let has_forks = detect_forks(&audit_log);
    let has_nullified = audit_log.iter().any(|e| e.nullified);

    if has_forks || has_nullified {
        if !quiet {
            if has_forks {
                println!("⚠️  Fork detected - multiple operations reference the same prev CID");
            }
            if has_nullified {
                println!("⚠️  Nullified operations detected - will validate canonical chain only");
            }
            println!();
        }

        // Use fork resolution to build canonical chain
        if verbose {
            println!("Step 1: Fork Resolution & Canonical Chain Building");
            println!("===================================================");
        }

        // Build operations and timestamps for fork resolution
        let operations: Vec<_> = audit_log.iter().map(|e| e.operation.clone()).collect();
        let timestamps: Vec<_> = audit_log
            .iter()
            .map(|e| {
                e.created_at
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .unwrap_or_else(|_| chrono::Utc::now())
            })
            .collect();

        // Resolve forks and get canonical chain
        match OperationChainValidator::validate_chain_with_forks(&operations, &timestamps) {
            Ok(final_state) => {
                if verbose {
                    println!("  ✅ Fork resolution complete");
                    println!("  ✅ Canonical chain validated successfully");
                    println!();

                    // Show which operations are in the canonical chain
                    println!("Canonical Chain Operations:");
                    println!("===========================");
                    let canonical_indices = build_canonical_chain_indices(&audit_log);
                    for idx in &canonical_indices {
                        let entry = &audit_log[*idx];
                        println!("  [{}] ✅ {} - {}", idx, entry.cid, entry.created_at);
                    }
                    println!();

                    if has_nullified {
                        println!("Nullified/Rejected Operations:");
                        println!("==============================");
                        for (i, entry) in audit_log.iter().enumerate() {
                            if entry.nullified && !canonical_indices.contains(&i) {
                                println!(
                                    "  [{}] ❌ {} - {} (nullified)",
                                    i, entry.cid, entry.created_at
                                );
                                if let Some(prev) = entry.operation.prev() {
                                    println!("      Referenced: {}", prev);
                                }
                            }
                        }
                        println!();
                    }
                }

                // Display final state
                display_final_state(&final_state, quiet);
                return Ok(());
            }
            Err(e) => {
                eprintln!();
                eprintln!("❌ Validation failed: {}", e);
                return Err(anyhow::anyhow!("Validation failed: {}", e));
            }
        }
    }

    // Simple linear chain validation (no forks or nullified operations)
    if verbose {
        println!("Step 1: Linear Chain Validation");
        println!("================================");
    }

    for i in 1..audit_log.len() {
        let prev_cid = audit_log[i - 1].cid.clone();
        let expected_prev = audit_log[i].operation.prev();

        if verbose {
            println!("  [{}] Checking prev reference...", i);
            println!("      Expected: {}", prev_cid);
        }

        if let Some(actual_prev) = expected_prev {
            if verbose {
                println!("      Actual:   {}", actual_prev);
            }

            if actual_prev != prev_cid {
                eprintln!();
                eprintln!(
                    "❌ Validation failed: Chain linkage broken at operation {}",
                    i
                );
                eprintln!("   Expected prev: {}", prev_cid);
                eprintln!("   Actual prev: {}", actual_prev);
                return Err(anyhow::anyhow!("Chain linkage broken"));
            }

            if verbose {
                println!("      ✅ Match - chain link valid");
            }
        } else if i > 0 {
            eprintln!();
            eprintln!(
                "❌ Validation failed: Non-genesis operation {} missing prev field",
                i
            );
            return Err(anyhow::anyhow!("Missing prev field"));
        }
    }

    if verbose {
        println!();
        println!("✅ Chain linkage validation complete");
        println!();
    }

    // Step 2: Validate cryptographic signatures
    if verbose {
        println!("Step 2: Cryptographic Signature Validation");
        println!("==========================================");
    }

    let mut current_rotation_keys: Vec<String> = Vec::new();

    for (i, entry) in audit_log.iter().enumerate() {
        if entry.nullified {
            if verbose {
                println!("  [{}] ⊘ Skipped (nullified)", i);
            }
            continue;
        }

        // For genesis operation, we can't validate signature without rotation keys
        // But we can extract them and validate subsequent operations
        if i == 0 {
            if verbose {
                println!("  [{}] Genesis operation - extracting rotation keys", i);
            }
            if let Some(rotation_keys) = entry.operation.rotation_keys() {
                current_rotation_keys = rotation_keys.to_vec();
                if verbose {
                    println!("      Rotation keys: {}", rotation_keys.len());
                    for (j, key) in rotation_keys.iter().enumerate() {
                        println!("        [{}] {}", j, key);
                    }
                    println!("      ⚠️  Genesis signature cannot be verified (bootstrapping trust)");
                }
            }
            continue;
        }

        if verbose {
            println!("  [{}] Validating signature...", i);
            println!("      CID: {}", entry.cid);
            println!("      Signature: {}", entry.operation.signature());
        }

        // Validate signature using current rotation keys
        if !current_rotation_keys.is_empty() {
            if verbose {
                println!("      Available rotation keys: {}", current_rotation_keys.len());
                for (j, key) in current_rotation_keys.iter().enumerate() {
                    println!("        [{}] {}", j, key);
                }
            }

            let verifying_keys: Vec<VerifyingKey> = current_rotation_keys
                .iter()
                .filter_map(|k| VerifyingKey::from_did_key(k).ok())
                .collect();

            if verbose {
                println!(
                    "      Parsed verifying keys: {}/{}",
                    verifying_keys.len(),
                    current_rotation_keys.len()
                );
            }

            // Try to verify with each key and track which one worked
            let mut verified = false;
            let mut verification_key_index = None;

            for (j, key) in verifying_keys.iter().enumerate() {
                if entry.operation.verify(&[*key]).is_ok() {
                    verified = true;
                    verification_key_index = Some(j);
                    break;
                }
            }

            if !verified {
                // Final attempt with all keys (for comprehensive error)
                if let Err(e) = entry.operation.verify(&verifying_keys) {
                    eprintln!();
                    eprintln!(
                        "❌ Validation failed: Invalid signature at operation {}",
                        i
                    );
                    eprintln!("   Error: {}", e);
                    eprintln!("   CID: {}", entry.cid);
                    eprintln!(
                        "   Tried {} rotation keys, none verified the signature",
                        verifying_keys.len()
                    );
                    return Err(anyhow::anyhow!("Invalid signature"));
                }
            }

            if verbose {
                if let Some(key_idx) = verification_key_index {
                    println!(
                        "      ✅ Signature verified with rotation key [{}]",
                        key_idx
                    );
                    println!("         {}", current_rotation_keys[key_idx]);
                } else {
                    println!("      ✅ Signature verified");
                }
            }
        }

        // Update rotation keys if this operation changes them
        if let Some(new_rotation_keys) = entry.operation.rotation_keys() {
            if new_rotation_keys != &current_rotation_keys {
                if verbose {
                    println!("      🔄 Rotation keys updated by this operation");
                    println!("         Old keys: {}", current_rotation_keys.len());
                    println!("         New keys: {}", new_rotation_keys.len());
                    for (j, key) in new_rotation_keys.iter().enumerate() {
                        println!("           [{}] {}", j, key);
                    }
                }
                current_rotation_keys = new_rotation_keys.to_vec();
            }
        }
    }

    if verbose {
        println!();
        println!("✅ Cryptographic signature validation complete");
        println!();
    }

    // Build final state
    let final_entry = audit_log.last().unwrap();
    if let Some(_rotation_keys) = final_entry.operation.rotation_keys() {
        let final_state = match &final_entry.operation {
            Operation::PlcOperation {
                rotation_keys,
                verification_methods,
                also_known_as,
                services,
                ..
            } => PlcState {
                rotation_keys: rotation_keys.clone(),
                verification_methods: verification_methods.clone(),
                also_known_as: also_known_as.clone(),
                services: services.clone(),
            },
            _ => PlcState::new(),
        };

        display_final_state(&final_state, quiet);
    } else {
        eprintln!("❌ Error: Could not extract final state");
        return Err(anyhow::anyhow!("Could not extract final state"));
    }

    Ok(())
}

/// Detect if there are fork points in the audit log
fn detect_forks(audit_log: &[AuditLogEntry]) -> bool {
    let mut prev_counts: HashMap<String, usize> = HashMap::new();
    for entry in audit_log {
        if let Some(prev) = entry.operation.prev() {
            *prev_counts.entry(prev.to_string()).or_insert(0) += 1;
        }
    }
    // If any prev CID is referenced by more than one operation, there's a fork
    prev_counts.values().any(|&count| count > 1)
}

/// Build a list of indices that form the canonical chain
fn build_canonical_chain_indices(audit_log: &[AuditLogEntry]) -> Vec<usize> {
    // Build a map of prev CID to operations
    let mut prev_to_indices: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, entry) in audit_log.iter().enumerate() {
        if let Some(prev) = entry.operation.prev() {
            prev_to_indices
                .entry(prev.to_string())
                .or_default()
                .push(i);
        }
    }

    // Start from genesis and follow the canonical chain
    let mut canonical = Vec::new();

    // Find genesis (first operation)
    let genesis = match audit_log.first() {
        Some(g) => g,
        None => return canonical,
    };

    canonical.push(0);
    let mut current_cid = genesis.cid.clone();

    // Follow the chain, preferring non-nullified operations
    loop {
        if let Some(indices) = prev_to_indices.get(&current_cid) {
            // Find the first non-nullified operation
            if let Some(&next_idx) = indices.iter().find(|&&idx| !audit_log[idx].nullified) {
                canonical.push(next_idx);
                current_cid = audit_log[next_idx].cid.clone();
            } else {
                // All operations at this point are nullified - try to find any operation
                if let Some(&next_idx) = indices.first() {
                    canonical.push(next_idx);
                    current_cid = audit_log[next_idx].cid.clone();
                } else {
                    break;
                }
            }
        } else {
            // No more operations
            break;
        }
    }

    canonical
}

/// Display the final state after validation
fn display_final_state(final_state: &PlcState, quiet: bool) {
    if quiet {
        println!("✅ VALID");
    } else {
        println!("✅ Validation successful!");
        println!();

        println!("📄 Final DID State:");
        println!("   Rotation keys: {}", final_state.rotation_keys.len());
        for (i, key) in final_state.rotation_keys.iter().enumerate() {
            println!("     [{}] {}", i, key);
        }
        println!();

        println!(
            "   Verification methods: {}",
            final_state.verification_methods.len()
        );
        for (name, key) in &final_state.verification_methods {
            println!("     {}: {}", name, key);
        }
        println!();

        if !final_state.also_known_as.is_empty() {
            println!("   Also known as: {}", final_state.also_known_as.len());
            for uri in &final_state.also_known_as {
                println!("     - {}", uri);
            }
            println!();
        }

        if !final_state.services.is_empty() {
            println!("   Services: {}", final_state.services.len());
            for (name, service) in &final_state.services {
                println!(
                    "     {}: {} ({})",
                    name, service.endpoint, service.service_type
                );
            }
        }
    }
}

/// Convert local bundle operations to audit log format
fn convert_to_audit_log(
    ops_with_loc: Vec<plcbundle::OperationWithLocation>,
) -> Result<Vec<AuditLogEntry>> {
    let mut audit_log = Vec::new();

    for owl in ops_with_loc {
        // Extract the operation JSON and convert to atproto_plc::Operation
        // Note: atproto_plc uses serde, so we use serde_json here (not parsing from bundle)
        let operation_json = serde_json::to_value(&owl.operation.operation)?;
        let operation: Operation = serde_json::from_value(operation_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse operation: {}", e))?;

        // Get CID from bundle operation (should always be present)
        let cid = owl
            .operation
            .cid
            .clone()
            .unwrap_or_else(|| {
                // Fallback: this shouldn't happen in real data, but provide a placeholder
                format!("bundle_{}_pos_{}", owl.bundle, owl.position)
            });

        audit_log.push(AuditLogEntry {
            did: owl.operation.did.clone(),
            operation,
            cid,
            created_at: owl.operation.created_at.clone(),
            nullified: owl.nullified || owl.operation.nullified,
        });
    }

    Ok(audit_log)
}

/// Visualize forks in the audit log
fn visualize_forks(
    audit_log: &[AuditLogEntry],
    did_str: &str,
    verbose: bool,
) -> Result<()> {
    println!("🔍 Analyzing forks in: {}", did_str);
    println!("   Source: local bundles");
    println!();

    println!("📊 Audit log contains {} operations", audit_log.len());

    // Detect forks
    let forks = detect_forks_detailed(audit_log, verbose);

    if forks.is_empty() {
        println!("\n✅ No forks detected - this is a linear operation chain");
        println!("   All operations form a single canonical path from genesis to tip.");

        if verbose {
            println!("\n📋 Linear chain visualization:");
            visualize_linear_chain(audit_log);
        }

        return Ok(());
    }

    println!("⚠️  Detected {} fork point(s)", forks.len());
    println!();

    visualize_tree(audit_log, &forks, verbose);

    Ok(())
}

/// Detect fork points in the audit log with detailed information
fn detect_forks_detailed(audit_log: &[AuditLogEntry], verbose: bool) -> Vec<ForkPoint> {
    let mut prev_to_operations: HashMap<String, Vec<AuditLogEntry>> = HashMap::new();

    // Group operations by their prev CID
    for entry in audit_log {
        if let Some(prev) = entry.operation.prev() {
            prev_to_operations
                .entry(prev.to_string())
                .or_default()
                .push(entry.clone());
        }
    }

    // Build operation map for state reconstruction
    let operation_map: HashMap<String, AuditLogEntry> = audit_log
        .iter()
        .map(|e| (e.cid.clone(), e.clone()))
        .collect();

    let mut forks = Vec::new();

    // Find fork points (where multiple operations reference the same prev)
    for (prev_cid, operations) in prev_to_operations {
        if operations.len() > 1 {
            if verbose {
                println!("🔀 Fork detected at {}", truncate_cid(&prev_cid));
                println!("   {} competing operations", operations.len());
            }

            // Get the state at the prev operation to determine rotation keys
            let state = if let Some(prev_entry) = operation_map.get(&prev_cid) {
                get_state_from_operation(&prev_entry.operation)
            } else {
                // This shouldn't happen in a valid chain
                continue;
            };

            // Analyze each operation in the fork
            let mut fork_ops = Vec::new();
            for entry in &operations {
                let timestamp = parse_timestamp(&entry.created_at);

                // Determine which rotation key signed this operation
                let (signing_key_index, signing_key) = if !state.rotation_keys.is_empty() {
                    find_signing_key(&entry.operation, &state.rotation_keys)
                } else {
                    (None, None)
                };

                fork_ops.push(ForkOperation {
                    cid: entry.cid.clone(),
                    operation: entry.operation.clone(),
                    timestamp,
                    signing_key_index,
                    signing_key,
                    is_winner: false,
                    rejection_reason: None,
                });
            }

            // Resolve the fork to determine winner
            let winner_cid = resolve_fork(&mut fork_ops);

            forks.push(ForkPoint {
                prev_cid,
                operations: fork_ops,
                winner_cid,
            });
        }
    }

    // Sort forks chronologically
    forks.sort_by_key(|f| {
        f.operations
            .iter()
            .map(|op| op.timestamp)
            .min()
            .unwrap_or_else(chrono::Utc::now)
    });

    forks
}

/// Resolve a fork point and mark the winner
fn resolve_fork(fork_ops: &mut [ForkOperation]) -> String {
    // Sort by timestamp (chronological order)
    fork_ops.sort_by_key(|op| op.timestamp);

    // First-received is the default winner
    let mut winner_idx = 0;
    fork_ops[0].is_winner = true;

    // Check if any later operation can invalidate based on priority
    for i in 1..fork_ops.len() {
        let competing_key_idx = fork_ops[i].signing_key_index;
        let winner_key_idx = fork_ops[winner_idx].signing_key_index;

        match (competing_key_idx, winner_key_idx) {
            (Some(competing_idx), Some(winner_idx_val)) => {
                if competing_idx < winner_idx_val {
                    // Higher priority (lower index)
                    let time_diff = fork_ops[i].timestamp - fork_ops[winner_idx].timestamp;

                    if time_diff <= chrono::Duration::hours(72) {
                        // Within recovery window - this operation wins
                        fork_ops[winner_idx].is_winner = false;
                        fork_ops[winner_idx].rejection_reason = Some(format!(
                            "Invalidated by higher-priority key[{}] within recovery window",
                            competing_idx
                        ));

                        fork_ops[i].is_winner = true;
                        winner_idx = i;
                    } else {
                        // Outside recovery window
                        fork_ops[i].rejection_reason = Some(format!(
                            "Higher-priority key[{}] but outside 72-hour recovery window ({:.1}h late)",
                            competing_idx,
                            time_diff.num_hours() as f64
                        ));
                    }
                } else {
                    // Lower priority
                    fork_ops[i].rejection_reason = Some(format!(
                        "Lower-priority key[{}] (current winner has key[{}])",
                        competing_idx, winner_idx_val
                    ));
                }
            }
            _ => {
                fork_ops[i].rejection_reason = Some("Could not determine signing key".to_string());
            }
        }
    }

    fork_ops[winner_idx].cid.clone()
}

/// Find which rotation key signed an operation
fn find_signing_key(operation: &Operation, rotation_keys: &[String]) -> (Option<usize>, Option<String>) {
    for (index, key_did) in rotation_keys.iter().enumerate() {
        if let Ok(verifying_key) = VerifyingKey::from_did_key(key_did) {
            if operation.verify(&[verifying_key]).is_ok() {
                return (Some(index), Some(key_did.clone()));
            }
        }
    }
    (None, None)
}

/// Get state from an operation
fn get_state_from_operation(operation: &Operation) -> PlcState {
    match operation {
        Operation::PlcOperation {
            rotation_keys,
            verification_methods,
            also_known_as,
            services,
            ..
        } => PlcState {
            rotation_keys: rotation_keys.clone(),
            verification_methods: verification_methods.clone(),
            also_known_as: also_known_as.clone(),
            services: services.clone(),
        },
        _ => PlcState::new(),
    }
}

/// Parse ISO 8601 timestamp
fn parse_timestamp(timestamp: &str) -> chrono::DateTime<chrono::Utc> {
    timestamp
        .parse::<chrono::DateTime<chrono::Utc>>()
        .unwrap_or_else(|_| chrono::Utc::now())
}

/// Visualize forks as a tree
fn visualize_tree(audit_log: &[AuditLogEntry], forks: &[ForkPoint], verbose: bool) {
    println!("📊 Fork Visualization (Tree Format)");
    println!("═══════════════════════════════════════════════════════════════");
    println!();

    // Build a map of which operations are part of forks
    let mut fork_map: HashMap<String, &ForkPoint> = HashMap::new();
    for fork in forks {
        for op in &fork.operations {
            fork_map.insert(op.cid.clone(), fork);
        }
    }

    // Track which prev CIDs have been processed
    let mut processed_forks: std::collections::HashSet<String> = std::collections::HashSet::new();

    for entry in audit_log.iter() {
        let is_genesis = entry.operation.is_genesis();
        let prev = entry.operation.prev();

        // Check if this operation is part of a fork
        if let Some(_prev_cid) = prev {
            if let Some(fork) = fork_map.get(&entry.cid) {
                // This is a fork point
                if !processed_forks.contains(&fork.prev_cid) {
                    processed_forks.insert(fork.prev_cid.clone());

                    println!("Fork at operation referencing {}", truncate_cid(&fork.prev_cid));

                    for (j, fork_op) in fork.operations.iter().enumerate() {
                        let symbol = if fork_op.is_winner { "✓" } else { "✗" };
                        let color = if fork_op.is_winner { "🟢" } else { "🔴" };
                        let prefix = if j == fork.operations.len() - 1 {
                            "└─"
                        } else {
                            "├─"
                        };

                        println!(
                            "  {} {} {} CID: {}",
                            prefix,
                            color,
                            symbol,
                            truncate_cid(&fork_op.cid)
                        );

                        if let Some(key_idx) = fork_op.signing_key_index {
                            println!("     │  Signed by: rotation_key[{}]", key_idx);
                            if verbose {
                                if let Some(key) = &fork_op.signing_key {
                                    println!("     │  Key: {}", truncate_cid(key));
                                }
                            }
                        }

                        println!(
                            "     │  Timestamp: {}",
                            fork_op.timestamp.format("%Y-%m-%d %H:%M:%S UTC")
                        );

                        if !fork_op.is_winner {
                            if let Some(reason) = &fork_op.rejection_reason {
                                println!("     │  Reason: {}", reason);
                            }
                        } else {
                            println!("     │  Status: CANONICAL (winner)");
                        }

                        if j < fork.operations.len() - 1 {
                            println!("     │");
                        }
                    }
                    println!();
                }
                continue;
            }
        }

        // Regular operation (not part of a fork)
        if is_genesis {
            println!("🌱 Genesis");
            println!("   CID: {}", truncate_cid(&entry.cid));
            println!("   Timestamp: {}", entry.created_at);
            if verbose {
                if let Operation::PlcOperation { rotation_keys, .. } = &entry.operation {
                    println!("   Rotation keys: {}", rotation_keys.len());
                }
            }
            println!();
        }
    }

    // Summary
    println!("═══════════════════════════════════════════════════════════════");
    println!("📈 Summary:");
    println!("   Total operations: {}", audit_log.len());
    println!("   Fork points: {}", forks.len());

    let total_competing_ops: usize = forks.iter().map(|f| f.operations.len()).sum();
    let rejected_ops = total_competing_ops - forks.len();
    println!("   Rejected operations: {}", rejected_ops);

    if !forks.is_empty() {
        println!("\n🔐 Fork Resolution Details:");
        for (i, fork) in forks.iter().enumerate() {
            let winner = fork.operations.iter().find(|op| op.is_winner).unwrap();
            println!(
                "   Fork {}: Winner is {} (signed by key[{}])",
                i + 1,
                truncate_cid(&winner.cid),
                winner.signing_key_index.unwrap_or(999)
            );
        }
    }
}

/// Visualize linear chain (no forks)
fn visualize_linear_chain(audit_log: &[AuditLogEntry]) {
    for (i, entry) in audit_log.iter().enumerate() {
        let symbol = if i == 0 { "🌱" } else { "  ↓" };
        println!("{} Operation {}: {}", symbol, i, truncate_cid(&entry.cid));
        println!("     Timestamp: {}", entry.created_at);
        if let Some(prev) = entry.operation.prev() {
            println!("     Previous: {}", truncate_cid(prev));
        }
    }
}

/// Truncate a CID for display
fn truncate_cid(cid: &str) -> String {
    if cid.len() > 20 {
        format!("{}...{}", &cid[..8], &cid[cid.len() - 8..])
    } else {
        cid.to_string()
    }
}
