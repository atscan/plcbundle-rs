use anyhow::Result;
use clap::Args;
use plcbundle::BundleManager;
use plcbundle::format::format_duration_compact;
use std::path::PathBuf;

#[derive(Args)]
pub struct LsCommand {
    /// Show only last N bundles (0 = all)
    #[arg(short = 'n', long, default_value = "0")]
    pub last: usize,

    /// Show oldest first (default: newest first)
    #[arg(long)]
    pub reverse: bool,

    /// Output format: bundle,hash,date,ops,dids,size,uncompressed,ratio,timespan
    #[arg(long, default_value = "bundle,hash,date,ops,dids,size")]
    pub format: String,

    /// Omit header row
    #[arg(long)]
    pub no_header: bool,

    /// Field separator (default: tab)
    #[arg(long, default_value = "\t")]
    pub separator: String,
}

pub fn run(cmd: LsCommand, dir: PathBuf) -> Result<()> {
    let manager = BundleManager::new(dir)?;

    // Get all bundle metadata from the index
    let bundles = super::utils::get_all_bundle_metadata(&manager);

    if bundles.is_empty() {
        return Ok(());
    }

    // Apply limit
    let display_bundles = if cmd.last > 0 && cmd.last < bundles.len() {
        bundles[bundles.len() - cmd.last..].to_vec()
    } else {
        bundles
    };

    // Reverse if not --reverse (default is newest first, like log)
    let display_bundles = if !cmd.reverse {
        display_bundles.into_iter().rev().collect::<Vec<_>>()
    } else {
        display_bundles
    };

    // Parse format string
    let fields = parse_format_string(&cmd.format);

    // Print header (unless disabled)
    if !cmd.no_header {
        print_header(&fields, &cmd.separator);
    }

    // Print each bundle
    for meta in display_bundles {
        print_bundle_fields(&meta, &fields, &cmd.separator);
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

fn print_header(fields: &[String], sep: &str) {
    let headers: Vec<String> = fields.iter().map(|f| get_field_header(f)).collect();
    println!("{}", headers.join(sep));
}

fn get_field_header(field: &str) -> String {
    match field {
        "bundle" => "bundle",
        "hash" => "hash",
        "hash_short" => "hash",
        "content" => "content_hash",
        "content_short" => "content_hash",
        "parent" => "parent_hash",
        "parent_short" => "parent_hash",
        "date" | "time" => "date",
        "date_short" => "date",
        "timestamp" | "unix" => "timestamp",
        "age" => "age",
        "age_seconds" => "age_seconds",
        "ops" | "operations" => "ops",
        "dids" => "dids",
        "size" | "compressed" => "size",
        "size_mb" => "size_mb",
        "uncompressed" => "uncompressed",
        "uncompressed_mb" => "uncompressed_mb",
        "ratio" => "ratio",
        "timespan" | "duration" => "timespan",
        "timespan_seconds" => "timespan_seconds",
        "start" => "start_time",
        "end" => "end_time",
        "created" => "created_at",
        _ => field,
    }
    .to_string()
}

fn print_bundle_fields(meta: &plcbundle::index::BundleMetadata, fields: &[String], sep: &str) {
    let values: Vec<String> = fields.iter().map(|f| get_field_value(meta, f)).collect();
    println!("{}", values.join(sep));
}

fn get_field_value(meta: &plcbundle::index::BundleMetadata, field: &str) -> String {
    match field {
        "bundle" => format!("{:06}", meta.bundle_number),

        "hash" => meta.hash.clone(),
        "hash_short" => {
            if meta.hash.len() >= 12 {
                meta.hash[..12].to_string()
            } else {
                meta.hash.clone()
            }
        }

        "content" => meta.content_hash.clone(),
        "content_short" => {
            if meta.content_hash.len() >= 12 {
                meta.content_hash[..12].to_string()
            } else {
                meta.content_hash.clone()
            }
        }

        "parent" => meta.parent.clone(),
        "parent_short" => {
            if meta.parent.len() >= 12 {
                meta.parent[..12].to_string()
            } else {
                meta.parent.clone()
            }
        }

        "date" | "time" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.end_time) {
                dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
            } else {
                meta.end_time.clone()
            }
        }

        "date_short" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.end_time) {
                dt.format("%Y-%m-%d").to_string()
            } else {
                meta.end_time.clone()
            }
        }

        "timestamp" | "unix" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.end_time) {
                format!("{}", dt.timestamp())
            } else {
                "0".to_string()
            }
        }

        "age" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.end_time) {
                let age = chrono::Utc::now().signed_duration_since(dt);
                format_duration_compact(age)
            } else {
                "unknown".to_string()
            }
        }

        "age_seconds" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.end_time) {
                let age = chrono::Utc::now().signed_duration_since(dt);
                format!("{:.0}", age.num_seconds())
            } else {
                "0".to_string()
            }
        }

        "ops" | "operations" => format!("{}", meta.operation_count),
        "dids" => format!("{}", meta.did_count),

        "size" | "compressed" => format!("{}", meta.compressed_size),
        "size_mb" => format!("{:.2}", meta.compressed_size as f64 / (1024.0 * 1024.0)),

        "uncompressed" => format!("{}", meta.uncompressed_size),
        "uncompressed_mb" => format!("{:.2}", meta.uncompressed_size as f64 / (1024.0 * 1024.0)),

        "ratio" => {
            if meta.compressed_size > 0 {
                let ratio = meta.uncompressed_size as f64 / meta.compressed_size as f64;
                format!("{:.2}", ratio)
            } else {
                "0".to_string()
            }
        }

        "timespan" | "duration" => {
            if let (Ok(start), Ok(end)) = (
                chrono::DateTime::parse_from_rfc3339(&meta.start_time),
                chrono::DateTime::parse_from_rfc3339(&meta.end_time),
            ) {
                let duration = end.signed_duration_since(start);
                format_duration_compact(duration)
            } else {
                "unknown".to_string()
            }
        }

        "timespan_seconds" => {
            if let (Ok(start), Ok(end)) = (
                chrono::DateTime::parse_from_rfc3339(&meta.start_time),
                chrono::DateTime::parse_from_rfc3339(&meta.end_time),
            ) {
                let duration = end.signed_duration_since(start);
                format!("{:.0}", duration.num_seconds())
            } else {
                "0".to_string()
            }
        }

        "start" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.start_time) {
                dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
            } else {
                meta.start_time.clone()
            }
        }

        "end" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.end_time) {
                dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
            } else {
                meta.end_time.clone()
            }
        }

        "created" => {
            if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&meta.created_at) {
                dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
            } else {
                meta.created_at.clone()
            }
        }

        _ => String::new(),
    }
}
