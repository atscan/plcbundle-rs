// src/iterators.rs
use crate::manager::{BundleManager, ExportSpec, LoadOptions, QuerySpec};
use crate::operations::{Operation, OperationFilter};
use anyhow::Result;
use std::sync::Arc;

pub struct RangeIterator {
    manager: Arc<BundleManager>,
    current: u32,
    end: u32,
    filter: Option<OperationFilter>,
    current_ops: Vec<Operation>,
    current_idx: usize,
}

impl RangeIterator {
    pub fn new(
        manager: Arc<BundleManager>,
        start: u32,
        end: u32,
        filter: Option<OperationFilter>,
    ) -> Self {
        Self {
            manager,
            current: start,
            end,
            filter,
            current_ops: Vec::new(),
            current_idx: 0,
        }
    }
}

impl Iterator for RangeIterator {
    type Item = Result<Operation>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_idx < self.current_ops.len() {
                let op = self.current_ops[self.current_idx].clone();
                self.current_idx += 1;
                return Some(Ok(op));
            }

            if self.current > self.end {
                return None;
            }

            match self.manager.load_bundle(
                self.current,
                LoadOptions {
                    filter: self.filter.clone(),
                    ..Default::default()
                },
            ) {
                Ok(result) => {
                    self.current_ops = result.operations;
                    self.current_idx = 0;
                    self.current += 1;
                }
                Err(e) => {
                    self.current += 1;
                    return Some(Err(e));
                }
            }
        }
    }
}

pub struct QueryIterator {
    _manager: Arc<BundleManager>,
    _spec: QuerySpec,
    // TODO: Implement query iterator
}

impl QueryIterator {
    pub fn new(manager: Arc<BundleManager>, spec: QuerySpec) -> Self {
        Self {
            _manager: manager,
            _spec: spec,
        }
    }
}

impl Iterator for QueryIterator {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: Implement
        None
    }
}

pub struct ExportIterator {
    manager: Arc<BundleManager>,
    spec: ExportSpec,
    bundle_numbers: Vec<u32>,
    current_bundle_idx: usize,
    current_ops: Vec<Operation>,
    current_op_idx: usize,
    count_remaining: Option<usize>,
}

impl ExportIterator {
    pub fn new(manager: Arc<BundleManager>, spec: ExportSpec) -> Self {
        let bundle_numbers = match &spec.bundles {
            crate::manager::BundleRange::All => {
                let last = manager.get_last_bundle();
                (1..=last).collect()
            }
            crate::manager::BundleRange::Single(n) => vec![*n],
            crate::manager::BundleRange::Range(start, end) => (*start..=*end).collect(),
            crate::manager::BundleRange::List(list) => list.clone(),
        };

        Self {
            manager,
            spec,
            bundle_numbers,
            current_bundle_idx: 0,
            current_ops: Vec::new(),
            current_op_idx: 0,
            count_remaining: None,
        }
    }

    fn format_operation(&self, op: &Operation) -> Result<String> {
        match self.spec.format {
            crate::manager::ExportFormat::JsonLines => Ok(sonic_rs::to_string(op)?),
            crate::manager::ExportFormat::Csv => {
                // Simple CSV format - could be enhanced
                Ok(format!(
                    "{},{},{},{}",
                    op.did, op.operation, op.created_at, op.nullified
                ))
            }
            crate::manager::ExportFormat::Parquet => {
                // Parquet would require additional dependencies
                // For now, fall back to JSON
                Ok(sonic_rs::to_string(op)?)
            }
        }
    }

    fn matches_timestamp_filter(&self, op: &Operation) -> bool {
        if let Some(ref after) = self.spec.after_timestamp {
            // Compare timestamps as strings (ISO 8601 format)
            op.created_at >= *after
        } else {
            true
        }
    }
}

impl Iterator for ExportIterator {
    type Item = Result<String>;

    fn next(&mut self) -> Option<Self::Item> {
        // Check count limit
        if let Some(ref mut remaining) = self.count_remaining {
            if *remaining == 0 {
                return None;
            }
        }

        loop {
            // Check if we have more operations in current bundle
            if self.current_op_idx < self.current_ops.len() {
                let op = &self.current_ops[self.current_op_idx];
                self.current_op_idx += 1;

                // Apply timestamp filter
                if !self.matches_timestamp_filter(op) {
                    continue;
                }

                // Decrement count if limit is set
                if let Some(ref mut remaining) = self.count_remaining {
                    *remaining -= 1;
                }

                return Some(self.format_operation(op));
            }

            // Load next bundle
            if self.current_bundle_idx >= self.bundle_numbers.len() {
                return None;
            }

            let bundle_num = self.bundle_numbers[self.current_bundle_idx];
            self.current_bundle_idx += 1;

            // Initialize count_remaining on first bundle load
            if self.count_remaining.is_none() {
                self.count_remaining = self.spec.count;
            }

            match self.manager.load_bundle(
                bundle_num,
                LoadOptions {
                    filter: self.spec.filter.clone(),
                    ..Default::default()
                },
            ) {
                Ok(result) => {
                    self.current_ops = result.operations;
                    self.current_op_idx = 0;
                }
                Err(_e) => {
                    // Skip bundles that fail to load
                    continue;
                }
            }
        }
    }
}
