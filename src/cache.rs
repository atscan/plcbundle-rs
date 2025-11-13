// src/cache.rs
use crate::operations::Operation;
use std::collections::HashMap;
use std::sync::RwLock;

pub struct BundleCache {
    capacity: usize,
    cache: RwLock<HashMap<u32, Vec<Operation>>>,
}

impl BundleCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub fn get(&self, bundle: u32) -> Option<Vec<Operation>> {
        self.cache.read().unwrap().get(&bundle).cloned()
    }

    pub fn insert(&self, bundle: u32, ops: Vec<Operation>) {
        let mut cache = self.cache.write().unwrap();
        if cache.len() >= self.capacity {
            if let Some(&key) = cache.keys().next() {
                cache.remove(&key);
            }
        }
        cache.insert(bundle, ops);
    }

    pub fn contains(&self, bundle: u32) -> bool {
        self.cache.read().unwrap().contains_key(&bundle)
    }

    pub fn remove(&self, bundle: u32) {
        self.cache.write().unwrap().remove(&bundle);
    }

    pub fn clear(&self) {
        self.cache.write().unwrap().clear();
    }
}
