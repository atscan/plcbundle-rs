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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operations::Operation;
    use sonic_rs::Value;

    fn create_test_operation(did: &str) -> Operation {
        Operation {
            did: did.to_string(),
            operation: Value::new(),
            cid: None,
            nullified: false,
            created_at: "2024-01-01T00:00:00Z".to_string(),
            extra: Value::new(),
            raw_json: None,
        }
    }

    #[test]
    fn test_cache_new() {
        let cache = BundleCache::new(10);
        assert!(!cache.contains(1));
    }

    #[test]
    fn test_cache_insert_and_get() {
        let cache = BundleCache::new(10);
        let ops = vec![
            create_test_operation("did:plc:test1"),
            create_test_operation("did:plc:test2"),
        ];

        cache.insert(1, ops.clone());
        assert!(cache.contains(1));
        
        let retrieved = cache.get(1);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().len(), 2);
    }

    #[test]
    fn test_cache_contains() {
        let cache = BundleCache::new(10);
        assert!(!cache.contains(1));
        
        cache.insert(1, vec![create_test_operation("did:plc:test1")]);
        assert!(cache.contains(1));
        assert!(!cache.contains(2));
    }

    #[test]
    fn test_cache_remove() {
        let cache = BundleCache::new(10);
        cache.insert(1, vec![create_test_operation("did:plc:test1")]);
        assert!(cache.contains(1));
        
        cache.remove(1);
        assert!(!cache.contains(1));
    }

    #[test]
    fn test_cache_clear() {
        let cache = BundleCache::new(10);
        cache.insert(1, vec![create_test_operation("did:plc:test1")]);
        cache.insert(2, vec![create_test_operation("did:plc:test2")]);
        assert!(cache.contains(1));
        assert!(cache.contains(2));
        
        cache.clear();
        assert!(!cache.contains(1));
        assert!(!cache.contains(2));
    }

    #[test]
    fn test_cache_capacity_eviction() {
        let cache = BundleCache::new(2);
        
        // Fill cache to capacity
        cache.insert(1, vec![create_test_operation("did:plc:test1")]);
        cache.insert(2, vec![create_test_operation("did:plc:test2")]);
        assert!(cache.contains(1));
        assert!(cache.contains(2));
        
        // Adding third should evict one (HashMap iteration order is not guaranteed)
        cache.insert(3, vec![create_test_operation("did:plc:test3")]);
        // One of the first two should be evicted, and 3 should be present
        assert!(cache.contains(3));
        // Cache should only have 2 items
        let count = [cache.contains(1), cache.contains(2), cache.contains(3)]
            .iter()
            .filter(|&&x| x)
            .count();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_cache_multiple_bundles() {
        let cache = BundleCache::new(10);
        
        for i in 1..=5 {
            cache.insert(i, vec![create_test_operation(&format!("did:plc:test{}", i))]);
        }
        
        for i in 1..=5 {
            assert!(cache.contains(i));
            let ops = cache.get(i).unwrap();
            assert_eq!(ops.len(), 1);
            assert_eq!(ops[0].did, format!("did:plc:test{}", i));
        }
    }

    #[test]
    fn test_cache_empty_operations() {
        let cache = BundleCache::new(10);
        cache.insert(1, vec![]);
        
        let ops = cache.get(1);
        assert!(ops.is_some());
        assert_eq!(ops.unwrap().len(), 0);
    }
}
