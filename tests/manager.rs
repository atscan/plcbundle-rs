mod common;

use anyhow::Result;
// PathBuf is not needed directly in this test file
use std::sync::Arc;

#[tokio::test]
async fn test_bundle_manager_core() -> Result<()> {
    let dir = common::setup_temp_dir()?;
    let dir_path = dir.path().to_path_buf();

    // Initialize repo and add a dummy bundle
    common::setup_manager(&dir_path)?;
    common::add_dummy_bundle(&dir_path)?;

    // Create manager and verify index/last bundle
    let manager = plcbundle::BundleManager::new(dir_path.clone(), ())?;
    assert_eq!(manager.get_last_bundle(), 1);
    let index = manager.get_index();
    assert_eq!(index.bundles.len(), 1);

    // Load bundle and check operations
    let load = manager.load_bundle(1, plcbundle::LoadOptions::default())?;
    assert_eq!(load.operations.len(), 10);

    // Get raw operation JSON and verify DID is present
    let raw = manager.get_operation_raw(1, 0)?;
    assert!(raw.contains("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"));

    // Build DID index so DID lookups work
    manager
        .batch_update_did_index_async(1, manager.get_last_bundle(), false)
        .await?;

    // Query DID operations and resolve DID
    let did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    let did_ops = manager.get_did_operations(did, false, false)?;
    assert!(!did_ops.operations.is_empty());

    let resolved = manager.resolve_did(did)?;
    assert_eq!(resolved.document.id, did);

    Ok(())
}

#[tokio::test]
async fn test_operation_raw_positions() -> Result<()> {
    let dir = common::setup_temp_dir()?;
    let dir_path = dir.path().to_path_buf();
    common::setup_manager(&dir_path)?;
    common::add_dummy_bundle(&dir_path)?;

    let manager = plcbundle::BundleManager::new(dir_path, ())?;

    // position 0 -> last char 'a'
    let raw0 = manager.get_operation_raw(1, 0)?;
    assert!(raw0.contains("did:plc:aaaaaaaaaaaaaaaaaaaaaaaa"));

    // position 9 -> last char 'j'
    let mut id9 = "a".repeat(23);
    id9.push('j');
    let did9 = format!("did:plc:{}", id9);
    let raw9 = manager.get_operation_raw(1, 9)?;
    assert!(raw9.contains(&did9));

    Ok(())
}

#[tokio::test]
async fn test_range_and_export_iterators() -> Result<()> {
    let dir = common::setup_temp_dir()?;
    let dir_path = dir.path().to_path_buf();
    common::setup_manager(&dir_path)?;
    common::add_dummy_bundle(&dir_path)?;

    let manager = plcbundle::BundleManager::new(dir_path.clone(), ())?;
    let arc_mgr = Arc::new(manager.clone_for_arc());

    // Range iterator over bundle 1 should yield 10 operations
    let range_iter = arc_mgr.get_operations_range(1, 1, None);
    let mut count = 0usize;
    for res in range_iter {
        let op = res?;
        assert!(op.did.starts_with("did:plc:"));
        count += 1;
    }
    assert_eq!(count, 10);

    // Export iterator (jsonl) with count limit
    let spec = plcbundle::ExportSpec {
        bundles: plcbundle::BundleRange::Single(1),
        format: plcbundle::ExportFormat::JsonLines,
        filter: None,
        count: Some(5),
        after_timestamp: None,
    };
    let export_iter = plcbundle::ExportIterator::new(Arc::clone(&arc_mgr), spec);
    let mut exported = Vec::new();
    for item in export_iter {
        let s = item?;
        exported.push(s);
    }
    assert_eq!(exported.len(), 5);

    Ok(())
}

#[tokio::test]
async fn test_delete_bundle_and_sample_dids() -> Result<()> {
    let dir = common::setup_temp_dir()?;
    let dir_path = dir.path().to_path_buf();
    common::setup_manager(&dir_path)?;
    common::add_dummy_bundle(&dir_path)?;

    let manager = plcbundle::BundleManager::new(dir_path.clone(), ())?;

    // Ensure bundle file exists, then delete
    let bundle_path = plcbundle::constants::bundle_path(&dir_path, 1);
    assert!(bundle_path.exists());
    manager.delete_bundle_file(1)?;
    assert!(!bundle_path.exists());

    // For DID sampling, create a fresh repo so index/delta state is consistent
    let dir2 = common::setup_temp_dir()?;
    let dir2_path = dir2.path().to_path_buf();
    common::setup_manager(&dir2_path)?;
    common::add_dummy_bundle(&dir2_path)?;
    let manager2 = plcbundle::BundleManager::new(dir2_path.clone(), ())?;
    manager2
        .batch_update_did_index_async(1, manager2.get_last_bundle(), false)
        .await?;

    // Verify we can query DID operations from the newly built index
    let first_did = "did:plc:aaaaaaaaaaaaaaaaaaaaaaaa";
    let did_ops2 = manager2.get_did_operations(first_did, false, false)?;
    assert!(!did_ops2.operations.is_empty());

    Ok(())
}
