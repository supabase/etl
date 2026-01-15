//! Integration tests for the changelog merger.
//!
//! These tests verify the end-to-end behavior of the merge logic:
//! - Deduplication of CDC events (keeping latest by sequence number)
//! - Handling of duplicate events with same PK but different sequences
//! - Proper filtering of DELETE operations
//! - Index creation and lookup correctness
//! - Hash function consistency across batches

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use etl_iceberg_merger::index::{PuffinIndex, compute_pk_hash, splitmix64};
use std::collections::HashMap;
use std::sync::Arc;

/// Creates a sample changelog batch with the given data.
///
/// Schema: id (PK), value, cdc_sequence_number, cdc_operation
fn create_changelog_batch(
    ids: Vec<i64>,
    values: Vec<&str>,
    sequences: Vec<i64>,
    operations: Vec<&str>,
) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("cdc_sequence_number", DataType::Int64, false),
        Field::new("cdc_operation", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values)),
            Arc::new(Int64Array::from(sequences)),
            Arc::new(StringArray::from(operations)),
        ],
    )
    .expect("failed to create record batch")
}

/// Simulates the deduplication logic from merge.rs.
///
/// Returns a map of pk_hash -> (sequence, row_idx, operation).
fn deduplicate_batch(
    batch: &RecordBatch,
    pk_col_indices: &[usize],
    seq_col_idx: usize,
    op_col_idx: usize,
) -> HashMap<u64, (i64, usize, String)> {
    let mut deduplicated: HashMap<u64, (i64, usize, String)> = HashMap::new();

    for row_idx in 0..batch.num_rows() {
        let pk_hash = compute_pk_hash(batch, pk_col_indices, row_idx);

        // Extract sequence number.
        let seq_col = batch.column(seq_col_idx);
        let sequence = seq_col
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|arr| arr.value(row_idx))
            .unwrap_or(0);

        // Extract operation.
        let op_col = batch.column(op_col_idx);
        let operation = op_col
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|arr| arr.value(row_idx).to_string())
            .unwrap_or_default();

        // Keep only the latest record per PK (highest sequence).
        let should_update = match deduplicated.get(&pk_hash) {
            Some((existing_seq, _, _)) => sequence > *existing_seq,
            None => true,
        };

        if should_update {
            deduplicated.insert(pk_hash, (sequence, row_idx, operation));
        }
    }

    deduplicated
}

/// Filters out DELETE operations from deduplicated records.
fn filter_deletes(
    deduplicated: HashMap<u64, (i64, usize, String)>,
) -> HashMap<u64, (i64, usize, String)> {
    deduplicated
        .into_iter()
        .filter(|(_, (_, _, op))| !matches!(op.to_uppercase().as_str(), "DELETE" | "D"))
        .collect()
}

// ============================================================================
// Test: Basic Deduplication
// ============================================================================

#[test]
fn test_basic_deduplication_keeps_latest() {
    // Scenario: Same PK appears twice with different sequence numbers.
    // Expected: Keep only the record with highest sequence.
    let batch = create_changelog_batch(
        vec![1, 1], // Same PK=1 twice
        vec!["old_value", "new_value"],
        vec![100, 200], // seq 200 is newer
        vec!["INSERT", "UPDATE"],
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);

    assert_eq!(deduplicated.len(), 1, "should have 1 deduplicated record");

    let pk_hash = compute_pk_hash(&batch, &[0], 0);
    let (seq, row_idx, op) = deduplicated.get(&pk_hash).unwrap();

    assert_eq!(*seq, 200, "should keep sequence 200");
    assert_eq!(*row_idx, 1, "should keep row 1 (the newer one)");
    assert_eq!(op, "UPDATE", "should be UPDATE operation");
}

#[test]
fn test_deduplication_with_out_of_order_events() {
    // Scenario: Events arrive out of order (older event arrives after newer).
    // Expected: Still keep the one with highest sequence.
    let batch = create_changelog_batch(
        vec![1, 1, 1],
        vec!["v2", "v1", "v3"],
        vec![200, 100, 300], // Out of order
        vec!["UPDATE", "INSERT", "UPDATE"],
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);

    assert_eq!(deduplicated.len(), 1);

    let pk_hash = compute_pk_hash(&batch, &[0], 0);
    let (seq, row_idx, _) = deduplicated.get(&pk_hash).unwrap();

    assert_eq!(*seq, 300, "should keep highest sequence 300");
    assert_eq!(*row_idx, 2, "should keep row 2");
}

// ============================================================================
// Test: Duplicate Events (Exactly Same Data)
// ============================================================================

#[test]
fn test_exact_duplicate_events_same_sequence() {
    // Scenario: Exact duplicate events with same sequence (should keep one).
    // This can happen due to retry mechanisms in CDC.
    let batch = create_changelog_batch(
        vec![1, 1],
        vec!["value", "value"], // Exact same value
        vec![100, 100],         // Same sequence
        vec!["INSERT", "INSERT"],
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);

    // Should have exactly 1 record (first one wins when sequences are equal).
    assert_eq!(deduplicated.len(), 1);

    let pk_hash = compute_pk_hash(&batch, &[0], 0);
    let (seq, _, _) = deduplicated.get(&pk_hash).unwrap();
    assert_eq!(*seq, 100);
}

#[test]
fn test_duplicate_pk_different_sequences() {
    // Scenario: Multiple records with same PK but different sequences.
    let batch = create_changelog_batch(
        vec![1, 1, 1, 1],
        vec!["a", "b", "c", "d"],
        vec![1, 2, 3, 4],
        vec!["INSERT", "UPDATE", "UPDATE", "UPDATE"],
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);

    assert_eq!(deduplicated.len(), 1);

    let pk_hash = compute_pk_hash(&batch, &[0], 0);
    let (seq, row_idx, _) = deduplicated.get(&pk_hash).unwrap();

    assert_eq!(*seq, 4, "should keep highest sequence");
    assert_eq!(*row_idx, 3, "should keep last row");
}

// ============================================================================
// Test: Multiple PKs with Duplicates
// ============================================================================

#[test]
fn test_multiple_pks_each_with_duplicates() {
    // Scenario: Multiple different PKs, each with duplicate events.
    let batch = create_changelog_batch(
        vec![1, 2, 1, 2, 3],
        vec!["1-old", "2-old", "1-new", "2-new", "3-only"],
        vec![100, 100, 200, 200, 150],
        vec!["INSERT", "INSERT", "UPDATE", "UPDATE", "INSERT"],
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);

    assert_eq!(deduplicated.len(), 3, "should have 3 unique PKs");

    // Check PK=1 kept seq=200
    let pk1_hash = compute_pk_hash(&batch, &[0], 0);
    let (seq1, _, _) = deduplicated.get(&pk1_hash).unwrap();
    assert_eq!(*seq1, 200);

    // Check PK=2 kept seq=200
    let pk2_hash = compute_pk_hash(&batch, &[0], 1);
    let (seq2, _, _) = deduplicated.get(&pk2_hash).unwrap();
    assert_eq!(*seq2, 200);

    // Check PK=3 kept seq=150
    let pk3_hash = compute_pk_hash(&batch, &[0], 4);
    let (seq3, _, _) = deduplicated.get(&pk3_hash).unwrap();
    assert_eq!(*seq3, 150);
}

// ============================================================================
// Test: DELETE Operation Handling
// ============================================================================

#[test]
fn test_delete_followed_by_insert_keeps_insert() {
    // Scenario: DELETE then INSERT for same PK (INSERT has higher sequence).
    // Expected: Keep INSERT, filter out nothing (DELETE has lower seq).
    let batch = create_changelog_batch(
        vec![1, 1],
        vec!["", "resurrected"],
        vec![100, 200],
        vec!["DELETE", "INSERT"],
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);
    let filtered = filter_deletes(deduplicated);

    assert_eq!(filtered.len(), 1);

    let pk_hash = compute_pk_hash(&batch, &[0], 0);
    let (seq, _, op) = filtered.get(&pk_hash).unwrap();
    assert_eq!(*seq, 200);
    assert_eq!(op, "INSERT");
}

#[test]
fn test_insert_followed_by_delete_removes_record() {
    // Scenario: INSERT then DELETE for same PK.
    // Expected: After filtering, record should be gone.
    let batch = create_changelog_batch(
        vec![1, 1],
        vec!["created", ""],
        vec![100, 200],
        vec!["INSERT", "DELETE"],
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);

    // Before filtering: 1 record with DELETE operation.
    assert_eq!(deduplicated.len(), 1);
    let pk_hash = compute_pk_hash(&batch, &[0], 0);
    assert_eq!(deduplicated.get(&pk_hash).unwrap().2, "DELETE");

    // After filtering: no records.
    let filtered = filter_deletes(deduplicated);
    assert_eq!(filtered.len(), 0, "DELETE record should be filtered out");
}

#[test]
fn test_multiple_deletes_still_filtered() {
    // Scenario: Multiple DELETE operations for same PK.
    let batch = create_changelog_batch(
        vec![1, 1, 1],
        vec!["", "", ""],
        vec![100, 200, 300],
        vec!["DELETE", "DELETE", "DELETE"],
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);
    let filtered = filter_deletes(deduplicated);

    assert_eq!(filtered.len(), 0, "all DELETEs should be filtered");
}

#[test]
fn test_mixed_operations_across_pks() {
    // Scenario: Mixed INSERT, UPDATE, DELETE across different PKs.
    let batch = create_changelog_batch(
        vec![1, 2, 3, 1, 2],
        vec!["v1", "v2", "v3", "v1-updated", ""],
        vec![100, 100, 100, 200, 200],
        vec!["INSERT", "INSERT", "INSERT", "UPDATE", "DELETE"],
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);
    let filtered = filter_deletes(deduplicated);

    // PK=1: UPDATE (seq=200) - kept
    // PK=2: DELETE (seq=200) - filtered out
    // PK=3: INSERT (seq=100) - kept
    assert_eq!(filtered.len(), 2);

    // Verify PK=1 is kept.
    let pk1_hash = compute_pk_hash(&batch, &[0], 0);
    assert!(filtered.contains_key(&pk1_hash));

    // Verify PK=2 is not present (was DELETE).
    let pk2_hash = compute_pk_hash(&batch, &[0], 1);
    assert!(!filtered.contains_key(&pk2_hash));

    // Verify PK=3 is kept.
    let pk3_hash = compute_pk_hash(&batch, &[0], 2);
    assert!(filtered.contains_key(&pk3_hash));
}

#[test]
fn test_delete_operation_variants() {
    // Test different DELETE operation string formats.
    let batch = create_changelog_batch(
        vec![1, 2, 3, 4],
        vec!["", "", "", ""],
        vec![100, 100, 100, 100],
        vec!["DELETE", "D", "delete", "Insert"], // Various formats
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);
    let filtered = filter_deletes(deduplicated);

    // DELETE, D, delete should all be filtered. Insert should remain.
    assert_eq!(filtered.len(), 1);

    let pk4_hash = compute_pk_hash(&batch, &[0], 3);
    assert!(filtered.contains_key(&pk4_hash));
}

// ============================================================================
// Test: Index Integration
// ============================================================================

#[test]
fn test_index_stores_and_retrieves_correctly() {
    let mut index = PuffinIndex::new(64);
    let batch = create_changelog_batch(
        vec![1, 2, 3],
        vec!["a", "b", "c"],
        vec![100, 100, 100],
        vec!["INSERT", "INSERT", "INSERT"],
    );

    // Build index entries.
    for row_idx in 0..batch.num_rows() {
        let pk_hash = compute_pk_hash(&batch, &[0], row_idx);
        index.insert(pk_hash, format!("file_{}.parquet", row_idx), row_idx);
    }

    assert_eq!(index.len(), 3);

    // Verify retrieval.
    let pk1_hash = compute_pk_hash(&batch, &[0], 0);
    let loc = index.get(pk1_hash).expect("should find pk1");
    assert_eq!(loc.file_path, "file_0.parquet");
    assert_eq!(loc.row_offset, 0);
}

#[test]
fn test_index_batch_lookup() {
    let mut index = PuffinIndex::new(64);

    // Insert 1000 entries.
    for i in 0..1000u64 {
        let hash = splitmix64(i);
        index.insert(hash, format!("file_{}.parquet", i % 10), i as usize);
    }

    // Batch lookup: some exist, some don't.
    let lookup_hashes: Vec<u64> = (0..100).map(splitmix64).collect();
    let extra_hashes: Vec<u64> = (10000..10010).map(splitmix64).collect();
    let all_hashes: Vec<u64> = lookup_hashes.iter().chain(&extra_hashes).copied().collect();

    let results = index.search_values(&all_hashes);

    // Should find 100, not find 10.
    assert_eq!(results.len(), 100);
}

#[test]
fn test_index_roundtrip_with_changelog_data() {
    let mut index = PuffinIndex::new(64);
    let batch = create_changelog_batch(
        vec![1, 2, 3, 4, 5],
        vec!["a", "b", "c", "d", "e"],
        vec![100, 200, 300, 400, 500],
        vec!["INSERT", "INSERT", "INSERT", "INSERT", "INSERT"],
    );

    // Build index.
    for row_idx in 0..batch.num_rows() {
        let pk_hash = compute_pk_hash(&batch, &[0], row_idx);
        index.insert(pk_hash, "changelog.parquet".to_string(), row_idx);
    }

    // Serialize and deserialize.
    let parquet_bytes = index.to_parquet().expect("serialization should work");
    let loaded =
        PuffinIndex::from_parquet(&parquet_bytes, 64).expect("deserialization should work");

    assert_eq!(loaded.len(), index.len());

    // Verify all entries are preserved.
    for row_idx in 0..batch.num_rows() {
        let pk_hash = compute_pk_hash(&batch, &[0], row_idx);
        let original = index.get(pk_hash).expect("original should have entry");
        let loaded_loc = loaded.get(pk_hash).expect("loaded should have entry");
        assert_eq!(original.file_path, loaded_loc.file_path);
        assert_eq!(original.row_offset, loaded_loc.row_offset);
    }
}

// ============================================================================
// Test: Hash Consistency
// ============================================================================

#[test]
fn test_hash_consistency_across_batches() {
    // Same data in different batches should produce same hash.
    let batch1 = create_changelog_batch(vec![42], vec!["test"], vec![100], vec!["INSERT"]);

    let batch2 = create_changelog_batch(
        vec![42],
        vec!["different_value"], // Different value, same PK
        vec![200],
        vec!["UPDATE"],
    );

    let hash1 = compute_pk_hash(&batch1, &[0], 0);
    let hash2 = compute_pk_hash(&batch2, &[0], 0);

    assert_eq!(hash1, hash2, "same PK should produce same hash");
}

#[test]
fn test_splitmix64_produces_unique_hashes() {
    let hashes: Vec<u64> = (0..10000).map(splitmix64).collect();
    let unique: std::collections::HashSet<_> = hashes.iter().collect();

    assert_eq!(unique.len(), hashes.len(), "all hashes should be unique");
}

// ============================================================================
// Test: Edge Cases
// ============================================================================

#[test]
fn test_empty_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("cdc_sequence_number", DataType::Int64, false),
        Field::new("cdc_operation", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ],
    )
    .unwrap();

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);
    assert!(deduplicated.is_empty());
}

#[test]
fn test_single_record() {
    let batch = create_changelog_batch(vec![1], vec!["only_one"], vec![100], vec!["INSERT"]);

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);
    let filtered = filter_deletes(deduplicated);

    assert_eq!(filtered.len(), 1);
}

#[test]
fn test_all_deletes() {
    let batch = create_changelog_batch(
        vec![1, 2, 3],
        vec!["", "", ""],
        vec![100, 100, 100],
        vec!["DELETE", "DELETE", "DELETE"],
    );

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);
    let filtered = filter_deletes(deduplicated);

    assert!(filtered.is_empty(), "all records were DELETEs");
}

#[test]
fn test_composite_primary_key() {
    // Schema with composite PK (id1, id2).
    let schema = Arc::new(Schema::new(vec![
        Field::new("id1", DataType::Int64, false),
        Field::new("id2", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("cdc_sequence_number", DataType::Int64, false),
        Field::new("cdc_operation", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 1, 2])),
            Arc::new(Int64Array::from(vec![1, 2, 1, 1])), // (1,1), (1,2), (1,1), (2,1)
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int64Array::from(vec![100, 100, 200, 100])),
            Arc::new(StringArray::from(vec![
                "INSERT", "INSERT", "UPDATE", "INSERT",
            ])),
        ],
    )
    .unwrap();

    // Use both id1 and id2 as PK.
    let deduplicated = deduplicate_batch(&batch, &[0, 1], 3, 4);

    // (1,1) appears twice: rows 0 and 2. Should keep row 2 (seq=200).
    // (1,2) appears once: row 1.
    // (2,1) appears once: row 3.
    assert_eq!(deduplicated.len(), 3, "3 unique composite PKs");

    // Verify (1,1) kept the seq=200 version.
    let pk_1_1_hash = compute_pk_hash(&batch, &[0, 1], 0);
    let (seq, row_idx, _) = deduplicated.get(&pk_1_1_hash).unwrap();
    assert_eq!(*seq, 200);
    assert_eq!(*row_idx, 2);
}

// ============================================================================
// Test: Large Scale
// ============================================================================

#[test]
fn test_large_batch_deduplication() {
    // Create a batch with 10000 records, where each PK appears 10 times.
    let num_unique_pks = 1000;
    let duplicates_per_pk = 10;
    let total_records = num_unique_pks * duplicates_per_pk;

    let mut ids = Vec::with_capacity(total_records);
    let mut values = Vec::with_capacity(total_records);
    let mut sequences = Vec::with_capacity(total_records);
    let mut operations = Vec::with_capacity(total_records);

    for pk in 0..num_unique_pks {
        for dup in 0..duplicates_per_pk {
            ids.push(pk as i64);
            values.push(format!("value_{}_{}", pk, dup));
            sequences.push((pk * duplicates_per_pk + dup) as i64);
            operations.push("INSERT".to_string());
        }
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new("cdc_sequence_number", DataType::Int64, false),
        Field::new("cdc_operation", DataType::Utf8, false),
    ]));

    let values_refs: Vec<&str> = values.iter().map(|s| s.as_str()).collect();
    let ops_refs: Vec<&str> = operations.iter().map(|s| s.as_str()).collect();

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values_refs)),
            Arc::new(Int64Array::from(sequences)),
            Arc::new(StringArray::from(ops_refs)),
        ],
    )
    .unwrap();

    let deduplicated = deduplicate_batch(&batch, &[0], 2, 3);

    assert_eq!(
        deduplicated.len(),
        num_unique_pks,
        "should have {} unique PKs",
        num_unique_pks
    );

    // Each PK should have kept the highest sequence (last duplicate).
    for pk in 0..num_unique_pks {
        // Create a temp batch just to compute the hash for this PK.
        let pk_batch = create_changelog_batch(vec![pk as i64], vec![""], vec![0], vec![""]);
        let pk_hash = compute_pk_hash(&pk_batch, &[0], 0);

        let (seq, _, _) = deduplicated.get(&pk_hash).expect("should find pk");
        let expected_seq = (pk * duplicates_per_pk + (duplicates_per_pk - 1)) as i64;
        assert_eq!(
            *seq, expected_seq,
            "pk {} should have seq {}",
            pk, expected_seq
        );
    }
}
