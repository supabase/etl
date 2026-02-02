//! Manifest writing utilities for deletion vectors.
//!
//! This module provides utilities for writing deletion vector manifest entries
//! directly to Iceberg tables, bypassing the Transaction API which does not
//! support deletion vectors.
//!
//! The approach is inspired by the moonlink crate, which rewrites manifest
//! files before committing to include deletion vector entries.

use std::collections::HashMap;

use iceberg::spec::{
    DataFile, FormatVersion, MAIN_BRANCH, ManifestFile, ManifestListWriter, ManifestWriterBuilder,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary, TableMetadata,
};
use iceberg::table::Table;
use iceberg::{Catalog, TableRequirement, TableUpdate};

use super::proxy_structs::TableCommitProxy;

/// Writes a manifest file containing deletion vector entries.
///
/// This function creates a new manifest file with `ManifestContentType::Deletes`
/// containing the provided deletion vector `DataFile` entries. The manifest is
/// written to the table's metadata directory.
///
/// # Arguments
///
/// * `table` - The Iceberg table to write the manifest for.
/// * `delete_files` - The deletion vector DataFile entries to include.
/// * `snapshot_id` - The snapshot ID to associate with these entries.
/// * `sequence_number` - The sequence number for the entries.
///
/// # Returns
///
/// A [`ManifestFile`] reference that can be added to a manifest list.
pub async fn write_deletion_vector_manifest(
    table: &Table,
    delete_files: Vec<DataFile>,
    snapshot_id: i64,
    sequence_number: i64,
) -> Result<ManifestFile, iceberg::Error> {
    let metadata = table.metadata();
    let file_io = table.file_io();

    // Generate unique manifest path.
    let manifest_path = format!(
        "{}/metadata/{}-m0.avro",
        metadata.location(),
        uuid::Uuid::new_v4()
    );

    let output = file_io.new_output(&manifest_path)?;

    // Build a manifest writer for delete content.
    let builder = ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        None, // key_metadata
        metadata.current_schema().clone(),
        metadata.default_partition_spec().as_ref().clone(),
    );

    // Use the appropriate format version for deletes.
    let mut writer = match metadata.format_version() {
        FormatVersion::V1 => {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::FeatureUnsupported,
                "Deletion vectors are not supported in format version 1",
            ));
        }
        FormatVersion::V2 => builder.build_v2_deletes(),
        FormatVersion::V3 => builder.build_v3_deletes(),
    };

    // Add each deletion vector file using the public add_file API.
    // The sequence number is provided for proper ordering.
    for data_file in delete_files {
        writer.add_file(data_file, sequence_number)?;
    }

    // Write the manifest and return the ManifestFile reference.
    writer.write_manifest_file().await
}

/// Creates a new snapshot with deletion vectors appended to the current state.
///
/// This function:
/// 1. Preserves all existing manifests from the current snapshot.
/// 2. Creates a new manifest containing the deletion vector entries.
/// 3. Writes a new manifest list containing both old and new manifests.
/// 4. Builds a new snapshot referencing the manifest list.
///
/// # Arguments
///
/// * `table` - The Iceberg table to create the snapshot for.
/// * `delete_files` - The deletion vector DataFile entries to append.
/// * `snapshot_id` - The new snapshot ID to use.
/// * `sequence_number` - The sequence number for the new snapshot.
///
/// # Returns
///
/// A tuple of (new_snapshot, manifest_list_path) that can be used in a commit.
pub async fn create_snapshot_with_deletion_vectors(
    table: &Table,
    delete_files: Vec<DataFile>,
    snapshot_id: i64,
    sequence_number: i64,
) -> Result<(Snapshot, String), iceberg::Error> {
    let metadata = table.metadata();
    let file_io = table.file_io();

    // Collect existing manifests from current snapshot.
    let mut manifests: Vec<ManifestFile> = Vec::new();

    if let Some(current_snapshot) = metadata.current_snapshot() {
        let manifest_list = current_snapshot
            .load_manifest_list(file_io, metadata)
            .await?;
        manifests.extend(manifest_list.entries().iter().cloned());
    }

    // Write new manifest for deletion vectors.
    let dv_manifest =
        write_deletion_vector_manifest(table, delete_files, snapshot_id, sequence_number).await?;
    manifests.push(dv_manifest);

    // Write new manifest list.
    let manifest_list_path = format!(
        "{}/metadata/snap-{}-{}.avro",
        metadata.location(),
        snapshot_id,
        uuid::Uuid::new_v4()
    );

    let output = file_io.new_output(&manifest_list_path)?;
    let parent_snapshot_id = metadata.current_snapshot_id();

    let mut manifest_list_writer = match metadata.format_version() {
        FormatVersion::V1 => {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::FeatureUnsupported,
                "Deletion vectors are not supported in format version 1",
            ));
        }
        FormatVersion::V2 => {
            ManifestListWriter::v2(output, snapshot_id, parent_snapshot_id, sequence_number)
        }
        FormatVersion::V3 => ManifestListWriter::v3(
            output,
            snapshot_id,
            parent_snapshot_id,
            sequence_number,
            None, // first_row_id - None for delete manifests
        ),
    };

    manifest_list_writer.add_manifests(manifests.into_iter())?;
    manifest_list_writer.close().await?;

    // Build the snapshot summary.
    let summary = Summary {
        operation: Operation::Delete,
        additional_properties: HashMap::new(),
    };

    // Build the new snapshot.
    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_timestamp_ms(chrono::Utc::now().timestamp_millis())
        .with_sequence_number(sequence_number)
        .with_schema_id(metadata.current_schema_id())
        .with_manifest_list(&manifest_list_path)
        .with_parent_snapshot_id(parent_snapshot_id)
        .with_summary(summary)
        .build();

    Ok((snapshot, manifest_list_path))
}

/// Commits deletion vectors to an Iceberg table.
///
/// This function bypasses the standard Transaction API (which does not support
/// deletion vectors) and directly:
/// 1. Creates a new manifest containing the deletion vector entries.
/// 2. Writes a new manifest list preserving existing manifests.
/// 3. Creates a new snapshot referencing the manifest list.
/// 4. Commits the snapshot to the catalog using `update_table`.
///
/// # Arguments
///
/// * `table` - The Iceberg table to commit deletions to.
/// * `catalog` - The catalog to commit the transaction to.
/// * `delete_files` - The DataFile entries for the deletion vectors.
///
/// # Returns
///
/// The updated table after the commit, or an error if the commit fails.
pub async fn commit_deletion_vectors_impl(
    table: &Table,
    catalog: &dyn Catalog,
    delete_files: Vec<DataFile>,
) -> Result<Table, iceberg::Error> {
    let metadata = table.metadata();

    // Generate new snapshot ID and sequence number.
    let snapshot_id = generate_snapshot_id(metadata);
    let sequence_number = metadata.last_sequence_number() + 1;

    // Create the snapshot with deletion vectors.
    let (snapshot, _manifest_list_path) =
        create_snapshot_with_deletion_vectors(table, delete_files, snapshot_id, sequence_number)
            .await?;

    // Build the table updates.
    let updates = vec![
        TableUpdate::AddSnapshot {
            snapshot: snapshot.clone(),
        },
        TableUpdate::SetSnapshotRef {
            ref_name: MAIN_BRANCH.to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ];

    // Build the requirements for optimistic concurrency.
    let requirements = vec![
        TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        },
        TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: metadata.current_snapshot_id(),
        },
    ];

    // Create the TableCommit using our proxy (since TableCommit::builder().build() is private).
    let commit = TableCommitProxy::new(table.identifier().clone(), updates, requirements);

    catalog.update_table(commit.into_table_commit()).await
}

/// Generates a unique snapshot ID that doesn't conflict with existing snapshots.
fn generate_snapshot_id(metadata: &TableMetadata) -> i64 {
    loop {
        let id = uuid::Uuid::now_v7().as_u64_pair().0 as i64;
        if id > 0 && metadata.snapshot_by_id(id).is_none() {
            return id;
        }
    }
}
