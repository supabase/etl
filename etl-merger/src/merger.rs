//! Core merger that orchestrates reading from changelog and writing to mirror.

use etl::types::{ColumnSchema, TableRow};
use etl_destinations::iceberg::IcebergClient;
use tracing::{debug, info};

use crate::changelog_reader::{CdcOperation, ChangelogReader, arrow_value_to_cell};
use crate::config::MergerConfig;
use crate::error::MergerResult;
use crate::index::{IndexStats, PrimaryKey, RowLocation, SecondaryIndex};
use crate::mirror_writer::MirrorWriter;
use crate::progress::MergeProgress;

/// Result of merging a single batch.
#[derive(Debug, Default)]
pub struct MergeBatchResult {
    /// Total events processed in this batch.
    pub events_processed: usize,
    /// Number of insert operations.
    pub inserts: usize,
    /// Number of update operations.
    pub updates: usize,
    /// Number of delete operations.
    pub deletes: usize,
}

impl MergeBatchResult {
    /// Creates an empty result.
    pub fn empty() -> Self {
        Self::default()
    }
}

/// Summary of a complete merge operation.
#[derive(Debug, Default)]
pub struct MergeSummary {
    /// Total events processed across all batches.
    pub events_processed: usize,
    /// Number of batches processed.
    pub batches_processed: usize,
    /// Final progress state.
    pub final_progress: MergeProgress,
}

/// The main merger that orchestrates reading from changelog and writing to mirror.
pub struct Merger {
    config: MergerConfig,
    changelog_reader: ChangelogReader,
    mirror_writer: MirrorWriter,
    index: SecondaryIndex,
    progress: MergeProgress,
    mirror_table_name: String,
}

impl Merger {
    /// Creates a new merger instance.
    ///
    /// # Arguments
    ///
    /// * `client` - The Iceberg client for catalog operations.
    /// * `changelog_table_name` - Name of the changelog table to read from.
    /// * `mirror_table_name` - Name of the mirror table to write to.
    /// * `mirror_schema` - Schema for the mirror table (without CDC columns).
    /// * `pk_column_indices` - Indices of primary key columns in the schema.
    /// * `config` - Merger configuration.
    pub async fn new(
        client: IcebergClient,
        changelog_table_name: &str,
        mirror_table_name: &str,
        mirror_schema: Vec<ColumnSchema>,
        pk_column_indices: Vec<usize>,
        config: MergerConfig,
    ) -> MergerResult<Self> {
        info!(
            changelog = %changelog_table_name,
            mirror = %mirror_table_name,
            namespace = %config.changelog_namespace,
            "initializing merger"
        );

        let changelog_reader = ChangelogReader::new(
            &client,
            &config.changelog_namespace,
            changelog_table_name,
            pk_column_indices.clone(),
        )
        .await?;

        let mirror_writer = MirrorWriter::new(client.clone(), config.mirror_namespace.clone());

        // Create namespace if needed
        mirror_writer.create_namespace_if_missing().await?;

        // Create mirror table if needed
        mirror_writer
            .create_table_if_missing(mirror_table_name, &mirror_schema)
            .await?;

        let progress = if let Some(checkpoint) = &config.checkpoint {
            MergeProgress::from_checkpoint(mirror_table_name.to_string(), checkpoint.clone())
        } else {
            MergeProgress::new(mirror_table_name.to_string())
        };

        let index = SecondaryIndex::with_capacity(pk_column_indices, config.index_initial_capacity);

        Ok(Self {
            config,
            changelog_reader,
            mirror_writer,
            index,
            progress,
            mirror_table_name: mirror_table_name.to_string(),
        })
    }

    /// Builds the index from existing mirror table data.
    ///
    /// This should be called on startup to populate the index with existing rows.
    pub async fn build_index_from_mirror(&mut self, client: &IcebergClient) -> MergerResult<usize> {
        info!(table = %self.mirror_table_name, "building index from existing mirror table");

        // Check if table exists and has data
        if !self
            .mirror_writer
            .table_exists(&self.mirror_table_name)
            .await?
        {
            debug!("mirror table does not exist yet, index is empty");
            return Ok(0);
        }

        // Load and scan the mirror table
        let table = client
            .load_table(
                self.config.mirror_namespace.clone(),
                self.mirror_table_name.clone(),
            )
            .await?;

        let Some(current_snapshot) = table.metadata().current_snapshot_id() else {
            debug!("mirror table has no snapshot yet, index is empty");
            return Ok(0);
        };

        use futures::StreamExt;

        let mut stream = table
            .scan()
            .snapshot_id(current_snapshot)
            .select_all()
            .build()?
            .to_arrow()
            .await?;

        let mut total_rows = 0;
        let mut file_idx = 0;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            let pk_indices = self.index.pk_column_indices().to_vec();

            // For each row, extract PK and create index entry
            for row_idx in 0..batch.num_rows() {
                let mut cells = Vec::with_capacity(batch.num_columns());
                for col_idx in 0..batch.num_columns() {
                    let cell = arrow_value_to_cell(batch.column(col_idx), row_idx);
                    cells.push(cell);
                }

                let pk = PrimaryKey::from_row(&cells, &pk_indices);

                // Create a placeholder location
                // TODO: Track actual file paths when we have proper file tracking
                let location = RowLocation::new(
                    format!("data/existing-{}.parquet", file_idx),
                    row_idx as u64,
                    "initial".to_string(),
                );

                self.index.insert(pk, location);
                total_rows += 1;
            }
            file_idx += 1;
        }

        info!(rows = total_rows, "index built from mirror table");
        Ok(total_rows)
    }

    /// Runs the merge process for a single batch.
    ///
    /// Returns the number of events processed.
    pub async fn merge_batch(&mut self) -> MergerResult<MergeBatchResult> {
        let batch = self
            .changelog_reader
            .read_batch(
                self.progress.last_sequence_number.as_deref(),
                self.config.batch_size,
            )
            .await?;

        if batch.entries.is_empty() {
            return Ok(MergeBatchResult::empty());
        }

        debug!(entries = batch.entries.len(), "processing changelog batch");

        // Track operations
        let mut inserts_count = 0;
        let mut updates_count = 0;
        let mut deletes_count = 0;

        // Collect rows to insert and locations to delete
        let mut rows_to_insert: Vec<(TableRow, PrimaryKey, String)> = Vec::new();
        let mut deletions: Vec<RowLocation> = Vec::new();

        for entry in &batch.entries {
            match entry.operation {
                CdcOperation::Insert => {
                    rows_to_insert.push((
                        entry.row.clone(),
                        entry.primary_key.clone(),
                        entry.sequence_number.clone(),
                    ));
                    inserts_count += 1;
                }
                CdcOperation::Update => {
                    // Find old row location and mark for deletion
                    if let Some(old_location) = self.index.get(&entry.primary_key) {
                        deletions.push(old_location.clone());
                    }
                    // Queue new row insert
                    rows_to_insert.push((
                        entry.row.clone(),
                        entry.primary_key.clone(),
                        entry.sequence_number.clone(),
                    ));
                    updates_count += 1;
                }
                CdcOperation::Delete => {
                    // Find row location and mark for deletion
                    if let Some(old_location) = self.index.remove(&entry.primary_key) {
                        deletions.push(old_location);
                    }
                    deletes_count += 1;
                }
            }
        }

        // Apply deletions first (using deletion vectors)
        if !deletions.is_empty() {
            debug!(count = deletions.len(), "applying deletions");
            self.mirror_writer
                .delete_rows(&self.mirror_table_name, deletions)
                .await?;
        }

        // Apply inserts
        if !rows_to_insert.is_empty() {
            let rows: Vec<TableRow> = rows_to_insert.iter().map(|(r, _, _)| r.clone()).collect();
            debug!(count = rows.len(), "inserting rows");

            let insert_result = self
                .mirror_writer
                .insert_rows(&self.mirror_table_name, rows)
                .await?;

            // Update index with new row locations
            for (i, (_, pk, seq_num)) in rows_to_insert.iter().enumerate() {
                let location = RowLocation::new(
                    insert_result.data_file_path.clone(),
                    insert_result.starting_row_index + i as u64,
                    seq_num.clone(),
                );
                self.index.insert(pk.clone(), location);
            }
        }

        // Update progress
        if let Some(last_seq) = &batch.last_sequence_number {
            self.progress
                .update(last_seq.clone(), batch.entries.len() as u64);
        }

        Ok(MergeBatchResult {
            events_processed: batch.entries.len(),
            inserts: inserts_count,
            updates: updates_count,
            deletes: deletes_count,
        })
    }

    /// Runs the merge process continuously until caught up.
    pub async fn merge_until_caught_up(&mut self) -> MergerResult<MergeSummary> {
        info!("starting merge until caught up");

        let mut total = MergeSummary::default();

        loop {
            let result = self.merge_batch().await?;

            if result.events_processed == 0 {
                // Caught up with changelog
                info!("caught up with changelog");
                break;
            }

            total.events_processed += result.events_processed;
            total.batches_processed += 1;

            info!(
                events = result.events_processed,
                inserts = result.inserts,
                updates = result.updates,
                deletes = result.deletes,
                batch = total.batches_processed,
                "batch completed"
            );
        }

        total.final_progress = self.progress.clone();

        info!(
            total_events = total.events_processed,
            total_batches = total.batches_processed,
            "merge completed"
        );

        Ok(total)
    }

    /// Returns the current progress.
    pub fn progress(&self) -> &MergeProgress {
        &self.progress
    }

    /// Returns index statistics.
    pub fn index_stats(&self) -> &IndexStats {
        self.index.stats()
    }

    /// Returns the number of entries in the index.
    pub fn index_len(&self) -> usize {
        self.index.len()
    }
}
