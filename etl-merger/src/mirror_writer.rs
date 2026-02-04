//! Writes merged data to mirror tables using deletion vectors for updates/deletes.

use std::collections::HashMap;

use etl::types::{ColumnSchema, TableRow};
use etl_destinations::iceberg::{
    IcebergClient, commit_deletion_vectors, create_deletion_vector_data_file, write_deletion_vector,
};
use tracing::debug;

use crate::error::{MergerError, MergerResult};
use crate::index::RowLocation;

/// Information about rows inserted into a single data file.
#[derive(Debug, Clone)]
pub struct FileInsertInfo {
    /// Path to the data file containing the inserted rows.
    pub data_file_path: String,
    /// Starting row index in the data file (always 0 for new files).
    pub starting_row_index: u64,
    /// Number of rows inserted into this file.
    pub row_count: u64,
}

/// Result of an insert operation.
#[derive(Debug)]
pub struct InsertResult {
    /// Information about each data file created during the insert.
    /// Multiple files can be created when using RollingFileWriter.
    pub files: Vec<FileInsertInfo>,
}

/// Writes merged data to mirror tables using deletion vectors for updates/deletes.
pub struct MirrorWriter {
    client: IcebergClient,
    namespace: String,
}

impl MirrorWriter {
    /// Creates a new mirror writer.
    pub fn new(client: IcebergClient, namespace: String) -> Self {
        Self { client, namespace }
    }

    /// Returns the namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Creates a mirror table if it doesn't exist.
    ///
    /// The mirror table has the same schema as the source Postgres table
    /// (without the CDC columns that are in the changelog table).
    pub async fn create_table_if_missing(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
    ) -> MergerResult<()> {
        debug!(%table_name, namespace = %self.namespace, "creating mirror table if missing");
        self.client
            .create_table_if_missing(&self.namespace, table_name.to_string(), column_schemas)
            .await?;
        Ok(())
    }

    /// Creates the namespace if it doesn't exist.
    pub async fn create_namespace_if_missing(&self) -> MergerResult<()> {
        debug!(namespace = %self.namespace, "creating namespace if missing");
        self.client
            .create_namespace_if_missing(&self.namespace)
            .await?;
        Ok(())
    }

    /// Inserts new rows into the mirror table.
    ///
    /// Returns information about the created data files and row counts.
    /// Multiple data files may be created if the data exceeds the file size limit.
    pub async fn insert_rows(
        &self,
        table_name: &str,
        rows: Vec<TableRow>,
    ) -> MergerResult<InsertResult> {
        let row_count = rows.len() as u64;
        debug!(%table_name, %row_count, "inserting rows into mirror table");

        let insert_result = self
            .client
            .insert_rows(self.namespace.clone(), table_name.to_string(), rows)
            .await
            .map_err(MergerError::Etl)?;

        // Convert IcebergClient result to MirrorWriter result
        let files: Vec<FileInsertInfo> = insert_result
            .data_files
            .into_iter()
            .map(|df| FileInsertInfo {
                data_file_path: df.file_path,
                starting_row_index: 0, // Each file starts at row 0
                row_count: df.record_count,
            })
            .collect();

        Ok(InsertResult { files })
    }

    /// Marks rows as deleted using deletion vectors.
    ///
    /// Groups deletions by data file and writes Puffin files.
    pub async fn delete_rows(
        &self,
        table_name: &str,
        deletions: Vec<RowLocation>,
    ) -> MergerResult<()> {
        if deletions.is_empty() {
            return Ok(());
        }

        debug!(%table_name, count = deletions.len(), "marking rows as deleted");

        let table = self
            .client
            .load_table(self.namespace.clone(), table_name.to_string())
            .await?;

        // Group deletions by data file
        let mut deletions_by_file: HashMap<String, Vec<u64>> = HashMap::new();
        for loc in deletions {
            deletions_by_file
                .entry(loc.data_file_path)
                .or_default()
                .push(loc.row_index);
        }

        // Write deletion vectors for each data file
        let mut delete_files = Vec::new();
        for (data_file_path, mut row_positions) in deletions_by_file {
            // Sort row positions (required by RoaringTreemap::append)
            row_positions.sort_unstable();

            debug!(%data_file_path, positions = ?row_positions, "creating deletion vector");

            let descriptor = write_deletion_vector(
                table.file_io(),
                table.metadata().location(),
                &row_positions,
                &data_file_path,
            )
            .await?;

            let delete_file = create_deletion_vector_data_file(&descriptor, &data_file_path);
            delete_files.push(delete_file);
        }

        // Commit all deletion vectors in a single transaction
        commit_deletion_vectors(&table, &**self.client.catalog(), delete_files).await?;

        Ok(())
    }

    /// Checks if a table exists.
    pub async fn table_exists(&self, table_name: &str) -> MergerResult<bool> {
        Ok(self
            .client
            .table_exists(&self.namespace, table_name.to_string())
            .await?)
    }
}
