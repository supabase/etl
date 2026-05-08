mod destination_table_metadata;
mod table_replication_state;
mod table_schema;

pub use destination_table_metadata::{
    delete_destination_table_metadata, delete_destination_tables_metadata_for_all_tables,
    load_destination_tables_metadata, store_destination_table_metadata,
};
pub use table_replication_state::{
    delete_replication_state, delete_replication_state_for_all_tables, reset_replication_state,
    rollback_replication_state, table_replication_state_rows, update_replication_state_raw,
};
pub use table_schema::{
    delete_obsolete_table_schema_versions, delete_table_schemas,
    delete_table_schemas_for_all_tables, load_table_schema_at_snapshot, load_table_schemas,
    load_table_schemas_at_snapshot, store_table_schema,
};

use crate::{tokio::PgSourceError, types::SnapshotId};

/// Parses a Postgres LSN string into a [`SnapshotId`].
fn parse_snapshot_id(value: &str) -> Result<SnapshotId, PgSourceError> {
    SnapshotId::from_pg_lsn_string(value).map_err(|err| {
        PgSourceError::InvalidData(format!("Snapshot ID deserialization failed: {err}"))
    })
}
