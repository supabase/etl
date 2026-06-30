use std::{collections::HashSet, fmt, num::NonZeroI32};

use etl_postgres::{below_version, version::POSTGRES_15};
use pg_escape::{quote_identifier, quote_literal};
use tokio::sync::watch;
use tokio_postgres::{CopyOutStream, SimpleQueryMessage, Transaction};
use tracing::warn;

use super::{
    child::ChildPgReplicationClient,
    query::PgReplicationQueryTarget,
    raw::{PgReplicationClient, PgReplicationConnectionConfig},
    types::{CtidPartition, PostgresConnectionUpdate},
    utils::get_row_value,
};
use crate::{
    bail,
    error::{ErrorKind, EtlResult},
    etl_error,
    postgres::codec::{ColumnSchemaMessage, IdentityMessage, build_table_schema},
    schema::{ColumnSchema, SnapshotId, TableId, TableName, TableSchema},
};

/// Builds a `COPY ... TO STDOUT` query that selects rows within a ctid range.
///
/// The query applies an optional publication row filter in addition to the ctid
/// bounds.
fn build_ctid_copy_query(
    table_name: &TableName,
    column_list: &str,
    row_filter: Option<&str>,
    partition: &CtidPartition,
) -> String {
    let quoted_table_name = table_name.as_quoted_identifier();
    let ctid_predicate = match partition {
        CtidPartition::OpenStart { end_tid } => {
            format!("ctid < {}::tid", quote_literal(end_tid))
        }
        CtidPartition::Closed { start_tid, end_tid } => {
            format!(
                "ctid >= {}::tid and ctid < {}::tid",
                quote_literal(start_tid),
                quote_literal(end_tid),
            )
        }
        CtidPartition::OpenEnd { start_tid } => {
            format!("ctid >= {}::tid", quote_literal(start_tid))
        }
    };

    if let Some(row_filter) = row_filter {
        format!(
            "copy (select {column_list} from {quoted_table_name} where {ctid_predicate} and \
             ({row_filter})) to stdout with (format text);",
        )
    } else {
        format!(
            "copy (select {column_list} from {quoted_table_name} where {ctid_predicate}) to \
             stdout with (format text);",
        )
    }
}

/// Planning statistics used to split table copy work.
#[derive(Debug)]
pub(crate) struct TableCopyPlanningEstimate {
    /// Current physical relation size in Postgres blocks.
    pub(crate) table_blocks: i64,
    /// Estimated row count from Postgres statistics.
    pub(crate) estimated_rows: i64,
}

/// Common state for an open replication transaction.
///
/// This stays private so parent and child transaction wrappers can expose
/// different capabilities while reusing the same transaction plumbing.
struct PgReplicationTransactionCore<'a> {
    /// The underlying PostgreSQL transaction.
    transaction: Transaction<'a>,
    /// The server version observed on the owning connection.
    server_version: Option<NonZeroI32>,
    /// Updates emitted by the owning connection task.
    connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
}

impl<'a> PgReplicationTransactionCore<'a> {
    /// Wraps a transaction and its connection metadata.
    fn new(
        transaction: Transaction<'a>,
        server_version: Option<NonZeroI32>,
        connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
    ) -> Self {
        Self { transaction, server_version, connection_updates_rx }
    }

    /// Returns this transaction as a query target.
    fn target(&self) -> PgReplicationQueryTarget<'_, 'a> {
        PgReplicationQueryTarget::Transaction(&self.transaction)
    }

    /// Retrieves the schema information for the supplied table.
    async fn get_table_schema(&self, table_id: TableId) -> EtlResult<TableSchema> {
        let (table_name, columns, identity) = self.get_table_schema_snapshot(table_id).await?;

        Ok(build_table_schema(
            table_id,
            table_name,
            columns,
            identity.primary_key_attnums,
            SnapshotId::initial(),
        ))
    }

    /// Retrieves the schema and identity information for the supplied table.
    async fn get_table_schema_with_identity(
        &self,
        table_id: TableId,
    ) -> EtlResult<(TableSchema, IdentityMessage)> {
        let (table_name, columns, identity) = self.get_table_schema_snapshot(table_id).await?;
        let table_schema = build_table_schema(
            table_id,
            table_name,
            columns,
            identity.primary_key_attnums.clone(),
            SnapshotId::initial(),
        );

        Ok((table_schema, identity))
    }

    /// Retrieves the names of columns being replicated for a table in a
    /// publication.
    async fn get_replicated_column_names(
        &self,
        table_id: TableId,
        table_schema: &TableSchema,
        publication_name: &str,
    ) -> EtlResult<HashSet<String>> {
        // Column filtering in publications was added in Postgres 15. For earlier
        // versions, all columns are replicated.
        if below_version!(self.server_version, POSTGRES_15) {
            return Ok(table_schema
                .column_schemas
                .iter()
                .map(|column_schema| column_schema.name.clone())
                .collect());
        }

        // Query pg_publication_tables using unnest() to properly decode the attnames
        // array. This correctly handles column names containing special
        // characters (spaces, commas, quotes) that would break naive string
        // parsing.
        let column_query = format!(
            "select true as table_in_publication, u.column_name
             from pg_publication_tables pt
             left join lateral unnest(pt.attnames) as u(column_name) on true
             join pg_namespace n on n.nspname = pt.schemaname
             join pg_class c on c.relnamespace = n.oid and c.relname = pt.tablename
             where pt.pubname = {} and c.oid = {};",
            quote_literal(publication_name),
            table_id,
        );

        let rows = self.transaction.simple_query(&column_query).await?;
        let mut column_names = HashSet::new();
        let mut table_in_publication = false;

        for row in rows {
            if let SimpleQueryMessage::Row(row) = row {
                table_in_publication = true;

                if let Some(column_name) = row.try_get::<&str>("column_name")? {
                    column_names.insert(column_name.to_owned());
                }
            }
        }

        if !table_in_publication {
            bail!(
                ErrorKind::ConfigError,
                "Table not in publication",
                format!(
                    "Table '{}' is not included in publication '{}'. The table must be added to \
                     the publication to receive events.",
                    table_schema.name, publication_name
                )
            );
        }

        if column_names.is_empty() {
            return Ok(table_schema
                .column_schemas
                .iter()
                .map(|column_schema| column_schema.name.clone())
                .collect());
        }

        Ok(column_names)
    }

    /// Creates a COPY stream for reading data from the specified table.
    async fn get_table_copy_stream(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        self.get_table_copy_stream_with_filter_table(
            table_id,
            table_id,
            column_schemas,
            publication_name,
        )
        .await
    }

    /// Creates a COPY stream for reading data from `table_id`, using
    /// `filter_table_id` to resolve publication row filters.
    ///
    /// Serial copy passes the same table ID for both values. Parallel copy of a
    /// partitioned table can pass a leaf partition as the physical copy source
    /// while resolving row filters from the tracked published root or subtree.
    async fn get_table_copy_stream_with_filter_table(
        &self,
        table_id: TableId,
        filter_table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        let column_list = column_schemas
            .iter()
            .map(|col| quote_identifier(&col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let table_name = self.get_table_name(table_id).await?;
        let row_filter = self.get_row_filter(filter_table_id, publication_name).await?;

        let copy_query = if let Some(row_filter) = row_filter {
            format!(
                r#"copy (select {} from {} where {}) to stdout with (format text);"#,
                column_list,
                table_name.as_quoted_identifier(),
                row_filter,
            )
        } else {
            format!(
                r#"copy (select {} from {}) to stdout with (format text);"#,
                column_list,
                table_name.as_quoted_identifier(),
            )
        };

        let stream = self.transaction.client().copy_out_simple(&copy_query).await?;

        Ok(stream)
    }

    /// Exports the current transaction snapshot.
    async fn export_snapshot(&self) -> EtlResult<String> {
        let results = self.transaction.simple_query("select pg_export_snapshot();").await?;

        for msg in results {
            if let SimpleQueryMessage::Row(row) = msg {
                let snapshot_id = row
                    .try_get(0)?
                    .ok_or_else(|| {
                        etl_error!(
                            ErrorKind::InvalidState,
                            "PostgreSQL pg_export_snapshot returned NULL"
                        )
                    })?
                    .to_owned();
                return Ok(snapshot_id);
            }
        }

        Err(etl_error!(ErrorKind::InvalidState, "PostgreSQL pg_export_snapshot returned no rows"))
    }

    /// Computes balanced ctid partition ranges using page-based estimation.
    async fn plan_ctid_partitions(
        &self,
        table_id: TableId,
        num_partitions: u16,
    ) -> EtlResult<Vec<CtidPartition>> {
        if num_partitions == 0 {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "Number of ctid partitions must be greater than zero"
            ));
        }

        let table_blocks = self.get_table_copy_planning_estimate(table_id).await?.table_blocks;

        if table_blocks == 0 {
            return Ok(vec![]);
        }

        let requested_partitions = i64::from(num_partitions);
        let effective_partitions = requested_partitions.min(table_blocks);
        // We perform ceil-division with the classic formula to avoid having undersized
        // partitions.
        let blocks_per_partition = (table_blocks + effective_partitions - 1) / effective_partitions;

        let mut partitions = Vec::with_capacity(effective_partitions as usize);
        for i in 0..effective_partitions {
            let start_block = i * blocks_per_partition;
            // We use the next block as exclusive delimiter for the query to avoid possible
            // issues in the way we determine boundaries.
            let end_block_exclusive = ((i + 1) * blocks_per_partition).min(table_blocks);

            let partition = if effective_partitions == 1 {
                CtidPartition::OpenEnd { start_tid: "(0,1)".to_owned() }
            } else if i == 0 {
                CtidPartition::OpenStart { end_tid: format!("({end_block_exclusive},1)") }
            } else if i == effective_partitions - 1 {
                CtidPartition::OpenEnd { start_tid: format!("({start_block},1)") }
            } else {
                CtidPartition::Closed {
                    start_tid: format!("({start_block},1)"),
                    end_tid: format!("({end_block_exclusive},1)"),
                }
            };

            partitions.push(partition);
        }

        Ok(partitions)
    }

    /// Returns quick planner statistics for table copy.
    async fn get_table_copy_planning_estimate(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableCopyPlanningEstimate> {
        // This query does not use MVCC, so it reflects the relation size at the
        // time partition planning runs. The row count is an estimate from
        // pg_class so planning stays cheap for huge tables.
        let estimate_query = format!(
            "select pg_relation_size({table_id}::regclass)::bigint / \
             current_setting('block_size')::bigint as table_blocks,
             greatest(c.reltuples, 0)::bigint as estimated_rows
             from pg_class c
             where c.oid = {table_id};"
        );

        for message in self.transaction.simple_query(&estimate_query).await? {
            if let SimpleQueryMessage::Row(row) = message {
                let Some(table_blocks) = row.try_get::<&str>("table_blocks")? else {
                    continue;
                };
                let Some(estimated_rows) = row.try_get::<&str>("estimated_rows")? else {
                    continue;
                };

                if let (Ok(table_blocks), Ok(estimated_rows)) =
                    (table_blocks.parse(), estimated_rows.parse())
                {
                    return Ok(TableCopyPlanningEstimate { table_blocks, estimated_rows });
                }
            }
        }

        Err(etl_error!(
            ErrorKind::SourceSchemaError,
            "Could not retrieve table copy planning estimate",
            format!("table_id: {table_id}")
        ))
    }

    /// Checks whether the given table is a partitioned parent (`relkind =
    /// 'p'`).
    async fn is_partitioned_table(&self, table_id: TableId) -> EtlResult<bool> {
        self.target().has_partitioned_tables(&[table_id]).await
    }

    /// Returns the OIDs of all leaf partitions for a partitioned table.
    async fn get_leaf_partitions(&self, table_id: TableId) -> EtlResult<Vec<TableId>> {
        let query = format!(
            "select relid::oid as oid from pg_partition_tree({table_id}::regclass) where isleaf \
             and relid != {table_id}::regclass order by relid::oid;"
        );

        let mut leaves = Vec::new();
        for msg in self.transaction.simple_query(&query).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                let oid = get_row_value::<TableId>(&row, "oid", "pg_class")?;
                leaves.push(oid);
            }
        }

        Ok(leaves)
    }

    /// Creates a COPY stream for a ctid partition range of the specified table.
    async fn get_table_copy_stream_with_ctid_partition(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
        partition: &CtidPartition,
    ) -> EtlResult<CopyOutStream> {
        self.get_table_copy_stream_with_ctid_partition_and_filter_table(
            table_id,
            table_id,
            column_schemas,
            publication_name,
            partition,
        )
        .await
    }

    /// Creates a COPY stream for a ctid range from `table_id`, using
    /// `filter_table_id` to resolve publication row filters.
    async fn get_table_copy_stream_with_ctid_partition_and_filter_table(
        &self,
        table_id: TableId,
        filter_table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
        partition: &CtidPartition,
    ) -> EtlResult<CopyOutStream> {
        let table_name = self.get_table_name(table_id).await?;
        let row_filter = self.get_row_filter(filter_table_id, publication_name).await?;

        let column_list = column_schemas
            .iter()
            .map(|col| quote_identifier(&col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let query =
            build_ctid_copy_query(&table_name, &column_list, row_filter.as_deref(), partition);

        let stream = self.transaction.client().copy_out_simple(&query).await?;

        Ok(stream)
    }

    /// Loads the table name.
    async fn get_table_name(&self, table_id: TableId) -> EtlResult<TableName> {
        let table_info_query = format!(
            "select n.nspname as schema_name, c.relname as table_name
            from pg_class c
            join pg_namespace n on c.relnamespace = n.oid
            where c.oid = {table_id}",
        );

        for message in self.transaction.simple_query(&table_info_query).await? {
            if let SimpleQueryMessage::Row(row) = message {
                let schema_name = get_row_value::<String>(&row, "schema_name", "pg_namespace")?;
                let table_name = get_row_value::<String>(&row, "table_name", "pg_class")?;

                return Ok(TableName { schema: schema_name, name: table_name });
            }
        }

        bail!(
            ErrorKind::SourceSchemaError,
            "Table not found in source database",
            format!("Table with ID {} not found in database", table_id)
        );
    }

    /// Warns when the source table contains generated columns.
    async fn warn_if_generated_columns_exist(&self, table_id: TableId) -> EtlResult<()> {
        let generated_columns_check_query = format!(
            r#"select exists (
                select 1
                from pg_attribute
                where attrelid = {table_id}
                    and attnum > 0
                    and not attisdropped
                    and attgenerated != ''
            ) as has_generated;"#
        );

        for message in self.transaction.simple_query(&generated_columns_check_query).await? {
            if let SimpleQueryMessage::Row(row) = message {
                let has_generated_columns =
                    get_row_value::<String>(&row, "has_generated", "pg_attribute")? == "t";
                if has_generated_columns {
                    warn!(
                        "Table {} contains generated columns that will NOT be replicated. \
                         Generated columns are not supported in PostgreSQL logical replication \
                         and will be excluded from the ETL schema. These columns will NOT appear \
                         in the destination.",
                        table_id
                    );
                }

                // Explicity break for clarity; this query returns a single
                // SimpleQueryMessage::Row.
                break;
            }
        }

        Ok(())
    }

    /// Retrieves the raw schema snapshot.
    async fn get_table_schema_snapshot(
        &self,
        table_id: TableId,
    ) -> EtlResult<(TableName, Vec<ColumnSchemaMessage>, IdentityMessage)> {
        self.warn_if_generated_columns_exist(table_id).await?;

        let schema_snapshot_query = format!(
            r#"
            with table_info as (
                select
                    n.nspname as schema_name,
                    c.relname as table_name
                from pg_class c
                join pg_namespace n on c.relnamespace = n.oid
                where c.oid = {table_id}
            ),
            schema_snapshot as (
                select coalesce(
                    pg_catalog.jsonb_agg(
                        pg_catalog.jsonb_build_object(
                            'attname', s.attname,
                            'attnum', s.attnum,
                            'atttypid', s.atttypid::pg_catalog.int8,
                            'atttypmod', s.atttypmod,
                            'attnotnull', s.attnotnull,
                            'default_expression', s.default_expression
                        )
                        order by s.attnum
                    ),
                    '[]'::pg_catalog.jsonb
                ) as columns
                from etl.describe_table_schema({table_id}) s
            )
            select
                ti.schema_name,
                ti.table_name,
                ss.columns::pg_catalog.text as columns,
                etl.describe_table_identity({table_id})::pg_catalog.text as identity
            from table_info ti
            cross join schema_snapshot ss
            "#,
        );

        // TODO: there's a lot of code using simple_query but only checking for
        // SimpleQueryMessage::Row, a small optimization could be done here if we
        // upgraded tokio-postgres to a newer version in order to use
        // https://docs.rs/tokio-postgres/0.7.15/tokio_postgres/struct.Client.html#method.simple_query_raw
        // to filter on SimpleQueryMessage::Row and avoid useless allocations.
        for message in self.transaction.simple_query(&schema_snapshot_query).await? {
            if let SimpleQueryMessage::Row(row) = message {
                let schema_name = get_row_value::<String>(&row, "schema_name", "pg_namespace")?;
                let table_name = get_row_value::<String>(&row, "table_name", "pg_class")?;
                let columns_json = get_row_value::<String>(&row, "columns", "etl")?;
                let identity_json = get_row_value::<String>(&row, "identity", "etl")?;
                let columns: Vec<ColumnSchemaMessage> = serde_json::from_str(&columns_json)
                    .map_err(|err| {
                        etl_error!(
                            ErrorKind::DeserializationError,
                            "Invalid table schema snapshot",
                            format!(
                                "Failed to parse describe_table_schema payload for table {}: {}",
                                table_id, err
                            )
                        )
                    })?;
                let identity: IdentityMessage =
                    serde_json::from_str(&identity_json).map_err(|err| {
                        etl_error!(
                            ErrorKind::DeserializationError,
                            "Invalid table identity snapshot",
                            format!(
                                "Failed to parse describe_table_identity payload for table {}: {}",
                                table_id, err
                            )
                        )
                    })?;

                return Ok((TableName::new(schema_name, table_name), columns, identity));
            }
        }

        bail!(
            ErrorKind::SourceSchemaError,
            "Table schema snapshot not found in source database",
            format!("Table with ID {} not found in database", table_id)
        )
    }

    /// Retrieves the publication row filter for a table.
    async fn get_row_filter(
        &self,
        table_id: TableId,
        publication_name: Option<&str>,
    ) -> EtlResult<Option<String>> {
        // Row filters on publications were added in Postgres 15. For any earlier
        // versions we know that there is no row filter.
        if below_version!(self.server_version, POSTGRES_15) {
            return Ok(None);
        }

        // If we don't have a publication the row filter is implicitly non-existent.
        let Some(publication_name) = publication_name else {
            return Ok(None);
        };

        // This uses the same query as the `pg_publication_tables`, but with some minor
        // tweaks (COALESCE, only return the rowfilter, filter on oid and
        // pubname). All of these are available >= Postgres 15.
        let row_filter_query = format!(
            "select pt.rowfilter as row_filter
                from pg_publication_tables pt
                join pg_namespace n on n.nspname = pt.schemaname
                join pg_class c on c.relnamespace = n.oid AND c.relname = pt.tablename
                where pt.pubname = {} and c.oid = {};",
            quote_literal(publication_name),
            table_id,
        );

        let row_filters = self.transaction.simple_query(&row_filter_query).await?;

        for row_filter in row_filters {
            if let SimpleQueryMessage::Row(row) = row_filter {
                let row_filter = row.try_get("row_filter")?;
                match row_filter {
                    None => return Ok(None),
                    Some(row_filter) => return Ok(Some(row_filter.to_owned())),
                }
            }
        }

        Ok(None)
    }

    /// Returns a receiver for background connection task updates.
    fn connection_updates_rx(&self) -> watch::Receiver<PostgresConnectionUpdate> {
        self.connection_updates_rx.clone()
    }

    /// Commits the current transaction.
    async fn commit(self) -> EtlResult<()> {
        self.transaction.commit().await?;

        Ok(())
    }
}

impl fmt::Debug for PgReplicationTransactionCore<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgReplicationTransactionCore")
            .field("server_version", &self.server_version)
            .finish_non_exhaustive()
    }
}

/// A transaction that operates within the context of a replication slot.
///
/// The transaction borrows the client that created the slot. As long as this
/// value is alive, the parent connection cannot be used for other commands.
/// Child connections forked from this transaction rely on snapshots exported by
/// it and must finish their snapshot-dependent work before this transaction is
/// committed.
#[derive(Debug)]
pub struct PgReplicationTransaction<'a> {
    /// Common transaction state and query helpers.
    core: PgReplicationTransactionCore<'a>,
    /// Shared settings used to fork child connections that import this
    /// transaction's snapshot.
    connection_config: PgReplicationConnectionConfig,
}

impl<'a> PgReplicationTransaction<'a> {
    /// Wraps a transaction created from a replication client.
    pub(super) fn new(
        transaction: Transaction<'a>,
        connection_config: PgReplicationConnectionConfig,
        server_version: Option<NonZeroI32>,
        connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
    ) -> Self {
        let core =
            PgReplicationTransactionCore::new(transaction, server_version, connection_updates_rx);

        Self { core, connection_config }
    }

    /// Retrieves the schema information for the supplied table.
    pub async fn get_table_schema(&self, table_id: TableId) -> EtlResult<TableSchema> {
        self.core.get_table_schema(table_id).await
    }

    /// Retrieves the schema and identity information for the supplied table.
    pub(crate) async fn get_table_schema_with_identity(
        &self,
        table_id: TableId,
    ) -> EtlResult<(TableSchema, IdentityMessage)> {
        self.core.get_table_schema_with_identity(table_id).await
    }

    /// Retrieves the names of columns being replicated for a table in a
    /// publication.
    ///
    /// Returns a [`HashSet`] containing the names of columns from the given
    /// [`TableSchema`] that are included in the specified publication for
    /// the given [`TableId`].
    pub async fn get_replicated_column_names(
        &self,
        table_id: TableId,
        table_schema: &TableSchema,
        publication_name: &str,
    ) -> EtlResult<HashSet<String>> {
        self.core.get_replicated_column_names(table_id, table_schema, publication_name).await
    }

    /// Creates a COPY stream for reading data from the specified table.
    ///
    /// The stream will include only the columns specified in `column_schemas`.
    pub async fn get_table_copy_stream(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        self.core.get_table_copy_stream(table_id, column_schemas, publication_name).await
    }

    /// Exports the current transaction snapshot so child connections can share
    /// it.
    ///
    /// Calls `pg_export_snapshot()` within the slot's `REPEATABLE READ`
    /// transaction.
    pub async fn export_snapshot(&self) -> EtlResult<String> {
        self.core.export_snapshot().await
    }

    /// Computes balanced ctid partition ranges using page-based estimation.
    ///
    /// Returns one [`CtidPartition`] per partition, or an empty vec if the
    /// table has no rows.
    pub async fn plan_ctid_partitions(
        &self,
        table_id: TableId,
        num_partitions: u16,
    ) -> EtlResult<Vec<CtidPartition>> {
        self.core.plan_ctid_partitions(table_id, num_partitions).await
    }

    /// Returns quick planner statistics for table copy.
    pub(crate) async fn get_table_copy_planning_estimate(
        &self,
        table_id: TableId,
    ) -> EtlResult<TableCopyPlanningEstimate> {
        self.core.get_table_copy_planning_estimate(table_id).await
    }

    /// Checks whether the given table is a partitioned parent (`relkind =
    /// 'p'`).
    pub(crate) async fn is_partitioned_table(&self, table_id: TableId) -> EtlResult<bool> {
        self.core.is_partitioned_table(table_id).await
    }

    /// Returns the OIDs of all leaf partitions for a partitioned table.
    ///
    /// Walks `pg_inherits` recursively and returns only leaf nodes (`relkind =
    /// 'r'`). For a non-partitioned table this returns an empty vec.
    pub(crate) async fn get_leaf_partitions(&self, table_id: TableId) -> EtlResult<Vec<TableId>> {
        self.core.get_leaf_partitions(table_id).await
    }

    /// Creates a new child connection that can import this transaction's
    /// snapshot.
    ///
    /// The returned child is not statically tied to this transaction's lifetime
    /// because child copy work is moved into spawned async tasks. Callers must
    /// keep this parent transaction open until every child transaction that
    /// imports its exported snapshot has finished.
    pub async fn fork_child(&self) -> EtlResult<ChildPgReplicationClient> {
        PgReplicationClient::connect_child_from_config(self.connection_config.clone()).await
    }

    /// Commits the current transaction.
    pub async fn commit(self) -> EtlResult<()> {
        self.core.commit().await
    }
}

/// A transaction on a child connection pinned to an exported snapshot.
///
/// The transaction borrows the child connection for its full lifetime, ensuring
/// no other commands can be issued on that connection until it is committed or
/// dropped. The exported parent snapshot it imports must remain valid for the
/// duration of the child transaction; this dependency is maintained by callers
/// rather than by this type's lifetime.
#[derive(Debug)]
pub struct PgChildReplicationTransaction<'a> {
    /// Common transaction state and query helpers.
    core: PgReplicationTransactionCore<'a>,
}

impl<'a> PgChildReplicationTransaction<'a> {
    /// Creates a new transaction for an already pinned child connection.
    pub(super) fn new(
        transaction: Transaction<'a>,
        server_version: Option<NonZeroI32>,
        connection_updates_rx: watch::Receiver<PostgresConnectionUpdate>,
    ) -> Self {
        let core =
            PgReplicationTransactionCore::new(transaction, server_version, connection_updates_rx);

        Self { core }
    }

    /// Creates a COPY stream for a ctid partition range of the specified table.
    ///
    /// Resolves the table name and row filter internally, then streams rows
    /// whose ctid falls within the given partition bounds.
    pub async fn get_table_copy_stream_with_ctid_partition(
        &self,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
        partition: &CtidPartition,
    ) -> EtlResult<CopyOutStream> {
        self.core
            .get_table_copy_stream_with_ctid_partition(
                table_id,
                column_schemas,
                publication_name,
                partition,
            )
            .await
    }

    /// Creates a COPY stream for a ctid partition of `table_id`, using
    /// `filter_table_id` to resolve publication row filters.
    pub(crate) async fn get_table_copy_stream_with_ctid_partition_and_filter_table(
        &self,
        table_id: TableId,
        filter_table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
        partition: &CtidPartition,
    ) -> EtlResult<CopyOutStream> {
        self.core
            .get_table_copy_stream_with_ctid_partition_and_filter_table(
                table_id,
                filter_table_id,
                column_schemas,
                publication_name,
                partition,
            )
            .await
    }

    /// Returns a receiver for background connection task updates.
    pub(crate) fn connection_updates_rx(&self) -> watch::Receiver<PostgresConnectionUpdate> {
        self.core.connection_updates_rx()
    }

    /// Commits the current transaction.
    pub async fn commit(self) -> EtlResult<()> {
        self.core.commit().await
    }
}

#[cfg(test)]
mod tests {
    use super::{CtidPartition, build_ctid_copy_query};
    use crate::schema::TableName;

    #[test]
    fn build_ctid_copy_query_quotes_mixed_case_table_names() {
        let query = build_ctid_copy_query(
            &TableName::new("public".to_owned(), "User".to_owned()),
            "\"id\", \"email\"",
            None,
            &CtidPartition::OpenEnd { start_tid: "(0,1)".to_owned() },
        );

        assert!(query.contains("from public.\"User\""));
        assert!(!query.contains("from public.User"));
    }

    #[test]
    fn build_ctid_copy_query_quotes_mixed_case_table_names_with_row_filter() {
        let query = build_ctid_copy_query(
            &TableName::new("public".to_owned(), "CommentReadStatus".to_owned()),
            "\"id\"",
            Some("\"tenantId\" is not null"),
            &CtidPartition::OpenStart { end_tid: "(10,1)".to_owned() },
        );

        assert!(query.contains("from public.\"CommentReadStatus\""));
        assert!(query.contains("and (\"tenantId\" is not null)"));
        assert!(!query.contains("from public.CommentReadStatus"));
    }
}
