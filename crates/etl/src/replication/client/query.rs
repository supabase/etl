use std::{collections::HashSet, num::NonZeroI32};

use etl_postgres::{below_version, version::POSTGRES_15};
use pg_escape::{quote_identifier, quote_literal};
use tokio_postgres::{Client, CopyOutStream, SimpleQueryMessage, Transaction, error::SqlState};
use tracing::warn;

use super::{
    types::{CreateSlotResult, CtidPartition, SnapshotAction},
    utils::get_row_value,
};
use crate::{
    bail,
    conversions::{ColumnSchemaMessage, IdentityMessage, build_table_schema},
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{ColumnSchema, PgLsn, SnapshotId, TableId, TableName, TableSchema},
};

/// Private executor for query helpers shared by clients and open transactions.
///
/// This keeps the SQL implementation in one place while the public client and
/// transaction types decide which capabilities are exposed to callers.
#[derive(Clone, Copy)]
pub(super) enum PgReplicationQueryTarget<'a, 'tx> {
    /// A plain PostgreSQL client connection.
    Client(&'a Client),
    /// An open PostgreSQL transaction.
    Transaction(&'a Transaction<'tx>),
}

impl PgReplicationQueryTarget<'_, '_> {
    /// Executes a simple query on the target.
    async fn simple_query(
        self,
        query: &str,
    ) -> Result<Vec<SimpleQueryMessage>, tokio_postgres::Error> {
        match self {
            PgReplicationQueryTarget::Client(client) => client.simple_query(query).await,
            PgReplicationQueryTarget::Transaction(transaction) => {
                transaction.simple_query(query).await
            }
        }
    }

    /// Executes a copy-out query on the target.
    async fn copy_out(self, query: &str) -> Result<CopyOutStream, tokio_postgres::Error> {
        match self {
            PgReplicationQueryTarget::Client(client) => client.copy_out_simple(query).await,
            PgReplicationQueryTarget::Transaction(transaction) => transaction.copy_out(query).await,
        }
    }

    /// Creates a replication slot on this target.
    pub(super) async fn create_slot(
        self,
        slot_name: &str,
        snapshot_action: SnapshotAction,
    ) -> EtlResult<CreateSlotResult> {
        // Do not convert the query or the options to lowercase, since the lexer for
        // replication commands (repl_scanner.l) in Postgres code expects the commands
        // in uppercase. This probably should be fixed in upstream, but for now we will
        // keep the commands in uppercase.
        let snapshot_option = match snapshot_action {
            SnapshotAction::Use => "USE_SNAPSHOT",
            SnapshotAction::NoExport => "NOEXPORT_SNAPSHOT",
        };
        let query = format!(
            r#"CREATE_REPLICATION_SLOT {} LOGICAL pgoutput {}"#,
            quote_identifier(slot_name),
            snapshot_option
        );
        match self.simple_query(&query).await {
            Ok(results) => {
                for result in results {
                    if let SimpleQueryMessage::Row(row) = result {
                        let consistent_point = get_row_value::<PgLsn>(
                            &row,
                            "consistent_point",
                            "pg_replication_slots",
                        )?;
                        let slot = CreateSlotResult { consistent_point };

                        return Ok(slot);
                    }
                }
            }
            Err(err) => {
                if let Some(code) = err.code()
                    && *code == SqlState::DUPLICATE_OBJECT
                {
                    bail!(
                        ErrorKind::ReplicationSlotAlreadyExists,
                        "Replication slot already exists",
                        format!("Replication slot '{}' already exists in database", slot_name)
                    );
                }

                return Err(err.into());
            }
        }

        Err(etl_error!(ErrorKind::ReplicationSlotNotCreated, "Replication slot creation failed"))
    }

    /// Retrieves the full schema for a single table.
    pub(super) async fn get_table_schema(self, table_id: TableId) -> EtlResult<TableSchema> {
        let (table_name, columns, identity) = self.get_table_schema_snapshot(table_id).await?;

        Ok(build_table_schema(
            table_id,
            table_name,
            columns,
            identity.primary_key_attnums,
            SnapshotId::initial(),
        ))
    }

    /// Retrieves the full schema and identity information.
    pub(super) async fn get_table_schema_with_identity(
        self,
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

    /// Loads the table name.
    async fn get_table_name(self, table_id: TableId) -> EtlResult<TableName> {
        let table_info_query = format!(
            "select n.nspname as schema_name, c.relname as table_name
            from pg_class c
            join pg_namespace n on c.relnamespace = n.oid
            where c.oid = {table_id}",
        );

        for message in self.simple_query(&table_info_query).await? {
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
    async fn warn_if_generated_columns_exist(self, table_id: TableId) -> EtlResult<()> {
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

        for message in self.simple_query(&generated_columns_check_query).await? {
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
    pub(super) async fn get_table_schema_snapshot(
        self,
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
                            'attnotnull', s.attnotnull
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
        for message in self.simple_query(&schema_snapshot_query).await? {
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
        self,
        server_version: Option<NonZeroI32>,
        table_id: TableId,
        publication_name: Option<&str>,
    ) -> EtlResult<Option<String>> {
        // Row filters on publications were added in Postgres 15. For any earlier
        // versions we know that there is no row filter.
        if below_version!(server_version, POSTGRES_15) {
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

        let row_filters = self.simple_query(&row_filter_query).await?;

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

    /// Retrieves the names of columns being replicated for a table in a
    /// publication.
    pub(super) async fn get_replicated_column_names(
        self,
        server_version: Option<NonZeroI32>,
        table_id: TableId,
        table_schema: &TableSchema,
        publication_name: &str,
    ) -> EtlResult<HashSet<String>> {
        // Column filtering in publications was added in Postgres 15. For earlier
        // versions, all columns are replicated.
        if below_version!(server_version, POSTGRES_15) {
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

        let rows = self.simple_query(&column_query).await?;
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

    /// Returns the OIDs of all leaf partitions for a partitioned table.
    pub(super) async fn get_leaf_partitions(self, table_id: TableId) -> EtlResult<Vec<TableId>> {
        let query = format!(
            "select relid::oid as oid from pg_partition_tree({table_id}::regclass) where isleaf \
             and relid != {table_id}::regclass order by relid::oid;"
        );

        let mut leaves = Vec::new();
        for msg in self.simple_query(&query).await? {
            if let SimpleQueryMessage::Row(row) = msg {
                let oid = get_row_value::<TableId>(&row, "oid", "pg_class")?;
                leaves.push(oid);
            }
        }

        Ok(leaves)
    }

    /// Checks if any of the provided table IDs are partitioned tables.
    pub(super) async fn has_partitioned_tables(self, table_ids: &[TableId]) -> EtlResult<bool> {
        if table_ids.is_empty() {
            return Ok(false);
        }

        let table_oids_list =
            table_ids.iter().map(|id| id.0.to_string()).collect::<Vec<_>>().join(", ");

        let query = format!(
            "select 1 from pg_class where oid in ({table_oids_list}) and relkind = 'p' limit 1;"
        );

        for msg in self.simple_query(&query).await? {
            if let SimpleQueryMessage::Row(_) = msg {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Creates a COPY stream for reading data from a table.
    pub(super) async fn get_table_copy_stream(
        self,
        server_version: Option<NonZeroI32>,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
    ) -> EtlResult<CopyOutStream> {
        let column_list = column_schemas
            .iter()
            .map(|col| quote_identifier(&col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let table_name = self.get_table_name(table_id).await?;
        let row_filter = self.get_row_filter(server_version, table_id, publication_name).await?;

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

        let stream = self.copy_out(&copy_query).await?;

        Ok(stream)
    }

    /// Creates a COPY stream for a ctid partition.
    pub(super) async fn get_table_copy_stream_with_ctid_partition(
        self,
        server_version: Option<NonZeroI32>,
        table_id: TableId,
        column_schemas: &[ColumnSchema],
        publication_name: Option<&str>,
        partition: &CtidPartition,
    ) -> EtlResult<CopyOutStream> {
        let table_name = self.get_table_name(table_id).await?;
        let row_filter = self.get_row_filter(server_version, table_id, publication_name).await?;

        let column_list = column_schemas
            .iter()
            .map(|col| quote_identifier(&col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let query =
            build_ctid_copy_query(&table_name, &column_list, row_filter.as_deref(), partition);

        let stream = self.copy_out(&query).await?;

        Ok(stream)
    }

    /// Computes balanced ctid partition ranges using relation-size-based
    /// blocks.
    pub(super) async fn plan_ctid_partitions(
        self,
        table_id: TableId,
        num_partitions: u16,
    ) -> EtlResult<Vec<CtidPartition>> {
        if num_partitions == 0 {
            return Err(etl_error!(
                ErrorKind::ConfigError,
                "Number of ctid partitions must be greater than zero"
            ));
        }

        // We query how many blocks the table has at this point in time. Note that this
        // query doesn't use MVCC, so it's a real-time snapshot.
        let size_query = format!(
            "select pg_relation_size({table_id}::regclass)::bigint / \
             current_setting('block_size')::bigint as table_blocks"
        );
        let size_results = self.simple_query(&size_query).await?;
        let table_blocks: i64 = size_results
            .iter()
            .find_map(|msg| {
                if let SimpleQueryMessage::Row(row) = msg {
                    row.try_get("table_blocks").ok()??.parse().ok()
                } else {
                    None
                }
            })
            .ok_or_else(|| {
                etl_error!(
                    ErrorKind::SourceSchemaError,
                    "Could not retrieve table block count for partition planning",
                    format!("table_id: {table_id}")
                )
            })?;

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
}

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

#[cfg(test)]
mod tests {
    use etl_postgres::types::TableName;

    use super::{CtidPartition, build_ctid_copy_query};

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
