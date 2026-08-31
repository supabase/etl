use std::collections::BTreeMap;

use etl_postgres::version::{POSTGRES_15, POSTGRES_18};
use pg_escape::{quote_identifier, quote_literal};
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, FromRow, PgConnection, PgPool, postgres::types::Oid};
use thiserror::Error;
use utoipa::ToSchema;

use crate::data::{
    tables::{SourceTable as ResolvedSourceTable, source_table_from_postgres},
    v2::tables::{SourceTable, SourceTableRow, source_table_from_row},
};

/// An error returned while interacting with source publications.
#[derive(Debug, Error)]
pub enum PublicationsV2DbError {
    /// The source database query failed.
    #[error("Error while interacting with Postgres for publications")]
    Database(#[from] sqlx::Error),

    /// A table ID did not resolve to a publication-eligible table.
    #[error("Table reference with id {table_id} is invalid")]
    InvalidTableReference { table_id: u32 },

    /// A row filter could escape its enclosing SQL expression.
    #[error("Row filter for table with id {table_id} is not a single SQL expression")]
    InvalidRowFilter { table_id: u32 },

    /// PostgreSQL returned an unknown generated-column publication mode.
    #[error("Postgres returned an unsupported generated-column publication mode")]
    UnsupportedGeneratedColumnsMode,

    /// A publication was not visible immediately after it was created.
    #[error("The created publication could not be read in its transaction")]
    CreatedPublicationNotFound,

    /// An open-ended publication cannot be replaced through this endpoint.
    #[error("Open-ended publications cannot be updated")]
    OpenEndedPublicationCannotBeUpdated,

    /// An existing explicit publication cannot become open-ended through this
    /// endpoint.
    #[error("An existing publication cannot be changed to an open-ended selection")]
    ExistingPublicationCannotBecomeOpenEnded,

    /// Partition-root identity is immutable after publication creation.
    #[error("Publish via partition root cannot be changed after publication creation")]
    PublishViaPartitionRootCannotBeUpdated,
}

/// Publication settings read from `pg_catalog.pg_publication`.
#[derive(FromRow)]
struct PublicationCatalogRow {
    /// Whether the publication includes all tables.
    puballtables: bool,
    /// Whether inserts are published.
    pubinsert: bool,
    /// Whether updates are published.
    pubupdate: bool,
    /// Whether deletes are published.
    pubdelete: bool,
    /// Whether truncations are published.
    pubtruncate: bool,
    /// Whether partition changes are published through their root.
    pubviaroot: bool,
    /// The PostgreSQL server version number.
    server_version_num: i32,
}

/// An explicit publication table read from PostgreSQL's catalogs.
#[derive(FromRow)]
struct PublicationTableRow {
    /// The table OID represented as a signed PostgreSQL integer.
    id: i64,
    /// The schema containing the table.
    schema: String,
    /// The unqualified table name.
    name: String,
    /// The explicit publication column list.
    columns: Option<Vec<String>>,
    /// The explicit publication row filter.
    row_filter: Option<String>,
}

/// A publication returned by the source-publication list endpoint.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PublicationSummary {
    /// The publication name.
    pub name: String,
}

/// A data-change operation published by PostgreSQL.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicationOperation {
    /// Publish inserted rows.
    Insert,
    /// Publish updated rows.
    Update,
    /// Publish deleted rows.
    Delete,
    /// Publish table truncations.
    Truncate,
}

/// Generated columns published when a table has no explicit column list.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PublicationGeneratedColumns {
    /// Do not publish generated columns implicitly.
    None,
    /// Publish stored generated columns.
    Stored,
}

impl PublicationOperation {
    /// Returns the PostgreSQL publication option value.
    fn as_postgres_value(self) -> &'static str {
        match self {
            PublicationOperation::Insert => "insert",
            PublicationOperation::Update => "update",
            PublicationOperation::Delete => "delete",
            PublicationOperation::Truncate => "truncate",
        }
    }
}

/// An explicitly configured table in a publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PublicationTableConfig {
    /// The table's Postgres OID in the source database.
    pub id: u32,
    /// The current schema containing the table.
    ///
    /// This is response display metadata. Publication writes resolve the
    /// current schema from [`PublicationTableConfig::id`].
    #[serde(default)]
    #[schema(read_only)]
    pub schema: String,
    /// The current unqualified table name.
    ///
    /// This is response display metadata. Publication writes resolve the
    /// current name from [`PublicationTableConfig::id`].
    #[serde(default)]
    #[schema(read_only)]
    pub name: String,
    /// Columns to publish, or `null` to use PostgreSQL's default column set.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// A self-contained PostgreSQL row-filter expression, or `null` for none.
    ///
    /// SQL comments are not accepted. PostgreSQL validates the expression's
    /// syntax, referenced columns, and publication restrictions.
    #[serde(default)]
    pub row_filter: Option<String>,
}

/// The tables selected by a publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PublicationTableSelection {
    /// Publish every eligible table in the database.
    ///
    /// This is PostgreSQL's open-ended `FOR ALL TABLES` mode. ETL pipeline
    /// validation currently rejects these publications because they also pick
    /// up ETL's internal schema tables when that schema is installed.
    AllTables,
    /// Publish every eligible table in the selected schemas.
    TablesInSchema {
        /// Schemas whose current and future tables are published.
        schemas: Vec<String>,
    },
    /// Publish only the explicitly configured tables.
    Tables {
        /// Tables and their optional column and row filters.
        tables: Vec<PublicationTableConfig>,
    },
}

/// A complete source-publication configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PublicationConfig {
    /// The table-selection strategy.
    #[serde(flatten)]
    pub table_selection: PublicationTableSelection,
    /// Data-change operations to publish.
    pub operations: Vec<PublicationOperation>,
    /// Whether partition changes use the published partition root's identity.
    ///
    /// Omission preserves PostgreSQL's `false` default. This value cannot be
    /// changed after the publication is created.
    #[serde(default)]
    pub publish_via_partition_root: bool,
    /// PostgreSQL 18 generated-column behavior, when explicitly available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_generated_columns: Option<PublicationGeneratedColumns>,
}

/// The request-body counterpart of [`PublicationTableConfig`].
///
/// A table is identified for writes by [`PublicationTableConfigInput::id`]
/// alone, so this type has no `schema`/`name` fields to set: there is
/// nothing to ignore, because there is nothing to supply. Publication reads
/// use [`PublicationTableConfig`], which adds those fields back as
/// server-resolved display metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PublicationTableConfigInput {
    /// The table's Postgres OID in the source database.
    pub id: u32,
    /// Columns to publish, or `null` to use PostgreSQL's default column set.
    #[serde(default)]
    pub columns: Option<Vec<String>>,
    /// A self-contained PostgreSQL row-filter expression, or `null` for none.
    ///
    /// SQL comments are not accepted. PostgreSQL validates the expression's
    /// syntax, referenced columns, and publication restrictions.
    #[serde(default)]
    pub row_filter: Option<String>,
}

impl From<PublicationTableConfigInput> for PublicationTableConfig {
    fn from(input: PublicationTableConfigInput) -> Self {
        PublicationTableConfig {
            id: input.id,
            schema: String::new(),
            name: String::new(),
            columns: input.columns,
            row_filter: input.row_filter,
        }
    }
}

/// The request-body counterpart of [`PublicationTableSelection`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PublicationTableSelectionInput {
    /// Publish every eligible table in the database.
    AllTables,
    /// Publish every eligible table in the selected schemas.
    TablesInSchema {
        /// Schemas whose current and future tables are published.
        schemas: Vec<String>,
    },
    /// Publish only the explicitly configured tables.
    Tables {
        /// Tables and their optional column and row filters.
        tables: Vec<PublicationTableConfigInput>,
    },
}

impl From<PublicationTableSelectionInput> for PublicationTableSelection {
    fn from(input: PublicationTableSelectionInput) -> Self {
        match input {
            PublicationTableSelectionInput::AllTables => PublicationTableSelection::AllTables,
            PublicationTableSelectionInput::TablesInSchema { schemas } => {
                PublicationTableSelection::TablesInSchema { schemas }
            }
            PublicationTableSelectionInput::Tables { tables } => {
                PublicationTableSelection::Tables {
                    tables: tables.into_iter().map(Into::into).collect(),
                }
            }
        }
    }
}

/// The request body accepted by the put-publication endpoint.
///
/// This is [`PublicationConfig`] with [`PublicationTableSelectionInput`] in
/// place of [`PublicationTableSelection`], so that an explicit-table
/// publication's entries carry only `id`, `columns`, and `row_filter` — the
/// type itself has no `schema`/`name` fields for a client to set.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PublicationConfigInput {
    /// The table-selection strategy.
    #[serde(flatten)]
    pub table_selection: PublicationTableSelectionInput,
    /// Data-change operations to publish.
    pub operations: Vec<PublicationOperation>,
    /// Whether partition changes use the published partition root's identity.
    ///
    /// Omission preserves PostgreSQL's `false` default. This value cannot be
    /// changed after the publication is created.
    #[serde(default)]
    pub publish_via_partition_root: bool,
    /// PostgreSQL 18 generated-column behavior, when explicitly available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_generated_columns: Option<PublicationGeneratedColumns>,
}

impl From<PublicationConfigInput> for PublicationConfig {
    fn from(input: PublicationConfigInput) -> Self {
        PublicationConfig {
            table_selection: input.table_selection.into(),
            operations: input.operations,
            publish_via_partition_root: input.publish_via_partition_root,
            publish_generated_columns: input.publish_generated_columns,
        }
    }
}

/// Complete details for a publication in a source database.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PublicationDetails {
    /// The publication name.
    pub name: String,
    /// The publication's current configuration, with tables resolved to
    /// their current schema and name. [`PublicationConfigInput`] is the
    /// counterpart accepted by the put endpoint.
    pub config: PublicationConfig,
    /// Tables currently exposed by the publication.
    pub tables: Vec<SourceTable>,
}

/// The result of putting a complete publication configuration.
pub(crate) struct PutPublicationResult {
    /// The publication after applying the requested configuration.
    pub(crate) publication: PublicationDetails,
    /// Whether the publication was created by this request.
    pub(crate) created: bool,
}

/// Returns publication names ordered alphabetically.
pub async fn read_publications(
    pool: &PgPool,
) -> Result<Vec<PublicationSummary>, PublicationsV2DbError> {
    let names = sqlx::query_scalar::<_, String>(
        "select pubname from pg_catalog.pg_publication order by pubname",
    )
    .fetch_all(pool)
    .await?;

    Ok(names.into_iter().map(|name| PublicationSummary { name }).collect())
}

/// Returns the complete configuration and effective tables for a publication.
pub async fn read_publication(
    pool: &PgPool,
    publication_name: &str,
) -> Result<Option<PublicationDetails>, PublicationsV2DbError> {
    let mut connection = pool.acquire().await?;
    read_publication_with_connection(&mut connection, publication_name).await
}

/// Reads one publication using a single source connection.
async fn read_publication_with_connection(
    connection: &mut PgConnection,
    publication_name: &str,
) -> Result<Option<PublicationDetails>, PublicationsV2DbError> {
    let row = read_publication_catalog_row(connection, publication_name).await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let server_version_num = row.server_version_num;
    let table_selection = if row.puballtables {
        PublicationTableSelection::AllTables
    } else {
        read_table_selection(connection, publication_name, server_version_num).await?
    };
    let operations = read_operations(&row);
    let publish_via_partition_root = row.pubviaroot;
    let publish_generated_columns =
        read_generated_columns(connection, publication_name, server_version_num).await?;
    let tables = read_effective_tables(connection, publication_name).await?;

    Ok(Some(PublicationDetails {
        name: publication_name.to_owned(),
        config: PublicationConfig {
            table_selection,
            operations,
            publish_via_partition_root,
            publish_generated_columns,
        },
        tables,
    }))
}

/// Atomically creates or replaces a publication from a complete V2
/// configuration.
pub(crate) async fn put_publication(
    pool: &PgPool,
    publication_name: &str,
    config: &PublicationConfig,
) -> Result<PutPublicationResult, PublicationsV2DbError> {
    let table_configs = match &config.table_selection {
        PublicationTableSelection::AllTables | PublicationTableSelection::TablesInSchema { .. } => {
            &[][..]
        }
        PublicationTableSelection::Tables { tables } => tables,
    };

    validate_row_filters(table_configs)?;

    let mut transaction = pool.begin().await?;
    sqlx::query("set local standard_conforming_strings = on").execute(&mut *transaction).await?;

    let existing = read_publication_catalog_row(&mut transaction, publication_name).await?;
    let created = existing.is_none();

    if let Some(existing) = &existing {
        let has_schema_selection = existing.server_version_num >= POSTGRES_15
            && publication_has_schema_selection(&mut transaction, publication_name).await?;
        if existing.puballtables || has_schema_selection {
            return Err(PublicationsV2DbError::OpenEndedPublicationCannotBeUpdated);
        }

        let PublicationTableSelection::Tables { .. } = &config.table_selection else {
            return Err(PublicationsV2DbError::ExistingPublicationCannotBecomeOpenEnded);
        };

        if config.publish_via_partition_root != existing.pubviaroot {
            return Err(PublicationsV2DbError::PublishViaPartitionRootCannotBeUpdated);
        }
    }

    let resolved_tables = resolve_table_references(&mut transaction, table_configs).await?;

    if let Some(existing) = existing {
        let PublicationTableSelection::Tables { tables } = &config.table_selection else {
            return Err(PublicationsV2DbError::ExistingPublicationCannotBecomeOpenEnded);
        };
        replace_explicit_tables(&mut transaction, publication_name, tables, &resolved_tables)
            .await?;
        alter_publication_options(
            &mut transaction,
            publication_name,
            config,
            existing.server_version_num,
        )
        .await?;
    } else {
        let mut query = format!("create publication {}", quote_identifier(publication_name));
        append_table_selection(&mut query, &config.table_selection, &resolved_tables);
        append_publication_options(&mut query, config);

        sqlx::query(AssertSqlSafe(query)).execute(&mut *transaction).await?;
    }

    let publication = read_publication_with_connection(&mut transaction, publication_name)
        .await?
        .ok_or(PublicationsV2DbError::CreatedPublicationNotFound)?;

    transaction.commit().await?;

    Ok(PutPublicationResult { publication, created })
}

/// Reads the catalog settings for a named publication.
async fn read_publication_catalog_row(
    connection: &mut PgConnection,
    publication_name: &str,
) -> Result<Option<PublicationCatalogRow>, PublicationsV2DbError> {
    Ok(sqlx::query_as::<_, PublicationCatalogRow>(
        r#"
        select
            p.puballtables,
            p.pubinsert,
            p.pubupdate,
            p.pubdelete,
            p.pubtruncate,
            p.pubviaroot,
            pg_catalog.current_setting('server_version_num')::int as server_version_num
        from pg_catalog.pg_publication p
        where p.pubname = $1;
        "#,
    )
    .bind(publication_name)
    .fetch_optional(&mut *connection)
    .await?)
}

/// Returns whether a publication includes every table in any schema.
async fn publication_has_schema_selection(
    connection: &mut PgConnection,
    publication_name: &str,
) -> Result<bool, PublicationsV2DbError> {
    Ok(sqlx::query_scalar(
        r#"
        select exists(
            select 1
            from pg_catalog.pg_publication_namespace pn
            join pg_catalog.pg_publication p on p.oid = pn.pnpubid
            where p.pubname = $1
        );
        "#,
    )
    .bind(publication_name)
    .fetch_one(&mut *connection)
    .await?)
}

/// Replaces every explicit table definition in a publication.
async fn replace_explicit_tables(
    connection: &mut PgConnection,
    publication_name: &str,
    tables: &[PublicationTableConfig],
    resolved_tables: &BTreeMap<u32, ResolvedSourceTable>,
) -> Result<(), PublicationsV2DbError> {
    let query = if tables.is_empty() {
        let existing_tables = sqlx::query_as::<_, (String, String)>(
            r#"
            select n.nspname, c.relname
            from pg_catalog.pg_publication_rel pr
            join pg_catalog.pg_publication p on p.oid = pr.prpubid
            join pg_catalog.pg_class c on c.oid = pr.prrelid
            join pg_catalog.pg_namespace n on n.oid = c.relnamespace
            where p.pubname = $1
            order by n.nspname, c.relname;
            "#,
        )
        .bind(publication_name)
        .fetch_all(&mut *connection)
        .await?;
        if existing_tables.is_empty() {
            return Ok(());
        }

        format!(
            "alter publication {} drop table {}",
            quote_identifier(publication_name),
            format_table_names(&existing_tables)
        )
    } else {
        format!(
            "alter publication {} set table {}",
            quote_identifier(publication_name),
            format_table_configs(tables, resolved_tables)
        )
    };

    sqlx::query(AssertSqlSafe(query)).execute(&mut *connection).await?;
    Ok(())
}

/// Replaces publication-wide options while preserving the publication object.
async fn alter_publication_options(
    connection: &mut PgConnection,
    publication_name: &str,
    config: &PublicationConfig,
    server_version_num: i32,
) -> Result<(), PublicationsV2DbError> {
    let mut query = format!("alter publication {} set", quote_identifier(publication_name));
    append_publication_options_for_version(&mut query, config, server_version_num, false);
    sqlx::query(AssertSqlSafe(query)).execute(&mut *connection).await?;
    Ok(())
}

/// Drops a publication if it exists.
pub async fn drop_publication(
    pool: &PgPool,
    publication_name: &str,
) -> Result<(), PublicationsV2DbError> {
    let query = format!("drop publication if exists {}", quote_identifier(publication_name));
    sqlx::query(AssertSqlSafe(query)).execute(pool).await?;
    Ok(())
}

/// Reads the non-database-wide selection declared for a publication.
async fn read_table_selection(
    connection: &mut PgConnection,
    publication_name: &str,
    server_version_num: i32,
) -> Result<PublicationTableSelection, PublicationsV2DbError> {
    if server_version_num < POSTGRES_15 {
        let tables = read_explicit_tables_before_postgres_15(connection, publication_name).await?;
        return Ok(PublicationTableSelection::Tables { tables });
    }

    let schemas = sqlx::query_scalar::<_, String>(
        r#"
        select n.nspname
        from pg_catalog.pg_publication_namespace pn
        join pg_catalog.pg_publication p on p.oid = pn.pnpubid
        join pg_catalog.pg_namespace n on n.oid = pn.pnnspid
        where p.pubname = $1
        order by n.nspname;
        "#,
    )
    .bind(publication_name)
    .fetch_all(&mut *connection)
    .await?;
    if !schemas.is_empty() {
        return Ok(PublicationTableSelection::TablesInSchema { schemas });
    }

    let tables = read_explicit_tables(connection, publication_name).await?;
    Ok(PublicationTableSelection::Tables { tables })
}

/// Reads explicit publication tables on PostgreSQL 15 and newer.
async fn read_explicit_tables(
    connection: &mut PgConnection,
    publication_name: &str,
) -> Result<Vec<PublicationTableConfig>, PublicationsV2DbError> {
    let rows = sqlx::query_as::<_, PublicationTableRow>(
        r#"
        select
            c.oid::bigint as id,
            n.nspname as schema,
            c.relname as name,
            case
                when pr.prattrs is null then null
                else array(
                    select a.attname::text
                    from pg_catalog.pg_attribute a
                    where a.attrelid = pr.prrelid
                        and a.attnum = any(pr.prattrs::smallint[])
                    order by pg_catalog.array_position(pr.prattrs::smallint[], a.attnum)
                )
            end as columns,
            pg_catalog.pg_get_expr(pr.prqual, pr.prrelid) as row_filter
        from pg_catalog.pg_publication_rel pr
        join pg_catalog.pg_publication p on p.oid = pr.prpubid
        join pg_catalog.pg_class c on c.oid = pr.prrelid
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where p.pubname = $1
        order by n.nspname, c.relname;
        "#,
    )
    .bind(publication_name)
    .fetch_all(&mut *connection)
    .await?;

    Ok(rows.into_iter().map(publication_table_from_row).collect())
}

/// Reads explicit tables on PostgreSQL versions without row and column filters.
async fn read_explicit_tables_before_postgres_15(
    connection: &mut PgConnection,
    publication_name: &str,
) -> Result<Vec<PublicationTableConfig>, PublicationsV2DbError> {
    let rows = sqlx::query_as::<_, (i64, String, String)>(
        r#"
        select
            c.oid::bigint as id,
            n.nspname as schema,
            c.relname as name
        from pg_catalog.pg_publication_rel pr
        join pg_catalog.pg_publication p on p.oid = pr.prpubid
        join pg_catalog.pg_class c on c.oid = pr.prrelid
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where p.pubname = $1
        order by n.nspname, c.relname;
        "#,
    )
    .bind(publication_name)
    .fetch_all(&mut *connection)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(id, schema, name)| {
            let table = source_table_from_postgres(id, schema, name);
            PublicationTableConfig {
                id: table.id,
                schema: table.schema,
                name: table.name,
                columns: None,
                row_filter: None,
            }
        })
        .collect())
}

/// Reads the tables currently exposed by a publication.
async fn read_effective_tables(
    connection: &mut PgConnection,
    publication_name: &str,
) -> Result<Vec<SourceTable>, PublicationsV2DbError> {
    let rows = sqlx::query_as::<_, SourceTableRow>(
        r#"
        select distinct
            c.oid::bigint as id,
            pt.schemaname as schema,
            pt.tablename as name,
            c.relkind = 'p' as is_partitioned,
            case when c.relispartition then i.inhparent::bigint end as partition_parent_id
        from pg_catalog.pg_publication_tables pt
        join pg_catalog.pg_namespace n on n.nspname = pt.schemaname
        join pg_catalog.pg_class c
            on c.relnamespace = n.oid
            and c.relname = pt.tablename
        left join pg_catalog.pg_inherits i
            on i.inhrelid = c.oid
            and c.relispartition
        where pt.pubname = $1
        order by pt.schemaname, pt.tablename;
        "#,
    )
    .bind(publication_name)
    .fetch_all(&mut *connection)
    .await?;

    Ok(rows.into_iter().map(source_table_from_row).collect())
}

/// Reads PostgreSQL 18 generated-column publication behavior.
async fn read_generated_columns(
    connection: &mut PgConnection,
    publication_name: &str,
    server_version_num: i32,
) -> Result<Option<PublicationGeneratedColumns>, PublicationsV2DbError> {
    if server_version_num < POSTGRES_18 {
        return Ok(None);
    }

    let value: String = sqlx::query_scalar(
        "select pubgencols::text from pg_catalog.pg_publication where pubname = $1",
    )
    .bind(publication_name)
    .fetch_one(&mut *connection)
    .await?;

    Ok(match value.as_str() {
        "n" => Some(PublicationGeneratedColumns::None),
        "s" => Some(PublicationGeneratedColumns::Stored),
        _ => return Err(PublicationsV2DbError::UnsupportedGeneratedColumnsMode),
    })
}

/// Resolves request table IDs to their current PostgreSQL names.
async fn resolve_table_references(
    connection: &mut PgConnection,
    tables: &[PublicationTableConfig],
) -> Result<BTreeMap<u32, ResolvedSourceTable>, PublicationsV2DbError> {
    if tables.is_empty() {
        return Ok(BTreeMap::new());
    }

    let table_ids = tables.iter().map(|table| Oid(table.id)).collect::<Vec<_>>();
    let rows = sqlx::query_as::<_, (i64, String, String)>(
        r#"
        select c.oid::bigint as id, n.nspname as schema, c.relname as name
        from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where c.oid = any($1::oid[])
            and c.relkind in ('r', 'p');
        "#,
    )
    .bind(table_ids)
    .fetch_all(&mut *connection)
    .await?;

    let mut resolved = BTreeMap::new();
    for (id, schema, name) in rows {
        let table = source_table_from_postgres(id, schema, name);
        resolved.insert(table.id, table);
    }

    for table in tables {
        if !resolved.contains_key(&table.id) {
            return Err(PublicationsV2DbError::InvalidTableReference { table_id: table.id });
        }
    }

    Ok(resolved)
}

/// Validates that row filters cannot escape their enclosing SQL expression.
fn validate_row_filters(tables: &[PublicationTableConfig]) -> Result<(), PublicationsV2DbError> {
    for table in tables {
        if let Some(row_filter) = &table.row_filter
            && !is_contained_sql_expression(row_filter)
        {
            return Err(PublicationsV2DbError::InvalidRowFilter { table_id: table.id });
        }
    }

    Ok(())
}

/// Returns whether SQL text stays inside one surrounding parenthesized
/// expression.
///
/// PostgreSQL remains responsible for parsing and validating the expression.
/// This scanner only prevents the text from closing the parenthesis supplied
/// by ETL or commenting out the trusted suffix of the DDL statement.
fn is_contained_sql_expression(expression: &str) -> bool {
    /// The quoted construct currently being scanned.
    enum Quote<'a> {
        /// No quoted construct is open.
        None,
        /// A single-quoted string is open.
        Single {
            /// Whether backslashes escape the following character.
            backslash_escapes: bool,
        },
        /// A double-quoted identifier is open.
        Double,
        /// A dollar-quoted string is open.
        Dollar(&'a [u8]),
    }

    let bytes = expression.as_bytes();
    let mut quote = Quote::None;
    let mut parenthesis_depth = 0_u32;
    let mut index = 0;

    while index < bytes.len() {
        match &quote {
            Quote::None => match bytes[index] {
                b'\'' => {
                    quote = Quote::Single {
                        backslash_escapes: single_quote_uses_backslash_escapes(bytes, index),
                    };
                    index += 1;
                }
                b'"' => {
                    quote = Quote::Double;
                    index += 1;
                }
                b'$' => {
                    if let Some(delimiter) = dollar_quote_delimiter(bytes, index) {
                        index += delimiter.len();
                        quote = Quote::Dollar(delimiter);
                    } else {
                        index += 1;
                    }
                }
                b'(' => {
                    parenthesis_depth += 1;
                    index += 1;
                }
                b')' => {
                    let Some(depth) = parenthesis_depth.checked_sub(1) else {
                        return false;
                    };
                    parenthesis_depth = depth;
                    index += 1;
                }
                b';' => return false,
                b'-' if bytes.get(index + 1) == Some(&b'-') => return false,
                b'/' if bytes.get(index + 1) == Some(&b'*') => return false,
                _ => index += 1,
            },
            Quote::Single { backslash_escapes } => match bytes[index] {
                b'\\' if *backslash_escapes => {
                    if bytes.get(index + 1).is_none() {
                        return false;
                    }
                    index += 2;
                }
                b'\'' if bytes.get(index + 1) == Some(&b'\'') => index += 2,
                b'\'' => {
                    if let Some(continuation_index) = continued_single_quote_index(bytes, index) {
                        index = continuation_index;
                    } else {
                        quote = Quote::None;
                        index += 1;
                    }
                }
                _ => index += 1,
            },
            Quote::Double => match bytes[index] {
                b'"' if bytes.get(index + 1) == Some(&b'"') => index += 2,
                b'"' => {
                    quote = Quote::None;
                    index += 1;
                }
                _ => index += 1,
            },
            Quote::Dollar(delimiter) => {
                if bytes[index..].starts_with(delimiter) {
                    index += delimiter.len();
                    quote = Quote::None;
                } else {
                    index += 1;
                }
            }
        }
    }

    matches!(quote, Quote::None) && parenthesis_depth == 0
}

/// Returns whether a quote starts PostgreSQL's `E'...'` string syntax.
fn single_quote_uses_backslash_escapes(bytes: &[u8], quote_index: usize) -> bool {
    let Some(prefix_index) = quote_index.checked_sub(1) else {
        return false;
    };
    if !matches!(bytes[prefix_index], b'e' | b'E') {
        return false;
    }

    prefix_index == 0
        || !matches!(
            bytes[prefix_index - 1],
            byte if could_continue_identifier(byte)
        )
}

/// Returns the index after a continued single-quoted string's opening quote.
fn continued_single_quote_index(bytes: &[u8], quote_index: usize) -> Option<usize> {
    let mut index = quote_index.checked_add(1)?;
    let mut saw_newline = false;

    while let Some(byte) = bytes.get(index)
        && matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0c | 0x0b)
    {
        saw_newline |= matches!(byte, b'\n' | b'\r');
        index += 1;
    }

    (saw_newline && bytes.get(index) == Some(&b'\'')).then_some(index + 1)
}

/// Returns a PostgreSQL dollar-quote delimiter at `index`.
fn dollar_quote_delimiter(bytes: &[u8], index: usize) -> Option<&[u8]> {
    if bytes.get(index) != Some(&b'$')
        || index
            .checked_sub(1)
            .and_then(|index| bytes.get(index))
            .is_some_and(|byte| could_continue_identifier(*byte))
    {
        return None;
    }
    let candidate = &bytes[index..];
    if candidate.get(1) == Some(&b'$') {
        return Some(&candidate[..2]);
    }

    let first = *candidate.get(1)?;
    if !could_start_identifier(first) {
        return None;
    }

    let mut delimiter_end = 2;
    while let Some(byte) = candidate.get(delimiter_end)
        && could_continue_identifier(*byte)
        && *byte != b'$'
    {
        delimiter_end += 1;
    }

    (candidate.get(delimiter_end) == Some(&b'$')).then(|| &candidate[..=delimiter_end])
}

/// Returns whether a byte could start an unquoted PostgreSQL identifier.
fn could_start_identifier(byte: u8) -> bool {
    matches!(byte, b'a'..=b'z' | b'A'..=b'Z' | b'_' | 0x80..=0xff)
}

/// Returns whether a byte could continue an unquoted PostgreSQL identifier.
fn could_continue_identifier(byte: u8) -> bool {
    could_start_identifier(byte) || matches!(byte, b'0'..=b'9' | b'$')
}

/// Appends the publication's table-selection clause.
fn append_table_selection(
    query: &mut String,
    selection: &PublicationTableSelection,
    resolved_tables: &BTreeMap<u32, ResolvedSourceTable>,
) {
    match selection {
        PublicationTableSelection::AllTables => query.push_str(" for all tables"),
        PublicationTableSelection::TablesInSchema { schemas } => {
            query.push_str(" for tables in schema ");
            query.push_str(
                &schemas
                    .iter()
                    .map(|schema| quote_identifier(schema))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        PublicationTableSelection::Tables { tables } if !tables.is_empty() => {
            query.push_str(" for table ");
            query.push_str(&format_table_configs(tables, resolved_tables));
        }
        PublicationTableSelection::Tables { .. } => {}
    }
}

/// Appends PostgreSQL publication parameters.
fn append_publication_options(query: &mut String, config: &PublicationConfig) {
    query.push_str(" with");
    append_publication_options_for_version(query, config, 0, true);
}

/// Appends PostgreSQL publication parameters for a known server version.
fn append_publication_options_for_version(
    query: &mut String,
    config: &PublicationConfig,
    server_version_num: i32,
    include_publish_via_partition_root: bool,
) {
    let operations = config
        .operations
        .iter()
        .map(|operation| operation.as_postgres_value())
        .collect::<Vec<_>>()
        .join(", ");
    query.push_str(" (publish = ");
    query.push_str(&quote_literal(&operations));
    if include_publish_via_partition_root {
        query.push_str(", publish_via_partition_root = ");
        query.push_str(if config.publish_via_partition_root { "true" } else { "false" });
    }
    let publish_generated_columns = config.publish_generated_columns.or_else(|| {
        (server_version_num >= POSTGRES_18).then_some(PublicationGeneratedColumns::None)
    });
    if let Some(publish_generated_columns) = publish_generated_columns {
        query.push_str(", publish_generated_columns = ");
        query.push_str(match publish_generated_columns {
            PublicationGeneratedColumns::None => "none",
            PublicationGeneratedColumns::Stored => "stored",
        });
    }
    query.push(')');
}

/// Formats schema-qualified table names without per-table options.
fn format_table_names(tables: &[(String, String)]) -> String {
    tables
        .iter()
        .map(|(schema, name)| format!("{}.{}", quote_identifier(schema), quote_identifier(name)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Formats explicit publication table objects.
fn format_table_configs(
    tables: &[PublicationTableConfig],
    resolved_tables: &BTreeMap<u32, ResolvedSourceTable>,
) -> String {
    tables
        .iter()
        .map(|table| {
            let resolved =
                resolved_tables.get(&table.id).expect("validated table reference should resolve");
            let mut value = format!(
                "{}.{}",
                quote_identifier(&resolved.schema),
                quote_identifier(&resolved.name)
            );
            if let Some(columns) = &table.columns {
                value.push_str(" (");
                value.push_str(
                    &columns
                        .iter()
                        .map(|column| quote_identifier(column))
                        .collect::<Vec<_>>()
                        .join(", "),
                );
                value.push(')');
            }
            if let Some(row_filter) = &table.row_filter {
                value.push_str(" where (");
                value.push_str(row_filter);
                value.push(')');
            }
            value
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Decodes publication operation flags in their stable API order.
fn read_operations(row: &PublicationCatalogRow) -> Vec<PublicationOperation> {
    let mut operations = Vec::new();
    if row.pubinsert {
        operations.push(PublicationOperation::Insert);
    }
    if row.pubupdate {
        operations.push(PublicationOperation::Update);
    }
    if row.pubdelete {
        operations.push(PublicationOperation::Delete);
    }
    if row.pubtruncate {
        operations.push(PublicationOperation::Truncate);
    }
    operations
}

/// Decodes an explicit table row.
fn publication_table_from_row(row: PublicationTableRow) -> PublicationTableConfig {
    let table = source_table_from_postgres(row.id, row.schema, row.name);
    PublicationTableConfig {
        id: table.id,
        schema: table.schema,
        name: table.name,
        columns: row.columns,
        row_filter: row.row_filter,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_filter_containment_accepts_quoted_delimiters_and_balanced_parentheses() {
        for expression in [
            "",
            "id > 0",
            "(id > 0 and payload = '); --') or payload = 'it''s safe'",
            "payload = $$); /* still a string */$$",
            "payload = $tag$); -- still a string$tag$",
            "payload = $é$); -- still a string$é$",
            "payload = U&'d\\0061t\\+000061'",
            "payload = 'backslash\\value'",
            "payload = E'backslash\\\\value'",
            "payload = E'quote: \\''",
            "payload = E'continued'\n'quote: \\''",
            "flags = B'1010' and code = X'1f'",
            "payload$value is not null",
            "payload$é$ = 'value'",
            "payload = $1",
            "\"strange)column\" is not null",
            "\"strange\"\";column\" is not null",
        ] {
            assert!(is_contained_sql_expression(expression), "{expression}");
        }
    }

    #[test]
    fn row_filter_containment_rejects_statement_escape_sequences() {
        for expression in [
            "true), public.other_table where (true",
            "true) with (publish = 'truncate') --",
            "true /* comment */",
            "true -- line comment\n",
            "true; select true",
            "(true",
            "true)",
            "payload = E'escaped\\') with (publish = 'truncate') --",
            "payload = E'continued'\n'escaped\\') with (publish = 'truncate') --",
            "payload = E'unterminated\\",
            "payload = 'unterminated",
            "payload = $tag$unterminated",
            "payload = $TAG$case-sensitive$tag$",
            "payload$tag$), public.other_table where (visible$tag$",
        ] {
            assert!(!is_contained_sql_expression(expression), "{expression}");
        }
    }
}
