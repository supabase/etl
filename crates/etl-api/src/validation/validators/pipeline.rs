use async_trait::async_trait;
use etl_config::shared::TableSyncCopyConfig;
use etl_postgres::store::catalog::ETL_SCHEMA_NAME;
use sqlx::FromRow;

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};

/// PostgreSQL default for `wal_level`.
const DEFAULT_WAL_LEVEL: &str = "replica";
/// PostgreSQL default for `max_replication_slots`.
const DEFAULT_MAX_REPLICATION_SLOTS: i32 = 10;
/// PostgreSQL default for `max_wal_senders`.
const DEFAULT_MAX_WAL_SENDERS: i32 = 10;
/// PostgreSQL default for `max_slot_wal_keep_size`.
const DEFAULT_MAX_SLOT_WAL_KEEP_SIZE_MB: i64 = -1;
/// PostgreSQL default for `idle_replication_slot_timeout`.
const DEFAULT_IDLE_REPLICATION_SLOT_TIMEOUT_SECONDS: i64 = 0;
/// Minimum practical per-slot WAL retention for logical replication.
const MIN_SLOT_WAL_KEEP_SIZE_MB: i64 = 1024;
/// Low idle replication slot timeout warning threshold in seconds.
///
/// Values at or below this are too aggressive for ordinary ETL deploys,
/// maintenance windows, and incident pauses.
const LOW_IDLE_REPLICATION_SLOT_TIMEOUT_SECONDS: i64 = 300;

/// Validates that the required publication exists in the source database.
#[derive(Debug)]
pub(super) struct PublicationExistsValidator {
    publication_name: String,
}

impl PublicationExistsValidator {
    pub(super) fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PublicationExistsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool =
            ctx.source_pool.as_ref().expect("source pool required for publication validation");

        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from pg_publication where pubname = $1)")
                .bind(&self.publication_name)
                .fetch_one(source_pool)
                .await?;

        if exists {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Publication Not Found",
                format!(
                    "Publication `{}` does not exist in your source database.\n\nCreate the \
                     publication, or choose an existing publication for this pipeline. For \
                     example: `CREATE PUBLICATION {} FOR TABLE <table_name>, ...`.",
                    self.publication_name, self.publication_name
                ),
            )])
        }
    }
}

/// Validates source settings needed for reliable logical replication.
#[derive(Debug)]
pub(super) struct LogicalReplicationSettingsValidator {
    max_table_sync_workers: u16,
}

impl LogicalReplicationSettingsValidator {
    pub(super) fn new(max_table_sync_workers: u16) -> Self {
        Self { max_table_sync_workers }
    }
}

#[derive(Debug, FromRow)]
struct LogicalReplicationSettingsAudit {
    /// Current WAL level.
    wal_level: String,
    /// Whether the connected role can use logical replication.
    has_replication_permission: bool,
    /// Maximum configured replication slots.
    max_replication_slots: i32,
    /// Currently allocated replication slots.
    used_replication_slots: i64,
    /// Maximum configured WAL sender processes.
    max_wal_senders: i32,
    /// Currently active WAL sender processes.
    active_wal_senders: i64,
    /// Configured per-slot WAL retention limit in megabytes, or -1 for
    /// unlimited.
    max_slot_wal_keep_size_mb: i64,
    /// Idle slot invalidation timeout in seconds, or 0 when unsupported.
    idle_replication_slot_timeout_seconds: i64,
}

#[async_trait]
impl Validator for LogicalReplicationSettingsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for logical replication settings validation");

        let audit = sqlx::query_as::<_, LogicalReplicationSettingsAudit>(
            r#"
            select
                coalesce(current_setting('wal_level', true), $1::text) as wal_level,
                (
                    select rolsuper or rolreplication
                    from pg_roles
                    where rolname = current_user
                ) as has_replication_permission,
                coalesce(
                    (
                        select setting::int
                        from pg_settings
                        where name = 'max_replication_slots'
                    ),
                    $2::int
                ) as max_replication_slots,
                (
                    select count(*)
                    from pg_replication_slots
                ) as used_replication_slots,
                coalesce(
                    (
                        select setting::int
                        from pg_settings
                        where name = 'max_wal_senders'
                    ),
                    $3::int
                ) as max_wal_senders,
                (
                    select count(*)
                    from pg_stat_replication
                ) as active_wal_senders,
                coalesce(
                    (
                        select setting::bigint
                        from pg_settings
                        where name = 'max_slot_wal_keep_size'
                    ),
                    $4::bigint
                ) as max_slot_wal_keep_size_mb,
                coalesce(
                    (
                        select setting::bigint
                        from pg_settings
                        where name = 'idle_replication_slot_timeout'
                    ),
                    $5::bigint
                ) as idle_replication_slot_timeout_seconds
            "#,
        )
        .bind(DEFAULT_WAL_LEVEL)
        .bind(DEFAULT_MAX_REPLICATION_SLOTS)
        .bind(DEFAULT_MAX_WAL_SENDERS)
        .bind(DEFAULT_MAX_SLOT_WAL_KEEP_SIZE_MB)
        .bind(DEFAULT_IDLE_REPLICATION_SLOT_TIMEOUT_SECONDS)
        .fetch_one(source_pool)
        .await?;

        Ok(logical_replication_settings_failures(audit, self.max_table_sync_workers))
    }
}

/// Validates that a publication contains at least one table.
#[derive(Debug)]
pub(super) struct PublicationHasTablesValidator {
    publication_name: String,
}

impl PublicationHasTablesValidator {
    pub(super) fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PublicationHasTablesValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for publication tables validation");

        // Check if publication publishes all tables or has specific tables
        let result: Option<(bool, i64)> = sqlx::query_as(
            r#"
            select
                p.puballtables,
                (select count(*) from pg_publication_tables pt where pt.pubname = p.pubname)
            from pg_publication p
            where p.pubname = $1
            "#,
        )
        .bind(&self.publication_name)
        .fetch_optional(source_pool)
        .await?;

        // If publication doesn't exist, skip this check (PublicationExistsValidator
        // handles it)
        let Some((puballtables, table_count)) = result else {
            return Ok(vec![]);
        };

        if puballtables || table_count > 0 {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Publication Empty",
                format!(
                    "Publication `{}` exists, but it does not publish any tables.\n\nAdd tables \
                     to the publication before starting the pipeline. For example: `ALTER \
                     PUBLICATION {} ADD TABLE <table_name>`.",
                    self.publication_name, self.publication_name
                ),
            )])
        }
    }
}

/// Validates that selective table-copy ids belong to the pipeline's
/// publication.
///
/// Duplicate ids are harmless: [`TableSyncCopyConfig::should_copy_table`]
/// checks membership, so repeating an id has no effect on which tables are
/// copied.
#[derive(Debug)]
pub(super) struct SelectedTableIdsInPublicationValidator {
    publication_name: String,
    table_sync_copy: TableSyncCopyConfig,
}

impl SelectedTableIdsInPublicationValidator {
    pub(super) fn new(publication_name: String, table_sync_copy: TableSyncCopyConfig) -> Self {
        Self { publication_name, table_sync_copy }
    }
}

#[async_trait]
impl Validator for SelectedTableIdsInPublicationValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let table_ids: &[u32] = match &self.table_sync_copy {
            TableSyncCopyConfig::IncludeAllTables | TableSyncCopyConfig::SkipAllTables => &[],
            TableSyncCopyConfig::IncludeTables { table_ids }
            | TableSyncCopyConfig::SkipTables { table_ids } => table_ids,
        };

        if table_ids.is_empty() {
            return Ok(vec![]);
        }

        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for selected table ids validation");

        let oids = table_ids.iter().map(|id| *id as i64).collect::<Vec<_>>();
        let published_oids: Vec<i64> = sqlx::query_scalar(
            r#"
            select c.oid::bigint
            from pg_publication_tables pt
            join pg_namespace n on n.nspname = pt.schemaname
            join pg_class c on c.relnamespace = n.oid and c.relname = pt.tablename
            where pt.pubname = $1
              and c.oid = any($2::oid[])
            "#,
        )
        .bind(&self.publication_name)
        .bind(&oids)
        .fetch_all(source_pool)
        .await?;

        let published_oids = published_oids.into_iter().collect::<std::collections::HashSet<_>>();
        let mut missing_ids = table_ids
            .iter()
            .filter(|id| !published_oids.contains(&(**id as i64)))
            .copied()
            .collect::<Vec<_>>();
        missing_ids.sort_unstable();

        if missing_ids.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::critical(
                "Selected Tables Not In Publication",
                format!(
                    "These selected table ids do not belong to publication `{}`: {}.\n\nAdd the \
                     tables to the publication, or remove them from the initial table-copy \
                     selection.",
                    self.publication_name,
                    missing_ids.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
                ),
            )])
        }
    }
}

/// Validates that a publication does not include ETL-owned tables.
#[derive(Debug)]
pub(super) struct PublicationExcludesEtlTablesValidator {
    publication_name: String,
}

impl PublicationExcludesEtlTablesValidator {
    pub(super) fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for PublicationExcludesEtlTablesValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool =
            ctx.source_pool.as_ref().expect("source pool required for publication validation");

        let Some((puballtables, server_version_num, published_etl_tables)) =
            sqlx::query_as::<_, (bool, i32, Vec<String>)>(
                r#"
                select
                    p.puballtables,
                    current_setting('server_version_num')::int,
                    coalesce(
                        (
                            select array_agg(t.table_name order by t.table_name)
                            from (
                                select pt.schemaname || '.' || pt.tablename as table_name
                                from pg_publication_tables pt
                                where pt.pubname = p.pubname
                                  and pt.schemaname = $2
                                order by pt.tablename
                                limit 100
                            ) t
                        ),
                        array[]::text[]
                    )
                from pg_publication p
                where p.pubname = $1
                "#,
            )
            .bind(&self.publication_name)
            .bind(ETL_SCHEMA_NAME)
            .fetch_optional(source_pool)
            .await?
        else {
            // If publication doesn't exist, skip this check. The existence
            // validator reports the actionable failure.
            return Ok(vec![]);
        };

        if puballtables {
            return Ok(vec![ValidationFailure::critical(
                "Publication Includes ETL Tables",
                format!(
                    "Publication `{}` is defined `FOR ALL TABLES`, which also publishes ETL's \
                     internal schema tables when they exist.\n\nUse an explicit table list, or \
                     `FOR TABLES IN SCHEMA` with only customer-owned schemas. Exclude the \
                     `{ETL_SCHEMA_NAME}` schema.",
                    self.publication_name
                ),
            )]);
        }

        if !published_etl_tables.is_empty() {
            return Ok(vec![ValidationFailure::critical(
                "Publication Includes ETL Tables",
                format!(
                    "Publication `{}` includes ETL internal tables: {}.\n\nRemove the \
                     `{ETL_SCHEMA_NAME}` schema from the publication before starting the \
                     pipeline. ETL state tables are implementation details and must not be \
                     replicated.",
                    self.publication_name,
                    format_code_list(&published_etl_tables)
                ),
            )]);
        }

        if server_version_num >= 15_00_00 {
            let publishes_etl_schema: bool = sqlx::query_scalar(
                r#"
                select exists (
                    select 1
                    from pg_publication_namespace pn
                    join pg_publication p on p.oid = pn.pnpubid
                    join pg_namespace n on n.oid = pn.pnnspid
                    where p.pubname = $1
                      and n.nspname = $2
                )
                "#,
            )
            .bind(&self.publication_name)
            .bind(ETL_SCHEMA_NAME)
            .fetch_one(source_pool)
            .await?;

            if publishes_etl_schema {
                return Ok(vec![ValidationFailure::critical(
                    "Publication Includes ETL Tables",
                    format!(
                        "Publication `{}` includes the `{ETL_SCHEMA_NAME}` schema.\n\nRemove that \
                         schema from the publication and publish only customer-owned schemas or \
                         explicit customer tables.",
                        self.publication_name
                    ),
                )]);
            }
        }

        Ok(vec![])
    }
}

/// Validates that tables in a publication don't have generated columns.
#[derive(Debug)]
pub(super) struct GeneratedColumnsValidator {
    publication_name: String,
}

impl GeneratedColumnsValidator {
    pub(super) fn new(publication_name: String) -> Self {
        Self { publication_name }
    }
}

#[async_trait]
impl Validator for GeneratedColumnsValidator {
    async fn validate(
        &self,
        ctx: &ValidationContext,
    ) -> Result<Vec<ValidationFailure>, ValidationError> {
        let source_pool = ctx
            .source_pool
            .as_ref()
            .expect("source pool required for generated columns validation");

        // Find tables with generated columns using pg_publication_rel for direct OID
        // access
        let tables_with_generated: Vec<String> = sqlx::query_scalar(
            r#"
            select distinct n.nspname || '.' || c.relname
            from pg_publication_rel pr
            join pg_publication p on p.oid = pr.prpubid
            join pg_class c on c.oid = pr.prrelid
            join pg_namespace n on n.oid = c.relnamespace
            where p.pubname = $1
              and exists (
                select 1
                from pg_attribute a
                where a.attrelid = pr.prrelid
                  and a.attnum > 0
                  and not a.attisdropped
                  and a.attgenerated != ''
              )
            order by 1
            limit 100
            "#,
        )
        .bind(&self.publication_name)
        .fetch_all(source_pool)
        .await?;

        if tables_with_generated.is_empty() {
            Ok(vec![])
        } else {
            Ok(vec![ValidationFailure::warning(
                "Tables With Generated Columns",
                format!(
                    "These publication tables have generated columns: {}.\n\nThe pipeline can \
                     start, but generated columns are not replicated and will be omitted from the \
                     destination.",
                    format_code_list(&tables_with_generated)
                ),
            )])
        }
    }
}

/// Formats validation values as inline code for UI rendering.
fn format_code_list(values: &[String]) -> String {
    values.iter().map(|value| format!("`{value}`")).collect::<Vec<_>>().join(", ")
}

/// Builds validation failures for logical replication source settings.
fn logical_replication_settings_failures(
    audit: LogicalReplicationSettingsAudit,
    max_table_sync_workers: u16,
) -> Vec<ValidationFailure> {
    let mut failures = Vec::new();

    failures.extend(wal_level_failures(&audit.wal_level));
    failures.extend(replication_permission_failures(audit.has_replication_permission));
    failures.extend(replication_slot_failures(
        audit.max_replication_slots,
        audit.used_replication_slots,
        max_table_sync_workers,
    ));
    failures.extend(wal_sender_failures(
        audit.max_wal_senders,
        audit.max_replication_slots,
        audit.active_wal_senders,
        max_table_sync_workers,
    ));
    failures.extend(slot_wal_keep_size_failures(
        audit.max_slot_wal_keep_size_mb,
        audit.idle_replication_slot_timeout_seconds,
    ));

    failures
}

/// Builds validation failures for WAL level.
fn wal_level_failures(wal_level: &str) -> Vec<ValidationFailure> {
    if wal_level == "logical" {
        Vec::new()
    } else {
        vec![ValidationFailure::critical(
            "Invalid WAL Level",
            format!(
                "Your source database has `wal_level` set to `{wal_level}`, but logical \
                 replication requires `logical`.\n\nSet `wal_level = 'logical'` in \
                 `postgresql.conf` and restart PostgreSQL."
            ),
        )]
    }
}

/// Builds validation failures for role replication permissions.
fn replication_permission_failures(has_replication_permission: bool) -> Vec<ValidationFailure> {
    if has_replication_permission {
        Vec::new()
    } else {
        vec![ValidationFailure::critical(
            "Missing Replication Permission",
            "Your source database user does not have replication privileges.\n\nGrant the user \
             the `REPLICATION` attribute or connect with a role that already has it.",
        )]
    }
}

/// Builds validation failures for replication slot capacity.
fn replication_slot_failures(
    max_replication_slots: i32,
    used_replication_slots: i64,
    max_table_sync_workers: u16,
) -> Vec<ValidationFailure> {
    let free_slots = max_replication_slots as i64 - used_replication_slots;
    let required_slots = max_table_sync_workers as i64 + 1;

    if required_slots <= free_slots {
        Vec::new()
    } else {
        vec![ValidationFailure::critical(
            "Insufficient Replication Slots",
            format!(
                "Your source database has {free_slots} free replication slots, but this pipeline \
                 may need up to {required_slots} during the initial table copy \
                 ({used_replication_slots}/{max_replication_slots} slots are currently in \
                 use).\n\nThis includes 1 apply slot plus up to `max_table_sync_workers` table \
                 sync slots. Increase `max_replication_slots`, remove unused replication slots, \
                 or reduce `max_table_sync_workers`. After the initial copy, this pipeline only \
                 uses 1 slot.",
            ),
        )]
    }
}

/// Builds validation failures for WAL sender capacity.
fn wal_sender_failures(
    max_wal_senders: i32,
    max_replication_slots: i32,
    active_wal_senders: i64,
    max_table_sync_workers: u16,
) -> Vec<ValidationFailure> {
    let free_wal_senders = max_wal_senders as i64 - active_wal_senders;
    let required_wal_senders = max_table_sync_workers as i64 + 1;
    let mut failures = Vec::new();

    if required_wal_senders > free_wal_senders {
        failures.push(ValidationFailure::critical(
            "Insufficient WAL Senders",
            format!(
                "Your source database has {free_wal_senders} free WAL sender processes, but this \
                 pipeline may need up to {required_wal_senders} during the initial table copy \
                 ({active_wal_senders}/{max_wal_senders} WAL senders are currently \
                 active).\n\nEach active replication slot needs a WAL sender connection. Increase \
                 `max_wal_senders`, stop unused replication clients, or reduce \
                 `max_table_sync_workers`."
            ),
        ));
    }

    if max_wal_senders < max_replication_slots {
        failures.push(ValidationFailure::warning(
            "WAL Senders Below Replication Slots",
            format!(
                "`max_wal_senders` is {max_wal_senders}, but `max_replication_slots` is \
                 {max_replication_slots}.\n\nLogical replication needs a WAL sender connection \
                 for each active slot, and PostgreSQL recommends setting `max_wal_senders` at \
                 least as high as `max_replication_slots`, plus any physical replicas. Some \
                 replication clients may fail to connect even when slots are available."
            ),
        ));
    }

    failures
}

/// Builds validation failures for slot WAL retention settings.
fn slot_wal_keep_size_failures(
    max_slot_wal_keep_size_mb: i64,
    idle_replication_slot_timeout_seconds: i64,
) -> Vec<ValidationFailure> {
    let mut failures = Vec::new();

    match max_slot_wal_keep_size_mb {
        -1 => failures.push(ValidationFailure::warning(
            "Unlimited Slot WAL Retention",
            "`max_slot_wal_keep_size` is unlimited.\n\nLogical replication slots can retain WAL \
             indefinitely when ETL is paused, disconnected, or stuck on a table error. This does \
             not prevent the pipeline from starting, but an abandoned or stalled slot can fill \
             the source database disk.\n\nSet a bounded value large enough for your write volume \
             and longest expected initial copy.",
        )),
        0 => failures.push(ValidationFailure::critical(
            "Slot WAL Retention Disabled",
            "`max_slot_wal_keep_size` is 0 MB, leaving no per-slot WAL retention headroom.\n\nA \
             logical replication slot can be invalidated as soon as it falls behind at a \
             checkpoint, which can force ETL to restart table copies or require slot recreation. \
             Increase `max_slot_wal_keep_size` to leave WAL headroom for normal replication lag.",
        )),
        1..MIN_SLOT_WAL_KEEP_SIZE_MB => failures.push(ValidationFailure::warning(
            "Low Slot WAL Retention",
            format!(
                "`max_slot_wal_keep_size` is {max_slot_wal_keep_size_mb} MB, which is below ETL's \
                 recommended minimum of {MIN_SLOT_WAL_KEEP_SIZE_MB} MB.\n\nThis may be too small \
                 for logical replication during large transactions, destination outages, or long \
                 initial table copies. Increase it based on source write volume, available disk, \
                 and the longest time ETL may need to catch up."
            ),
        )),
        _ => {}
    }

    if (1..=LOW_IDLE_REPLICATION_SLOT_TIMEOUT_SECONDS)
        .contains(&idle_replication_slot_timeout_seconds)
    {
        failures.push(ValidationFailure::warning(
            "Low Idle Replication Slot Timeout",
            format!(
                "`idle_replication_slot_timeout` is {idle_replication_slot_timeout_seconds} \
                 seconds, which is below ETL's recommended minimum of \
                 {LOW_IDLE_REPLICATION_SLOT_TIMEOUT_SECONDS} seconds.\n\nPostgreSQL can \
                 invalidate ETL's logical replication slot after a short deploy, maintenance \
                 window, or incident pause, with `pg_replication_slots.invalidation_reason = \
                 'idle_timeout'`.\n\nUse `0` to disable idle-slot invalidation, or choose a value \
                 safely above expected ETL downtime."
            ),
        ));
    }

    failures
}
