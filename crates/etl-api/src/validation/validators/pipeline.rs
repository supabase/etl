use async_trait::async_trait;
use etl_postgres::replication::catalog::ETL_SCHEMA_NAME;
use sqlx::FromRow;

use super::super::{ValidationContext, ValidationError, ValidationFailure, Validator};

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
    /// Optional idle slot invalidation timeout in seconds.
    idle_replication_slot_timeout_seconds: Option<i64>,
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
                current_setting('wal_level') as wal_level,
                (
                    select rolsuper or rolreplication
                    from pg_roles
                    where rolname = current_user
                ) as has_replication_permission,
                (
                    select setting::int
                    from pg_settings
                    where name = 'max_replication_slots'
                ) as max_replication_slots,
                (
                    select count(*)
                    from pg_replication_slots
                ) as used_replication_slots,
                (
                    select setting::int
                    from pg_settings
                    where name = 'max_wal_senders'
                ) as max_wal_senders,
                (
                    select count(*)
                    from pg_stat_replication
                ) as active_wal_senders,
                (
                    select setting::bigint
                    from pg_settings
                    where name = 'max_slot_wal_keep_size'
                ) as max_slot_wal_keep_size_mb,
                (
                    select setting::bigint
                    from pg_settings
                    where name = 'idle_replication_slot_timeout'
                ) as idle_replication_slot_timeout_seconds
            "#,
        )
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
    idle_replication_slot_timeout_seconds: Option<i64>,
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

    if let Some(timeout_seconds) = idle_replication_slot_timeout_seconds
        && (1..=LOW_IDLE_REPLICATION_SLOT_TIMEOUT_SECONDS).contains(&timeout_seconds)
    {
        failures.push(ValidationFailure::warning(
            "Low Idle Replication Slot Timeout",
            format!(
                "`idle_replication_slot_timeout` is {timeout_seconds} seconds, which is below \
                 ETL's recommended minimum of {LOW_IDLE_REPLICATION_SLOT_TIMEOUT_SECONDS} \
                 seconds.\n\nPostgreSQL can invalidate ETL's logical replication slot after a \
                 short deploy, maintenance window, or incident pause, with \
                 `pg_replication_slots.invalidation_reason = 'idle_timeout'`.\n\nUse `0` to \
                 disable idle-slot invalidation, or choose a value safely above expected ETL \
                 downtime."
            ),
        ));
    }

    failures
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validation::FailureType;

    fn valid_logical_replication_settings_audit() -> LogicalReplicationSettingsAudit {
        LogicalReplicationSettingsAudit {
            wal_level: "logical".to_owned(),
            has_replication_permission: true,
            max_replication_slots: 10,
            used_replication_slots: 0,
            max_wal_senders: 10,
            active_wal_senders: 0,
            max_slot_wal_keep_size_mb: MIN_SLOT_WAL_KEEP_SIZE_MB,
            idle_replication_slot_timeout_seconds: None,
        }
    }

    #[test]
    fn logical_replication_settings_failures_pass_for_good_settings() {
        let failures =
            logical_replication_settings_failures(valid_logical_replication_settings_audit(), 2);

        assert!(failures.is_empty());
    }

    #[test]
    fn logical_replication_settings_failures_collect_multiple_issues() {
        let mut audit = valid_logical_replication_settings_audit();
        audit.wal_level = "replica".to_owned();
        audit.has_replication_permission = false;
        audit.max_replication_slots = 2;
        audit.used_replication_slots = 1;

        let failures = logical_replication_settings_failures(audit, 2);

        assert!(
            failures.iter().any(|failure| failure.name == "Invalid WAL Level"),
            "should report invalid WAL level"
        );
        assert!(
            failures.iter().any(|failure| failure.name == "Missing Replication Permission"),
            "should report missing replication permissions"
        );
        assert!(
            failures.iter().any(|failure| failure.name == "Insufficient Replication Slots"),
            "should report insufficient replication slots"
        );
    }

    #[test]
    fn replication_slot_failures_detect_insufficient_free_slots() {
        let failures = replication_slot_failures(3, 2, 2);

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].name, "Insufficient Replication Slots");
        assert_eq!(failures[0].failure_type, FailureType::Critical);
    }

    #[test]
    fn wal_sender_failures_detect_insufficient_free_senders() {
        let failures = wal_sender_failures(4, 4, 2, 3);

        let failure = failures
            .iter()
            .find(|failure| failure.name == "Insufficient WAL Senders")
            .expect("should report insufficient WAL sender capacity");
        assert_eq!(failure.failure_type, FailureType::Critical);
    }

    #[test]
    fn wal_sender_failures_warn_when_senders_are_below_slots() {
        let failures = wal_sender_failures(4, 8, 0, 2);

        let failure = failures
            .iter()
            .find(|failure| failure.name == "WAL Senders Below Replication Slots")
            .expect("should warn when sender capacity is lower than slot capacity");
        assert_eq!(failure.failure_type, FailureType::Warning);
    }

    #[test]
    fn slot_wal_keep_size_failures_warn_for_unlimited_retention() {
        let failures = slot_wal_keep_size_failures(-1, None);

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].name, "Unlimited Slot WAL Retention");
        assert_eq!(failures[0].failure_type, FailureType::Warning);
    }

    #[test]
    fn slot_wal_keep_size_failures_critical_for_zero_retention() {
        let failures = slot_wal_keep_size_failures(0, None);

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].name, "Slot WAL Retention Disabled");
        assert_eq!(failures[0].failure_type, FailureType::Critical);
    }

    #[test]
    fn slot_wal_keep_size_failures_warn_for_low_idle_slot_timeout() {
        let failures = slot_wal_keep_size_failures(MIN_SLOT_WAL_KEEP_SIZE_MB, Some(0));
        assert!(failures.is_empty());

        let failures = slot_wal_keep_size_failures(MIN_SLOT_WAL_KEEP_SIZE_MB, Some(300));

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].name, "Low Idle Replication Slot Timeout");
        assert_eq!(failures[0].failure_type, FailureType::Warning);
        assert!(
            failures[0].reason.contains("recommended minimum of 300 seconds"),
            "warning should include the recommended minimum"
        );

        let failures = slot_wal_keep_size_failures(MIN_SLOT_WAL_KEEP_SIZE_MB, Some(301));
        assert!(failures.is_empty());
    }
}
