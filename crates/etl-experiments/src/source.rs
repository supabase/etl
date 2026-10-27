//! Dynamic SQL interpolates only UUID-derived namespaces and generated table
//! names; values use bind parameters. Synthetic fixtures, persistent run
//! ownership, and source observations.

use std::{str::FromStr, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{
    ConnectOptions, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};

use crate::plan::{Plan, TableGroup};

/// Database connections and the run-specific schema.
#[derive(Clone)]
pub(crate) struct Source {
    /// Bounded connection pool for fixture operations and workload writers.
    pub(crate) pool: PgPool,
    /// UUID-derived namespace owned by this experiment.
    pub(crate) schema: String,
}

/// Source failures retain a stable SQLSTATE without exposing SQL, credentials,
/// or cell values.
pub(crate) fn db_error(error: sqlx::Error) -> anyhow::Error {
    let code = error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(std::borrow::Cow::into_owned);
    anyhow::Error::new(error).context(format!(
        "Source database operation failed (SQLSTATE {})",
        code.as_deref().unwrap_or("unavailable")
    ))
}

/// Computes repeatable, poorly compressible ASCII text without storing a giant
/// seed fixture.
pub(crate) fn payload(seed: u64, table: usize, row: u64, revision: u64, bytes: u32) -> String {
    let mut text = String::with_capacity(bytes as usize + 64);
    let mut block = 0_u64;
    while text.len() < bytes as usize {
        let hash = Sha256::digest(format!("{seed}:{table}:{row}:{revision}:{block}"));
        for byte in hash {
            text.push_str(&format!("{byte:02x}"));
        }
        block += 1;
    }
    text.truncate(bytes as usize);
    text
}

/// Expands the table groups into stable names and their row-size contracts.
pub(crate) fn tables(plan: &Plan) -> Vec<(String, &TableGroup)> {
    plan.groups
        .iter()
        .flat_map(|g| std::iter::repeat_n(g, usize::from(g.tables)))
        .enumerate()
        .map(|(i, g)| (format!("t{i:04}"), g))
        .collect()
}

impl Source {
    /// Requires TLS identity verification and an exact source-host match.
    pub(crate) async fn connect(plan: &Plan) -> Result<Self> {
        let url =
            std::env::var("EXPERIMENT_SOURCE_URL").context("EXPERIMENT_SOURCE_URL is required")?;
        let options = PgConnectOptions::from_str(&url)
            .map_err(|_| anyhow::anyhow!("Invalid source connection configuration"))?;
        ensure!(options.get_host() == plan.source_host, "Source hostname does not match the plan");
        let options = options
            .ssl_mode(PgSslMode::VerifyFull)
            .application_name(&format!("etl_experiment_{}", plan.run_id.simple()))
            .disable_statement_logging();
        let max_writers = plan
            .cdc_loads
            .iter()
            .chain([&plan.copy_load])
            .map(|l| u32::from(l.writers))
            .max()
            .unwrap_or(1);
        let pool = PgPoolOptions::new()
            .max_connections(max_writers + 6)
            .acquire_timeout(Duration::from_secs(30))
            .after_connect(|conn, _| {
                Box::pin(async move {
                    sqlx::raw_sql("set statement_timeout = '60s'; set lock_timeout = '10s'")
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .map_err(db_error)?;
        Ok(Self { pool, schema: plan.schema() })
    }

    /// Creates a fresh owned namespace. A partial preparation is deliberately
    /// not resumed.
    pub(crate) async fn prepare(&self, plan: &Plan, source_id: u64) -> Result<()> {
        sqlx::query(sqlx::AssertSqlSafe(format!("create schema \"{}\"", self.schema)))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "create table {}.experiment_run (singleton boolean primary key default true \
             check(singleton), fixture_hash text not null, phase text not null, source_id bigint \
             not null, pipeline_id bigint, destination_id bigint, heartbeat timestamptz not null \
             default clock_timestamp(), started_at timestamptz, start_lsn pg_lsn not null default \
             pg_current_wal_lsn(), stop_requested boolean not null default false, observer_ready \
             timestamptz, snowflake_created boolean not null default false)",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "insert into {}.experiment_run (fixture_hash,phase,source_id) values \
             ($1,'preparing',$2)",
            self.schema
        )))
        .bind(plan.fixture_hash()?)
        .bind(i64::try_from(source_id)?)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        for (index, (table, group)) in tables(plan).iter().enumerate() {
            println!("preparing table {} of {}", index + 1, plan.table_count());
            let name = format!("{}.{}", self.schema, table);
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "create table {name} (id bigint primary key, revision bigint not null, payload \
                 text, metadata jsonb); alter table {name} replica identity {}",
                if group.replica_identity_full { "full" } else { "default" }
            )))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
            if group.external_storage {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "alter table {name} alter column payload set storage external"
                )))
                .execute(&self.pool)
                .await
                .map_err(db_error)?;
            }
            let batch = (1_048_576 / u64::from(group.payload_bytes)).clamp(1, 256);
            let mut first = 1;
            while first <= group.rows_per_table {
                let last = (first + batch - 1).min(group.rows_per_table);
                let ids: Vec<i64> =
                    (first..=last).map(i64::try_from).collect::<std::result::Result<_, _>>()?;
                let texts: Vec<String> = (first..=last)
                    .map(|row| payload(plan.seed, index, row, 0, group.payload_bytes))
                    .collect();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "insert into {name} select id, 0, payload, jsonb_build_object('kind',id % 7) \
                     from unnest($1::bigint[],$2::text[]) as input(id,payload)"
                )))
                .bind(ids)
                .bind(texts)
                .execute(&self.pool)
                .await
                .map_err(db_error)?;
                first = last + 1;
            }
            sqlx::query(sqlx::AssertSqlSafe(format!("analyze {name}")))
                .execute(&self.pool)
                .await
                .map_err(db_error)?;
        }
        let names = tables(plan)
            .iter()
            .map(|(t, _)| format!("{}.{t}", self.schema))
            .collect::<Vec<_>>()
            .join(",");
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "create publication {} for table {names}",
            plan.publication()
        )))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "grant usage on schema {} to supabase_etl_admin; grant select on {} to \
             supabase_etl_admin",
            self.schema, names
        )))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        self.phase("prepared").await
    }

    /// Claims execution once, including when Kubernetes starts duplicate Job
    /// pods.
    pub(crate) async fn claim(&self, plan: &Plan) -> Result<()> {
        let changed = sqlx::query(sqlx::AssertSqlSafe(format!(
            "update {}.experiment_run set \
             phase='running',started_at=clock_timestamp(),heartbeat=clock_timestamp(), \
             start_lsn=pg_current_wal_lsn() where phase='prepared' and snowflake_created and \
             fixture_hash=$1",
            self.schema
        )))
        .bind(plan.fixture_hash()?)
        .execute(&self.pool)
        .await
        .map_err(db_error)?
        .rows_affected();
        ensure!(
            changed == 1,
            "Run must be fully prepared, unclaimed, and match its fixture; create a fresh run \
             after failure"
        );
        Ok(())
    }

    /// Persists a lifecycle transition and refreshes the owner heartbeat.
    pub(crate) async fn phase(&self, phase: &str) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), self.write_phase(phase))
            .await
            .context("Source lifecycle update timed out")?
    }

    /// Writes a lifecycle state inside the caller's transport deadline.
    async fn write_phase(&self, phase: &str) -> Result<()> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "update {}.experiment_run set phase=$1,heartbeat=clock_timestamp()",
            self.schema
        )))
        .bind(phase)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    /// Persists the exact hosted resources for subsequent reconciliation.
    pub(crate) async fn resources(&self, pipeline: u64, destination: u64) -> Result<()> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "update {}.experiment_run set pipeline_id=$1,destination_id=$2",
            self.schema
        )))
        .bind(i64::try_from(pipeline)?)
        .bind(i64::try_from(destination)?)
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    /// The control row is never part of the replication publication.
    pub(crate) async fn control(&self) -> Result<Value> {
        tokio::time::timeout(Duration::from_secs(10), self.read_control())
            .await
            .context("Source control read timed out")?
    }

    /// Reads the ownership record inside the caller's transport deadline.
    async fn read_control(&self) -> Result<Value> {
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "select to_jsonb(r) || jsonb_build_object('heartbeat_age_seconds',extract(epoch from \
             clock_timestamp()-heartbeat),'observer_age_seconds',extract(epoch from \
             clock_timestamp()-observer_ready)) from {}.experiment_run r",
            self.schema
        )))
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)
    }

    /// Refreshes the runner lease without changing its lifecycle state.
    pub(crate) async fn heartbeat(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), self.write_heartbeat())
            .await
            .context("Source heartbeat timed out")?
    }

    /// Writes the owner lease inside the caller's transport deadline.
    async fn write_heartbeat(&self) -> Result<()> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "update {}.experiment_run set heartbeat=clock_timestamp()",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    /// Refreshes independent observation readiness with a transport deadline.
    pub(crate) async fn observer_heartbeat(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "update {}.experiment_run set observer_ready=clock_timestamp()",
                self.schema
            )))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
            Ok(())
        })
        .await
        .context("Observer heartbeat timed out")?
    }

    /// Asks the runner to cancel after a watchdog condition.
    pub(crate) async fn request_stop(&self) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), self.write_stop_request())
            .await
            .context("Source stop request timed out")?
    }

    /// Persists cancellation inside the caller's transport deadline.
    async fn write_stop_request(&self) -> Result<()> {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "update {}.experiment_run set stop_requested=true",
            self.schema
        )))
        .execute(&self.pool)
        .await
        .map_err(db_error)?;
        Ok(())
    }

    /// Confirms that pipeline deletion removed both apply and table-copy slots.
    /// An API success alone does not prove source resource reclamation.
    pub(crate) async fn slots_reclaimed(&self, pipeline: u64) -> Result<()> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let absent: bool = sqlx::query_scalar(
                    "select not exists(select 1 from pg_replication_slots where slot_name=$1 or \
                     starts_with(slot_name,$2))",
                )
                .bind(format!("supabase_etl_apply_{pipeline}"))
                .bind(format!("supabase_etl_table_sync_{pipeline}_"))
                .fetch_one(&self.pool)
                .await
                .map_err(db_error)?;
                if absent {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        })
        .await
        .context("Replication slots remain after pipeline deletion")?
    }

    /// Reads durable lifecycle states; the dashboard's following-WAL state also
    /// includes table-worker catchup and does not prove handoff is complete.
    pub(crate) async fn table_states(&self, pipeline: u64) -> Result<Vec<Value>> {
        if sqlx::query_scalar::<_, Option<String>>(
            "select to_regclass('etl.replication_state')::text",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?
        .is_none()
        {
            return Ok(Vec::new());
        }
        let states: Value = sqlx::query_scalar(
            "select coalesce(jsonb_agg(jsonb_build_object('schema',n.nspname,'name',c.relname,'\
             state',r.state::text)), '[]'::jsonb) from etl.replication_state r join pg_class c on \
             c.oid=r.table_id join pg_namespace n on n.oid=c.relnamespace where r.pipeline_id=$1 \
             and r.is_current",
        )
        .bind(i64::try_from(pipeline)?)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        serde_json::from_value(states).context("Invalid durable table states")
    }

    /// Requires every planned table to have handed off to steady-state apply.
    pub(crate) async fn initial_sync_complete(&self, plan: &Plan, pipeline: u64) -> Result<bool> {
        let states = self.table_states(pipeline).await?;
        ensure!(
            !states.iter().any(|state| state["state"] == "errored"),
            "A table failed initial synchronization"
        );
        Ok(states.len() == plan.table_count()
            && tables(plan).iter().all(|(table, _)| {
                states.iter().any(|state| {
                    state["schema"] == plan.schema()
                        && state["name"] == *table
                        && state["state"] == "ready"
                })
            }))
    }

    /// Observes an active copy after a source transaction committed.
    pub(crate) async fn copy_active(&self, pipeline: u64) -> Result<bool> {
        Ok(self.table_states(pipeline).await?.iter().any(|state| state["state"] == "data_sync"))
    }

    /// Records source settings and physical fixture size without source row
    /// values.
    pub(crate) async fn environment(&self) -> Result<Value> {
        sqlx::query_scalar(
            "select jsonb_build_object('postgres_version',version(),'wal_level',current_setting('\
             wal_level'),'max_replication_slots',current_setting('max_replication_slots'),'\
             max_slot_wal_keep_size',current_setting('max_slot_wal_keep_size'),'max_connections',\
             current_setting('max_connections'),'database_bytes',\
             pg_database_size(current_database()))",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)
    }

    /// Returns the current source WAL insertion boundary.
    pub(crate) async fn wal_lsn(&self) -> Result<String> {
        sqlx::query_scalar("select pg_current_wal_lsn()::text")
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)
    }

    /// Measures cluster-wide WAL since the supplied LSN.
    pub(crate) async fn wal_since(&self, lsn: &str) -> Result<i64> {
        sqlx::query_scalar("select pg_wal_lsn_diff(pg_current_wal_lsn(), $1::pg_lsn)::bigint")
            .bind(lsn)
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)
    }

    /// Samples raw LSNs, retained WAL, transaction ages and logical-decoding
    /// spill counters.
    pub(crate) async fn sample(&self, pipeline: u64) -> Result<Value> {
        tokio::time::timeout(Duration::from_secs(30), self.read_sample(pipeline))
            .await
            .context("Source observation timed out")?
    }

    /// Collects source observations inside one client-side transport deadline.
    async fn read_sample(&self, pipeline: u64) -> Result<Value> {
        let apply = format!("supabase_etl_apply_{pipeline}");
        let prefix = format!("supabase_etl_table_sync_{pipeline}_");
        let slots: Value = sqlx::query_scalar(
            "select coalesce(jsonb_agg(jsonb_build_object('slot_name',s.slot_name,'active',s.\
             active,'restart_lsn',s.restart_lsn::text,'confirmed_flush_lsn',s.\
             confirmed_flush_lsn::text,'wal_status',s.wal_status,'safe_wal_size',s.safe_wal_size,'\
             retained_bytes',pg_wal_lsn_diff(pg_current_wal_lsn(),s.restart_lsn)::bigint,'\
             ack_lag_bytes',pg_wal_lsn_diff(pg_current_wal_lsn(),s.confirmed_flush_lsn)::bigint,'\
             invalidation_reason',to_jsonb(s)->'invalidation_reason')), '[]'::jsonb) from \
             pg_replication_slots s where s.slot_name=$1 or starts_with(s.slot_name,$2)",
        )
        .bind(&apply)
        .bind(&prefix)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let transactions: Value = sqlx::query_scalar(
            "select coalesce(jsonb_agg(jsonb_build_object('pid',pid,'application_name',\
             application_name,'state',state,'age_seconds',extract(epoch from \
             clock_timestamp()-xact_start),'backend_xid',backend_xid::text,'backend_xmin',\
             backend_xmin::text,'wait_event_type',wait_event_type,'wait_event',wait_event)), \
             '[]'::jsonb) from pg_stat_activity where datname=current_database() and xact_start \
             is not null and pid<>pg_backend_pid()",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let spill: Value = sqlx::query_scalar(
            "select coalesce(jsonb_agg(to_jsonb(s)), '[]'::jsonb) from pg_stat_replication_slots \
             s where s.slot_name=$1 or starts_with(s.slot_name,$2)",
        )
        .bind(&apply)
        .bind(&prefix)
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let generated: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "select pg_wal_lsn_diff(pg_current_wal_lsn(),start_lsn)::bigint from {}.experiment_run",
            self.schema
        )))
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?;
        let checkpoints = if sqlx::query_scalar::<_, Option<String>>(
            "select to_regclass('etl.replication_progress')::text",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(db_error)?
        .is_some()
        {
            sqlx::query_scalar::<_, Value>(
                "select coalesce(jsonb_agg(jsonb_build_object('worker_type',worker_type,'table_id'\
                 ,table_id,'checkpoint_lsn',flush_lsn::text)), '[]'::jsonb) from \
                 etl.replication_progress where pipeline_id=$1",
            )
            .bind(i64::try_from(pipeline)?)
            .fetch_one(&self.pool)
            .await
            .map_err(db_error)?
        } else {
            Value::Null
        };
        Ok(
            json!({"table_states":self.table_states(pipeline).await?,"checkpoints":checkpoints,"lsn":self.wal_lsn().await?,"generated_wal_bytes":generated,"slots":slots,"transactions":transactions,"spill":spill,"settings":self.environment().await?}),
        )
    }

    /// Final marker rows establish convergence after every writer has stopped.
    pub(crate) async fn markers(&self, plan: &Plan) -> Result<()> {
        for (table, _) in tables(plan) {
            // Reserved row 1 retains its original large payload across a
            // revision-only CDC update.
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "update {}.{table} set revision=revision+1 where id=1",
                self.schema
            )))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "insert into {}.{table} values (0,-1,null,'{{\"marker\":true}}')",
                self.schema
            )))
            .execute(&self.pool)
            .await
            .map_err(db_error)?;
        }
        Ok(())
    }

    /// Fetches one bounded verification page with integers represented as
    /// decimal strings.
    pub(crate) async fn rows(&self, table: &str, after: i64, limit: i64) -> Result<Vec<Value>> {
        crate::plan::identifier(table)?;
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "select jsonb_build_array(id::text,revision::text,payload,metadata,metadata is null) \
             as row from {}.{table} where id>$1 order by id limit $2",
            self.schema
        )))
        .bind(after)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(db_error)?;
        rows.iter().map(|r| r.try_get("row").map_err(db_error)).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use serde_json::json;
    use sqlx::{
        ConnectOptions,
        postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
    };
    use tokio::sync::Mutex;
    use tokio_util::sync::CancellationToken;

    use crate::{
        journal::Journal,
        plan::{Plan, Profile},
        source::{Source, payload},
        workload::run_load,
    };

    #[tokio::test]
    #[ignore = "Requires an isolated local PostgreSQL 17 database via EXPERIMENT_TEST_DATABASE_URL"]
    async fn fixture_claim_load_and_partial_probe_use_real_postgres() {
        let url = std::env::var("EXPERIMENT_TEST_DATABASE_URL").unwrap();
        let options = PgConnectOptions::from_str(&url).unwrap();
        assert!(matches!(options.get_host(), "127.0.0.1" | "localhost"));
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options.ssl_mode(PgSslMode::Disable).disable_statement_logging())
            .await
            .unwrap();
        sqlx::raw_sql(
            "do $$ begin create role supabase_etl_admin; exception when duplicate_object then \
             null; end $$",
        )
        .execute(&pool)
        .await
        .unwrap();
        let base: Plan = serde_json::from_str(include_str!("../specs/baseline.json")).unwrap();
        let mut plan = base.derive(Profile::Smoke).unwrap();
        plan.groups[0].tables = 2;
        plan.copy_load.operations = 192;
        plan.copy_load.operations_per_second = 10000;
        let source = Source { pool, schema: plan.schema() };
        source.prepare(&plan, 1).await.unwrap();
        assert_eq!(source.control().await.unwrap()["phase"], "prepared");
        assert!(source.claim(&plan).await.is_err());
        // This source-only test records the independently prepared destination
        // prerequisite without contacting Snowflake.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "update {}.experiment_run set snowflake_created=true",
            source.schema
        )))
        .execute(&source.pool)
        .await
        .unwrap();
        source.claim(&plan).await.unwrap();
        assert!(source.claim(&plan).await.is_err());
        assert!(!source.initial_sync_complete(&plan, 1).await.unwrap());
        sqlx::raw_sql(
            "create schema etl; create table etl.replication_state (pipeline_id bigint,table_id \
             oid,state text,is_current boolean)",
        )
        .execute(&source.pool)
        .await
        .unwrap();
        sqlx::query(
            "insert into etl.replication_state select 1,c.oid,'data_sync',true from pg_class c \
             join pg_namespace n on n.oid=c.relnamespace where n.nspname=$1 and c.relname in \
             ('t0000','t0001')",
        )
        .bind(&source.schema)
        .execute(&source.pool)
        .await
        .unwrap();
        assert!(source.copy_active(1).await.unwrap());
        let journal_dir = tempfile::tempdir().unwrap();
        let journal = Arc::new(Mutex::new(
            Journal::create(journal_dir.path(), plan.max_artifact_bytes).unwrap(),
        ));
        let load = run_load(
            &plan,
            &source,
            &plan.copy_load,
            Arc::clone(&journal),
            CancellationToken::new(),
            Some(1),
        )
        .await
        .unwrap();
        assert!(load.copy_overlap);
        sqlx::query("update etl.replication_state set state='sync_done'")
            .execute(&source.pool)
            .await
            .unwrap();
        assert!(!source.initial_sync_complete(&plan, 1).await.unwrap());
        sqlx::query("update etl.replication_state set state='ready'")
            .execute(&source.pool)
            .await
            .unwrap();
        assert!(source.initial_sync_complete(&plan, 1).await.unwrap());
        source.markers(&plan).await.unwrap();
        let rows = source.rows("t0000", -1, 2).await.unwrap();
        assert_eq!(rows[0], json!(["0","-1",null,{"marker":true},false]));
        assert_eq!(rows[1], json!(["1","1",payload(plan.seed,0,1,0,16384),{"kind":1},false]));
        assert!(source.sample(1).await.unwrap()["slots"].as_array().unwrap().is_empty());
        // Adjacent pipeline IDs must not share a slot-cleanup boundary.
        sqlx::query(
            "select * from pg_create_logical_replication_slot('supabase_etl_apply_1','pgoutput')",
        )
        .execute(&source.pool)
        .await
        .unwrap();
        sqlx::query(
            "select * from pg_create_logical_replication_slot('supabase_etl_apply_10','pgoutput')",
        )
        .execute(&source.pool)
        .await
        .unwrap();
        assert_eq!(source.sample(1).await.unwrap()["slots"].as_array().unwrap().len(), 1);
        sqlx::query("select pg_drop_replication_slot('supabase_etl_apply_1')")
            .execute(&source.pool)
            .await
            .unwrap();
        source.slots_reclaimed(1).await.unwrap();
        sqlx::query("select pg_drop_replication_slot('supabase_etl_apply_10')")
            .execute(&source.pool)
            .await
            .unwrap();
        sqlx::raw_sql(
            "create table etl.replication_progress (pipeline_id bigint,worker_type text,table_id \
             bigint,flush_lsn pg_lsn); insert into etl.replication_progress values \
             (1,'apply',null,'0/1234')",
        )
        .execute(&source.pool)
        .await
        .unwrap();
        assert_eq!(
            source.sample(1).await.unwrap()["checkpoints"],
            json!([{"worker_type":"apply","table_id":null,"checkpoint_lsn":"0/1234"}])
        );
        sqlx::raw_sql(
            "drop table etl.replication_progress; drop table etl.replication_state; drop schema \
             etl",
        )
        .execute(&source.pool)
        .await
        .unwrap();
        source.observer_heartbeat().await.unwrap();
        let control = source.control().await.unwrap();
        assert!(control["observer_age_seconds"].as_f64().is_some_and(|age| age < 10.0));
        assert!(control["start_lsn"].as_str().is_some());
        let stop = CancellationToken::new();
        stop.cancel();
        assert!(run_load(&plan, &source, &plan.copy_load, journal, stop, None).await.is_err());
        sqlx::query(sqlx::AssertSqlSafe(format!("drop publication {}", plan.publication())))
            .execute(&source.pool)
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!("drop schema {} cascade", source.schema)))
            .execute(&source.pool)
            .await
            .unwrap();
        source.pool.close().await;
    }
}
