//! Dynamic SQL interpolates only UUID-derived namespaces and generated table
//! names; values use bind parameters. Finite source load with explicit
//! transaction sizes and disjoint writer key ranges.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::json;
use tokio::{sync::Mutex, task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    journal::Journal,
    plan::{Load, Plan},
    source::{Source, db_error, payload, tables},
};

/// Measured load delivery and initial-copy overlap, when requested.
pub(crate) struct LoadResult {
    /// Whether the committed operation rate met the plan's lower bound.
    pub(crate) rate_pass: bool,
    /// Whether at least one committed transaction overlapped an observed copy.
    pub(crate) copy_overlap: bool,
}

/// Runs a load stage and records both requested and achieved committed
/// operations.
pub(crate) async fn run_load(
    plan: &Plan,
    source: &Source,
    load: &Load,
    journal: Arc<Mutex<Journal>>,
    stop: CancellationToken,
    copy_pipeline: Option<u64>,
) -> Result<LoadResult> {
    journal.lock().await.record("load_started", json!({"name":load.name}))?;
    let start = Instant::now();
    let mut workers = JoinSet::new();
    for writer in 0..load.writers {
        let plan = plan.clone();
        let source = source.clone();
        let load = load.clone();
        let stop = stop.clone();
        workers
            .spawn(async move { write(&plan, &source, &load, writer, &stop, copy_pipeline).await });
    }
    let mut operations = 0_u64;
    let mut affected = 0_u64;
    let mut payload_bytes = 0_u64;
    let mut transactions = 0_u64;
    let mut copy_overlap = false;
    while let Some(result) = workers.join_next().await {
        let (o, a, b, t, overlap) =
            result.context("Workload task panicked or was unexpectedly cancelled")??;
        operations += o;
        affected += a;
        payload_bytes += b;
        transactions += t;
        copy_overlap |= overlap;
    }
    ensure!(!stop.is_cancelled(), "Experiment interrupted");
    ensure!(operations == load.operations, "Load ended before all operations committed");
    let elapsed = start.elapsed().as_secs_f64();
    let rate = operations as f64 / elapsed;
    let rate_pass = rate >= f64::from(load.operations_per_second) * plan.minimum_rate_ratio;
    journal.lock().await.record("load_complete",json!({"name":load.name,"operations":operations,"affected_rows":affected,"written_text_bytes":payload_bytes,"transactions":transactions,"seconds":elapsed,"achieved_operations_per_second":rate,"requested_operations_per_second":load.operations_per_second,"rate_pass":rate_pass,"copy_overlap":copy_pipeline.map(|_|copy_overlap)}))?;
    Ok(LoadResult { rate_pass, copy_overlap })
}

/// One connection owns each transaction; cancellation rolls back unfinished
/// work before returning.
async fn write(
    plan: &Plan,
    source: &Source,
    load: &Load,
    writer: u16,
    stop: &CancellationToken,
    copy_pipeline: Option<u64>,
) -> Result<(u64, u64, u64, u64, bool)> {
    let table_list = tables(plan);
    let mut sequence = u64::from(writer);
    let mut committed = 0;
    let mut affected = 0;
    let mut bytes = 0;
    let mut transactions = 0;
    let mut copy_overlap = false;
    let rate = f64::from(load.operations_per_second) / f64::from(load.writers);
    while sequence < load.operations {
        if stop.is_cancelled() {
            break;
        }
        let transaction_start = Instant::now();
        let mut tx = source.pool.begin().await.map_err(db_error)?;
        let mut pending = 0;
        let mut pending_affected = 0;
        let mut pending_bytes = 0;
        while pending < u64::from(load.transaction_rows) && sequence < load.operations {
            if stop.is_cancelled() {
                tx.rollback().await.map_err(db_error)?;
                return Ok((committed, affected, bytes, transactions, copy_overlap));
            }
            let table_index = usize::try_from(
                (sequence / u64::from(load.writers)) % u64::try_from(table_list.len())?,
            )?;
            let (table, group) = &table_list[table_index];
            let local = sequence / u64::from(load.writers) / u64::try_from(table_list.len())?;
            let range = (group.rows_per_table - 2) / u64::from(load.writers);
            let row = 2 + u64::from(writer) + ((local / 6) % range) * u64::from(load.writers);
            let id = i64::try_from(row)?;
            let name = format!("{}.{table}", source.schema);
            let revision = i64::try_from(sequence + 1)?;
            let operation = local % 6;
            let count = match operation {
                0 => {
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "update {name} set revision=$1 where id=$2"
                    )))
                    .bind(revision)
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                }
                1 => {
                    let text =
                        payload(plan.seed, table_index, row, sequence + 1, group.payload_bytes);
                    pending_bytes += u64::from(group.payload_bytes);
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "update {name} set revision=$1,payload=$2 where id=$3"
                    )))
                    .bind(revision)
                    .bind(text)
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                }
                2 => {
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "update {name} set revision=$1,payload=null where id=$2"
                    )))
                    .bind(revision)
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                }
                3 => {
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "update {name} set revision=$1,metadata=case when $1::bigint % 2=0 then \
                         'null'::jsonb else jsonb_build_object('kind',$1::bigint) end where id=$2"
                    )))
                    .bind(revision)
                    .bind(id)
                    .execute(&mut *tx)
                    .await
                }
                4 => {
                    sqlx::query(sqlx::AssertSqlSafe(format!("delete from {name} where id=$1")))
                        .bind(id)
                        .execute(&mut *tx)
                        .await
                }
                _ => {
                    let text =
                        payload(plan.seed, table_index, row, sequence + 1, group.payload_bytes);
                    pending_bytes += u64::from(group.payload_bytes);
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "insert into {name} values ($1,$2,$3,null) on conflict (id) do update set \
                         revision=excluded.revision,payload=excluded.payload,metadata=excluded.\
                         metadata"
                    )))
                    .bind(id)
                    .bind(revision)
                    .bind(text)
                    .execute(&mut *tx)
                    .await
                }
            }
            .map_err(db_error)?
            .rows_affected();
            pending_affected += count;
            pending += 1;
            sequence += u64::from(load.writers);
        }
        tx.commit().await.map_err(db_error)?;
        committed += pending;
        affected += pending_affected;
        bytes += pending_bytes;
        transactions += 1;
        if let Some(pipeline) = copy_pipeline
            && !copy_overlap
        {
            copy_overlap = source.copy_active(pipeline).await?;
        }
        // Pace committed transactions without issuing catch-up bursts when the
        // source slows down.
        let next = transaction_start + Duration::from_secs_f64(pending as f64 / rate);
        tokio::select! {
            _=stop.cancelled()=>break,
            _=tokio::time::sleep_until(next)=>{}
        }
    }
    Ok((committed, affected, bytes, transactions, copy_overlap))
}
