//! Dynamic SQL interpolates only UUID-derived namespaces and generated table
//! names; values use bind parameters. Experiment lifecycle, ownership
//! validation, and behavior assertions.

use std::{path::Path, sync::Arc, time::Duration};

use anyhow::{Context, Result, ensure};
use reqwest::Method;
use serde_json::json;
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    journal::Journal,
    plan::{Launch, Plan, Retention},
    platform::{Platform, id, is_stopped},
    snowflake::Snowflake,
    source::{Source, db_error, tables},
    workload::run_load,
};

/// Prepares a fresh source and destination before Studio attachment or
/// automated launch.
pub(crate) async fn prepare(plan: &Plan, output: &Path) -> Result<()> {
    let mut journal = Journal::create(output, plan.max_artifact_bytes)?;
    journal.save("plan.json", &serde_json::to_value(plan)?)?;
    let platform = Platform::from_env(plan)?;
    let source = Source::connect(plan).await?;
    let snowflake = Snowflake::new(plan)?;
    let source_id = platform.source().await?;
    let sources = platform.request(Method::GET, "/sources", None).await?;
    ensure!(
        sources["sources"].as_array().is_some_and(|sources| sources
            .iter()
            .any(|s| s["id"] == source_id && s["config"]["host"] == plan.source_host)),
        "Platform source hostname does not match the experiment source"
    );
    journal.record("prepare_started",json!({"source_id":source_id,"fixture_text_bytes":plan.fixture_bytes()?,"tables":plan.table_count()}))?;
    // The source namespace is the ownership record; existing namespaces fail
    // closed.
    source.prepare(plan, source_id).await?;
    snowflake.prepare().await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "update {}.experiment_run set snowflake_created=true",
        source.schema
    )))
    .execute(&source.pool)
    .await
    .map_err(db_error)?;
    journal.record("prepared",json!({"source_schema":plan.schema(),"publication":plan.publication(),"snowflake_schema":plan.schema(),"environment":source.environment().await?}))?;
    journal.save(
        "summary.json",
        &json!({"component":"prepare","run_id":plan.run_id,"prepared":true}),
    )?;
    Ok(())
}

/// Creates or validates an ordinary stopped pipeline, with no direct runtime
/// deployment.
async fn select_pipeline(
    plan: &Plan,
    source: &Source,
    platform: &Platform,
    snowflake: &Snowflake,
) -> Result<u64> {
    let source_id =
        source.control().await?["source_id"].as_u64().context("Missing prepared source ID")?;
    let pipeline = match plan.launch {
        Launch::Attach { pipeline_id } => pipeline_id,
        Launch::Platform => {
            let destinations = platform.request(Method::GET, "/destinations", None).await?;
            ensure!(
                !destinations["destinations"]
                    .as_array()
                    .context("Missing destinations")?
                    .iter()
                    .any(|d| d["name"] == plan.schema()),
                "Destination name already exists; reconcile the previous creation before retrying"
            );
            let destination = match platform
                .request(Method::POST, "/destinations", Some(snowflake.destination_body()?))
                .await
            {
                Ok(value) => id(&value)?,
                Err(error) => match find_destination(plan, platform).await? {
                    Some(destination) => destination,
                    None => return Err(error),
                },
            };
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "update {}.experiment_run set destination_id=$1",
                source.schema
            )))
            .bind(i64::try_from(destination)?)
            .execute(&source.pool)
            .await
            .map_err(db_error)?;
            let mut config = plan.pipeline_config.clone();
            config["publication_name"] = plan.publication().into();
            config["table_sync_copy"] = json!({"type":"include_all_tables"});
            let request =
                json!({"source_id":source_id,"destination_id":destination,"config":config});
            let pipeline = match platform.request(Method::POST, "/pipelines", Some(request)).await {
                Ok(value) => id(&value)?,
                Err(error) => {
                    // A lost response may still have committed a pipeline;
                    // recover its exact IDs.
                    let inventory = platform.request(Method::GET, "/pipelines", None).await?;
                    let matches = inventory["pipelines"]
                        .as_array()
                        .context("Missing pipeline inventory")?
                        .iter()
                        .filter(|p| {
                            p["source_id"] == source_id
                                && p["destination_id"] == destination
                                && p["config"]["publication_name"] == plan.publication()
                        })
                        .collect::<Vec<_>>();
                    if let [pipeline] = matches.as_slice() {
                        id(pipeline)?
                    } else {
                        return Err(error);
                    }
                }
            };
            // Persist owned resource IDs before subsequent configuration checks
            // can fail; teardown must still find a successfully created
            // pipeline.
            source.resources(pipeline, destination).await?;
            pipeline
        }
    };
    let details = platform.pipeline(pipeline).await?;
    ensure!(
        details["source_id"] == source_id
            && details["config"]["publication_name"] == plan.publication(),
        "Selected pipeline does not belong to this fixture"
    );
    for (key, value) in plan.pipeline_config.as_object().context("Invalid pipeline config")? {
        ensure!(
            config_matches(value, &details["config"][key]),
            "Pipeline option {key} differs from the plan"
        );
    }
    let destination = details["destination_id"].as_u64().context("Missing destination ID")?;
    let target =
        platform.request(Method::GET, &format!("/destinations/{destination}"), None).await?;
    let config = &target["config"]["snowflake"];
    ensure!(
        config["schema"] == plan.schema()
            && config["database"] == plan.snowflake.database
            && config["account_id"] == plan.snowflake.account,
        "Selected pipeline targets a different Snowflake namespace"
    );
    let status =
        platform.request(Method::GET, &format!("/pipelines/{pipeline}/status"), None).await?;
    ensure!(is_stopped(&status), "Selected pipeline must be stopped before the experiment starts");
    source.resources(pipeline, destination).await?;
    if let Some(version) = plan.version_id {
        platform
            .request(
                Method::POST,
                &format!("/pipelines/{pipeline}/version"),
                Some(json!({"version_id":version})),
            )
            .await?;
    }
    Ok(pipeline)
}

/// Waits for every planned table, rather than treating one aggregate state as
/// initial-sync completion.
async fn initial_sync(
    plan: &Plan,
    source: &Source,
    pipeline: u64,
    stop: &CancellationToken,
) -> Result<()> {
    loop {
        ensure!(!stop.is_cancelled(), "Experiment interrupted");
        if source.initial_sync_complete(plan, pipeline).await? {
            return Ok(());
        }
        tokio::select! {
            _=stop.cancelled()=>{},
            _=tokio::time::sleep(Duration::from_secs(plan.sample_seconds))=>{}
        }
    }
}

/// Waits for copy before releasing the copy load; a missed fast copy is
/// recorded as incomplete concurrency coverage, never silently qualified.
async fn wait_for_copy(
    plan: &Plan,
    source: &Source,
    pipeline: u64,
    stop: &CancellationToken,
) -> Result<()> {
    loop {
        ensure!(!stop.is_cancelled(), "Experiment interrupted");
        if source.copy_active(pipeline).await?
            || source.initial_sync_complete(plan, pipeline).await?
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Runs one explicit intervention, followed by the configured finite CDC
/// stages.
async fn cdc(
    plan: &Plan,
    source: &Source,
    journal: Arc<Mutex<Journal>>,
    stop: &CancellationToken,
) -> Result<bool> {
    let start = source.wal_lsn().await?;
    let mut retention_observed = true;
    let mut held = match plan.retention {
        Retention::HeldTransaction { .. } => {
            let mut tx = source.pool.begin().await.map_err(db_error)?;
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "update {}.t0000 set revision=revision+1 where id=1",
                source.schema
            )))
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
            Some(tx)
        }
        _ => None,
    };
    if let Retention::LargeTransaction { rows } = plan.retention {
        let mut tx = source.pool.begin().await.map_err(db_error)?;
        for first in (1..=rows).step_by(1000) {
            ensure!(!stop.is_cancelled(), "Experiment interrupted");
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "update {}.t0000 set revision=revision+1 where id between $1 and $2",
                source.schema
            )))
            .bind(i64::try_from(first)?)
            .bind(i64::try_from((first + 999).min(rows))?)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        }
        tx.commit().await.map_err(db_error)?;
        journal.lock().await.record(
            "large_transaction_committed",
            json!({"rows":rows,"wal_bytes":source.wal_since(&start).await?}),
        )?;
    }
    let mut rate_pass = true;
    for load in &plan.cdc_loads {
        let work = run_load(plan, source, load, Arc::clone(&journal), stop.clone(), None);
        tokio::pin!(work);
        loop {
            tokio::select! {
                result=&mut work=>{rate_pass &= result?.rate_pass;break;}
                _=tokio::time::sleep(Duration::from_secs(plan.sample_seconds)),if held.is_some()=>{
                    if let Retention::HeldTransaction{release_after_wal_bytes}=plan.retention {
                        let wal=source.wal_since(&start).await?;
                        if wal>=i64::try_from(release_after_wal_bytes)? {
                            let pipeline=source.control().await?["pipeline_id"].as_u64().context("Missing pipeline ID")?;
                            retention_observed=sqlx::query_scalar("select confirmed_flush_lsn > $1::pg_lsn and restart_lsn <= $1::pg_lsn from pg_replication_slots where slot_name=$2")
                                .bind(&start).bind(format!("supabase_etl_apply_{pipeline}")).fetch_optional(&source.pool).await.map_err(db_error)?.unwrap_or(false);
                            held.take().unwrap().commit().await.map_err(db_error)?;
                            journal.lock().await.record("held_transaction_released",json!({"wal_bytes":wal,"ack_progress_with_restart_pinned":retention_observed}))?;
                        }
                    }
                }
            }
        }
    }
    if let Some(tx) = held {
        tx.rollback().await.map_err(db_error)?;
        anyhow::bail!(
            "Workload did not reach the held-transaction release threshold; increase its \
             committed WAL volume"
        );
    }
    if matches!(plan.retention, Retention::HeldTransaction { .. }) {
        let pipeline =
            source.control().await?["pipeline_id"].as_u64().context("Missing pipeline ID")?;
        let deadline = Instant::now() + Duration::from_secs(plan.convergence_seconds);
        loop {
            ensure!(!stop.is_cancelled(), "Experiment interrupted");
            let released = sqlx::query_scalar::<_, bool>(
                "select coalesce(restart_lsn > $1::pg_lsn,false) from pg_replication_slots where \
                 slot_name=$2",
            )
            .bind(&start)
            .bind(format!("supabase_etl_apply_{pipeline}"))
            .fetch_optional(&source.pool)
            .await
            .map_err(db_error)?
            .unwrap_or(false);
            if released {
                break;
            }
            ensure!(
                Instant::now() < deadline,
                "Restart LSN did not advance after the held transaction committed"
            );
            tokio::time::sleep(Duration::from_secs(plan.sample_seconds)).await;
        }
        journal
            .lock()
            .await
            .record("restart_lsn_released", json!({"transaction_start_lsn":start}))?;
    }
    Ok(rate_pass && retention_observed)
}

/// Proves final contents in bounded ordered pages after every writer has
/// stopped.
async fn verify(
    plan: &Plan,
    source: &Source,
    snowflake: &Snowflake,
    journal: Arc<Mutex<Journal>>,
    stop: &CancellationToken,
) -> Result<()> {
    source.markers(plan).await?;
    let deadline = Instant::now() + Duration::from_secs(plan.convergence_seconds);
    for (table, group) in tables(plan) {
        while !snowflake.marker(&table).await? {
            ensure!(Instant::now() < deadline, "Destination convergence deadline exceeded");
            ensure!(!stop.is_cancelled(), "Experiment interrupted");
            tokio::time::sleep(Duration::from_secs(plan.sample_seconds)).await;
        }
        let page = i64::from((524_288 / group.payload_bytes).clamp(1, 256));
        let mut after = -1;
        let mut checked = 0_u64;
        loop {
            ensure!(!stop.is_cancelled(), "Experiment interrupted");
            let expected = source.rows(&table, after, page).await?;
            let actual = snowflake.rows(&table, after, page).await?;
            // Never include rows in an assertion failure or artifact.
            ensure!(
                actual == expected,
                "Destination row mismatch in {table} after primary key {after}"
            );
            if expected.is_empty() {
                break;
            }
            checked += u64::try_from(expected.len())?;
            after = expected
                .last()
                .and_then(|row| row[0].as_str())
                .context("Invalid source primary key")?
                .parse()?;
        }
        journal.lock().await.record(
            "table_verified",
            json!({"table":table,"rows":checked,"all_values_equal":true}),
        )?;
    }
    Ok(())
}

/// Owns background work, controlled cancellation, pipeline stopping, and the
/// final report.
pub(crate) async fn run(plan: &Plan, output: &Path) -> Result<()> {
    let journal = Arc::new(Mutex::new(Journal::create(output, plan.max_artifact_bytes)?));
    journal.lock().await.save("plan.json", &serde_json::to_value(plan)?)?;
    let source = Source::connect(plan).await?;
    let platform = Platform::from_env(plan)?;
    let snowflake = Snowflake::new(plan)?;
    source.claim(plan).await?;
    journal.lock().await.record("run_started",json!({"run_id":plan.run_id,"runner_image":std::env::var("EXPERIMENT_RUNNER_IMAGE").ok(),"tables":plan.table_count(),"fixture_text_bytes":plan.fixture_bytes()?,"profile":plan.profile}))?;
    let stop = CancellationToken::new();
    let pipeline_known = std::cell::Cell::new(None);
    let observer_started = std::cell::Cell::new(false);
    let mut interrupted = false;
    let result = {
        let execution = async {
            let pipeline = select_pipeline(plan, &source, &platform, &snowflake).await?;
            pipeline_known.set(Some(pipeline));
            journal.lock().await.record("pipeline_selected",json!({"pipeline_id":pipeline,"details":platform.pipeline(pipeline).await?,"version":platform.request(Method::GET,&format!("/pipelines/{pipeline}/version"),None).await?}))?;
            let observer_deadline = Instant::now() + Duration::from_secs(120);
            loop {
                ensure!(!stop.is_cancelled(), "Experiment interrupted");
                let control = source.control().await?;
                if control["observer_age_seconds"].as_f64().is_some_and(|age| age < 30.0) {
                    observer_started.set(true);
                    break;
                }
                ensure!(
                    Instant::now() < observer_deadline,
                    "Independent observer did not become ready"
                );
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            platform.action(pipeline, "start").await?;
            journal
                .lock()
                .await
                .record("pipeline_start_accepted", json!({"pipeline_id":pipeline}))?;
            wait_for_copy(plan, &source, pipeline, &stop).await?;
            journal
                .lock()
                .await
                .record("initial_sync_observed", json!({"pipeline_id":pipeline}))?;
            let copy = run_load(
                plan,
                &source,
                &plan.copy_load,
                Arc::clone(&journal),
                stop.clone(),
                Some(pipeline),
            );
            let sync = async {
                initial_sync(plan, &source, pipeline, &stop).await?;
                journal
                    .lock()
                    .await
                    .record("initial_sync_complete", json!({"tables":plan.table_count()}))
            };
            let (copy_rate, ()) = tokio::try_join!(copy, sync)?;
            let cdc_rate = cdc(plan, &source, Arc::clone(&journal), &stop).await?;
            verify(plan, &source, &snowflake, Arc::clone(&journal), &stop).await?;
            Ok::<_, anyhow::Error>(copy_rate.rate_pass && copy_rate.copy_overlap && cdc_rate)
        };
        tokio::pin!(execution);
        let deadline = tokio::time::sleep(Duration::from_secs(plan.deadline_seconds));
        tokio::pin!(deadline);
        let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
        let mut grace = None;
        let shutdown_signal = shutdown();
        tokio::pin!(shutdown_signal);
        loop {
            tokio::select! {
                result=&mut execution=>break result,
                signal=&mut shutdown_signal,if !interrupted=>{
                    if signal.is_err(){break Err(anyhow::anyhow!("Shutdown signal registration failed"));}
                    interrupted=true;stop.cancel();grace=Some(Instant::now()+Duration::from_secs(180));
                }
                _=&mut deadline,if !interrupted=>{interrupted=true;stop.cancel();grace=Some(Instant::now()+Duration::from_secs(180));}
                _=heartbeat.tick()=>{
                    if let Err(error)=source.heartbeat().await{break Err(error);}
                    let control=match source.control().await{Ok(control)=>control,Err(error)=>break Err(error)};
                    if observer_started.get() && control["observer_age_seconds"].as_f64().is_none_or(|age| age > 120.0) {
                        break Err(anyhow::anyhow!("Independent observer heartbeat lost"));
                    }
                    if control["stop_requested"]==true&&!interrupted{interrupted=true;stop.cancel();grace=Some(Instant::now()+Duration::from_secs(180));}
                    if grace.is_some_and(|deadline|Instant::now()>=deadline){break Err(anyhow::anyhow!("Cancellation grace deadline exceeded"));}
                }
            }
        }
    };
    // Every workload task has completed or been aborted on failure before
    // stopping replication.
    let control = source.control().await;
    let pipeline =
        control.as_ref().ok().and_then(|c| c["pipeline_id"].as_u64()).or(pipeline_known.get());
    let stopped = match pipeline {
        Some(id) => platform.stop(id).await.is_ok(),
        None => control.is_ok(),
    };
    // Capture evidence first, then remove the owned pipeline so its slots
    // cannot retain WAL indefinitely.
    let reclaimed = if stopped {
        match pipeline {
            Some(id) => {
                platform.request(Method::DELETE, &format!("/pipelines/{id}"), None).await.is_ok()
                    && source.slots_reclaimed(id).await.is_ok()
            }
            // An unknown create outcome needs inventory reconciliation, even if
            // no resource ID reached the control record.
            None => false,
        }
    } else {
        false
    };
    let phase_saved =
        source.phase(if reclaimed { "finished" } else { "teardown_failed" }).await.is_ok();
    let initial_sync_observed_from_start = matches!(plan.launch, Launch::Platform);
    let verdict = if interrupted {
        "interrupted"
    } else {
        match &result {
            Ok(true) if initial_sync_observed_from_start => "passed",
            Ok(true) => "inconclusive",
            Ok(false) => "inconclusive",
            Err(_) => "failed",
        }
    };
    let summary = json!({"component":"runner","run_id":plan.run_id,"verdict":verdict,"pipeline_id":pipeline,"stop_confirmed":stopped,"pipeline_reclaimed":reclaimed,"phase_saved":phase_saved,"requires_observer_report":true,"behavior_checks_passed":matches!(result,Ok(true)),"initial_sync_observed_from_start":initial_sync_observed_from_start,"error":result.as_ref().err().map(ToString::to_string),"cleanup":"Source and Snowflake schemas retained for inspection. Pipeline deletion and slot reclamation are reported separately."});
    journal.lock().await.record("finished", summary.clone())?;
    journal.lock().await.save("summary.json", &summary)?;
    ensure!(
        stopped && reclaimed,
        "Pipeline reclamation is unconfirmed; inspect it before proceeding"
    );
    result?;
    ensure!(!interrupted, "Experiment interrupted");
    Ok(())
}

/// Checks the run-specific Snowflake namespace without inspecting credentials.
fn destination_matches(plan: &Plan, destination: &serde_json::Value) -> bool {
    let config = &destination["config"]["snowflake"];
    config["schema"] == plan.schema()
        && config["database"] == plan.snowflake.database
        && config["account_id"] == plan.snowflake.account
}

/// Reconciles destination creation by the unique fixture namespace.
async fn find_destination(plan: &Plan, platform: &Platform) -> Result<Option<u64>> {
    let inventory = platform.request(Method::GET, "/destinations", None).await?;
    let matches = inventory["destinations"]
        .as_array()
        .context("Missing destination inventory")?
        .iter()
        .filter(|d| destination_matches(plan, d))
        .collect::<Vec<_>>();
    ensure!(
        matches.len() <= 1,
        "Multiple destinations reference this fixture; reconcile them manually"
    );
    matches.first().map(|destination| id(destination)).transpose()
}

/// Deletes only a run whose source marker and pipeline publication prove
/// ownership.
pub(crate) async fn cleanup(plan: &Plan, drop_data: bool, abandoned: bool) -> Result<()> {
    let source = Source::connect(plan).await?;
    let platform = Platform::from_env(plan)?;
    let control = source.control().await?;
    ensure!(control["fixture_hash"] == plan.fixture_hash()?, "Cleanup fixture identity mismatch");
    ensure!(
        abandoned
            || control["phase"] == "finished"
            || control["phase"] == "prepared"
            || control["stop_requested"] == true,
        "Run is active; stop it before cleanup"
    );
    // Reconcile inventories even when a create response or persistence failed.
    // Never infer absence from an empty local control record.
    let inventory = platform.request(Method::GET, "/pipelines", None).await?;
    let owned = inventory["pipelines"]
        .as_array()
        .context("Missing pipeline inventory")?
        .iter()
        .filter(|p| p["config"]["publication_name"] == plan.publication())
        .collect::<Vec<_>>();
    ensure!(owned.len() <= 1, "Multiple pipelines reference this fixture; reconcile them manually");
    let pipeline = match owned.first() {
        Some(details) => {
            ensure!(
                details["source_id"] == control["source_id"],
                "Cleanup source ownership mismatch"
            );
            Some(id(details)?)
        }
        None => control["pipeline_id"].as_u64(),
    };
    if let Some(pipeline) = pipeline {
        let details = platform.pipeline(pipeline).await?;
        if !details.is_null() {
            ensure!(
                details["config"]["publication_name"] == plan.publication(),
                "Cleanup pipeline ownership mismatch"
            );
            platform.reclaim(pipeline).await?;
        }
        source.slots_reclaimed(pipeline).await?;
    }
    let destination =
        find_destination(plan, &platform).await?.or(control["destination_id"].as_u64());
    if let Some(destination) = destination {
        let target =
            platform.request(Method::GET, &format!("/destinations/{destination}"), None).await?;
        if !target.is_null() {
            ensure!(destination_matches(plan, &target), "Cleanup destination ownership mismatch");
            platform.request(Method::DELETE, &format!("/destinations/{destination}"), None).await?;
        }
    }
    if drop_data {
        let snowflake = Snowflake::new(plan)?;
        ensure!(
            control["snowflake_created"] == true || !snowflake.schema_exists().await?,
            "Snowflake schema creation was not confirmed; verify ownership and remove that schema \
             manually before retrying cleanup"
        );
        if control["snowflake_created"] == true {
            Snowflake::new(plan)?
                .query(&format!(
                    "drop schema if exists \"{}\".\"{}\"",
                    plan.snowflake.database,
                    plan.schema()
                ))
                .await?;
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "drop publication if exists {}",
            plan.publication()
        )))
        .execute(&source.pool)
        .await
        .map_err(db_error)?;
        sqlx::query(sqlx::AssertSqlSafe(format!("drop schema {} cascade", source.schema)))
            .execute(&source.pool)
            .await
            .map_err(db_error)?;
    }
    Ok(())
}

/// Handles both terminal interruption and Kubernetes termination requests.
async fn shutdown() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {result=tokio::signal::ctrl_c()=>result?,_=terminate.recv()=>{}}
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

/// Checks explicitly requested fields while allowing server-supplied defaults
/// in nested objects.
fn config_matches(requested: &serde_json::Value, actual: &serde_json::Value) -> bool {
    if let Some(object) = requested.as_object() {
        actual.is_object() && object.iter().all(|(key, value)| config_matches(value, &actual[key]))
    } else {
        requested == actual
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::run::config_matches;

    #[test]
    fn compares_requested_nested_options_without_rejecting_server_defaults() {
        assert!(config_matches(
            &json!({"max_fill_ms":1000}),
            &json!({"max_fill_ms":1000,"max_bytes":33554432})
        ));
        assert!(!config_matches(&json!({"max_fill_ms":1000}), &json!({"max_fill_ms":5000})));
        assert!(!config_matches(&json!(null), &json!({"enabled":true})));
    }
}
