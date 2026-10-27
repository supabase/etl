//! Dynamic SQL interpolates only UUID-derived namespaces and generated table
//! names; values use bind parameters. Independent staging observer and
//! runner-loss watchdog.

use std::{path::Path, time::Duration};

use anyhow::{Context, Result, ensure};
use k8s_openapi::api::core::v1::Pod;
use kube::{Api, Client};
use serde_json::{Value, json};

use crate::{journal::Journal, plan::Plan, platform::Platform, source::Source};

/// Reads only the named experiment replicator, retaining resource limits and
/// runtime image identity.
async fn pod(client: &Client, project: &str, replicator: u64) -> Result<Value> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), "etl-data-plane");
    let name = format!("{project}-{replicator}-0");
    let Some(pod) = pods
        .get_opt(&name)
        .await
        .map_err(|_| anyhow::anyhow!("Cannot read experiment replicator pod"))?
    else {
        return Ok(json!({"present":false}));
    };
    let status = pod.status.context("Replicator pod has no status")?;
    let containers=status.container_statuses.unwrap_or_default().iter().map(|c|json!({"name":c.name,"image":c.image,"image_id":c.image_id,"ready":c.ready,"restart_count":c.restart_count,"exit_code":c.state.as_ref().and_then(|s|s.terminated.as_ref()).map(|t|t.exit_code)})).collect::<Vec<_>>();
    let node = pod.spec.as_ref().and_then(|s| s.node_name.clone());
    let resources = pod.spec.map(|s| {
        s.containers
            .into_iter()
            .map(|c| json!({"name":c.name,"resources":c.resources}))
            .collect::<Vec<_>>()
    });
    Ok(
        json!({"present":true,"name":name,"phase":status.phase,"containers":containers,"resources":resources,"node":node}),
    )
}

/// Queries pipeline-scoped metric samples. The configured endpoint is never
/// written to artifacts.
async fn metrics(http: &reqwest::Client, base: &str, pipeline: u64) -> Result<Value> {
    let query = format!(
        "{{pipeline_id=\"{pipeline}\",__name__=~\"etl_(bytes_.*|events_.*|table_copy_.*\
         |destination_.*|snowflake_.*|replication_.*|.*memory.*)\"}}"
    );
    let response = http
        .get(format!("{}/api/v1/query", base.trim_end_matches('/')))
        .query(&[("query", query)])
        .send()
        .await
        .map_err(|_| anyhow::anyhow!("Metrics request failed"))?;
    ensure!(
        response.status().is_success(),
        "Metrics endpoint returned HTTP {}",
        response.status().as_u16()
    );
    let value: Value =
        response.json().await.map_err(|_| anyhow::anyhow!("Invalid metrics response"))?;
    ensure!(value["status"] == "success", "Metrics query failed");
    Ok(value["data"].clone())
}

/// Stops the owned pipeline if the runner disappears or a WAL budget is
/// exceeded.
///
/// Failure to reach Platform is recorded as unconfirmed teardown, never
/// success.
pub(crate) async fn observe(plan: &Plan, output: &Path) -> Result<()> {
    let source = Source::connect(plan).await?;
    let platform = Platform::from_env(plan)?;
    let client = Client::try_default()
        .await
        .map_err(|_| anyhow::anyhow!("Observer needs staging Kubernetes credentials"))?;
    let metrics_base = std::env::var("EXPERIMENT_METRICS_URL")
        .context("EXPERIMENT_METRICS_URL is required for staging observations")?;
    let http = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
    let mut journal = Journal::create(output, plan.max_artifact_bytes)?;
    journal.save("plan.json", &serde_json::to_value(plan)?)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(plan.deadline_seconds + 300);
    let mut failures = 0_u64;
    let mut samples = 0_u64;
    let mut pod_seen = false;
    let mut metrics_seen = false;
    let mut stop_reason = None;
    let mut stopped = None;
    let mut reclaimed = None;
    let mut known_pipeline = None;
    let mut last_control = tokio::time::Instant::now();
    loop {
        if tokio::time::Instant::now() >= deadline {
            stop_reason = Some("observer_deadline");
            if let Some(pipeline) = known_pipeline {
                reclaimed = Some(
                    platform.reclaim(pipeline).await.is_ok()
                        && source.slots_reclaimed(pipeline).await.is_ok(),
                );
            }
            break;
        }
        let Ok(control) = source.control().await else {
            failures += 1;
            if last_control.elapsed() > Duration::from_secs(120) {
                stop_reason = Some("source_control_unavailable");
                if let Some(pipeline) = known_pipeline {
                    reclaimed = Some(
                        platform.reclaim(pipeline).await.is_ok()
                            && source.slots_reclaimed(pipeline).await.is_ok(),
                    );
                }
                break;
            }
            journal.record(
                "observation_gap",
                json!({"component":"source_control","count":failures}),
            )?;
            tokio::time::sleep(Duration::from_secs(plan.sample_seconds)).await;
            continue;
        };
        last_control = tokio::time::Instant::now();
        ensure!(
            control["fixture_hash"] == plan.fixture_hash()?,
            "Observer fixture identity mismatch"
        );
        if source.observer_heartbeat().await.is_err() {
            stop_reason = Some("observer_heartbeat_failed");
            if let Some(pipeline) = known_pipeline {
                reclaimed = Some(
                    platform.reclaim(pipeline).await.is_ok()
                        && source.slots_reclaimed(pipeline).await.is_ok(),
                );
            }
            break;
        }
        if control["phase"] == "finished" {
            break;
        }
        if control["phase"] == "teardown_failed" && control["pipeline_id"].is_null() {
            stop_reason = Some("unresolved_resource_creation");
            break;
        }
        if let Some(pipeline) = control["pipeline_id"].as_u64() {
            // Validate the pipeline before observing or stopping anything by
            // ID.
            let details = platform.pipeline(pipeline).await?;
            if details.is_null() {
                reclaimed = Some(source.slots_reclaimed(pipeline).await.is_ok());
                break;
            }
            ensure!(
                details["config"]["publication_name"] == plan.publication(),
                "Observer pipeline ownership mismatch"
            );
            known_pipeline = Some(pipeline);
            let runtime = async {
                match details["replicator_id"].as_u64() {
                    Some(id) => tokio::time::timeout(
                        Duration::from_secs(30),
                        pod(&client, &plan.project_ref, id),
                    )
                    .await
                    .context("Pod observation timed out")?,
                    None => Err(anyhow::anyhow!("Missing replicator ID")),
                }
            };
            let collection_started_at = chrono::Utc::now();
            let (replication, sample, runtime, metric) = tokio::join!(
                platform.replication(pipeline),
                source.sample(pipeline),
                runtime,
                metrics(&http, &metrics_base, pipeline)
            );
            let collection_finished_at = chrono::Utc::now();
            if sample.is_err() {
                stop_reason = Some("source_observation_failed");
            }
            if let Ok(sample) = &sample {
                if sample["generated_wal_bytes"]
                    .as_u64()
                    .is_some_and(|n| n > plan.max_generated_wal_bytes)
                {
                    stop_reason = Some("generated_wal_budget");
                }
                if sample["slots"].as_array().is_some_and(|slots| {
                    slots.iter().any(|s| {
                        s["retained_bytes"]
                            .as_u64()
                            .is_some_and(|n| n > plan.max_retained_wal_bytes)
                            || s["wal_status"] == "lost"
                    })
                }) {
                    stop_reason = Some("retained_wal_budget_or_lost_slot");
                }
            }
            if let Ok(runtime) = &runtime {
                pod_seen |= runtime["containers"].as_array().is_some_and(|a| {
                    a.iter().any(|c| c["image_id"].as_str().is_some_and(|s| !s.is_empty()))
                });
            }
            if let Ok(metric) = &metric {
                metrics_seen |= metric["result"].as_array().is_some_and(|a| !a.is_empty());
            }
            let complete =
                sample.is_ok() && replication.is_ok() && runtime.is_ok() && metric.is_ok();
            if !complete {
                failures += 1;
                journal.record("observation_gap",json!({"count":failures,"source":sample.is_ok(),"platform":replication.is_ok(),"pod":runtime.is_ok(),"metrics":metric.is_ok()}))?;
            }
            journal.record("observation",json!({"collection_started_at":collection_started_at,"collection_finished_at":collection_finished_at,"source":sample.ok(),"pipeline":replication.ok(),"runtime":runtime.ok(),"metrics":metric.ok()}))?;
            samples += 1;
            if control["phase"] == "teardown_failed" {
                stop_reason = Some("runner_teardown_failed");
            }
            if control["heartbeat_age_seconds"].as_f64().is_some_and(|age| age > 120.0) {
                stop_reason = Some("runner_heartbeat_lost");
            }
            if let Some(reason) = stop_reason {
                // Platform reclamation remains possible when source writes
                // fail.
                let stop_requested = source.request_stop().await.is_ok();
                stopped = Some(platform.stop(pipeline).await.is_ok());
                reclaimed = Some(if stopped == Some(true) {
                    platform
                        .request(reqwest::Method::DELETE, &format!("/pipelines/{pipeline}"), None)
                        .await
                        .is_ok()
                        && source.slots_reclaimed(pipeline).await.is_ok()
                } else {
                    false
                });
                journal
                    .record("watchdog_stop", json!({"reason":reason,"source_stop_requested":stop_requested,"stop_confirmed":stopped,"pipeline_reclaimed":reclaimed}))?;
                break;
            }
        }
        tokio::time::sleep(Duration::from_secs(plan.sample_seconds)).await;
    }
    let complete =
        failures == 0 && samples >= 2 && pod_seen && metrics_seen && stop_reason.is_none();
    journal.save("summary.json",&json!({"component":"observer","run_id":plan.run_id,"coverage_complete":complete,"samples":samples,"observation_gaps":failures,"runtime_image_observed":pod_seen,"metrics_observed":metrics_seen,"stop_reason":stop_reason,"stop_confirmed":stopped,"pipeline_reclaimed":reclaimed}))?;
    Ok(())
}
