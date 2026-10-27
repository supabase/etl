//! Durable, bounded artifacts and an offline HTML report.

use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use anyhow::{Result, ensure};
use chrono::Utc;
use serde_json::{Value, json};

/// Append-only event stream owned by one process, with a separate artifact
/// directory per observer.
pub(crate) struct Journal {
    /// Owned output directory.
    root: PathBuf,
    /// Exclusive append-only journal handle.
    file: File,
    /// Bytes charged to the journal budget.
    bytes: u64,
    /// Maximum journal size in bytes.
    limit: u64,
}

impl Journal {
    /// Refuses to overwrite evidence from an earlier process or duplicate
    /// execution.
    pub(crate) fn create(root: &Path, limit: u64) -> Result<Self> {
        std::fs::create_dir_all(root)?;
        let file =
            OpenOptions::new().write(true).create_new(true).open(root.join("events.jsonl"))?;
        Ok(Self { root: root.to_owned(), file, bytes: 0, limit })
    }

    /// Flushes each record so a killed pod leaves a useful prefix of evidence.
    pub(crate) fn record(&mut self, kind: &str, data: Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(&json!({"at":Utc::now(),"kind":kind,"data":data}))?;
        bytes.push(b'\n');
        self.bytes += u64::try_from(bytes.len())?;
        ensure!(self.bytes <= self.limit, "Artifact byte budget exceeded");
        self.file.write_all(&bytes)?;
        self.file.sync_data()?;
        println!("{} {kind}", Utc::now().to_rfc3339());
        Ok(())
    }

    /// Atomically replaces a small summary without rewriting the event journal.
    pub(crate) fn save(&self, name: &str, value: &Value) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(value)?;
        ensure!(u64::try_from(bytes.len())? <= self.limit / 4, "Summary exceeds artifact budget");
        let tmp = self.root.join(format!("{name}.tmp"));
        std::fs::write(&tmp, bytes)?;
        std::fs::rename(tmp, self.root.join(name))?;
        Ok(())
    }
}

/// Escapes report strings before inserting them into HTML, including embedded
/// script terminators.
fn html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('\"', "&quot;")
        .replace('\'', "&#39;")
}

/// Requires a usable WAL and metric sample inside every committed load stage.
/// Sparse sampling cannot qualify an unobserved load, even when the run's final
/// contents are correct.
fn load_coverage(events: &[Value]) -> Vec<Value> {
    let time = |event: &Value| {
        event["at"].as_str().and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
    };
    events
        .iter()
        .filter(|event| event["kind"] == "load_started")
        .map(|start| {
            let end = events.iter().find(|event| {
                event["kind"] == "load_complete" && event["data"]["name"] == start["data"]["name"]
            });
            let covered = match (time(start), end.and_then(time)) {
                (Some(start), Some(end)) => events.iter().any(|event| {
                    event["kind"] == "observation"
                        && event["data"]["collection_started_at"]
                            .as_str()
                            .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                            .is_some_and(|at| at >= start)
                        && event["data"]["collection_finished_at"]
                            .as_str()
                            .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                            .is_some_and(|at| at <= end)
                        && event["data"]["source"]["slots"].as_array().is_some_and(|slots| {
                            slots.iter().any(|slot| {
                                slot["slot_name"]
                                    .as_str()
                                    .is_some_and(|name| name.starts_with("supabase_etl_apply_"))
                                    && slot["restart_lsn"].is_string()
                                    && slot["confirmed_flush_lsn"].is_string()
                            })
                        })
                        && event["data"]["metrics"]["series_count"]
                            .as_u64()
                            .is_some_and(|count| count > 0)
                }),
                _ => false,
            };
            json!({"name":start["data"]["name"],"observed_during_load":covered})
        })
        .collect()
}

/// Combines runner and observer streams without contacting staging or a
/// destination.
pub(crate) fn report(directories: &[PathBuf], output: &Path) -> Result<()> {
    let mut events = Vec::<Value>::new();
    let mut summaries = Vec::<Value>::new();
    let mut damaged = false;
    for directory in directories {
        let file = File::open(directory.join("events.jsonl"))?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            match serde_json::from_str::<Value>(&line) {
                Ok(mut event) => {
                    if !event.is_object() || !event["data"].is_object() {
                        damaged = true;
                        events.push(json!({"kind":"incomplete_record","data":{"reason":"Invalid event structure"}}));
                        continue;
                    }
                    if event["kind"] == "observation" {
                        let data = &mut event["data"];
                        if !data["source"].is_object() {
                            data["source"] = json!({});
                        }
                        let slots = data["source"]["slots"].as_array().cloned().unwrap_or_default();
                        data["source"]["slot_count"] = slots.len().into();
                        data["source"]["slots"] = slots
                            .into_iter()
                            .filter(|s| {
                                s["slot_name"]
                                    .as_str()
                                    .is_some_and(|n| n.starts_with("supabase_etl_apply_"))
                            })
                            .collect();
                        let metric_count = data["metrics"]["result"].as_array().map_or(0, Vec::len);
                        data["metrics"] = json!({"series_count":metric_count,"detail":"Full metric samples are in the observer events.jsonl"});
                    }
                    events.push(event);
                }
                Err(_) => {
                    damaged = true;
                    events.push(json!({"kind":"incomplete_record","data":{"directory":directory.display().to_string()}}));
                }
            }
        }
        if let Ok(bytes) = std::fs::read(directory.join("summary.json")) {
            summaries.push(serde_json::from_slice(&bytes)?);
        }
    }
    let coverage = load_coverage(&events);
    let all_loads_observed =
        !coverage.is_empty() && coverage.iter().all(|load| load["observed_during_load"] == true);
    let verdict = verdict(&summaries, damaged || !all_loads_observed)?;
    std::fs::write(
        output.with_extension("json"),
        serde_json::to_vec_pretty(
            &json!({"verdict":verdict,"summaries":summaries,"damaged_journal":damaged,"load_observation_coverage":coverage}),
        )?,
    )?;
    events.sort_by(|a, b| a["at"].as_str().cmp(&b["at"].as_str()));
    let mut body = String::from(
        "<!doctype html><html lang=\"en\"><meta charset=\"utf-8\"><meta name=\"viewport\" \
         content=\"width=device-width\"><title>Snowflake experiment</title><style>body{font:16px \
         system-ui;margin:40px auto;max-width:1200px;padding:0 \
         20px;color:#172b3a;background:#f6f8fa}pre{white-space:pre-wrap;overflow-wrap:\
         anywhere}table{border-collapse:collapse;width:100%}td,th{text-align:left;border-bottom:\
         1px solid #d8e1e8;padding:8px;vertical-align:top}.card{background:white;padding:20px;\
         border-radius:8px;margin:16px \
         0}svg{width:100%;height:220px}small{color:#526779}</style><h1>Snowflake \
         experiment</h1><p>Observed behavior and explicit correctness assertions. Missing \
         observations are not a pass.</p>",
    );
    body.push_str(&format!(
        "<div class=\"card\"><h2>{}</h2><p>Pass requires the runner assertions, achieved load, \
         complete observer evidence, and confirmed pipeline reclamation.</p></div>",
        verdict.to_uppercase()
    ));
    let verified = events.iter().filter(|e| e["kind"] == "table_verified").count();
    let rows = events
        .iter()
        .filter(|e| e["kind"] == "table_verified")
        .filter_map(|e| e["data"]["rows"].as_u64())
        .sum::<u64>();
    body.push_str(&format!(
        "<div class=\"card\"><h2>Correctness</h2><p><strong>{verified}</strong> tables verified · \
         <strong>{rows}</strong> complete rows compared, including large text and JSON null \
         semantics.</p></div>"
    ));
    body.push_str(
        "<div class=\"card\"><h2>Source load</h2><table><tr><th>Stage</th><th>Committed \
         operations</th><th>Target ops/s</th><th>Actual ops/s</th><th>Seconds</th><th>Rate \
         met</th></tr>",
    );
    for event in events.iter().filter(|e| e["kind"] == "load_complete") {
        let d = &event["data"];
        body.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.1}</td><td>{:.1}</td><td>{}</td></tr>",
            html(d["name"].as_str().unwrap_or("unknown")),
            d["operations"],
            d["requested_operations_per_second"],
            d["achieved_operations_per_second"].as_f64().unwrap_or(0.0),
            d["seconds"].as_f64().unwrap_or(0.0),
            d["rate_pass"]
        ));
    }
    body.push_str("</table></div>");
    body.push_str(&format!(
        "<div class=\"card\"><h2>Load observation coverage</h2><pre>{}</pre><p>Each load needs a \
         source WAL and metrics sample within its start/end boundaries.</p></div>",
        html(&serde_json::to_string_pretty(&coverage)?)
    ));
    for summary in &summaries {
        body.push_str(&format!(
            "<div class=\"card\"><h2>Run outcome</h2><pre>{}</pre></div>",
            html(&serde_json::to_string_pretty(summary)?)
        ));
    }
    for (title, key) in [
        ("Retained WAL (bytes)", "retained_bytes"),
        ("Acknowledgement lag (bytes)", "ack_lag_bytes"),
    ] {
        let series: Vec<(String, f64)> = events
            .iter()
            .filter(|e| e["kind"] == "observation")
            .filter_map(|e| {
                let slots = e["data"]["source"]["slots"].as_array()?;
                let apply = slots.iter().find(|s| {
                    s["slot_name"].as_str().is_some_and(|n| n.starts_with("supabase_etl_apply_"))
                })?;
                Some((e["at"].as_str()?.to_owned(), apply[key].as_f64()?))
            })
            .collect();
        if series.len() > 1 {
            let max = series.iter().map(|(_, v)| *v).fold(1.0_f64, f64::max);
            let points = series
                .iter()
                .enumerate()
                .map(|(i, (_, v))| {
                    format!(
                        "{},{}",
                        30.0 + 940.0 * i as f64 / (series.len() - 1) as f64,
                        190.0 - 170.0 * v / max
                    )
                })
                .collect::<Vec<_>>()
                .join(" ");
            body.push_str(&format!(
                "<div class=\"card\"><h2>{title}</h2><small>Peak: {max:.0}; samples from {} to \
                 {}. Sample-index spacing; gaps are listed below.</small><svg viewBox=\"0 0 1000 \
                 220\" role=\"img\" aria-label=\"{title}\"><polyline points=\"{points}\" \
                 fill=\"none\" stroke=\"#167a65\" stroke-width=\"2\"/></svg></div>",
                html(&series[0].0),
                html(&series[series.len() - 1].0)
            ));
        }
    }
    body.push_str(
        "<div class=\"card\"><h2>Timeline and evidence</h2><p>Expand an event for measurements, \
         load rates, configuration, or assertion \
         results.</p><table><tr><th>UTC</th><th>Event</th><th>Evidence</th></tr>",
    );
    for event in events {
        body.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td><details><summary>Details</summary><pre>{}</pre></\
             details></td></tr>",
            html(event["at"].as_str().unwrap_or("unknown")),
            html(event["kind"].as_str().unwrap_or("unknown")),
            html(&serde_json::to_string_pretty(&event["data"])?)
        ));
    }
    body.push_str("</table></div></html>");
    std::fs::write(output, body)?;
    Ok(())
}

/// Combines behavior and evidence coverage without allowing a runner-only
/// success to become a pass.
fn verdict(summaries: &[Value], damaged: bool) -> Result<&'static str> {
    let runs: std::collections::BTreeSet<_> =
        summaries.iter().filter_map(|s| s["run_id"].as_str()).collect();
    ensure!(runs.len() <= 1, "Artifacts belong to different experiment runs");
    let runners = summaries.iter().filter(|s| s["component"] == "runner").collect::<Vec<_>>();
    let observers = summaries.iter().filter(|s| s["component"] == "observer").collect::<Vec<_>>();
    ensure!(runners.len() <= 1 && observers.len() <= 1, "Duplicate runner or observer summaries");
    let Some(runner) = runners.first() else {
        return Ok("inconclusive");
    };
    if runner["verdict"] == "failed" {
        return Ok("failed");
    }
    if runner["verdict"] == "interrupted" {
        return Ok("interrupted");
    }
    if !damaged
        && runner["verdict"] == "passed"
        && runner["stop_confirmed"] == true
        && runner["pipeline_reclaimed"] == true
        && observers.first().is_some_and(|s| s["coverage_complete"] == true)
    {
        return Ok("passed");
    }
    Ok("inconclusive")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::journal::{Journal, html, load_coverage, report, verdict};

    #[test]
    fn observations_outside_the_load_cannot_qualify_it() {
        let mut events = vec![
            json!({"at":"2026-01-01T00:00:00Z","kind":"load_started","data":{"name":"steady"}}),
            json!({"at":"2026-01-01T00:00:10Z","kind":"load_complete","data":{"name":"steady"}}),
            json!({"at":"2026-01-01T00:00:20Z","kind":"observation","data":{"collection_started_at":"2026-01-01T00:00:20Z","collection_finished_at":"2026-01-01T00:00:21Z","source":{"slots":[{"slot_name":"supabase_etl_apply_1","restart_lsn":"0/1234","confirmed_flush_lsn":"0/2345"}]},"metrics":{"series_count":1}}}),
        ];
        assert_eq!(load_coverage(&events)[0]["observed_during_load"], false);
        events[2]["at"] = "2026-01-01T00:00:05Z".into();
        events[2]["data"]["collection_started_at"] = "2026-01-01T00:00:04Z".into();
        events[2]["data"]["collection_finished_at"] = "2026-01-01T00:00:05Z".into();
        assert_eq!(load_coverage(&events)[0]["observed_during_load"], true);
        events[2]["data"]["collection_started_at"] = "2025-12-31T23:59:59Z".into();
        assert_eq!(load_coverage(&events)[0]["observed_during_load"], false);
        events[2]["data"]["collection_started_at"] = "2026-01-01T00:00:04Z".into();
        events[2]["data"]["metrics"]["series_count"] = 0.into();
        assert_eq!(load_coverage(&events)[0]["observed_during_load"], false);
    }

    #[test]
    fn preserves_previous_evidence_and_escapes_untrusted_text() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::create(dir.path(), 1_048_576).unwrap();
        journal.record("assertion", json!({"message":"<script>alert(1)</script>"})).unwrap();
        assert!(Journal::create(dir.path(), 1_048_576).is_err());
        let output = dir.path().join("report.html");
        report(&[dir.path().to_owned()], &output).unwrap();
        let report = std::fs::read_to_string(output).unwrap();
        assert!(!report.contains("<script>"));
        assert!(report.contains("INCONCLUSIVE"));
        assert_eq!(html("<&"), "&lt;&amp;");
    }
    #[test]
    fn pass_requires_complete_matching_observer_and_teardown() {
        let runner = json!({"component":"runner","run_id":"one","verdict":"passed","stop_confirmed":true,"pipeline_reclaimed":true});
        assert_eq!(verdict(std::slice::from_ref(&runner), false).unwrap(), "inconclusive");
        let observer = json!({"component":"observer","run_id":"one","coverage_complete":true});
        assert_eq!(verdict(&[runner.clone(), observer.clone()], false).unwrap(), "passed");
        assert_eq!(verdict(&[runner.clone(), observer], true).unwrap(), "inconclusive");
        assert!(verdict(&[runner, json!({"component":"observer","run_id":"two"})], false).is_err());
    }
}
