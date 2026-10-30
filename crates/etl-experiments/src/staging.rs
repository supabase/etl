//! Offline staging Job manifests with bounded lifetimes and persistent
//! evidence.

use std::path::Path;

use anyhow::{Result, ensure};
use clap::ValueEnum;
use serde_json::{Value, json};

use crate::plan::Plan;

/// Jobs are submitted deliberately in two stages so Studio setup can happen
/// between them.
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum Phase {
    /// Seed the source and create a fresh destination schema.
    Prepare,
    /// Start independent observation and the workload runner.
    Run,
    /// Remove retained fixture resources after evidence export.
    Cleanup,
}

/// Pod-safe deterministic resource prefix; the complete UUID stays in the plan
/// and source record.
fn prefix(plan: &Plan) -> String {
    format!("experiment-{}", plan.run_id.simple())
}

/// Creates one named Job. Kubernetes retries cannot replay a claimed source
/// run.
fn job(plan: &Plan, image: &str, mode: &str, pvc: &str) -> Value {
    let prefix = prefix(plan);
    json!({
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": format!("{prefix}-{mode}"),
            "namespace": "etl-testing-plane",
            "labels": {
                "app.kubernetes.io/name": "etl-experiments"
            }
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": plan.deadline_seconds+600,
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": "etl-experiments"
                    }
                },
                "spec": {
                    "serviceAccountName": "etl-experiments",
                    "automountServiceAccountToken": mode=="observe",
                    "restartPolicy": "Never",
                    "terminationGracePeriodSeconds": 240,
                    "securityContext": {
                        "runAsNonRoot": true,
                        "runAsUser": 65532,
                        "runAsGroup": 65532,
                        "fsGroup": 65532
                    },
                    "nodeSelector": {
                        "etl.supabase.com/node-role": "workloads"
                    },
                    "tolerations": [
                        {
                            "key": "etl.supabase.com/node-role",
                            "operator": "Equal",
                            "value": "workloads",
                            "effect": "NoSchedule"
                        }
                    ],
                    "containers": [
                        {
                            "name": "experiment",
                            "image": image,
                            "args": [
                                mode,
                                "--plan",
                                "/plan/plan.json",
                                "--output",
                                format!("/artifacts/{mode}")
                            ],
                            "resources": {
                                "requests": {
                                    "cpu": "1",
                                    "memory": "1Gi"
                                },
                                "limits": {
                                    "cpu": "2",
                                    "memory": "2Gi"
                                }
                            },
                            "securityContext": {
                                "allowPrivilegeEscalation": false,
                                "readOnlyRootFilesystem": true,
                                "capabilities": {
                                    "drop": [
                                        "ALL"
                                    ]
                                }
                            },
                            "env": [
                                {
                                    "name": "EXPERIMENT_RUNNER_IMAGE",
                                    "value": image
                                },
                                {
                                    "name": "EXPERIMENT_STAGING_PROJECT_REF",
                                    "value": plan.project_ref
                                },
                                {
                                    "name": "EXPERIMENT_PLATFORM_URL",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "etl-experiments-credentials",
                                            "key": "platform-url"
                                        }
                                    }
                                },
                                {
                                    "name": "EXPERIMENT_AUTH_URL",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "etl-experiments-credentials",
                                            "key": "auth-url"
                                        }
                                    }
                                },
                                {
                                    "name": "EXPERIMENT_AUTH_API_KEY",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "etl-experiments-credentials",
                                            "key": "auth-api-key"
                                        }
                                    }
                                },
                                {
                                    "name": "EXPERIMENT_PLATFORM_EMAIL",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "etl-experiments-credentials",
                                            "key": "platform-email"
                                        }
                                    }
                                },
                                {
                                    "name": "EXPERIMENT_PLATFORM_PASSWORD",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "etl-experiments-credentials",
                                            "key": "platform-password"
                                        }
                                    }
                                },
                                {
                                    "name": "EXPERIMENT_SOURCE_URL",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": "etl-experiments-credentials",
                                            "key": "source-url"
                                        }
                                    }
                                },
                                {
                                    "name": "EXPERIMENT_METRICS_URL",
                                    "valueFrom": {
                                        "configMapKeyRef": {
                                            "name": "etl-experiments-settings",
                                            "key": "metrics-url"
                                        }
                                    }
                                },
                                {
                                    "name": "EXPERIMENT_SNOWFLAKE_PRIVATE_KEY_FILE",
                                    "value": "/credentials/snowflake-private-key.pem"
                                }
                            ],
                            "volumeMounts": [
                                {
                                    "name": "plan",
                                    "mountPath": "/plan",
                                    "readOnly": true
                                },
                                {
                                    "name": "artifacts",
                                    "mountPath": "/artifacts"
                                },
                                {
                                    "name": "credentials",
                                    "mountPath": "/credentials",
                                    "readOnly": true
                                }
                            ]
                        }
                    ],
                    "volumes": [
                        {
                            "name": "plan",
                            "configMap": {
                                "name": prefix
                            }
                        },
                        {
                            "name": "artifacts",
                            "persistentVolumeClaim": {
                                "claimName": pvc
                            }
                        },
                        {
                            "name": "credentials",
                            "secret": {
                                "secretName": "etl-experiments-credentials",
                                "items": [
                                    {
                                        "key": "snowflake-private-key.pem",
                                        "path": "snowflake-private-key.pem"
                                    },
                                    {
                                        "key": "source-ca.pem",
                                        "path": "source-ca.pem"
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
    })
}

/// Renders a Kubernetes List for explicit review and application to etl-staging
/// only.
pub(crate) fn render(plan: &Plan, image: &str, output: &Path, phase: Phase) -> Result<()> {
    ensure!(
        image.contains("@sha256:")
            && image
                .rsplit_once("@sha256:")
                .is_some_and(|(_, digest)| digest.len() == 64
                    && digest.bytes().all(|b| b.is_ascii_hexdigit())),
        "Pin the experiment runner image by SHA-256 digest"
    );
    let prefix = prefix(plan);
    let mut items = Vec::new();
    items.push(json!({"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":prefix,"namespace":"etl-testing-plane"},"data":{"plan.json":serde_json::to_string_pretty(plan)?}}));
    match phase {
        Phase::Prepare => {
            for suffix in ["runner", "observer"] {
                items.push(json!({"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"name":format!("{prefix}-{suffix}"),"namespace":"etl-testing-plane"},"spec":{"accessModes":["ReadWriteOnce"],"resources":{"requests":{"storage":"4Gi"}}}}));
            }
            items.push(job(plan, image, "prepare", &format!("{prefix}-runner")));
        }
        Phase::Run => {
            items.push(job(plan, image, "observe", &format!("{prefix}-observer")));
            items.push(job(plan, image, "run", &format!("{prefix}-runner")));
        }
        Phase::Cleanup => {
            let mut cleanup = job(plan, image, "cleanup", &format!("{prefix}-runner"));
            cleanup["spec"]["template"]["spec"]["containers"][0]["args"] =
                json!(["cleanup", "--plan", "/plan/plan.json", "--drop-data"]);
            items.push(cleanup);
        }
    }
    std::fs::write(
        output,
        serde_json::to_vec_pretty(&json!({"apiVersion":"v1","kind":"List","items":items}))?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        plan::Plan,
        staging::{Phase, render},
    };

    #[test]
    fn distinct_evidence_volumes_and_no_automatic_retry() {
        let plan: Plan = serde_json::from_str(include_str!("../specs/baseline.json")).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jobs.json");
        render(&plan, &format!("example.com/runner@sha256:{}", "a".repeat(64)), &path, Phase::Run)
            .unwrap();
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let observer = &value["items"][1];
        let runner = &value["items"][2];
        assert_eq!(runner["spec"]["backoffLimit"], 0);
        assert_ne!(
            observer["spec"]["template"]["spec"]["volumes"][1],
            runner["spec"]["template"]["spec"]["volumes"][1]
        );
        assert_eq!(runner["metadata"]["namespace"], "etl-testing-plane");
    }
}
