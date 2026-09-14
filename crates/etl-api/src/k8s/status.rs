//! Observes the single-replica pipeline runtime without persisting a second
//! state machine.
//!
//! The public states describe a pipeline lifecycle, not individual Kubernetes
//! phases. Apply these rules in order: shutdown intent, runtime ownership,
//! replacement, observation uncertainty, current failure, then readiness. In
//! particular, a missing Pod under an active StatefulSet is starting; an old
//! process cannot complete a restart.
//!
//! StatefulSets do not provide a general Ready or Progressing condition. Like
//! `kubectl rollout status`, use observed generation and revision to establish
//! that the controller has processed the desired template, then inspect the
//! current Pod. See the [StatefulSet API](https://kubernetes.io/docs/reference/kubernetes-api/workload-resources/stateful-set-v1/)
//! and [Pod lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/).
//!
//! These are observations, not promises of future progress. Failed runtimes can
//! recover automatically; pending scheduling and graceful termination have no
//! local failure deadline. Readiness uses the existing Kubernetes signals and
//! does not establish source connectivity, initial-sync completion, or
//! destination durability.
//!
//! Pod generation is not a restart token: in-place resource resizing also
//! changes it. Pending resize conditions and ephemeral debugging containers do
//! not change pipeline lifecycle while the current application and its required
//! sidecars run.

use k8s_openapi::api::{
    apps::v1::StatefulSet,
    core::v1::{ContainerStatus, Pod},
};
use kube::Api;

use crate::k8s::{K8sError, PodStatus, restart::RESTARTED_AT_ANNOTATION};

/// Bounds retries when a lifecycle operation changes the workload during
/// observation.
const STATUS_OBSERVATION_ATTEMPTS: usize = 2;

/// Recognizes current startup and execution failures, ignoring historical
/// failures.
fn container_has_error(container: &ContainerStatus) -> bool {
    let Some(state) = &container.state else {
        return false;
    };

    if let Some(terminated) = &state.terminated {
        return terminated.exit_code != 0;
    }

    state.waiting.as_ref().and_then(|waiting| waiting.reason.as_deref()).is_some_and(|reason| {
        matches!(
            reason,
            "CrashLoopBackOff"
                | "ImagePullBackOff"
                | "ErrImagePull"
                | "ErrImageNeverPull"
                | "InvalidImageName"
                | "ImageInspectError"
                | "RegistryUnavailable"
                | "CreateContainerConfigError"
                | "CreateContainerError"
                | "RunContainerError"
        )
    })
}

/// Detects unavailable observations and waiting reasons with unknown semantics.
fn container_status_is_unknown(container: &ContainerStatus) -> bool {
    let Some(state) = &container.state else {
        return false;
    };

    if state.terminated.as_ref().and_then(|terminated| terminated.reason.as_deref())
        == Some("ContainerStatusUnknown")
    {
        return true;
    }

    // Waiting reasons are extensible strings, not a closed API enum. Preserve
    // known initialization/recovery states without inventing semantics for a new
    // reason. RestartingAllContainers is the kubelet's whole-Pod recovery state.
    state.waiting.as_ref().and_then(|waiting| waiting.reason.as_deref()).is_some_and(|reason| {
        !matches!(reason, "" | "ContainerCreating" | "PodInitializing" | "RestartingAllContainers")
            && !container_has_error(container)
    })
}

/// Classifies the current process after ownership and replacement checks
/// succeed.
fn derive_pod_status(pod: &Pod, replicator_container_name: &str) -> PodStatus {
    let Some(status) = &pod.status else {
        return PodStatus::Starting;
    };

    let ready = status
        .conditions
        .as_ref()
        .and_then(|conditions| conditions.iter().find(|condition| condition.type_ == "Ready"));
    let mut containers = status
        .container_statuses
        .iter()
        .flatten()
        .chain(status.init_container_statuses.iter().flatten());

    // Loss of observation takes precedence over retained container health. A node
    // can become unreachable while its last reported process still looks healthy.
    if status
        .phase
        .as_deref()
        .is_some_and(|phase| !matches!(phase, "Pending" | "Running" | "Succeeded" | "Failed"))
        || ready.is_some_and(|condition| !matches!(condition.status.as_str(), "True" | "False"))
        || containers.clone().any(container_status_is_unknown)
    {
        return PodStatus::Unknown;
    }

    if status.phase.as_deref() == Some("Failed") || containers.any(container_has_error) {
        return PodStatus::Failed;
    }

    let replicator = status.container_statuses.as_ref().and_then(|containers| {
        containers.iter().find(|container| container.name == replicator_container_name)
    });

    match status.phase.as_deref() {
        Some("Running") => {
            if replicator.is_some_and(|container| {
                container.ready
                    && container.state.as_ref().is_some_and(|state| state.running.is_some())
            }) && ready.is_some_and(|condition| condition.status == "True")
            {
                PodStatus::Started
            } else {
                PodStatus::Starting
            }
        }
        // Always restart policy means a clean exit is recovery, not a stopped
        // pipeline. Missing phase is normal before the kubelet first reports status.
        None | Some("Pending" | "Succeeded") => PodStatus::Starting,
        _ => PodStatus::Unknown,
    }
}

/// Derives pipeline lifecycle state from one observed workload and its named
/// Pod.
fn derive_replicator_status(
    stateful_set: Option<&StatefulSet>,
    pod: Option<&Pod>,
    replicator_container_name: &str,
) -> PodStatus {
    let Some(stateful_set) = stateful_set else {
        return match pod {
            None => PodStatus::Stopped,
            Some(pod) if pod.metadata.deletion_timestamp.is_some() => PodStatus::Stopping,
            Some(_) => PodStatus::Unknown,
        };
    };

    if stateful_set.metadata.deletion_timestamp.is_some() {
        return PodStatus::Stopping;
    }

    let Some(spec) = &stateful_set.spec else {
        return PodStatus::Unknown;
    };

    match spec.replicas.unwrap_or(1) {
        0 => return if pod.is_some() { PodStatus::Stopping } else { PodStatus::Stopped },
        1 => {}
        // This architecture runs one replicator. Inspecting ordinal zero cannot
        // establish the health of an externally scaled, multi-process pipeline.
        _ => return PodStatus::Unknown,
    }

    let Some(pod) = pod else {
        return PodStatus::Starting;
    };

    let Some(uid) = &stateful_set.metadata.uid else {
        return PodStatus::Unknown;
    };

    if !pod.metadata.owner_references.as_ref().is_some_and(|owners| {
        owners.iter().any(|owner| {
            owner.controller == Some(true) && owner.kind == "StatefulSet" && &owner.uid == uid
        })
    }) {
        return PodStatus::Unknown;
    }

    if pod.metadata.deletion_timestamp.is_some() {
        return PodStatus::Starting;
    }

    // The annotation changes immediately when a restart is accepted. Controller
    // status may still describe the old revision until it observes that mutation.
    let desired_restart = spec
        .template
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.annotations.as_ref())
        .and_then(|annotations| annotations.get(RESTARTED_AT_ANNOTATION));
    let pod_restart = pod
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(RESTARTED_AT_ANNOTATION));

    if desired_restart != pod_restart {
        return PodStatus::Starting;
    }

    let Some(generation) = stateful_set.metadata.generation else {
        return PodStatus::Unknown;
    };

    if stateful_set
        .status
        .as_ref()
        .and_then(|status| status.observed_generation)
        .is_none_or(|observed| observed < generation)
    {
        return PodStatus::Starting;
    }

    let revision = stateful_set.status.as_ref().and_then(|status| status.update_revision.as_ref());
    let pod_revision =
        pod.metadata.labels.as_ref().and_then(|labels| labels.get("controller-revision-hash"));

    // Missing rollout evidence is not proof that the requested process is running.
    if revision.is_none_or(String::is_empty) || revision != pod_revision {
        return PodStatus::Starting;
    }

    derive_pod_status(pod, replicator_container_name)
}

/// Compares desired workload identity while allowing controller status to
/// advance.
fn same_workload(before: Option<&StatefulSet>, after: Option<&StatefulSet>) -> bool {
    match (before, after) {
        (None, None) => true,
        (Some(before), Some(after)) => {
            before.metadata.uid == after.metadata.uid
                && before.metadata.generation == after.metadata.generation
                && before.metadata.deletion_timestamp == after.metadata.deletion_timestamp
        }
        _ => false,
    }
}

/// Reads a Pod between observations of its desired workload, retrying a
/// lifecycle race.
///
/// Kubernetes GETs across resource types are not an atomic snapshot. A start,
/// stop, or restart between reads must not pair a Pod with a different desired
/// workload. Compare UID, generation, and deletion intent around the Pod read;
/// use the later controller status when those agree. Resource versions include
/// ordinary status updates, so comparing them would unnecessarily reject a
/// progressing rollout.
///
/// One retry handles a crossing operation without waiting for readiness.
/// Continued churn returns [`PodStatus::Unknown`]; transport and authorization
/// errors remain errors, never absence. Observations can still become stale
/// after the final read and must not replace lifecycle locks or deletion
/// barriers in mutation endpoints.
pub(super) async fn read_replicator_status(
    stateful_sets: &Api<StatefulSet>,
    pods: &Api<Pod>,
    stateful_set_name: &str,
    pod_name: &str,
    replicator_container_name: &str,
) -> Result<PodStatus, K8sError> {
    let mut before = stateful_sets.get_opt(stateful_set_name).await?;

    for _ in 0..STATUS_OBSERVATION_ATTEMPTS {
        let pod = pods.get_opt(pod_name).await?;
        let after = stateful_sets.get_opt(stateful_set_name).await?;

        if same_workload(before.as_ref(), after.as_ref()) {
            return Ok(derive_replicator_status(
                after.as_ref(),
                pod.as_ref(),
                replicator_container_name,
            ));
        }

        before = after;
    }

    Ok(PodStatus::Unknown)
}

#[cfg(test)]
mod tests {
    //! Runtime classification and Kubernetes read-interleaving tests.

    use std::collections::BTreeMap;

    use chrono::Utc;
    use k8s_openapi::{
        api::{
            apps::v1::StatefulSet,
            core::v1::{
                ContainerState, ContainerStateRunning, ContainerStateTerminated,
                ContainerStateWaiting, ContainerStatus, Pod, PodCondition,
                PodStatus as KubernetesPodStatus,
            },
        },
        apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference, Time},
    };
    use serde_json::json;

    use crate::k8s::{
        K8sError, PodStatus,
        restart::RESTARTED_AT_ANNOTATION,
        status::{derive_replicator_status, read_replicator_status},
    };

    /// Name of the only application container in the test runtime.
    const REPLICATOR_CONTAINER_NAME: &str = "tenant-42-replicator";

    /// Builds a failed process observation, optionally during deletion.
    fn failed_replicator_pod(deleting: bool) -> Pod {
        Pod {
            metadata: ObjectMeta {
                deletion_timestamp: deleting.then(|| Time(Utc::now())),
                ..Default::default()
            },
            status: Some(KubernetesPodStatus {
                phase: Some("Failed".to_owned()),
                container_statuses: Some(vec![ContainerStatus {
                    name: REPLICATOR_CONTAINER_NAME.to_owned(),
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 1,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Builds a controller-observed workload and its ready, current Pod.
    fn ready_runtime() -> (StatefulSet, Pod) {
        let stateful_set: StatefulSet = serde_json::from_value(json!({
            "metadata": {"uid": "workload-uid", "generation": 1},
            "spec": {"replicas": 1, "selector": {}, "template": {}},
            "status": {"replicas": 1, "observedGeneration": 1, "updateRevision": "revision-1"}
        }))
        .unwrap();
        let pod = Pod {
            metadata: ObjectMeta {
                owner_references: Some(vec![OwnerReference {
                    api_version: "apps/v1".to_owned(),
                    kind: "StatefulSet".to_owned(),
                    name: "replicator".to_owned(),
                    uid: "workload-uid".to_owned(),
                    controller: Some(true),
                    ..Default::default()
                }]),
                labels: Some(BTreeMap::from([(
                    "controller-revision-hash".to_owned(),
                    "revision-1".to_owned(),
                )])),
                ..Default::default()
            },
            status: Some(KubernetesPodStatus {
                phase: Some("Running".to_owned()),
                conditions: Some(vec![PodCondition {
                    type_: "Ready".to_owned(),
                    status: "True".to_owned(),
                    ..Default::default()
                }]),
                container_statuses: Some(vec![ContainerStatus {
                    name: REPLICATOR_CONTAINER_NAME.to_owned(),
                    ready: true,
                    state: Some(ContainerState {
                        running: Some(ContainerStateRunning::default()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };
        (stateful_set, pod)
    }

    /// Desired workload state distinguishes replacements, shutdown, and
    /// orphans.
    #[test]
    fn replicator_status_distinguishes_replacement_from_shutdown() {
        let (active, running_pod) = ready_runtime();
        let mut deleting = active.clone();
        deleting.metadata.deletion_timestamp = Some(Time(Utc::now()));
        let mut deleting_pod = running_pod.clone();
        deleting_pod.metadata.deletion_timestamp = Some(Time(Utc::now()));
        let mut failed_pod = running_pod.clone();
        failed_pod.status = failed_replicator_pod(false).status;
        let mut scaled_down = active.clone();
        scaled_down.spec.as_mut().unwrap().replicas = Some(0);

        for (stateful_set, pod, expected) in [
            (None, None, PodStatus::Stopped),
            (None, Some(&running_pod), PodStatus::Unknown),
            (None, Some(&deleting_pod), PodStatus::Stopping),
            (Some(&deleting), None, PodStatus::Stopping),
            (Some(&deleting), Some(&running_pod), PodStatus::Stopping),
            (Some(&active), None, PodStatus::Starting),
            (Some(&active), Some(&deleting_pod), PodStatus::Starting),
            (Some(&active), Some(&failed_pod), PodStatus::Failed),
            (Some(&active), Some(&running_pod), PodStatus::Started),
            (Some(&scaled_down), None, PodStatus::Stopped),
            (Some(&scaled_down), Some(&running_pod), PodStatus::Stopping),
        ] {
            assert_eq!(
                derive_replicator_status(stateful_set, pod, REPLICATOR_CONTAINER_NAME),
                expected
            );
        }
    }

    /// Healthy old Pods cannot satisfy a different owner or pending revision.
    #[test]
    fn replicator_status_requires_current_workload_identity_and_revision() {
        let (active, pod) = ready_runtime();
        for case in 0..6 {
            let mut active = active.clone();
            let mut pod = pod.clone();
            let expected = match case {
                0 => {
                    pod.metadata.owner_references = None;
                    PodStatus::Unknown
                }
                1 => {
                    active.metadata.uid = Some("replacement-uid".to_owned());
                    PodStatus::Unknown
                }
                2 => {
                    active.metadata.generation = Some(2);
                    PodStatus::Starting
                }
                3 => {
                    active.status.as_mut().unwrap().observed_generation = None;
                    PodStatus::Starting
                }
                4 => {
                    active.status.as_mut().unwrap().update_revision = Some("revision-2".to_owned());
                    PodStatus::Starting
                }
                _ => {
                    active.spec.as_mut().unwrap().template.metadata = Some(ObjectMeta {
                        annotations: Some(BTreeMap::from([(
                            RESTARTED_AT_ANNOTATION.to_owned(),
                            "new-restart".to_owned(),
                        )])),
                        ..Default::default()
                    });
                    // An old failure should not hide a requested replacement.
                    pod.status = failed_replicator_pod(false).status;
                    PodStatus::Starting
                }
            };
            assert_eq!(
                derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
                expected,
                "case {case}"
            );
        }
    }

    /// Running phase alone does not establish readiness or successful recovery.
    #[test]
    fn replicator_status_requires_readiness_and_recognizes_failures() {
        let (active, pod) = ready_runtime();
        for case in 0..11 {
            let mut pod = pod.clone();
            let status = pod.status.as_mut().unwrap();
            let expected = match case {
                0 => {
                    status.container_statuses.as_mut().unwrap()[0].ready = false;
                    PodStatus::Starting
                }
                1 => {
                    status.conditions = None;
                    PodStatus::Starting
                }
                2 => {
                    status.conditions.as_mut().unwrap()[0].status = "False".to_owned();
                    PodStatus::Starting
                }
                3 => {
                    status.conditions.as_mut().unwrap()[0].status = "Unknown".to_owned();
                    PodStatus::Unknown
                }
                4 => {
                    status.container_statuses = None;
                    PodStatus::Starting
                }
                5 => {
                    status.phase = Some("Succeeded".to_owned());
                    PodStatus::Starting
                }
                6 => {
                    status.container_statuses.as_mut().unwrap()[0].state = Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 0,
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                    PodStatus::Starting
                }
                7 => {
                    status.init_container_statuses =
                        failed_replicator_pod(false).status.unwrap().container_statuses;
                    PodStatus::Failed
                }
                8 => {
                    status.container_statuses.as_mut().unwrap()[0].state = Some(ContainerState {
                        waiting: Some(ContainerStateWaiting {
                            reason: Some("ImagePullBackOff".to_owned()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                    PodStatus::Failed
                }
                9 => {
                    status.container_statuses.as_mut().unwrap()[0].last_state =
                        Some(ContainerState {
                            terminated: Some(ContainerStateTerminated {
                                exit_code: 1,
                                ..Default::default()
                            }),
                            ..Default::default()
                        });
                    PodStatus::Started
                }
                _ => {
                    status.phase = Some("Pending".to_owned());
                    PodStatus::Starting
                }
            };
            assert_eq!(
                derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
                expected,
                "case {case}"
            );
        }
    }

    /// Incomplete rollout evidence and unsupported replicas cannot report
    /// success.
    #[test]
    fn incomplete_workload_observations_never_report_started() {
        let (active, pod) = ready_runtime();
        for (case, expected) in [
            ("missing spec", PodStatus::Unknown),
            ("missing uid", PodStatus::Unknown),
            ("missing generation", PodStatus::Unknown),
            ("missing status", PodStatus::Starting),
            ("missing revision", PodStatus::Starting),
            ("empty revision", PodStatus::Starting),
            ("multiple replicas", PodStatus::Unknown),
        ] {
            let mut active = active.clone();
            match case {
                "missing spec" => active.spec = None,
                "missing uid" => active.metadata.uid = None,
                "missing generation" => active.metadata.generation = None,
                "missing status" => active.status = None,
                "missing revision" => active.status.as_mut().unwrap().update_revision = None,
                "empty revision" => {
                    active.status.as_mut().unwrap().update_revision = Some(String::new());
                }
                "multiple replicas" => active.spec.as_mut().unwrap().replicas = Some(2),
                _ => unreachable!("The cases above exhaust the fixture variants"),
            }

            assert_eq!(
                derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
                expected,
                "{case}"
            );
        }
    }

    /// Fresh observation uncertainty overrides retained healthy or failed
    /// state.
    #[test]
    fn unknown_observations_do_not_reuse_old_container_health() {
        let (active, pod) = ready_runtime();
        for phase in ["Running", "Unknown"] {
            for failed in [false, true] {
                let mut pod = pod.clone();
                let status = pod.status.as_mut().unwrap();
                status.phase = Some(phase.to_owned());
                status.conditions.as_mut().unwrap()[0].status = "Unknown".to_owned();
                if failed {
                    status.container_statuses =
                        failed_replicator_pod(false).status.unwrap().container_statuses;
                }

                assert_eq!(
                    derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
                    PodStatus::Unknown
                );
            }
        }

        for terminated in [false, true] {
            let mut pod = pod.clone();
            let container =
                &mut pod.status.as_mut().unwrap().container_statuses.as_mut().unwrap()[0];
            container.state = Some(if terminated {
                ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        exit_code: 137,
                        reason: Some("ContainerStatusUnknown".to_owned()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            } else {
                ContainerState {
                    waiting: Some(ContainerStateWaiting {
                        reason: Some("ContainerStatusUnknown".to_owned()),
                        ..Default::default()
                    }),
                    ..Default::default()
                }
            });

            assert_eq!(
                derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
                PodStatus::Unknown
            );
        }
    }

    /// Shutdown and replacement retain their meaning across every Pod health
    /// state.
    #[test]
    fn lifecycle_intent_precedes_process_health() {
        let (active, pod) = ready_runtime();
        for phase in ["Pending", "Running", "Succeeded", "Failed", "Unknown"] {
            let mut pod = pod.clone();
            pod.status.as_mut().unwrap().phase = Some(phase.to_owned());
            let mut deleting = active.clone();
            deleting.metadata.deletion_timestamp = Some(Time(Utc::now()));
            assert_eq!(
                derive_replicator_status(Some(&deleting), Some(&pod), REPLICATOR_CONTAINER_NAME),
                PodStatus::Stopping
            );

            let mut restarting = active.clone();
            restarting.spec.as_mut().unwrap().template.metadata = Some(ObjectMeta {
                annotations: Some(BTreeMap::from([(
                    RESTARTED_AT_ANNOTATION.to_owned(),
                    "restart-2".to_owned(),
                )])),
                ..Default::default()
            });
            assert_eq!(
                derive_replicator_status(Some(&restarting), Some(&pod), REPLICATOR_CONTAINER_NAME),
                PodStatus::Starting
            );

            pod.metadata.deletion_timestamp = Some(Time(Utc::now()));
            assert_eq!(
                derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
                PodStatus::Starting
            );
            assert_eq!(
                derive_replicator_status(None, Some(&pod), REPLICATOR_CONTAINER_NAME),
                PodStatus::Stopping
            );
        }
    }

    /// Tests real client GET ordering against explicit Kubernetes response
    /// snapshots.
    async fn observe_responses(
        responses: Vec<(&str, serde_json::Value)>,
    ) -> Result<PodStatus, K8sError> {
        use std::{
            collections::VecDeque,
            convert::Infallible,
            sync::{Arc, Mutex},
        };

        use axum::{
            body::Body,
            http::{Request, Response},
        };
        use kube::{Api, Client};

        let responses = Arc::new(Mutex::new(VecDeque::from_iter(
            responses.into_iter().map(|(kind, value)| (kind.to_owned(), value)),
        )));
        let service = tower::service_fn({
            let responses = Arc::clone(&responses);
            move |request: Request<kube::client::Body>| {
                let (kind, value) = responses.lock().unwrap().pop_front().unwrap();
                assert!(request.uri().path().contains(&format!("/{kind}/")));
                async move {
                    let code = value
                        .get("code")
                        .and_then(serde_json::Value::as_u64)
                        .map_or(200, |code| u16::try_from(code).unwrap());
                    Ok::<_, Infallible>(
                        Response::builder()
                            .status(code)
                            .header("content-type", "application/json")
                            .body(Body::from(serde_json::to_vec(&value).unwrap()))
                            .unwrap(),
                    )
                }
            }
        });
        let client = Client::new(service, "test");
        let result = read_replicator_status(
            &Api::namespaced(client.clone(), "test"),
            &Api::namespaced(client, "test"),
            "replicator",
            "replicator-0",
            REPLICATOR_CONTAINER_NAME,
        )
        .await;
        assert!(responses.lock().unwrap().is_empty());
        result
    }

    /// Represents an absent resource without conflating it with other API
    /// errors.
    fn absent_resource() -> serde_json::Value {
        json!({"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "NotFound", "message": "Test resource absent", "code": 404})
    }

    /// Crossing starts and stops retry instead of reporting a mixed
    /// observation.
    #[tokio::test]
    async fn observation_retries_workload_creation_deletion_and_recreation() {
        let (active, pod) = ready_runtime();
        let active = serde_json::to_value(active).unwrap();
        let pod = serde_json::to_value(pod).unwrap();
        let absent = absent_resource();

        assert_eq!(
            observe_responses(vec![
                ("statefulsets", absent.clone()),
                ("pods", absent.clone()),
                ("statefulsets", active.clone()),
                ("pods", pod.clone()),
                ("statefulsets", active.clone()),
            ])
            .await
            .unwrap(),
            PodStatus::Started
        );
        assert_eq!(
            observe_responses(vec![
                ("statefulsets", active.clone()),
                ("pods", pod.clone()),
                ("statefulsets", absent.clone()),
                ("pods", absent.clone()),
                ("statefulsets", absent),
            ])
            .await
            .unwrap(),
            PodStatus::Stopped
        );

        let mut replacement = active.clone();
        replacement["metadata"]["uid"] = json!("new-workload");
        assert_eq!(
            observe_responses(vec![
                ("statefulsets", active),
                ("pods", pod.clone()),
                ("statefulsets", replacement.clone()),
                ("pods", pod),
                ("statefulsets", replacement),
            ])
            .await
            .unwrap(),
            PodStatus::Unknown
        );
    }

    /// A restart or stop that crosses a read cannot be completed by the old
    /// Pod.
    #[tokio::test]
    async fn observation_retries_template_and_deletion_changes() {
        let (active, pod) = ready_runtime();
        let active = serde_json::to_value(active).unwrap();
        let pod = serde_json::to_value(pod).unwrap();
        for deleting in [false, true] {
            let mut changed = active.clone();
            if deleting {
                changed["metadata"]["deletionTimestamp"] = json!("2026-01-01T00:00:00Z");
            } else {
                changed["metadata"]["generation"] = json!(2);
                changed["spec"]["template"]["metadata"]["annotations"][RESTARTED_AT_ANNOTATION] =
                    json!("restart-2");
            }

            assert_eq!(
                observe_responses(vec![
                    ("statefulsets", active.clone()),
                    ("pods", pod.clone()),
                    ("statefulsets", changed.clone()),
                    ("pods", pod.clone()),
                    ("statefulsets", changed),
                ])
                .await
                .unwrap(),
                if deleting { PodStatus::Stopping } else { PodStatus::Starting }
            );
        }
    }

    /// Ordinary status writes do not cause retries, but repeated desired
    /// changes do.
    #[tokio::test]
    async fn observation_accepts_controller_progress_and_bounds_churn() {
        let (active, pod) = ready_runtime();
        let active = serde_json::to_value(active).unwrap();
        let pod = serde_json::to_value(pod).unwrap();
        let mut unobserved = active.clone();
        unobserved["status"] = json!({"replicas": 1});
        assert_eq!(
            observe_responses(vec![
                ("statefulsets", unobserved),
                ("pods", pod.clone()),
                ("statefulsets", active.clone()),
            ])
            .await
            .unwrap(),
            PodStatus::Started
        );

        let mut second = active.clone();
        second["metadata"]["generation"] = json!(2);
        let mut third = active.clone();
        third["metadata"]["generation"] = json!(3);
        assert_eq!(
            observe_responses(vec![
                ("statefulsets", active),
                ("pods", pod.clone()),
                ("statefulsets", second),
                ("pods", pod),
                ("statefulsets", third),
            ])
            .await
            .unwrap(),
            PodStatus::Unknown
        );
    }

    /// Unavailable or forbidden observations must never become a stopped
    /// pipeline.
    #[tokio::test]
    async fn observation_propagates_api_errors_at_each_read() {
        let (active, pod) = ready_runtime();
        for code in [403, 500, 503] {
            for failed_read in 0..3 {
                let mut responses = vec![
                    ("statefulsets", serde_json::to_value(&active).unwrap()),
                    ("pods", serde_json::to_value(&pod).unwrap()),
                    ("statefulsets", serde_json::to_value(&active).unwrap()),
                ];
                responses.truncate(failed_read + 1);
                responses[failed_read].1 = json!({"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "TestFailure", "message": "Test observation failure", "code": code});
                assert!(observe_responses(responses).await.is_err());
            }
        }
    }

    /// Recognized recovery stays starting; new waiting reasons remain unknown.
    #[test]
    fn waiting_reasons_distinguish_recovery_failures_and_unknown_states() {
        let (active, pod) = ready_runtime();
        for (reason, expected) in [
            ("ContainerCreating", PodStatus::Starting),
            ("PodInitializing", PodStatus::Starting),
            ("RestartingAllContainers", PodStatus::Starting),
            ("CrashLoopBackOff", PodStatus::Failed),
            ("ImagePullBackOff", PodStatus::Failed),
            ("ErrImagePull", PodStatus::Failed),
            ("ErrImageNeverPull", PodStatus::Failed),
            ("InvalidImageName", PodStatus::Failed),
            ("ImageInspectError", PodStatus::Failed),
            ("RegistryUnavailable", PodStatus::Failed),
            ("CreateContainerConfigError", PodStatus::Failed),
            ("CreateContainerError", PodStatus::Failed),
            ("RunContainerError", PodStatus::Failed),
            ("ContainerStatusUnknown", PodStatus::Unknown),
            ("FutureWaitingReason", PodStatus::Unknown),
        ] {
            let mut pod = pod.clone();
            let status = pod.status.as_mut().unwrap();
            status.conditions.as_mut().unwrap()[0].status = "False".to_owned();
            status.container_statuses.as_mut().unwrap()[0].state = Some(ContainerState {
                waiting: Some(ContainerStateWaiting {
                    reason: Some(reason.to_owned()),
                    ..Default::default()
                }),
                ..Default::default()
            });

            assert_eq!(
                derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
                expected,
                "{reason}"
            );
        }
    }

    /// Resource resizing and debugging containers do not change process
    /// lifecycle.
    #[test]
    fn healthy_runtime_survives_resize_and_debug_container_failures() {
        let (active, mut pod) = ready_runtime();
        pod.metadata.generation = Some(2);
        let status = pod.status.as_mut().unwrap();
        status.observed_generation = Some(1);
        status.conditions.as_mut().unwrap().push(PodCondition {
            type_: "PodResizePending".to_owned(),
            status: "True".to_owned(),
            reason: Some("Deferred".to_owned()),
            ..Default::default()
        });
        status.ephemeral_container_statuses =
            failed_replicator_pod(false).status.unwrap().container_statuses;

        assert_eq!(
            derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
            PodStatus::Started
        );
    }

    /// Completed initialization is healthy, while a current sidecar failure is
    /// not.
    #[test]
    fn init_container_completion_and_sidecar_recovery_are_not_sticky_failures() {
        let (active, mut pod) = ready_runtime();
        let mut init = failed_replicator_pod(false).status.unwrap().container_statuses.unwrap();
        pod.status.as_mut().unwrap().init_container_statuses = Some(init.clone());
        assert_eq!(
            derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
            PodStatus::Failed
        );

        init[0].state.as_mut().unwrap().terminated.as_mut().unwrap().exit_code = 0;
        pod.status.as_mut().unwrap().init_container_statuses = Some(init.clone());
        assert_eq!(
            derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
            PodStatus::Started
        );

        init[0].state.as_mut().unwrap().terminated.as_mut().unwrap().exit_code = 1;
        init[0].last_state = init[0].state.take();
        init[0].state = Some(ContainerState {
            running: Some(ContainerStateRunning::default()),
            ..Default::default()
        });
        pod.status.as_mut().unwrap().init_container_statuses = Some(init);
        assert_eq!(
            derive_replicator_status(Some(&active), Some(&pod), REPLICATOR_CONTAINER_NAME),
            PodStatus::Started
        );
    }
}
