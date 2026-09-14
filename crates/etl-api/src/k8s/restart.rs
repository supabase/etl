//! Replacement of outdated StatefulSet pods, including blocked rolling updates.

use std::time::Duration;

use k8s_openapi::api::{apps::v1::StatefulSet, core::v1::Pod};
use kube::{
    Api, ResourceExt,
    api::{DeleteParams, Preconditions},
};

use crate::k8s::K8sError;

/// Pod template annotation identifying an API-requested restart.
pub(super) const RESTARTED_AT_ANNOTATION: &str = "etl.supabase.com/restarted-at";
/// Maximum time to observe a template update and request pod replacement.
const RESTART_TIMEOUT: Duration = Duration::from_secs(30);
/// Delay between controller observation or deletion conflict retries.
const RESTART_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Ensures an outdated pod is replaced after its StatefulSet template is
/// applied.
///
/// Ordered rolling updates can wait indefinitely for an unready pod. Once the
/// controller observes the applied generation, delete that pod with a UID
/// precondition so a concurrent replacement cannot be deleted accidentally.
/// A newer generation supersedes this request. Missing or terminating resources
/// are left to Kubernetes, and an already updated pod is never restarted again.
///
/// Returns after deletion is accepted, without waiting for termination or
/// readiness. Default deletion options preserve the pod's shutdown grace
/// period.
pub(super) async fn restart_outdated_pod(
    stateful_sets: &Api<StatefulSet>,
    pods: &Api<Pod>,
    applied: &StatefulSet,
    pod_name: &str,
) -> Result<(), K8sError> {
    // Use the API server's accepted identity and template as the target for
    // this request, rather than following later updates to the StatefulSet.
    let name = applied.name_any();
    let invalid_stateful_set =
        || K8sError::InvalidRestartResource { kind: "StatefulSet", name: name.clone() };
    let uid = applied.metadata.uid.as_ref().ok_or_else(invalid_stateful_set)?;
    let generation = applied.metadata.generation.ok_or_else(invalid_stateful_set)?;
    let restart = applied
        .spec
        .as_ref()
        .and_then(|spec| spec.template.metadata.as_ref())
        .and_then(|metadata| metadata.annotations.as_ref())
        .and_then(|annotations| annotations.get(RESTARTED_AT_ANNOTATION))
        .ok_or_else(invalid_stateful_set)?;

    // Bound controller observation and conflict retries together, without
    // waiting for the pod's shutdown grace period or replacement readiness.
    tokio::time::timeout(RESTART_TIMEOUT, async {
        loop {
            // Read the pod before checking the generation so a newer request's
            // replacement cannot be mistaken for an outdated pod of this request.
            let pod = pods.get_opt(pod_name).await?;
            let Some(current) = stateful_sets.get_opt(&name).await? else {
                return Ok(());
            };

            // A stop, recreation, or newer update supersedes this request.
            // Leave the current workload to the operation that now owns it.
            if current.metadata.deletion_timestamp.is_some()
                || current.metadata.uid.as_ref() != Some(uid)
                || current.metadata.generation != Some(generation)
            {
                return Ok(());
            }

            // The controller must know the new template before we ask it to
            // replace a pod, otherwise it could recreate the old configuration.
            let observed = current.status.and_then(|status| status.observed_generation);
            if observed.is_none_or(|observed| observed < generation) {
                tokio::time::sleep(RESTART_POLL_INTERVAL).await;
                continue;
            }

            let Some(pod) = pod else {
                // A pod could have been created from the previous template
                // between the first read and controller observation.
                if pods.get_opt(pod_name).await?.is_some() {
                    continue;
                }

                return Ok(());
            };

            // Kubernetes already owns replacement of a terminating pod.
            if pod.metadata.deletion_timestamp.is_some() {
                return Ok(());
            }

            // A matching name alone does not establish ownership, especially
            // if the StatefulSet was deleted and recreated.
            if !pod.owner_references().iter().any(|owner| {
                owner.controller == Some(true) && owner.kind == "StatefulSet" && &owner.uid == uid
            }) {
                return Err(K8sError::InvalidRestartResource {
                    kind: "Pod",
                    name: pod_name.to_owned(),
                });
            }

            // An updated pod has already received this restart, even if its
            // new configuration still fails. Do not create a restart loop.
            if pod.annotations().get(RESTARTED_AT_ANNOTATION) == Some(restart) {
                return Ok(());
            }

            // Pod names are reused. Guard deletion with the observed UID so
            // a concurrent replacement survives, and retain normal graceful shutdown.
            let pod_uid = pod.metadata.uid.ok_or_else(|| K8sError::InvalidRestartResource {
                kind: "Pod",
                name: pod_name.to_owned(),
            })?;
            let params = DeleteParams {
                preconditions: Some(Preconditions { uid: Some(pod_uid), resource_version: None }),
                ..Default::default()
            };
            match pods.delete(pod_name, &params).await {
                // Acceptance is enough: the StatefulSet controller completes replacement.
                Ok(_) => return Ok(()),
                // Re-read both resources after a deletion race; never retry
                // deletion by name alone against a potentially different pod.
                Err(kube::Error::Api(error)) if matches!(error.code, 404 | 409) => {
                    tokio::time::sleep(RESTART_POLL_INTERVAL).await;
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await
    .map_err(|_| K8sError::StatefulSetRestartTimeout { name })?
}
