//! Replicator workload resource resolution.
//!
//! CPU and memory resolve independently through the same explicit hierarchy:
//!
//! 1. A pipeline request override provides the final startup value when set.
//! 2. Otherwise, the mandatory API-wide default provides the value.
//!
//! A non-empty pipeline request override disables the VPA for the entire
//! workload. Otherwise, configured VPA bounds are independent of the
//! StatefulSet startup allocation. When autoscaling is omitted, the resolved
//! startup value is used as both VPA bounds. Container limits have no separate
//! resolution: Kubernetes materialization uses each resolved request as its
//! limit to preserve Guaranteed QoS.

use crate::{config::K8sConfig, configs::pipeline::PipelineReplicatorResourceOverrideConfig};

/// CPU and memory allocated to one container.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ContainerResourceAllocation {
    /// Kubernetes CPU quantity.
    pub(super) cpu: String,
    /// Kubernetes memory quantity.
    pub(super) memory: String,
}

/// StatefulSet startup allocation after applying all request defaults and
/// overrides.
struct ReplicatorStatefulSetAllocation {
    /// CPU allocation, in millicores.
    cpu_millicores: i32,
    /// Memory allocation, in MiB.
    memory_mib: i32,
}

/// Allocations emitted for containers in the replicator StatefulSet.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ReplicatorStatefulSetResourceRequirements {
    /// Allocation used as both requests and limits for the replicator.
    pub(super) replicator: ContainerResourceAllocation,
    /// Allocation used as both requests and limits for Vector.
    pub(super) vector: ContainerResourceAllocation,
}

impl ReplicatorStatefulSetResourceRequirements {
    /// Resolves StatefulSet allocations from API defaults and a pipeline
    /// override.
    pub(super) fn resolve(
        k8s_config: &K8sConfig,
        pipeline_resource_override: Option<&PipelineReplicatorResourceOverrideConfig>,
    ) -> Self {
        let allocation =
            resolve_replicator_stateful_set_allocation(k8s_config, pipeline_resource_override);

        Self {
            replicator: ContainerResourceAllocation {
                cpu: format!("{}m", allocation.cpu_millicores),
                memory: format!("{}Mi", allocation.memory_mib),
            },
            vector: ContainerResourceAllocation {
                cpu: format!("{}m", k8s_config.vector_resources.cpu_request_millicores),
                memory: format!("{}Mi", k8s_config.vector_resources.memory_request_mib),
            },
        }
    }
}

/// Minimum and maximum VPA values for one resource.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct VpaResourceBounds {
    /// Minimum allocation accepted by the VPA.
    pub(super) min: i32,
    /// Maximum allocation accepted by the VPA.
    pub(super) max: i32,
}

/// CPU and memory policy emitted for a replicator VPA.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ReplicatorVpaResourcePolicy {
    /// CPU policy, in millicores.
    pub(super) cpu: VpaResourceBounds,
    /// Memory policy, in MiB.
    pub(super) memory: VpaResourceBounds,
}

impl ReplicatorVpaResourcePolicy {
    /// Resolves configured VPA bounds or fixes them to the startup allocation.
    pub(super) fn resolve(k8s_config: &K8sConfig) -> Self {
        if let Some(autoscaling_config) = &k8s_config.replicator_autoscaling {
            return Self {
                cpu: VpaResourceBounds {
                    min: autoscaling_config.min_cpu_millicores,
                    max: autoscaling_config.max_cpu_millicores,
                },
                memory: VpaResourceBounds {
                    min: autoscaling_config.min_memory_mib,
                    max: autoscaling_config.max_memory_mib,
                },
            };
        }

        let startup_allocation = resolve_replicator_stateful_set_allocation(k8s_config, None);

        Self {
            cpu: VpaResourceBounds {
                min: startup_allocation.cpu_millicores,
                max: startup_allocation.cpu_millicores,
            },
            memory: VpaResourceBounds {
                min: startup_allocation.memory_mib,
                max: startup_allocation.memory_mib,
            },
        }
    }
}

/// Resolves the replicator StatefulSet's startup allocation.
fn resolve_replicator_stateful_set_allocation(
    k8s_config: &K8sConfig,
    pipeline_resource_override: Option<&PipelineReplicatorResourceOverrideConfig>,
) -> ReplicatorStatefulSetAllocation {
    ReplicatorStatefulSetAllocation {
        cpu_millicores: pipeline_resource_override
            .and_then(|config| config.cpu_request_millicores)
            .unwrap_or(k8s_config.replicator_resources.cpu_request_millicores),
        memory_mib: pipeline_resource_override
            .and_then(|config| config.memory_request_mib)
            .unwrap_or(k8s_config.replicator_resources.memory_request_mib),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        ReplicatorResourceAutoscalingConfig, ReplicatorResourceDefaultsConfig,
        VectorResourceDefaultsConfig,
    };

    /// Returns a complete Kubernetes configuration for resource resolution.
    fn test_k8s_config() -> K8sConfig {
        K8sConfig {
            replicator_namespace: "etl-data-plane".to_owned(),
            replicator_service_account_name: "etl-replicator".to_owned(),
            replicator_node_selectors: Default::default(),
            replicator_tolerations: Default::default(),
            replicator_resources: ReplicatorResourceDefaultsConfig {
                memory_request_mib: 500,
                cpu_request_millicores: 300,
            },
            replicator_autoscaling: Some(ReplicatorResourceAutoscalingConfig {
                initial_update_mode: Default::default(),
                min_memory_mib: 768,
                max_memory_mib: 8_192,
                min_cpu_millicores: 250,
                max_cpu_millicores: 2_000,
            }),
            vector_image: "timberio/vector:test".to_owned(),
            vector_resources: VectorResourceDefaultsConfig {
                memory_request_mib: 192,
                cpu_request_millicores: 80,
            },
        }
    }

    #[test]
    fn api_defaults_choose_startup_allocation_independently_of_vpa_bounds() {
        let config = test_k8s_config();

        let resources = ReplicatorStatefulSetResourceRequirements::resolve(&config, None);
        let policy = ReplicatorVpaResourcePolicy::resolve(&config);

        assert_eq!(resources.replicator.cpu, "300m");
        assert_eq!(resources.replicator.memory, "500Mi");
        assert_eq!(resources.vector.cpu, "80m");
        assert_eq!(resources.vector.memory, "192Mi");
        assert_eq!(policy.cpu, VpaResourceBounds { min: 250, max: 2_000 });
        assert_eq!(policy.memory, VpaResourceBounds { min: 768, max: 8_192 });
    }

    #[test]
    fn omitted_autoscaling_fixes_vpa_to_startup_allocation() {
        let config = K8sConfig { replicator_autoscaling: None, ..test_k8s_config() };

        let policy = ReplicatorVpaResourcePolicy::resolve(&config);

        assert_eq!(policy.cpu, VpaResourceBounds { min: 300, max: 300 });
        assert_eq!(policy.memory, VpaResourceBounds { min: 500, max: 500 });
    }

    #[test]
    fn configured_vpa_bounds_are_independent_of_startup_defaults() {
        let policy = ReplicatorVpaResourcePolicy::resolve(&test_k8s_config());

        assert_eq!(policy.cpu, VpaResourceBounds { min: 250, max: 2_000 });
        assert_eq!(policy.memory, VpaResourceBounds { min: 768, max: 8_192 });
    }

    #[test]
    fn pipeline_requests_override_stateful_set_startup_allocation_independently() {
        let overrides = PipelineReplicatorResourceOverrideConfig {
            cpu_request_millicores: Some(900),
            memory_request_mib: None,
        };
        let config = test_k8s_config();

        let resources =
            ReplicatorStatefulSetResourceRequirements::resolve(&config, Some(&overrides));

        assert_eq!(resources.replicator.cpu, "900m");
        assert_eq!(resources.replicator.memory, "500Mi");
    }

    #[test]
    fn removed_persisted_limits_do_not_affect_stateful_set_or_vpa() {
        let overrides: PipelineReplicatorResourceOverrideConfig =
            serde_json::from_value(serde_json::json!({
                "cpu_limit_millicores": 1_200,
                "memory_limit_mib": 2_048
            }))
            .unwrap();
        let config = test_k8s_config();

        let resources =
            ReplicatorStatefulSetResourceRequirements::resolve(&config, Some(&overrides));
        let policy = ReplicatorVpaResourcePolicy::resolve(&config);

        assert_eq!(resources.replicator.cpu, "300m");
        assert_eq!(resources.replicator.memory, "500Mi");
        assert_eq!(policy.cpu, VpaResourceBounds { min: 250, max: 2_000 });
        assert_eq!(policy.memory, VpaResourceBounds { min: 768, max: 8_192 });
    }
}
