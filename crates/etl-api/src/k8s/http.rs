use std::collections::BTreeMap;

use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::Utc;
use etl_config::Environment;
#[cfg(test)]
use etl_config::shared::DestinationKind;
#[cfg(test)]
use etl_maintenance::DuckLakeMaintenancePolicy;
use k8s_openapi::{
    api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Namespace, Pod, Secret, ServiceAccount},
    },
    apimachinery::pkg::apis::meta::v1::ObjectMeta,
};
use kube::{
    Client,
    api::{Api, DeleteParams, ListParams, Patch, PatchParams},
    core::{ApiResource, DynamicObject, GroupVersionKind},
};
use serde_json::json;
use thiserror::Error;
use tracing::debug;

#[cfg(test)]
use crate::config::{DefaultReplicatorResourcesConfig, ReplicatorAutoscalingUpdateMode};
use crate::{
    config::{
        DefaultVectorResourcesConfig, K8sConfig, ReplicatorAutoscalingConfig,
        ResolvedReplicatorResourcesConfig,
    },
    configs::{
        log::LogLevel,
        pipeline::{DuckLakeMaintenanceConfig, ReplicatorResourcesConfig},
    },
    k8s::{
        DestinationType, DuckLakeMaintenanceResourceConfig, K8sClient, K8sError,
        PipelineRuntimeIdentity, PodPhase, PodStatus, ReplicatorConfigMapFile,
        ReplicatorStatefulSetConfig,
    },
};

/// Server-side apply field manager for resources owned by the API service.
const FIELD_MANAGER: &str = "etl-api";
/// Secret name suffix for the BigQuery service account key.
const BQ_SECRET_NAME_SUFFIX: &str = "bq-service-account-key";
/// Secret name suffix for the ClickHouse password.
const CLICKHOUSE_SECRET_NAME_SUFFIX: &str = "clickhouse-password";
/// Name of the password in the ClickHouse secret and its reference.
const CLICKHOUSE_PASSWORD_NAME: &str = "clickhouse-password";
/// Name of the service account key in the BigQuery secret and its reference.
const BQ_SERVICE_ACCOUNT_KEY_NAME: &str = "service-account-key";
/// Secret name suffix for iceberg secrets (includes catalog token,
/// s3 access key id and s3 secret access key)
const ICEBERG_SECRET_NAME_SUFFIX: &str = "iceberg";
/// Name of catalog token in the iceberg secret and its reference.
const ICEBERG_CATALOG_TOKEN_KEY_NAME: &str = "catalog-token";
/// Name of s3 acess key id in the iceberg secret and its reference.
const ICEBERG_S3_ACCESS_KEY_ID_KEY_NAME: &str = "s3-access-key-id";
/// Name of s3 acess key id in the iceberg secret and its reference.
const ICEBERG_S3_SECRET_ACCESS_KEY_KEY_NAME: &str = "s3-secret-access-key";
/// Secret name suffix for DuckLake secrets.
const DUCKLAKE_SECRET_NAME_SUFFIX: &str = "ducklake";
/// Name of catalog URL in the DuckLake secret and its reference.
const DUCKLAKE_CATALOG_URL_KEY_NAME: &str = "catalog-url";
/// Name of s3 access key id in the ducklake secret and its reference.
const DUCKLAKE_S3_ACCESS_KEY_ID_KEY_NAME: &str = "s3-access-key-id";
/// Name of s3 secret access key in the ducklake secret and its reference.
const DUCKLAKE_S3_SECRET_ACCESS_KEY_KEY_NAME: &str = "s3-secret-access-key";
/// Secret name suffix for Snowflake credentials.
const SNOWFLAKE_SECRET_NAME_SUFFIX: &str = "snowflake";
/// Name of the private key entry in the Snowflake secret and its reference.
const SNOWFLAKE_PRIVATE_KEY_NAME: &str = "private-key";
/// Name of the private key passphrase entry in the Snowflake secret and its
/// reference.
const SNOWFLAKE_PRIVATE_KEY_PASSPHRASE_NAME: &str = "private-key-passphrase";
/// Secret name suffix for the Postgres password.
const POSTGRES_SECRET_NAME_SUFFIX: &str = "postgres-password";
/// ConfigMap name suffix for the replicator configuration files.
const REPLICATOR_CONFIG_MAP_NAME_SUFFIX: &str = "replicator-config";
/// StatefulSet name suffix for the replicator workload.
const REPLICATOR_STATEFUL_SET_SUFFIX: &str = "replicator";
/// Previous StatefulSet suffix kept for existing pipeline cleanup/status.
const LEGACY_REPLICATOR_STATEFUL_SET_SUFFIX: &str = "replicator-stateful-set";
/// Application label suffix used to group resources.
const REPLICATOR_APP_SUFFIX: &str = "replicator-app";
/// Container name suffix for the replicator container.
const REPLICATOR_CONTAINER_NAME_SUFFIX: &str = "replicator";
/// Container name suffix for the Vector sidecar.
const VECTOR_CONTAINER_NAME_SUFFIX: &str = "vector";
/// Secret storing the Logflare API key.
const LOGFLARE_SECRET_NAME: &str = "replicator-logflare-api-key";
/// Name of the replicator container port that serves Prometheus metrics.
const REPLICATOR_METRICS_PORT_NAME: &str = "metrics";
/// Port the replicator listens on for Prometheus metrics.
const REPLICATOR_METRICS_PORT: i32 = 9000;
/// ConfigMap name containing the Vector configuration.
const VECTOR_CONFIG_MAP_NAME: &str = "replicator-vector-config";
/// Volume name for the replicator config file.
const REPLICATOR_CONFIG_FILE_VOLUME_NAME: &str = "replicator-config-file";
/// Volume name for the Vector config file.
const VECTOR_CONFIG_FILE_VOLUME_NAME: &str = "vector-config-file";
/// Secret storing the Sentry DSN.
const SENTRY_DSN_SECRET_NAME: &str = "replicator-sentry-dsn";
/// Secret storing the Supabase API key for error notifications.
const SUPABASE_API_KEY_SECRET_NAME: &str = "supabase-api-key";
/// Secret storing the ConfigCat API key for the replicator feature flags.
const CONFIGCAT_SDK_KEY: &str = "replicator-configcat-sdk-key";
/// EmptyDir volume name used to share logs.
const LOGS_VOLUME_NAME: &str = "logs";
/// Label used to identify replicator pods.
const REPLICATOR_APP_LABEL: &str = "etl-replicator-app";
/// Label used to identify DuckLake maintenance resources.
const DUCKLAKE_MAINTENANCE_APP_LABEL: &str = "etl-ducklake-maintenance-app";
/// Label that groups resources belonging to the same pipeline workload.
const APP_NAME_LABEL: &str = "etl.supabase.com/app-name";
/// Label that distinguishes replicator and maintenance workloads.
const APP_TYPE_LABEL: &str = "etl.supabase.com/app-type";
/// Tenant identity label used for observability and future ownership migration.
const TENANT_ID_LABEL: &str = "etl.supabase.com/tenant-id";
/// Pipeline identity label used for observability and future ownership
/// migration.
const PIPELINE_ID_LABEL: &str = "etl.supabase.com/pipeline-id";
/// Replicator runtime identity label.
const REPLICATOR_ID_LABEL: &str = "etl.supabase.com/replicator-id";
/// DuckLake maintenance CRD group.
const DUCKLAKE_MAINTENANCE_GROUP: &str = "etl.supabase.com";
/// DuckLake maintenance CRD version.
const DUCKLAKE_MAINTENANCE_VERSION: &str = "v1alpha1";
/// DuckLake maintenance CRD kind.
const DUCKLAKE_MAINTENANCE_KIND: &str = "DuckLakeMaintenance";
/// Vertical Pod Autoscaler CRD group.
const VERTICAL_POD_AUTOSCALER_GROUP: &str = "autoscaling.k8s.io";
/// Vertical Pod Autoscaler CRD version.
const VERTICAL_POD_AUTOSCALER_VERSION: &str = "v1";
/// Vertical Pod Autoscaler CRD kind.
const VERTICAL_POD_AUTOSCALER_KIND: &str = "VerticalPodAutoscaler";

/// Minimum Kubernetes CPU quantity emitted by the API, in millicores.
const MIN_K8S_CPU_MILLICORES: i32 = 1;
/// Minimum Kubernetes memory quantity emitted by the API, in Mi.
const MIN_K8S_MEMORY_MIB: i32 = 1;

/// Kubernetes resource settings for all containers in a replicator StatefulSet.
#[derive(Debug)]
struct ReplicatorStatefulSetResourcesConfig {
    replicator_memory_limit: String,
    replicator_memory_request: String,
    replicator_cpu_limit: String,
    replicator_cpu_request: String,
    vector_memory_limit: String,
    vector_memory_request: String,
    vector_cpu_limit: String,
    vector_cpu_request: String,
}

impl ReplicatorStatefulSetResourcesConfig {
    /// Builds StatefulSet resources from environment-specific test defaults.
    #[cfg(test)]
    fn for_environment(environment: &Environment) -> Result<Self, K8sError> {
        let k8s_config = test_k8s_config(environment);
        let default_replicator_resources =
            k8s_config.replicator_resources_for(DestinationKind::BigQuery);
        Self::from_default_resources(
            &default_replicator_resources,
            &k8s_config.replicator_autoscaling,
            &k8s_config.vector_resources,
            None,
        )
    }

    /// Builds StatefulSet resources from API defaults plus optional
    /// pipeline-level replicator resource overrides.
    ///
    /// Request precedence is pipeline override first, then the mandatory API
    /// default from `k8s.replicator_resources`. An explicit pipeline limit is
    /// treated as another allocation floor. The larger value is emitted as
    /// both request and limit, just as Vector limits match Vector requests, so
    /// every generated Pod qualifies for Kubernetes Guaranteed QoS.
    fn from_default_resources(
        default_replicator_resources: &ResolvedReplicatorResourcesConfig,
        autoscaling: &ReplicatorAutoscalingConfig,
        default_vector_resources: &DefaultVectorResourcesConfig,
        pipeline_replicator_resources: Option<&ReplicatorResourcesConfig>,
    ) -> Result<Self, K8sError> {
        let replicator_memory_request = pipeline_replicator_resources
            .and_then(|config| config.memory_request_mib)
            .unwrap_or(default_replicator_resources.memory_request_mib);
        let replicator_cpu_request = pipeline_replicator_resources
            .and_then(|config| config.cpu_request_millicores)
            .unwrap_or(default_replicator_resources.cpu_request_millicores);
        let vector_memory_request = clamp_k8s_resource_quantity(
            default_vector_resources.memory_request_mib,
            MIN_K8S_MEMORY_MIB,
        );
        let vector_cpu_request = clamp_k8s_resource_quantity(
            default_vector_resources.cpu_request_millicores,
            MIN_K8S_CPU_MILLICORES,
        );

        // Keep requests and limits equal even when a pipeline supplies a
        // larger historical limit. The replicator memory monitor reads the
        // container cgroup limit through sysinfo, so batch budgets and memory
        // backpressure scale with this single allocation value.
        let replicator_memory_limit = pipeline_replicator_resources
            .and_then(|config| config.memory_limit_mib)
            .unwrap_or(replicator_memory_request);
        let replicator_memory_allocation = replicator_memory_request
            .max(replicator_memory_limit)
            .clamp(autoscaling.min_memory_mib, autoscaling.max_memory_mib);
        let replicator_cpu_limit = pipeline_replicator_resources
            .and_then(|config| config.cpu_limit_millicores)
            .unwrap_or(replicator_cpu_request);
        let replicator_cpu_allocation = replicator_cpu_request
            .max(replicator_cpu_limit)
            .clamp(autoscaling.min_cpu_millicores, autoscaling.max_cpu_millicores);

        // Sidecars participate in pod QoS too, so Vector must also keep
        // limits equal to requests for the pod to stay Guaranteed.
        let vector_memory_limit = vector_memory_request;
        let vector_cpu_limit = vector_cpu_request;

        Ok(Self {
            replicator_memory_limit: format!("{replicator_memory_allocation}Mi"),
            replicator_memory_request: format!("{replicator_memory_allocation}Mi"),
            replicator_cpu_limit: format!("{replicator_cpu_allocation}m"),
            replicator_cpu_request: format!("{replicator_cpu_allocation}m"),
            vector_memory_limit: format!("{vector_memory_limit}Mi"),
            vector_memory_request: format!("{vector_memory_request}Mi"),
            vector_cpu_limit: format!("{vector_cpu_limit}m"),
            vector_cpu_request: format!("{vector_cpu_request}m"),
        })
    }
}

/// Clamps a Kubernetes resource quantity to the smallest value this API emits.
fn clamp_k8s_resource_quantity(value: i32, minimum: i32) -> i32 {
    value.max(minimum)
}

#[cfg(test)]
fn test_k8s_config(environment: &Environment) -> K8sConfig {
    let (memory_request_mib, cpu_request_millicores) = match environment {
        Environment::Prod => (500, 500),
        _ => (250, 125),
    };
    K8sConfig {
        replicator_namespace: "etl-data-plane".to_owned(),
        replicator_service_account_name: "etl-replicator".to_owned(),
        replicator_node_selectors: Default::default(),
        replicator_tolerations: Default::default(),
        replicator_resources: DefaultReplicatorResourcesConfig {
            memory_request_mib,
            cpu_request_millicores,
            destinations: Default::default(),
        },
        replicator_autoscaling: ReplicatorAutoscalingConfig::default(),
        vector_image: "timberio/vector:0.55.0-distroless-libc".to_owned(),
        vector_resources: DefaultVectorResourcesConfig {
            memory_request_mib: 192,
            cpu_request_millicores: 75,
        },
    }
}

/// HTTP-based implementation of [`K8sClient`].
///
/// The client is scoped to the configured replicator namespace and uses
/// server-side apply to keep resources in sync.
#[derive(Debug)]
pub struct HttpK8sClient {
    namespaces_api: Api<Namespace>,
    service_accounts_api: Api<ServiceAccount>,
    secrets_api: Api<Secret>,
    config_maps_api: Api<ConfigMap>,
    stateful_sets_api: Api<StatefulSet>,
    pods_api: Api<Pod>,
    ducklake_maintenance_api: Api<DynamicObject>,
    vertical_pod_autoscalers_api: Api<DynamicObject>,
    k8s_config: K8sConfig,
}

impl HttpK8sClient {
    /// Creates a new [`HttpK8sClient`] using the ambient Kubernetes config.
    ///
    /// Prefers in-cluster configuration and falls back to the local kubeconfig
    /// when running outside the cluster.
    pub fn new(client: Client, k8s_config: K8sConfig) -> Result<HttpK8sClient, K8sError> {
        let replicator_namespace = &k8s_config.replicator_namespace;
        let namespaces_api: Api<Namespace> = Api::all(client.clone());
        let service_accounts_api: Api<ServiceAccount> =
            Api::namespaced(client.clone(), replicator_namespace);
        let secrets_api: Api<Secret> = Api::namespaced(client.clone(), replicator_namespace);
        let config_maps_api: Api<ConfigMap> = Api::namespaced(client.clone(), replicator_namespace);
        let stateful_sets_api: Api<StatefulSet> =
            Api::namespaced(client.clone(), replicator_namespace);
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), replicator_namespace);
        let ducklake_maintenance_api: Api<DynamicObject> = Api::namespaced_with(
            client.clone(),
            replicator_namespace,
            &ducklake_maintenance_api_resource(),
        );
        let vertical_pod_autoscalers_api: Api<DynamicObject> = Api::namespaced_with(
            client,
            replicator_namespace,
            &vertical_pod_autoscaler_api_resource(),
        );

        Ok(HttpK8sClient {
            namespaces_api,
            service_accounts_api,
            secrets_api,
            config_maps_api,
            stateful_sets_api,
            pods_api,
            ducklake_maintenance_api,
            vertical_pod_autoscalers_api,
            k8s_config,
        })
    }

    /// Validates shared Kubernetes prerequisites required by replicators.
    ///
    /// This only checks resources owned outside per-pipeline reconciliation. It
    /// does not create or mutate cluster resources.
    pub async fn preflight(&self) -> Result<(), K8sPreflightError> {
        self.ensure_replicator_namespace().await?;
        self.ensure_replicator_service_account().await?;
        self.ensure_vertical_pod_autoscaler_api().await?;

        Ok(())
    }

    /// Ensures the replicator namespace exists before namespaced APIs are used.
    async fn ensure_replicator_namespace(&self) -> Result<(), K8sPreflightError> {
        let namespace = &self.k8s_config.replicator_namespace;
        match self.namespaces_api.get(namespace).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(err)) if err.code == 404 => {
                Err(K8sPreflightError::MissingReplicatorNamespace { namespace: namespace.clone() })
            }
            Err(source) => Err(K8sPreflightError::ReplicatorNamespaceCheck {
                namespace: namespace.clone(),
                source,
            }),
        }
    }

    /// Ensures replicator pods can be admitted with their configured identity.
    async fn ensure_replicator_service_account(&self) -> Result<(), K8sPreflightError> {
        let namespace = &self.k8s_config.replicator_namespace;
        let service_account_name = &self.k8s_config.replicator_service_account_name;
        match self.service_accounts_api.get(service_account_name).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(err)) if err.code == 404 => {
                Err(K8sPreflightError::MissingReplicatorServiceAccount {
                    namespace: namespace.clone(),
                    service_account_name: service_account_name.clone(),
                })
            }
            Err(source) => Err(K8sPreflightError::ReplicatorServiceAccountCheck {
                namespace: namespace.clone(),
                service_account_name: service_account_name.clone(),
                source,
            }),
        }
    }

    /// Ensures VPAs can be materialized before serving requests.
    async fn ensure_vertical_pod_autoscaler_api(&self) -> Result<(), K8sPreflightError> {
        let namespace = &self.k8s_config.replicator_namespace;
        match self.vertical_pod_autoscalers_api.list(&ListParams::default().limit(1)).await {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(err)) if err.code == 404 => {
                Err(K8sPreflightError::MissingVerticalPodAutoscalerApi)
            }
            Err(source) => Err(K8sPreflightError::VerticalPodAutoscalerApiCheck {
                namespace: namespace.clone(),
                source,
            }),
        }
    }

    /// Helper function to handle delete operations that should ignore 404
    /// errors but propagate other errors.
    fn handle_delete_with_404_ignore<T>(
        delete_result: Result<T, kube::Error>,
    ) -> Result<(), K8sError> {
        match delete_result {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(err)) if err.code == 404 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Returns true if the replicator container in the pod has terminated with
    /// error code
    fn has_replicator_container_error(pod: &Pod, replicator_container_name: &str) -> bool {
        // Find the replicator container status
        let container_status = pod.status.as_ref().and_then(|status| {
            status.container_statuses.as_ref().and_then(|container_statuses| {
                container_statuses.iter().find(|cs| cs.name == replicator_container_name).cloned()
            })
        });

        let Some(container_status) = container_status else {
            return false;
        };

        let Some(state) = &container_status.state else {
            return false;
        };

        // Currently terminated with non-zero exit code.
        if let Some(terminated) = &state.terminated {
            return terminated.exit_code != 0;
        }

        // Waiting state, we want to distinguish normal waiting reasons from abnormal
        // ones.
        if let Some(waiting) = &state.waiting
            && let Some(reason) = &waiting.reason
        {
            match reason.as_str() {
                // Crash/restart errors
                "CrashLoopBackOff" => return true,

                // Image-related errors (6 predefined in kubelet)
                "ImagePullBackOff"
                | "ErrImagePull"
                | "ErrImageNeverPull"
                | "InvalidImageName"
                | "ImageInspectError"
                | "RegistryUnavailable" => return true,

                // Container creation errors
                "CreateContainerConfigError" | "CreateContainerError" | "RunContainerError" => {
                    return true;
                }
                _ => {}
            }
        }

        false
    }
}

/// Errors found while checking Kubernetes prerequisites at startup.
#[derive(Debug, Error)]
pub enum K8sPreflightError {
    /// The namespace used for replicator resources does not exist.
    #[error(
        "Kubernetes replicator namespace `{namespace}` is missing. Create it before starting \
         etl-api."
    )]
    MissingReplicatorNamespace {
        /// Configured replicator namespace.
        namespace: String,
    },
    /// Checking the replicator namespace failed.
    #[error("Failed to check Kubernetes replicator namespace `{namespace}`")]
    ReplicatorNamespaceCheck {
        /// Configured replicator namespace.
        namespace: String,
        /// Kubernetes API error.
        #[source]
        source: kube::Error,
    },
    /// The ServiceAccount used by replicator pods does not exist.
    #[error(
        "Kubernetes ServiceAccount `{service_account_name}` is missing in namespace \
         `{namespace}`. Create it before starting etl-api so replicator pods can be admitted."
    )]
    MissingReplicatorServiceAccount {
        /// Configured replicator namespace.
        namespace: String,
        /// Configured replicator ServiceAccount name.
        service_account_name: String,
    },
    /// Checking the ServiceAccount failed.
    #[error(
        "Failed to check Kubernetes ServiceAccount `{service_account_name}` in namespace \
         `{namespace}`"
    )]
    ReplicatorServiceAccountCheck {
        /// Configured replicator namespace.
        namespace: String,
        /// Configured replicator ServiceAccount name.
        service_account_name: String,
        /// Kubernetes API error.
        #[source]
        source: kube::Error,
    },
    /// The VPA CustomResourceDefinition is not registered in the cluster.
    #[error(
        "Kubernetes VerticalPodAutoscaler v1 API is missing. Install the VPA CRDs before starting \
         etl-api."
    )]
    MissingVerticalPodAutoscalerApi,
    /// Checking the namespaced VPA API failed.
    #[error(
        "Failed to check the Kubernetes VerticalPodAutoscaler v1 API in namespace `{namespace}`"
    )]
    VerticalPodAutoscalerApiCheck {
        /// Configured replicator namespace.
        namespace: String,
        /// Kubernetes API error.
        #[source]
        source: kube::Error,
    },
}

#[async_trait]
impl K8sClient for HttpK8sClient {
    async fn create_or_update_postgres_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        postgres_password: &str,
    ) -> Result<(), K8sError> {
        debug!("patching postgres secret");

        let encoded_postgres_password = BASE64_STANDARD.encode(postgres_password);
        let postgres_secret_name = create_postgres_secret_name(resource_prefix);
        let replicator_app_name = create_replicator_app_name(resource_prefix);
        let postgres_secret_json = create_postgres_secret_json(
            &self.k8s_config,
            &postgres_secret_name,
            &replicator_app_name,
            identity,
            &encoded_postgres_password,
        );
        let secret: Secret = serde_json::from_value(postgres_secret_json)?;

        // We are forcing the update since we are the field manager that should own the
        // fields. If there is an override (likely during an incident or SREs
        // intervention), we want to override their changes. The API database is
        // the source of truth for credentials.
        let pp = PatchParams::apply(FIELD_MANAGER).force();
        self.secrets_api.patch(&postgres_secret_name, &pp, &Patch::Apply(secret)).await?;

        Ok(())
    }

    async fn create_or_update_bigquery_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError> {
        debug!("patching bq secret");

        let encoded_bq_service_account_key = BASE64_STANDARD.encode(bq_service_account_key);
        let bq_secret_name = create_bq_secret_name(resource_prefix);
        let replicator_app_name = create_replicator_app_name(resource_prefix);
        let bq_secret_json = create_bq_service_account_key_secret_json(
            &self.k8s_config,
            &bq_secret_name,
            &replicator_app_name,
            identity,
            &encoded_bq_service_account_key,
        );
        let secret: Secret = serde_json::from_value(bq_secret_json)?;

        // We are forcing the update since we are the field manager that should own the
        // fields. If there is an override (likely during an incident or SREs
        // intervention), we want to override their changes. The API database is
        // the source of truth for credentials.
        let pp = PatchParams::apply(FIELD_MANAGER).force();
        self.secrets_api.patch(&bq_secret_name, &pp, &Patch::Apply(secret)).await?;

        Ok(())
    }

    async fn create_or_update_clickhouse_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        password: Option<&str>,
    ) -> Result<(), K8sError> {
        debug!("patching clickhouse secret");

        if let Some(password) = password {
            let clickhouse_secret_name = create_clickhouse_secret_name(resource_prefix);
            let replicator_app_name = create_replicator_app_name(resource_prefix);
            let secret = create_clickhouse_password_secret(
                &self.k8s_config,
                &clickhouse_secret_name,
                &replicator_app_name,
                identity,
                password,
            );

            // We are forcing the update since we are the field manager that should own the
            // fields. If there is an override (likely during an incident or
            // SREs intervention), we want to override their changes. The API
            // database is the source of truth for credentials.
            let pp = PatchParams::apply(FIELD_MANAGER).force();
            self.secrets_api.patch(&clickhouse_secret_name, &pp, &Patch::Apply(secret)).await?;
        }

        Ok(())
    }

    async fn create_or_update_iceberg_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        catalog_token: &str,
        s3_access_key_id: &str,
        s3_secret_access_key: &str,
    ) -> Result<(), K8sError> {
        debug!("patching iceberg secret");

        let encoded_catalog_token = BASE64_STANDARD.encode(catalog_token);
        let encoded_s3_access_key_id = BASE64_STANDARD.encode(s3_access_key_id);
        let encoded_s3_secret_access_key = BASE64_STANDARD.encode(s3_secret_access_key);

        let iceberg_secret_name = create_iceberg_secret_name(resource_prefix);
        let replicator_app_name = create_replicator_app_name(resource_prefix);
        let iceberg_secret_json = create_iceberg_secret_json(
            &self.k8s_config,
            &iceberg_secret_name,
            &replicator_app_name,
            identity,
            &encoded_catalog_token,
            &encoded_s3_access_key_id,
            &encoded_s3_secret_access_key,
        );
        let secret: Secret = serde_json::from_value(iceberg_secret_json)?;

        // We are forcing the update since we are the field manager that should own the
        // fields. If there is an override (likely during an incident or SREs
        // intervention), we want to override their changes. The API database is
        // the source of truth for credentials.
        let pp = PatchParams::apply(FIELD_MANAGER).force();
        self.secrets_api.patch(&iceberg_secret_name, &pp, &Patch::Apply(secret)).await?;

        Ok(())
    }

    async fn create_or_update_ducklake_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        catalog_url: &str,
        s3_access_key_id: &str,
        s3_secret_access_key: &str,
    ) -> Result<(), K8sError> {
        debug!("patching ducklake secret");

        let encoded_catalog_url = BASE64_STANDARD.encode(catalog_url);
        let encoded_s3_access_key_id = BASE64_STANDARD.encode(s3_access_key_id);
        let encoded_s3_secret_access_key = BASE64_STANDARD.encode(s3_secret_access_key);

        let ducklake_secret_name = create_ducklake_secret_name(resource_prefix);
        let replicator_app_name = create_replicator_app_name(resource_prefix);
        let ducklake_secret_json = create_ducklake_secret_json(
            &self.k8s_config,
            &ducklake_secret_name,
            &replicator_app_name,
            identity,
            &encoded_catalog_url,
            &encoded_s3_access_key_id,
            &encoded_s3_secret_access_key,
        );
        let secret: Secret = serde_json::from_value(ducklake_secret_json)?;

        let pp = PatchParams::apply(FIELD_MANAGER).force();
        self.secrets_api.patch(&ducklake_secret_name, &pp, &Patch::Apply(secret)).await?;

        Ok(())
    }

    async fn delete_postgres_secret(&self, resource_prefix: &str) -> Result<(), K8sError> {
        debug!("deleting postgres secret");

        let postgres_secret_name = create_postgres_secret_name(resource_prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.secrets_api.delete(&postgres_secret_name, &dp).await,
        )?;

        Ok(())
    }

    async fn delete_clickhouse_secret(&self, resource_prefix: &str) -> Result<(), K8sError> {
        debug!("deleting clickhouse secret");

        let clickhouse_secret_name = create_clickhouse_secret_name(resource_prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.secrets_api.delete(&clickhouse_secret_name, &dp).await,
        )?;

        Ok(())
    }

    async fn delete_bigquery_secret(&self, resource_prefix: &str) -> Result<(), K8sError> {
        debug!("deleting bq secret");

        let bq_secret_name = create_bq_secret_name(resource_prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(self.secrets_api.delete(&bq_secret_name, &dp).await)?;

        Ok(())
    }

    async fn delete_iceberg_secret(&self, resource_prefix: &str) -> Result<(), K8sError> {
        debug!("deleting iceberg secret");

        let iceberg_secret_name = create_iceberg_secret_name(resource_prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.secrets_api.delete(&iceberg_secret_name, &dp).await,
        )?;

        Ok(())
    }

    async fn delete_ducklake_secret(&self, resource_prefix: &str) -> Result<(), K8sError> {
        debug!("deleting ducklake secret");

        let ducklake_secret_name = create_ducklake_secret_name(resource_prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.secrets_api.delete(&ducklake_secret_name, &dp).await,
        )?;

        Ok(())
    }

    async fn create_or_update_snowflake_secret(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        private_key: &str,
        private_key_passphrase: Option<&str>,
    ) -> Result<(), K8sError> {
        debug!("patching snowflake secret");

        let encoded_private_key = BASE64_STANDARD.encode(private_key);
        let encoded_passphrase = private_key_passphrase.map(|p| BASE64_STANDARD.encode(p));

        let snowflake_secret_name = create_snowflake_secret_name(resource_prefix);
        let replicator_app_name = create_replicator_app_name(resource_prefix);
        let snowflake_secret_json = create_snowflake_secret_json(
            &self.k8s_config,
            &snowflake_secret_name,
            &replicator_app_name,
            identity,
            &encoded_private_key,
            encoded_passphrase.as_deref(),
        );
        let secret: Secret = serde_json::from_value(snowflake_secret_json)?;

        let pp = PatchParams::apply(FIELD_MANAGER).force();
        self.secrets_api.patch(&snowflake_secret_name, &pp, &Patch::Apply(secret)).await?;

        Ok(())
    }

    async fn delete_snowflake_secret(&self, resource_prefix: &str) -> Result<(), K8sError> {
        debug!("deleting snowflake secret");

        let snowflake_secret_name = create_snowflake_secret_name(resource_prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.secrets_api.delete(&snowflake_secret_name, &dp).await,
        )?;

        Ok(())
    }

    async fn create_or_update_replicator_config_map(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        files: Vec<ReplicatorConfigMapFile>,
    ) -> Result<(), K8sError> {
        debug!("patching config map");

        let replicator_config_map_name = create_replicator_config_map_name(resource_prefix);
        let replicator_app_name = create_replicator_app_name(resource_prefix);

        let config_map_json = create_replicator_config_map_json(
            &self.k8s_config,
            &replicator_config_map_name,
            &replicator_app_name,
            identity,
            files,
        );
        let config_map: ConfigMap = serde_json::from_value(config_map_json)?;

        // We are forcing the update since we are the field manager that should own the
        // fields. If there is an override (likely during an incident or SREs
        // intervention), we want to override their changes. The API database is
        // the source of truth for configuration.
        let pp = PatchParams::apply(FIELD_MANAGER).force();
        self.config_maps_api
            .patch(&replicator_config_map_name, &pp, &Patch::Apply(config_map))
            .await?;

        Ok(())
    }

    async fn delete_replicator_config_map(&self, resource_prefix: &str) -> Result<(), K8sError> {
        debug!("deleting config map");

        let replicator_config_map_name = create_replicator_config_map_name(resource_prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.config_maps_api.delete(&replicator_config_map_name, &dp).await,
        )?;

        Ok(())
    }

    async fn create_or_update_replicator_stateful_set(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        request: ReplicatorStatefulSetConfig,
    ) -> Result<(), K8sError> {
        debug!("patching stateful set");

        let replicator_image = request.replicator_image.as_str();
        let default_replicator_resources =
            self.k8s_config.replicator_resources_for(request.destination_type.kind());
        let stateful_set_resources = ReplicatorStatefulSetResourcesConfig::from_default_resources(
            &default_replicator_resources,
            &self.k8s_config.replicator_autoscaling,
            &self.k8s_config.vector_resources,
            request.replicator_resources.as_ref(),
        )?;

        let stateful_set_name = create_stateful_set_name(resource_prefix);
        let legacy_stateful_set_name = create_legacy_stateful_set_name(resource_prefix);
        if legacy_stateful_set_name != stateful_set_name {
            let dp = DeleteParams::default();
            Self::handle_delete_with_404_ignore(
                self.stateful_sets_api.delete(&legacy_stateful_set_name, &dp).await,
            )?;
        }

        let environment = Environment::load().map_err(K8sError::Config)?;
        let container_environment = create_container_environment_json(
            &self.k8s_config,
            resource_prefix,
            &environment,
            replicator_image,
            request.destination_type,
            request.ducklake_maintenance.as_ref(),
            request.log_level,
        );

        let node_selector = node_selector_json(&self.k8s_config.replicator_node_selectors);
        let tolerations = tolerations_json(&self.k8s_config.replicator_tolerations);
        let init_containers = create_init_containers_json(
            &self.k8s_config,
            resource_prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(resource_prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &self.k8s_config,
            resource_prefix,
            identity,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            tolerations,
            init_containers,
            volumes,
            volume_mounts,
            &stateful_set_resources,
        );

        let stateful_set: StatefulSet = serde_json::from_value(stateful_set_json)?;

        // We are forcing the update since we are the field manager that should own the
        // fields. If there is an override (likely during an incident or SREs
        // intervention), we want to override their changes.
        let pp = PatchParams::apply(FIELD_MANAGER).force();
        self.stateful_sets_api.patch(&stateful_set_name, &pp, &Patch::Apply(stateful_set)).await?;

        Ok(())
    }

    async fn create_or_update_replicator_vertical_pod_autoscaler(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
    ) -> Result<(), K8sError> {
        let name = create_stateful_set_name(resource_prefix);
        let initial_update_mode =
            self.k8s_config.replicator_autoscaling.initial_update_mode.as_k8s_value();
        let existing_vertical_pod_autoscaler =
            self.vertical_pod_autoscalers_api.get_opt(&name).await?;
        let update_mode = existing_vertical_pod_autoscaler
            .as_ref()
            .and_then(vpa_update_mode)
            .unwrap_or(initial_update_mode);

        debug!(vpa = %name, update_mode, "creating or updating vertical pod autoscaler");

        let vertical_pod_autoscaler = create_replicator_vertical_pod_autoscaler_json(
            &self.k8s_config,
            resource_prefix,
            identity,
            &name,
            update_mode,
        )?;

        // We are forcing the update since we are the field manager that should own the
        // fields. If there is an override (likely during an incident or SREs
        // intervention), we want to override their changes.
        let pp = PatchParams::apply(FIELD_MANAGER).force();
        self.vertical_pod_autoscalers_api
            .patch(&name, &pp, &Patch::Apply(vertical_pod_autoscaler))
            .await?;

        Ok(())
    }

    async fn delete_replicator_stateful_set(&self, resource_prefix: &str) -> Result<(), K8sError> {
        debug!("deleting stateful set");

        let dp = DeleteParams::default();
        for stateful_set_name in stateful_set_names_for_lookup(resource_prefix) {
            Self::handle_delete_with_404_ignore(
                self.stateful_sets_api.delete(&stateful_set_name, &dp).await,
            )?;
        }

        Ok(())
    }

    async fn delete_replicator_vertical_pod_autoscaler(
        &self,
        resource_prefix: &str,
    ) -> Result<(), K8sError> {
        debug!("deleting vertical pod autoscaler");

        let name = create_stateful_set_name(resource_prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.vertical_pod_autoscalers_api.delete(&name, &dp).await,
        )?;

        Ok(())
    }

    async fn replicator_stateful_set_exists(
        &self,
        resource_prefix: &str,
    ) -> Result<bool, K8sError> {
        debug!("checking stateful set existence");

        for stateful_set_name in stateful_set_names_for_lookup(resource_prefix) {
            match self.stateful_sets_api.get(&stateful_set_name).await {
                Ok(_) => return Ok(true),
                Err(kube::Error::Api(err)) if err.code == 404 => {}
                Err(e) => return Err(e.into()),
            }
        }

        Ok(false)
    }

    async fn create_or_update_ducklake_maintenance(
        &self,
        resource_prefix: &str,
        identity: &PipelineRuntimeIdentity,
        config: DuckLakeMaintenanceResourceConfig,
    ) -> Result<(), K8sError> {
        debug!("patching ducklake maintenance");

        let name = create_ducklake_maintenance_name(resource_prefix);
        let ducklake_maintenance_json = create_ducklake_maintenance_json(
            &self.k8s_config,
            resource_prefix,
            &name,
            identity,
            config,
        );
        let pp = PatchParams::apply(FIELD_MANAGER).force();
        self.ducklake_maintenance_api
            .patch(&name, &pp, &Patch::Apply(ducklake_maintenance_json))
            .await?;

        Ok(())
    }

    async fn delete_ducklake_maintenance(&self, resource_prefix: &str) -> Result<(), K8sError> {
        debug!("deleting ducklake maintenance");

        let name = create_ducklake_maintenance_name(resource_prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.ducklake_maintenance_api.delete(&name, &dp).await,
        )?;

        Ok(())
    }

    async fn get_replicator_pod_status(
        &self,
        resource_prefix: &str,
    ) -> Result<PodStatus, K8sError> {
        debug!("getting pod status");

        let mut pod = None;
        for pod_name in pod_names_for_status(resource_prefix) {
            match self.pods_api.get(&pod_name).await {
                Ok(found_pod) => {
                    pod = Some(found_pod);
                    break;
                }
                Err(kube::Error::Api(err)) if err.code == 404 => {}
                Err(e) => return Err(e.into()),
            }
        }
        let Some(pod) = pod else {
            return Ok(PodStatus::Stopped);
        };

        let replicator_container_name = create_replicator_container_name(resource_prefix);

        if Self::has_replicator_container_error(&pod, &replicator_container_name) {
            return Ok(PodStatus::Failed);
        }

        if pod.metadata.deletion_timestamp.is_some() {
            return Ok(PodStatus::Stopping);
        }

        let phase = pod.status.map_or(PodPhase::Unknown, |status| {
            let phase: PodPhase = status.phase.map_or(PodPhase::Unknown, |phase| {
                let phase: PodPhase = phase.as_str().into();
                phase
            });
            phase
        });

        Ok(match phase {
            PodPhase::Pending => PodStatus::Starting,
            PodPhase::Running => PodStatus::Started,
            PodPhase::Succeeded => PodStatus::Stopped,
            PodPhase::Failed => PodStatus::Failed,
            PodPhase::Unknown => PodStatus::Unknown,
        })
    }
}

fn create_postgres_secret_name(prefix: &str) -> String {
    format!("{prefix}-{POSTGRES_SECRET_NAME_SUFFIX}")
}

fn create_bq_secret_name(prefix: &str) -> String {
    format!("{prefix}-{BQ_SECRET_NAME_SUFFIX}")
}

fn create_iceberg_secret_name(prefix: &str) -> String {
    format!("{prefix}-{ICEBERG_SECRET_NAME_SUFFIX}")
}

fn create_clickhouse_secret_name(prefix: &str) -> String {
    format!("{prefix}-{CLICKHOUSE_SECRET_NAME_SUFFIX}")
}

fn create_ducklake_secret_name(prefix: &str) -> String {
    format!("{prefix}-{DUCKLAKE_SECRET_NAME_SUFFIX}")
}

fn create_snowflake_secret_name(prefix: &str) -> String {
    format!("{prefix}-{SNOWFLAKE_SECRET_NAME_SUFFIX}")
}

fn create_ducklake_maintenance_name(prefix: &str) -> String {
    prefix.to_owned()
}

fn create_replicator_config_map_name(prefix: &str) -> String {
    format!("{prefix}-{REPLICATOR_CONFIG_MAP_NAME_SUFFIX}")
}

fn create_stateful_set_name(prefix: &str) -> String {
    format!("{prefix}-{REPLICATOR_STATEFUL_SET_SUFFIX}")
}

fn create_legacy_stateful_set_name(prefix: &str) -> String {
    format!("{prefix}-{LEGACY_REPLICATOR_STATEFUL_SET_SUFFIX}")
}

fn create_pod_name(prefix: &str) -> String {
    format!("{prefix}-{REPLICATOR_STATEFUL_SET_SUFFIX}-0")
}

fn create_legacy_pod_name(prefix: &str) -> String {
    format!("{prefix}-{LEGACY_REPLICATOR_STATEFUL_SET_SUFFIX}-0")
}

fn unique_current_and_legacy_names(current: String, legacy: String) -> Vec<String> {
    if current == legacy { vec![current] } else { vec![current, legacy] }
}

fn stateful_set_names_for_lookup(prefix: &str) -> Vec<String> {
    unique_current_and_legacy_names(
        create_stateful_set_name(prefix),
        create_legacy_stateful_set_name(prefix),
    )
}

fn pod_names_for_status(prefix: &str) -> Vec<String> {
    unique_current_and_legacy_names(create_pod_name(prefix), create_legacy_pod_name(prefix))
}

fn create_replicator_app_name(prefix: &str) -> String {
    format!("{prefix}-{REPLICATOR_APP_SUFFIX}")
}

fn ducklake_maintenance_api_resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind::gvk(
        DUCKLAKE_MAINTENANCE_GROUP,
        DUCKLAKE_MAINTENANCE_VERSION,
        DUCKLAKE_MAINTENANCE_KIND,
    ))
}

fn vertical_pod_autoscaler_api_resource() -> ApiResource {
    ApiResource::from_gvk(&GroupVersionKind::gvk(
        VERTICAL_POD_AUTOSCALER_GROUP,
        VERTICAL_POD_AUTOSCALER_VERSION,
        VERTICAL_POD_AUTOSCALER_KIND,
    ))
}

fn create_replicator_container_name(prefix: &str) -> String {
    format!("{prefix}-{REPLICATOR_CONTAINER_NAME_SUFFIX}")
}

fn create_vector_container_name(prefix: &str) -> String {
    format!("{prefix}-{VECTOR_CONTAINER_NAME_SUFFIX}")
}

fn create_app_selector_labels(app_name: &str, app_type: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (APP_NAME_LABEL.to_owned(), app_name.to_owned()),
        (APP_TYPE_LABEL.to_owned(), app_type.to_owned()),
    ])
}

fn create_app_identity_labels(
    app_name: &str,
    app_type: &str,
    identity: &PipelineRuntimeIdentity,
) -> BTreeMap<String, String> {
    let mut labels = create_app_selector_labels(app_name, app_type);
    labels.insert(TENANT_ID_LABEL.to_owned(), identity.tenant_id.clone());
    labels.insert(PIPELINE_ID_LABEL.to_owned(), identity.pipeline_id.to_string());
    labels.insert(REPLICATOR_ID_LABEL.to_owned(), identity.replicator_id.to_string());
    labels
}

fn create_replicator_identity_labels(
    replicator_app_name: &str,
    identity: &PipelineRuntimeIdentity,
) -> BTreeMap<String, String> {
    create_app_identity_labels(replicator_app_name, REPLICATOR_APP_LABEL, identity)
}

fn create_postgres_secret_json(
    k8s_config: &K8sConfig,
    secret_name: &str,
    replicator_app_name: &str,
    identity: &PipelineRuntimeIdentity,
    encoded_postgres_password: &str,
) -> serde_json::Value {
    json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": secret_name,
        "namespace": &k8s_config.replicator_namespace,
        "labels": create_replicator_identity_labels(replicator_app_name, identity)
      },
      "type": "Opaque",
      "data": {
        "password": encoded_postgres_password,
      }
    })
}

fn create_clickhouse_password_secret(
    k8s_config: &K8sConfig,
    secret_name: &str,
    replicator_app_name: &str,
    identity: &PipelineRuntimeIdentity,
    clickhouse_password: &str,
) -> Secret {
    Secret {
        metadata: ObjectMeta {
            name: Some(secret_name.to_owned()),
            namespace: Some(k8s_config.replicator_namespace.clone()),
            labels: Some(create_replicator_identity_labels(replicator_app_name, identity)),
            ..ObjectMeta::default()
        },
        type_: Some("Opaque".to_owned()),
        string_data: Some(BTreeMap::from([(
            CLICKHOUSE_PASSWORD_NAME.to_owned(),
            clickhouse_password.to_owned(),
        )])),
        ..Secret::default()
    }
}

fn create_snowflake_secret_json(
    k8s_config: &K8sConfig,
    secret_name: &str,
    replicator_app_name: &str,
    identity: &PipelineRuntimeIdentity,
    encoded_private_key: &str,
    encoded_private_key_passphrase: Option<&str>,
) -> serde_json::Value {
    let mut data = serde_json::Map::new();
    data.insert(
        SNOWFLAKE_PRIVATE_KEY_NAME.to_owned(),
        serde_json::Value::String(encoded_private_key.to_owned()),
    );
    if let Some(encoded_passphrase) = encoded_private_key_passphrase {
        data.insert(
            SNOWFLAKE_PRIVATE_KEY_PASSPHRASE_NAME.to_owned(),
            serde_json::Value::String(encoded_passphrase.to_owned()),
        );
    }
    json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": secret_name,
        "namespace": &k8s_config.replicator_namespace,
        "labels": create_replicator_identity_labels(replicator_app_name, identity)
      },
      "type": "Opaque",
      "data": data
    })
}

fn create_bq_service_account_key_secret_json(
    k8s_config: &K8sConfig,
    secret_name: &str,
    replicator_app_name: &str,
    identity: &PipelineRuntimeIdentity,
    encoded_bq_service_account_key: &str,
) -> serde_json::Value {
    json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": secret_name,
        "namespace": &k8s_config.replicator_namespace,
        "labels": create_replicator_identity_labels(replicator_app_name, identity)
      },
      "type": "Opaque",
      "data": {
        BQ_SERVICE_ACCOUNT_KEY_NAME: encoded_bq_service_account_key,
      }
    })
}

fn create_iceberg_secret_json(
    k8s_config: &K8sConfig,
    secret_name: &str,
    replicator_app_name: &str,
    identity: &PipelineRuntimeIdentity,
    encoded_catalog_token: &str,
    encoded_s3_access_key_id: &str,
    encoded_s3_secret_access_key: &str,
) -> serde_json::Value {
    json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": secret_name,
        "namespace": &k8s_config.replicator_namespace,
        "labels": create_replicator_identity_labels(replicator_app_name, identity)
      },
      "type": "Opaque",
      "data": {
        ICEBERG_CATALOG_TOKEN_KEY_NAME: encoded_catalog_token,
        ICEBERG_S3_ACCESS_KEY_ID_KEY_NAME: encoded_s3_access_key_id,
        ICEBERG_S3_SECRET_ACCESS_KEY_KEY_NAME: encoded_s3_secret_access_key
      }
    })
}

fn create_ducklake_secret_json(
    k8s_config: &K8sConfig,
    secret_name: &str,
    replicator_app_name: &str,
    identity: &PipelineRuntimeIdentity,
    encoded_catalog_url: &str,
    encoded_s3_access_key_id: &str,
    encoded_s3_secret_access_key: &str,
) -> serde_json::Value {
    json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": secret_name,
        "namespace": &k8s_config.replicator_namespace,
        "labels": create_replicator_identity_labels(replicator_app_name, identity)
      },
      "type": "Opaque",
      "data": {
        DUCKLAKE_CATALOG_URL_KEY_NAME: encoded_catalog_url,
        DUCKLAKE_S3_ACCESS_KEY_ID_KEY_NAME: encoded_s3_access_key_id,
        DUCKLAKE_S3_SECRET_ACCESS_KEY_KEY_NAME: encoded_s3_secret_access_key
      }
    })
}

fn create_replicator_config_map_json(
    k8s_config: &K8sConfig,
    config_map_name: &str,
    replicator_app_name: &str,
    identity: &PipelineRuntimeIdentity,
    files: Vec<ReplicatorConfigMapFile>,
) -> serde_json::Value {
    let mut data = serde_json::Map::new();
    for file in files {
        data.insert(file.filename, serde_json::Value::String(file.content));
    }

    json!({
      "kind": "ConfigMap",
      "apiVersion": "v1",
      "metadata": {
        "name": config_map_name,
        "namespace": &k8s_config.replicator_namespace,
        "labels": create_replicator_identity_labels(replicator_app_name, identity)
      },
      "data": data
    })
}

fn create_ducklake_maintenance_json(
    k8s_config: &K8sConfig,
    prefix: &str,
    name: &str,
    identity: &PipelineRuntimeIdentity,
    config: DuckLakeMaintenanceResourceConfig,
) -> serde_json::Value {
    let replicator_app_name = create_replicator_app_name(prefix);
    let postgres_secret_name = create_postgres_secret_name(prefix);
    let ducklake_secret_name = create_ducklake_secret_name(prefix);
    let config_map_name = create_replicator_config_map_name(prefix);
    json!({
      "apiVersion": format!("{DUCKLAKE_MAINTENANCE_GROUP}/{DUCKLAKE_MAINTENANCE_VERSION}"),
      "kind": DUCKLAKE_MAINTENANCE_KIND,
      "metadata": {
        "name": name,
        "namespace": &k8s_config.replicator_namespace,
        "labels": create_app_identity_labels(
          &replicator_app_name,
          DUCKLAKE_MAINTENANCE_APP_LABEL,
          identity
        )
      },
      "spec": {
        "pipelineRef": {
          "tenantId": identity.tenant_id,
          "pipelineId": identity.pipeline_id,
          "replicatorId": identity.replicator_id,
        },
        "schedule": {
          "minIntervalSeconds": config.policy.min_interval_seconds,
        },
        "pause": {
          "maxDurationSeconds": config.policy.max_pause_seconds,
        },
        "operations": {
          "inlineFlush": {
            "enabled": config.policy.operation_policy.inline_flush_enabled,
            "minInlinedBytes": config.policy.min_inlined_bytes,
          },
          "mergeAdjacentFiles": {
            "enabled": config.policy.operation_policy.merge_adjacent_files_enabled,
            "maxCompactedFiles": config.policy.max_compacted_files,
            "maxTablesPerRun": config.policy.max_tables_per_run,
            "targetFileSize": config.policy.target_file_size,
          },
          "rewriteDataFiles": {
            "enabled": config.policy.operation_policy.rewrite_data_files_enabled,
            "deleteThreshold": config.policy.delete_threshold,
            "maxTablesPerRun": config.policy.max_tables_per_run,
          },
          "expireSnapshots": {
            "enabled": config.policy.operation_policy.expire_snapshots_enabled,
          },
          "cleanupOldFiles": {
            "enabled": config.policy.operation_policy.cleanup_old_files_enabled,
          }
        },
        "jobTemplate": {
          "image": config.image,
          "cpuRequestMillicores": config.policy.cpu_request_millicores,
          "memoryRequestMiB": config.policy.memory_request_mib,
          "activeDeadlineSeconds": config.policy.active_deadline_seconds,
          "backoffLimit": 1,
          "ttlSecondsAfterFinished": 86400,
        },
        "runtimeRefs": {
          "configMapName": config_map_name,
          "postgresSecretName": postgres_secret_name,
          "ducklakeSecretName": ducklake_secret_name,
        }
      }
    })
}

fn create_container_environment_json(
    k8s_config: &K8sConfig,
    prefix: &str,
    environment: &Environment,
    replicator_image: &str,
    destination_type: DestinationType,
    ducklake_maintenance: Option<&DuckLakeMaintenanceConfig>,
    log_level: LogLevel,
) -> Vec<serde_json::Value> {
    let mut container_environment = vec![
        json!({
          "name": "APP_ENVIRONMENT",
          "value": environment.to_string()
        }),
        json!({
            "name": "APP_VERSION",
            //TODO: set APP_VERSION to proper version instead of the replicator image name
            "value": replicator_image
        }),
        json!({
            "name": "RUST_LOG",
            "value": log_level.to_string()
        }),
    ];

    if matches!(environment, Environment::Dev | Environment::Staging) {
        container_environment.push(json!({
            "name": "RUST_BACKTRACE",
            "value": "1"
        }));
    }

    match environment {
        Environment::Dev => {
            // We do not configure sentry for dev environments
        }
        Environment::Staging | Environment::Prod => {
            container_environment.push(json!({
              "name": "APP_SENTRY__DSN",
              "valueFrom": {
                "secretKeyRef": {
                  "name": SENTRY_DSN_SECRET_NAME,
                  "key": "dsn",
                  "optional": true
                }
              }
            }));
            container_environment.push(json!({
              "name": "APP_SUPABASE__API_KEY",
              "valueFrom": {
                "secretKeyRef": {
                  "name": SUPABASE_API_KEY_SECRET_NAME,
                  "key": "key",
                  "optional": true
                }
              }
            }));
            container_environment.push(json!({
              "name": "APP_SUPABASE__CONFIGCAT_SDK_KEY",
              "valueFrom": {
                "secretKeyRef": {
                  "name": CONFIGCAT_SDK_KEY,
                  "key": "key",
                  "optional": true
                }
              }
            }));
        }
    }

    match destination_type {
        DestinationType::BigQuery => {
            let postgres_secret_name = create_postgres_secret_name(prefix);
            let postgres_secret_env_var_json =
                create_postgres_secret_env_var_json(&postgres_secret_name);
            container_environment.push(postgres_secret_env_var_json);

            let bq_secret_name = create_bq_secret_name(prefix);
            let bq_secret_env_var_json = create_bq_secret_env_var_json(&bq_secret_name);
            container_environment.push(bq_secret_env_var_json);
        }
        DestinationType::ClickHouse { password_secret_required } => {
            let postgres_secret_name = create_postgres_secret_name(prefix);
            let postgres_secret_env_var_json =
                create_postgres_secret_env_var_json(&postgres_secret_name);
            container_environment.push(postgres_secret_env_var_json);

            if password_secret_required {
                let clickhouse_secret_name = create_clickhouse_secret_name(prefix);
                let clickhouse_secret_env_var_json =
                    create_clickhouse_secret_env_var_json(&clickhouse_secret_name);
                container_environment.push(clickhouse_secret_env_var_json);
            }
        }
        DestinationType::Iceberg => {
            let postgres_secret_name = create_postgres_secret_name(prefix);
            let postgres_secret_env_var_json =
                create_postgres_secret_env_var_json(&postgres_secret_name);

            container_environment.push(postgres_secret_env_var_json);
            let iceberg_secret_name = create_iceberg_secret_name(prefix);

            let iceberg_catlog_token_env_var_json =
                create_iceberg_catlog_token_env_var_json(&iceberg_secret_name);
            container_environment.push(iceberg_catlog_token_env_var_json);

            let iceberg_s3_access_key_id_env_var_json =
                create_iceberg_s3_access_key_id_env_var_json(&iceberg_secret_name);
            container_environment.push(iceberg_s3_access_key_id_env_var_json);

            let iceberg_s3_secret_access_key_env_var_json =
                create_iceberg_s3_secret_access_key_env_var_json(&iceberg_secret_name);
            container_environment.push(iceberg_s3_secret_access_key_env_var_json);
        }
        DestinationType::Ducklake => {
            let postgres_secret_name = create_postgres_secret_name(prefix);
            let postgres_secret_env_var_json =
                create_postgres_secret_env_var_json(&postgres_secret_name);
            container_environment.push(postgres_secret_env_var_json);

            let ducklake_secret_name = create_ducklake_secret_name(prefix);

            let ducklake_catalog_url_env_var_json =
                create_ducklake_catalog_url_env_var_json(&ducklake_secret_name);
            container_environment.push(ducklake_catalog_url_env_var_json);

            let ducklake_s3_access_key_id_env_var_json =
                create_ducklake_s3_access_key_id_env_var_json(&ducklake_secret_name);
            container_environment.push(ducklake_s3_access_key_id_env_var_json);

            let ducklake_s3_secret_access_key_env_var_json =
                create_ducklake_s3_secret_access_key_env_var_json(&ducklake_secret_name);
            container_environment.push(ducklake_s3_secret_access_key_env_var_json);

            if let Some(ducklake_maintenance) = ducklake_maintenance {
                container_environment.push(json!({
                    "name": "ETL_DUCKLAKE_MAINTENANCE_CR_NAME",
                    "value": create_ducklake_maintenance_name(prefix)
                }));
                container_environment.push(json!({
                    "name": "ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE",
                    "value": &k8s_config.replicator_namespace
                }));
                container_environment.push(json!({
                    "name": "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_INLINE_FLUSH_MIN_INLINED_BYTES",
                    "value": ducklake_maintenance.min_inlined_bytes.to_string()
                }));
                container_environment.push(json!({
                    "name": "ETL_DUCKLAKE_EXTERNAL_MAINTENANCE_REWRITE_DATA_FILES_MIN_ACTIVE_DATA_FILES",
                    "value": ducklake_maintenance.min_active_data_files.to_string()
                }));
            }
        }
        DestinationType::Snowflake { passphrase_secret_required } => {
            let postgres_secret_name = create_postgres_secret_name(prefix);
            let postgres_secret_env_var_json =
                create_postgres_secret_env_var_json(&postgres_secret_name);
            container_environment.push(postgres_secret_env_var_json);

            let snowflake_secret_name = create_snowflake_secret_name(prefix);
            container_environment
                .push(create_snowflake_private_key_env_var_json(&snowflake_secret_name));
            if passphrase_secret_required {
                container_environment
                    .push(create_snowflake_passphrase_env_var_json(&snowflake_secret_name));
            }
        }
    }
    container_environment
}

#[cfg(test)]
fn create_node_selector_json(environment: &Environment) -> serde_json::Value {
    node_selector_json(&test_k8s_config(environment).replicator_node_selectors)
}

fn node_selector_json(selectors: &[crate::config::NodeSelectorConfig]) -> serde_json::Value {
    json!(
        selectors
            .iter()
            .map(|selector| (selector.key.clone(), selector.value.clone()))
            .collect::<BTreeMap<_, _>>()
    )
}

#[cfg(test)]
fn create_tolerations_json(environment: &Environment) -> serde_json::Value {
    tolerations_json(&test_k8s_config(environment).replicator_tolerations)
}

fn tolerations_json(tolerations: &[crate::config::TolerationConfig]) -> serde_json::Value {
    serde_json::Value::Array(
        tolerations
            .iter()
            .map(|toleration| {
                json!({
                    "key": toleration.key,
                    "operator": "Equal",
                    "value": toleration.value,
                    "effect": toleration.effect,
                })
            })
            .collect(),
    )
}

fn create_init_containers_json(
    k8s_config: &K8sConfig,
    prefix: &str,
    environment: &Environment,
    stateful_set_resources: &ReplicatorStatefulSetResourcesConfig,
) -> serde_json::Value {
    let vector_container_name = create_vector_container_name(prefix);
    // In staging and prod, run vector init container to collect logs
    match environment {
        Environment::Dev => json!([]),
        Environment::Staging | Environment::Prod => json!([
          {
            "name": vector_container_name,
            "image": &k8s_config.vector_image,
            "restartPolicy": "Always",
            "securityContext": {
              "allowPrivilegeEscalation": false,
              "capabilities": {
                "drop": ["ALL"]
              }
            },
            "env": [
              {
                "name": "LOGFLARE_API_KEY",
                "valueFrom": {
                  "secretKeyRef": {
                    "name": LOGFLARE_SECRET_NAME,
                    "key": "key"
                  }
                }
              }
            ],
            "resources": {
              "limits": {
                "memory": stateful_set_resources.vector_memory_limit,
                "cpu": stateful_set_resources.vector_cpu_limit,
              },
              "requests": {
                "memory": stateful_set_resources.vector_memory_request,
                "cpu": stateful_set_resources.vector_cpu_request,
              }
            },
            "volumeMounts": [
              {
                "name": VECTOR_CONFIG_FILE_VOLUME_NAME,
                "mountPath": "/etc/vector"
              },
              {
                "name": LOGS_VOLUME_NAME,
                "mountPath": "/var/log"
              }
            ]
          }
        ]),
    }
}

fn create_volumes_json(prefix: &str, environment: &Environment) -> Vec<serde_json::Value> {
    let replicator_config_map_name = create_replicator_config_map_name(prefix);
    let mut volumes = vec![json!(
      {
        "name": REPLICATOR_CONFIG_FILE_VOLUME_NAME,
        "configMap": {
          "name": replicator_config_map_name
        }
      }
    )];

    match environment {
        Environment::Dev => {
            // We do not configure vector or logs volumes for dev environments
        }
        Environment::Staging | Environment::Prod => {
            volumes.push(json!(
            {
              "name": VECTOR_CONFIG_FILE_VOLUME_NAME,
              "configMap": {
                "name": VECTOR_CONFIG_MAP_NAME
              }
            }));
            volumes.push(json!({
              "name": LOGS_VOLUME_NAME,
              "emptyDir": {}
            }));
        }
    }

    volumes
}

fn create_volume_mounts_json(environment: &Environment) -> Vec<serde_json::Value> {
    let mut volume_mounts = vec![json!(
      {
        "name": REPLICATOR_CONFIG_FILE_VOLUME_NAME,
        "mountPath": "/app/configuration"
      }
    )];

    match environment {
        Environment::Dev => {
            // We do not configure logs volume mount for dev environments
        }
        Environment::Staging | Environment::Prod => {
            volume_mounts.push(json!(
            {
              "name": LOGS_VOLUME_NAME,
              "mountPath": "/app/logs"
            }));
        }
    }

    volume_mounts
}

fn create_postgres_secret_env_var_json(postgres_secret_name: &str) -> serde_json::Value {
    json!({
      "name": "APP_PIPELINE__PG_CONNECTION__PASSWORD",
      "valueFrom": {
        "secretKeyRef": {
          "name": postgres_secret_name,
          "key": "password"
        }
      }
    })
}

fn create_bq_secret_env_var_json(bq_secret_name: &str) -> serde_json::Value {
    json!({
      "name": "APP_DESTINATION__BIG_QUERY__SERVICE_ACCOUNT_KEY",
      "valueFrom": {
        "secretKeyRef": {
          "name": bq_secret_name,
          "key": BQ_SERVICE_ACCOUNT_KEY_NAME
        }
      }
    })
}

fn create_clickhouse_secret_env_var_json(clickhouse_secret_name: &str) -> serde_json::Value {
    json!({
      "name": "APP_DESTINATION__CLICKHOUSE__PASSWORD",
      "valueFrom": {
        "secretKeyRef": {
          "name": clickhouse_secret_name,
          "key": CLICKHOUSE_PASSWORD_NAME
        }
      }
    })
}

fn create_iceberg_catlog_token_env_var_json(iceberg_secret_name: &str) -> serde_json::Value {
    json!({
      "name": "APP_DESTINATION__ICEBERG__SUPABASE__CATALOG_TOKEN",
      "valueFrom": {
        "secretKeyRef": {
          "name": iceberg_secret_name,
          "key": ICEBERG_CATALOG_TOKEN_KEY_NAME
        }
      }
    })
}

fn create_iceberg_s3_access_key_id_env_var_json(iceberg_secret_name: &str) -> serde_json::Value {
    json!({
      "name": "APP_DESTINATION__ICEBERG__SUPABASE__S3_ACCESS_KEY_ID",
      "valueFrom": {
        "secretKeyRef": {
          "name": iceberg_secret_name,
          "key": ICEBERG_S3_ACCESS_KEY_ID_KEY_NAME
        }
      }
    })
}

fn create_iceberg_s3_secret_access_key_env_var_json(
    iceberg_secret_name: &str,
) -> serde_json::Value {
    json!({
      "name": "APP_DESTINATION__ICEBERG__SUPABASE__S3_SECRET_ACCESS_KEY",
      "valueFrom": {
        "secretKeyRef": {
          "name": iceberg_secret_name,
          "key": ICEBERG_S3_SECRET_ACCESS_KEY_KEY_NAME
        }
      }
    })
}

fn create_ducklake_catalog_url_env_var_json(ducklake_secret_name: &str) -> serde_json::Value {
    json!({
      "name": "APP_DESTINATION__DUCKLAKE__CATALOG_URL",
      "valueFrom": {
        "secretKeyRef": {
          "name": ducklake_secret_name,
          "key": DUCKLAKE_CATALOG_URL_KEY_NAME
        }
      }
    })
}

fn create_ducklake_s3_access_key_id_env_var_json(ducklake_secret_name: &str) -> serde_json::Value {
    json!({
      "name": "APP_DESTINATION__DUCKLAKE__S3_ACCESS_KEY_ID",
      "valueFrom": {
        "secretKeyRef": {
          "name": ducklake_secret_name,
          "key": DUCKLAKE_S3_ACCESS_KEY_ID_KEY_NAME
        }
      }
    })
}

fn create_ducklake_s3_secret_access_key_env_var_json(
    ducklake_secret_name: &str,
) -> serde_json::Value {
    json!({
      "name": "APP_DESTINATION__DUCKLAKE__S3_SECRET_ACCESS_KEY",
      "valueFrom": {
        "secretKeyRef": {
          "name": ducklake_secret_name,
          "key": DUCKLAKE_S3_SECRET_ACCESS_KEY_KEY_NAME
        }
      }
    })
}

fn create_snowflake_private_key_env_var_json(snowflake_secret_name: &str) -> serde_json::Value {
    json!({
      "name": "APP_DESTINATION__SNOWFLAKE__PRIVATE_KEY",
      "valueFrom": {
        "secretKeyRef": {
          "name": snowflake_secret_name,
          "key": SNOWFLAKE_PRIVATE_KEY_NAME
        }
      }
    })
}

fn create_snowflake_passphrase_env_var_json(snowflake_secret_name: &str) -> serde_json::Value {
    json!({
      "name": "APP_DESTINATION__SNOWFLAKE__PRIVATE_KEY_PASSPHRASE",
      "valueFrom": {
        "secretKeyRef": {
          "name": snowflake_secret_name,
          "key": SNOWFLAKE_PRIVATE_KEY_PASSPHRASE_NAME
        }
      }
    })
}

#[expect(clippy::too_many_arguments)]
fn create_replicator_stateful_set_json(
    k8s_config: &K8sConfig,
    prefix: &str,
    identity: &PipelineRuntimeIdentity,
    stateful_set_name: &str,
    replicator_image: &str,
    container_environment: Vec<serde_json::Value>,
    node_selector: serde_json::Value,
    tolerations: serde_json::Value,
    init_containers: serde_json::Value,
    volumes: Vec<serde_json::Value>,
    volume_mounts: Vec<serde_json::Value>,
    stateful_set_resources: &ReplicatorStatefulSetResourcesConfig,
) -> serde_json::Value {
    let replicator_app_name = create_replicator_app_name(prefix);
    let restarted_at_annotation = get_restarted_at_annotation_value();
    let replicator_container_name = create_replicator_container_name(prefix);
    let selector_labels = create_app_selector_labels(&replicator_app_name, REPLICATOR_APP_LABEL);
    let identity_labels = create_replicator_identity_labels(&replicator_app_name, identity);

    json!({
      "apiVersion": "apps/v1",
      "kind": "StatefulSet",
      "metadata": {
        "name": stateful_set_name,
        "namespace": &k8s_config.replicator_namespace,
        "labels": identity_labels,
      },
      "spec": {
        "replicas": 1,
        // Keep native rolling updates for API-driven template changes. VPA updates Pods in place
        // when possible; otherwise the updater evicts and admission mutates the replacement Pod.
        "updateStrategy": {
          "type": "RollingUpdate"
        },
        "selector": {
          "matchLabels": selector_labels
        },
        "template": {
          "metadata": {
            "labels": identity_labels,
            "annotations": {
              // Attach template annotations (e.g., restart checksum) to trigger a rolling restart.
              "etl.supabase.com/restarted-at": restarted_at_annotation,
            }
          },
          "spec": {
            "serviceAccountName": &k8s_config.replicator_service_account_name,
            "securityContext": {
              "seccompProfile": {
                "type": "RuntimeDefault"
              }
            },
            "volumes": volumes,
            "nodeSelector": node_selector,
            "tolerations": tolerations,
            // We want to wait at most 5 minutes before K8S sends a `SIGKILL` to the containers,
            // this way we let the system finish any in-flight transaction, if there are any.
            "terminationGracePeriodSeconds": 300,
            "initContainers": init_containers,
            "containers": [
              {
                "name": replicator_container_name,
                "image": replicator_image,
                "resizePolicy": [
                  {
                    "resourceName": "cpu",
                    "restartPolicy": "NotRequired"
                  },
                  {
                    "resourceName": "memory",
                    "restartPolicy": "NotRequired"
                  }
                ],
                "securityContext": {
                  "allowPrivilegeEscalation": false,
                  "capabilities": {
                    "drop": ["ALL"]
                  }
                },
                "ports": [
                  {
                    "name": REPLICATOR_METRICS_PORT_NAME,
                    "containerPort": REPLICATOR_METRICS_PORT,
                    "protocol": "TCP"
                  }
                ],
                "env": container_environment,
                "volumeMounts": volume_mounts,
                "resources": {
                  "limits": {
                    "memory": stateful_set_resources.replicator_memory_limit,
                    "cpu": stateful_set_resources.replicator_cpu_limit,
                  },
                  "requests": {
                    "memory": stateful_set_resources.replicator_memory_request,
                    "cpu": stateful_set_resources.replicator_cpu_request,
                  }
                }
              }
            ]
          }
        }
      }
    })
}

fn create_replicator_vertical_pod_autoscaler_json(
    k8s_config: &K8sConfig,
    prefix: &str,
    identity: &PipelineRuntimeIdentity,
    stateful_set_name: &str,
    update_mode: &str,
) -> Result<DynamicObject, serde_json::Error> {
    let replicator_app_name = create_replicator_app_name(prefix);
    let replicator_container_name = create_replicator_container_name(prefix);
    let identity_labels = create_replicator_identity_labels(&replicator_app_name, identity);

    serde_json::from_value(json!({
      "apiVersion": format!("{VERTICAL_POD_AUTOSCALER_GROUP}/{VERTICAL_POD_AUTOSCALER_VERSION}"),
      "kind": VERTICAL_POD_AUTOSCALER_KIND,
      "metadata": {
        "name": stateful_set_name,
        "namespace": &k8s_config.replicator_namespace,
        "labels": identity_labels
      },
      "spec": {
        "targetRef": {
          "apiVersion": "apps/v1",
          "kind": "StatefulSet",
          "name": stateful_set_name
        },
        "updatePolicy": {
          "updateMode": update_mode,
          "minReplicas": 1
        },
        "resourcePolicy": {
          "containerPolicies": [
            {
              "containerName": replicator_container_name,
              "mode": "Auto",
              "controlledResources": ["cpu", "memory"],
              "controlledValues": "RequestsAndLimits",
              "minAllowed": {
                "cpu": format!("{}m", k8s_config.replicator_autoscaling.min_cpu_millicores),
                "memory": format!("{}Mi", k8s_config.replicator_autoscaling.min_memory_mib)
              },
              "maxAllowed": {
                "cpu": format!("{}m", k8s_config.replicator_autoscaling.max_cpu_millicores),
                "memory": format!("{}Mi", k8s_config.replicator_autoscaling.max_memory_mib)
              }
            },
            {
              "containerName": "*",
              "mode": "Off"
            }
          ]
        }
      }
    }))
}

/// Reads the update mode that API reconciliation must preserve.
fn vpa_update_mode(vpa: &DynamicObject) -> Option<&str> {
    vpa.data.pointer("/spec/updatePolicy/updateMode")?.as_str()
}

fn get_restarted_at_annotation_value() -> String {
    let now = Utc::now();
    // We use nanoseconds to decrease the likelihood of generating the same
    // annotation in sequence, which would not result in a restart.
    now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

#[cfg(test)]
#[allow(clippy::redundant_test_prefix)]
mod tests {
    use etl_config::shared::{
        BatchConfig, DestinationConfig, InvalidatedSlotBehavior, MemoryBackpressureConfig,
        PgConnectionConfig, PipelineConfig, ReplicatorConfig, ReplicatorConfigWithoutSecrets,
        TableSyncCopyConfig, TcpKeepaliveConfig, TlsConfig,
    };
    use insta::{assert_json_snapshot, assert_snapshot};

    use super::*;
    use crate::{
        config::DefaultReplicatorResourcesOverrideConfig,
        configs::pipeline::ReplicatorResourcesConfig,
    };

    const TENANT_ID: &str = "abcdefghijklmnopqrst";
    const PIPELINE_ID: i64 = 24;
    const REPLICATOR_ID: i64 = 42;
    const MAX_TENANT_ID: &str = "abcdefghijklmnopqrst";
    const MAX_BIGINT_ID: i64 = 9_223_372_036_854_775_807;
    const MAX_K8S_LABEL_VALUE_LEN: usize = 63;
    const CONTROLLER_REVISION_HASH_LEN: usize = 10;

    fn replicator_identity_with(
        tenant_id: &str,
        pipeline_id: i64,
        replicator_id: i64,
    ) -> PipelineRuntimeIdentity {
        PipelineRuntimeIdentity { tenant_id: tenant_id.to_owned(), pipeline_id, replicator_id }
    }

    fn pipeline_runtime_identity() -> PipelineRuntimeIdentity {
        replicator_identity_with(TENANT_ID, PIPELINE_ID, REPLICATOR_ID)
    }

    fn max_pipeline_runtime_identity() -> PipelineRuntimeIdentity {
        replicator_identity_with(MAX_TENANT_ID, MAX_BIGINT_ID, MAX_BIGINT_ID)
    }

    fn default_k8s_config() -> K8sConfig {
        test_k8s_config(&Environment::Staging)
    }

    fn create_k8s_object_prefix(tenant_id: &str, replicator_id: i64) -> String {
        format!("{tenant_id}-{replicator_id}")
    }

    fn redact_restarted_at_annotation(stateful_set_json: &mut serde_json::Value) {
        let restarted_at = stateful_set_json
            .pointer_mut("/spec/template/metadata/annotations/etl.supabase.com~1restarted-at")
            .expect("stateful set should have a restarted-at annotation");

        *restarted_at = serde_json::Value::String("[timestamp]".to_owned());
    }

    macro_rules! assert_stateful_set_json_snapshot {
        ($stateful_set_json:expr) => {{
            let mut stateful_set_json = $stateful_set_json.clone();
            redact_restarted_at_annotation(&mut stateful_set_json);

            assert_snapshot!(serde_json::to_string_pretty(&stateful_set_json).unwrap());
        }};
    }

    fn assert_stateful_set_has_identity_metadata_labels(
        stateful_set_json: &serde_json::Value,
        tenant_id: &str,
        pipeline_id: i64,
        replicator_id: i64,
    ) {
        let labels = stateful_set_json
            .pointer("/metadata/labels")
            .and_then(serde_json::Value::as_object)
            .expect("stateful set should have metadata labels");
        let pipeline_id = pipeline_id.to_string();
        let replicator_id = replicator_id.to_string();

        assert_eq!(
            labels.get("etl.supabase.com/tenant-id").and_then(serde_json::Value::as_str),
            Some(tenant_id)
        );
        assert_eq!(
            labels.get("etl.supabase.com/pipeline-id").and_then(serde_json::Value::as_str),
            Some(pipeline_id.as_str())
        );
        assert!(
            stateful_set_json
                .pointer("/spec/selector/matchLabels/etl.supabase.com~1tenant-id")
                .is_none(),
            "tenant-id should not be part of the immutable selector"
        );
        assert!(
            stateful_set_json
                .pointer("/spec/selector/matchLabels/etl.supabase.com~1pipeline-id")
                .is_none(),
            "pipeline-id should not be part of the immutable selector"
        );
        assert!(
            stateful_set_json
                .pointer("/spec/selector/matchLabels/etl.supabase.com~1replicator-id")
                .is_none(),
            "replicator-id should not be part of the immutable selector"
        );
        assert_eq!(
            stateful_set_json.pointer("/spec/template/metadata/labels/etl.supabase.com~1tenant-id"),
            Some(&json!(tenant_id))
        );
        assert_eq!(
            stateful_set_json
                .pointer("/spec/template/metadata/labels/etl.supabase.com~1pipeline-id"),
            Some(&json!(pipeline_id))
        );
        assert_eq!(
            stateful_set_json
                .pointer("/spec/template/metadata/labels/etl.supabase.com~1replicator-id"),
            Some(&json!(replicator_id))
        );
    }

    fn assert_resource_identity_labels(
        resource_name: &str,
        resource: &serde_json::Value,
        expected_app_name: &str,
        expected_app_type: &str,
        tenant_id: &str,
        pipeline_id: i64,
        replicator_id: i64,
    ) {
        let expected_pipeline_id = pipeline_id.to_string();
        let expected_replicator_id = replicator_id.to_string();
        let labels = resource
            .pointer("/metadata/labels")
            .and_then(serde_json::Value::as_object)
            .unwrap_or_else(|| panic!("{resource_name} should have metadata labels"));

        for (key, expected) in [
            (APP_NAME_LABEL, expected_app_name),
            (APP_TYPE_LABEL, expected_app_type),
            (TENANT_ID_LABEL, tenant_id),
            (PIPELINE_ID_LABEL, expected_pipeline_id.as_str()),
            (REPLICATOR_ID_LABEL, expected_replicator_id.as_str()),
        ] {
            assert_eq!(
                labels.get(key).and_then(serde_json::Value::as_str),
                Some(expected),
                "{resource_name} has an unexpected {key} label"
            );
        }
    }

    fn container_environment_has_var(
        container_environment: &[serde_json::Value],
        name: &str,
    ) -> bool {
        container_environment
            .iter()
            .any(|entry| entry.get("name").and_then(serde_json::Value::as_str) == Some(name))
    }

    fn collect_kubernetes_label_values(
        value: &serde_json::Value,
        labels: &mut Vec<(String, String)>,
    ) {
        match value {
            serde_json::Value::Object(map) => {
                for label_field in ["labels", "matchLabels"] {
                    if let Some(serde_json::Value::Object(label_map)) = map.get(label_field) {
                        labels.extend(label_map.iter().filter_map(|(key, value)| {
                            value.as_str().map(|value| (key.clone(), value.to_owned()))
                        }));
                    }
                }

                for child in map.values() {
                    collect_kubernetes_label_values(child, labels);
                }
            }
            serde_json::Value::Array(values) => {
                for child in values {
                    collect_kubernetes_label_values(child, labels);
                }
            }
            _ => {}
        }
    }

    fn assert_kubernetes_label_values_are_safe(resource_name: &str, resource: &serde_json::Value) {
        let mut labels = Vec::new();
        collect_kubernetes_label_values(resource, &mut labels);
        assert!(!labels.is_empty(), "{resource_name} should contain Kubernetes labels");

        for (key, value) in labels {
            assert!(
                value.len() <= MAX_K8S_LABEL_VALUE_LEN,
                "{resource_name} generated label {key}={value} with length {}, exceeding \
                 {MAX_K8S_LABEL_VALUE_LEN}",
                value.len()
            );
        }
    }

    #[test]
    fn test_replicator_stateful_set_resources_uses_api_config_requests() {
        let prod =
            ReplicatorStatefulSetResourcesConfig::for_environment(&Environment::Prod).unwrap();
        let staging =
            ReplicatorStatefulSetResourcesConfig::for_environment(&Environment::Staging).unwrap();

        assert_eq!(prod.replicator_cpu_request, "500m");
        assert_eq!(prod.replicator_memory_request, "768Mi");
        assert_eq!(staging.replicator_cpu_request, "250m");
        assert_eq!(staging.replicator_memory_request, "768Mi");
        assert_eq!(prod.vector_cpu_request, "75m");
        assert_eq!(prod.vector_memory_request, "192Mi");
        assert_eq!(prod.vector_cpu_limit, "75m");
        assert_eq!(prod.vector_memory_limit, "192Mi");
    }

    #[test]
    fn test_replicator_stateful_set_resources_uses_pipeline_overrides() {
        let overrides = ReplicatorResourcesConfig {
            cpu_request_millicores: Some(750),
            memory_request_mib: Some(1536),
            ..ReplicatorResourcesConfig::default()
        };
        let k8s_config = test_k8s_config(&Environment::Prod);
        let default_replicator_resources =
            k8s_config.replicator_resources_for(DestinationKind::BigQuery);

        let stateful_set_resources = ReplicatorStatefulSetResourcesConfig::from_default_resources(
            &default_replicator_resources,
            &k8s_config.replicator_autoscaling,
            &k8s_config.vector_resources,
            Some(&overrides),
        )
        .unwrap();

        assert_eq!(stateful_set_resources.replicator_cpu_request, "750m");
        assert_eq!(stateful_set_resources.replicator_memory_request, "1536Mi");
        assert_eq!(stateful_set_resources.replicator_cpu_limit, "750m");
        assert_eq!(stateful_set_resources.replicator_memory_limit, "1536Mi");
    }

    #[test]
    fn test_replicator_stateful_set_resources_uses_pipeline_limits() {
        let overrides = ReplicatorResourcesConfig {
            cpu_request_millicores: Some(750),
            memory_request_mib: Some(1536),
            cpu_limit_millicores: Some(1000),
            memory_limit_mib: Some(2048),
        };
        let k8s_config = test_k8s_config(&Environment::Prod);
        let default_replicator_resources =
            k8s_config.replicator_resources_for(DestinationKind::BigQuery);

        let stateful_set_resources = ReplicatorStatefulSetResourcesConfig::from_default_resources(
            &default_replicator_resources,
            &k8s_config.replicator_autoscaling,
            &k8s_config.vector_resources,
            Some(&overrides),
        )
        .unwrap();

        assert_eq!(stateful_set_resources.replicator_cpu_request, "1000m");
        assert_eq!(stateful_set_resources.replicator_memory_request, "2048Mi");
        assert_eq!(stateful_set_resources.replicator_cpu_limit, "1000m");
        assert_eq!(stateful_set_resources.replicator_memory_limit, "2048Mi");
    }

    #[test]
    fn test_replicator_stateful_set_resources_clamps_to_autoscaling_minimums() {
        let overrides = ReplicatorResourcesConfig {
            cpu_request_millicores: Some(0),
            memory_request_mib: Some(-20),
            cpu_limit_millicores: Some(0),
            memory_limit_mib: Some(-1),
        };
        let k8s_config = K8sConfig {
            replicator_resources: DefaultReplicatorResourcesConfig {
                cpu_request_millicores: -10,
                memory_request_mib: 0,
                destinations: Default::default(),
            },
            vector_resources: DefaultVectorResourcesConfig {
                cpu_request_millicores: 0,
                memory_request_mib: -20,
            },
            ..default_k8s_config()
        };
        let default_replicator_resources =
            k8s_config.replicator_resources_for(DestinationKind::BigQuery);

        let stateful_set_resources = ReplicatorStatefulSetResourcesConfig::from_default_resources(
            &default_replicator_resources,
            &k8s_config.replicator_autoscaling,
            &k8s_config.vector_resources,
            Some(&overrides),
        )
        .unwrap();

        assert_eq!(stateful_set_resources.replicator_cpu_request, "250m");
        assert_eq!(stateful_set_resources.replicator_memory_request, "768Mi");
        assert_eq!(stateful_set_resources.replicator_cpu_limit, "250m");
        assert_eq!(stateful_set_resources.replicator_memory_limit, "768Mi");
        assert_eq!(stateful_set_resources.vector_cpu_request, "1m");
        assert_eq!(stateful_set_resources.vector_memory_request, "1Mi");
        assert_eq!(stateful_set_resources.vector_cpu_limit, "1m");
        assert_eq!(stateful_set_resources.vector_memory_limit, "1Mi");
    }

    #[test]
    fn test_replicator_stateful_set_resources_clamps_limits_to_requests() {
        let overrides = ReplicatorResourcesConfig {
            cpu_request_millicores: Some(750),
            memory_request_mib: Some(1536),
            cpu_limit_millicores: Some(10),
            memory_limit_mib: Some(100),
        };
        let k8s_config = test_k8s_config(&Environment::Prod);
        let default_replicator_resources =
            k8s_config.replicator_resources_for(DestinationKind::BigQuery);

        let stateful_set_resources = ReplicatorStatefulSetResourcesConfig::from_default_resources(
            &default_replicator_resources,
            &k8s_config.replicator_autoscaling,
            &k8s_config.vector_resources,
            Some(&overrides),
        )
        .unwrap();

        assert_eq!(stateful_set_resources.replicator_cpu_request, "750m");
        assert_eq!(stateful_set_resources.replicator_memory_request, "1536Mi");
        assert_eq!(stateful_set_resources.replicator_cpu_limit, "750m");
        assert_eq!(stateful_set_resources.replicator_memory_limit, "1536Mi");
    }

    #[test]
    fn test_replicator_resource_config_uses_api_vector_resources() {
        let k8s_config = K8sConfig {
            replicator_resources: DefaultReplicatorResourcesConfig {
                cpu_request_millicores: 500,
                memory_request_mib: 500,
                destinations: Default::default(),
            },
            vector_resources: DefaultVectorResourcesConfig {
                cpu_request_millicores: 80,
                memory_request_mib: 192,
            },
            ..default_k8s_config()
        };
        let default_replicator_resources =
            k8s_config.replicator_resources_for(DestinationKind::BigQuery);

        let stateful_set_resources = ReplicatorStatefulSetResourcesConfig::from_default_resources(
            &default_replicator_resources,
            &k8s_config.replicator_autoscaling,
            &k8s_config.vector_resources,
            None,
        )
        .unwrap();

        assert_eq!(stateful_set_resources.vector_cpu_request, "80m");
        assert_eq!(stateful_set_resources.vector_memory_request, "192Mi");
        assert_eq!(stateful_set_resources.vector_cpu_limit, "80m");
        assert_eq!(stateful_set_resources.vector_memory_limit, "192Mi");
    }

    #[test]
    fn default_replicator_allocation_can_start_at_the_autoscaling_maximum() {
        let k8s_config = K8sConfig {
            replicator_resources: DefaultReplicatorResourcesConfig {
                cpu_request_millicores: 2_000,
                memory_request_mib: 8_192,
                destinations: Default::default(),
            },
            ..default_k8s_config()
        };
        let default_replicator_resources =
            k8s_config.replicator_resources_for(DestinationKind::BigQuery);

        let resources = ReplicatorStatefulSetResourcesConfig::from_default_resources(
            &default_replicator_resources,
            &k8s_config.replicator_autoscaling,
            &k8s_config.vector_resources,
            None,
        )
        .unwrap();

        assert_eq!(resources.replicator_cpu_request, "2000m");
        assert_eq!(resources.replicator_memory_request, "8192Mi");
        assert_eq!(resources.replicator_cpu_limit, resources.replicator_cpu_request);
        assert_eq!(resources.replicator_memory_limit, resources.replicator_memory_request);
    }

    #[test]
    fn generated_kubernetes_labels_fit_with_max_tenant_and_replicator_ids() {
        let prefix = create_k8s_object_prefix(MAX_TENANT_ID, MAX_BIGINT_ID);
        let replicator_app_name = create_replicator_app_name(&prefix);
        let postgres_secret_name = create_postgres_secret_name(&prefix);
        let clickhouse_secret_name = create_clickhouse_secret_name(&prefix);
        let bq_secret_name = create_bq_secret_name(&prefix);
        let iceberg_secret_name = create_iceberg_secret_name(&prefix);
        let ducklake_secret_name = create_ducklake_secret_name(&prefix);
        let snowflake_secret_name = create_snowflake_secret_name(&prefix);
        let config_map_name = create_replicator_config_map_name(&prefix);
        let ducklake_maintenance_name = create_ducklake_maintenance_name(&prefix);
        let stateful_set_name = create_stateful_set_name(&prefix);
        let controller_revision_label =
            format!("{stateful_set_name}-{hash}", hash = "0".repeat(CONTROLLER_REVISION_HASH_LEN));

        assert!(
            controller_revision_label.len() <= MAX_K8S_LABEL_VALUE_LEN,
            "stateful set controller revision label {controller_revision_label} has length {}, \
             exceeding {MAX_K8S_LABEL_VALUE_LEN}",
            controller_revision_label.len()
        );

        let environment = Environment::Prod;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();
        let replicator_image = "supabase/replicator:1.2.3";
        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::Ducklake,
            None,
            LogLevel::Info,
        );
        let node_selector = create_node_selector_json(&environment);
        let tolerations = create_tolerations_json(&environment);
        let init_containers = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);
        let identity = max_pipeline_runtime_identity();

        let resources = vec![
            (
                "postgres secret",
                create_postgres_secret_json(
                    &default_k8s_config(),
                    &postgres_secret_name,
                    &replicator_app_name,
                    &identity,
                    "secret",
                ),
            ),
            (
                "clickhouse secret",
                serde_json::to_value(create_clickhouse_password_secret(
                    &default_k8s_config(),
                    &clickhouse_secret_name,
                    &replicator_app_name,
                    &identity,
                    "secret",
                ))
                .unwrap(),
            ),
            (
                "bigquery secret",
                create_bq_service_account_key_secret_json(
                    &default_k8s_config(),
                    &bq_secret_name,
                    &replicator_app_name,
                    &identity,
                    "secret",
                ),
            ),
            (
                "iceberg secret",
                create_iceberg_secret_json(
                    &default_k8s_config(),
                    &iceberg_secret_name,
                    &replicator_app_name,
                    &identity,
                    "secret",
                    "secret",
                    "secret",
                ),
            ),
            (
                "ducklake secret",
                create_ducklake_secret_json(
                    &default_k8s_config(),
                    &ducklake_secret_name,
                    &replicator_app_name,
                    &identity,
                    "secret",
                    "secret",
                    "secret",
                ),
            ),
            (
                "snowflake secret",
                create_snowflake_secret_json(
                    &default_k8s_config(),
                    &snowflake_secret_name,
                    &replicator_app_name,
                    &identity,
                    &BASE64_STANDARD.encode("secret"),
                    Some(&BASE64_STANDARD.encode("secret")),
                ),
            ),
            (
                "replicator config map",
                create_replicator_config_map_json(
                    &default_k8s_config(),
                    &config_map_name,
                    &replicator_app_name,
                    &identity,
                    vec![ReplicatorConfigMapFile {
                        filename: "prod.json".to_owned(),
                        content: "{}".to_owned(),
                    }],
                ),
            ),
            (
                "ducklake maintenance",
                create_ducklake_maintenance_json(
                    &default_k8s_config(),
                    &prefix,
                    &ducklake_maintenance_name,
                    &identity,
                    DuckLakeMaintenanceResourceConfig {
                        image: replicator_image.to_owned(),
                        policy: DuckLakeMaintenancePolicy::default(),
                    },
                ),
            ),
            (
                "replicator vertical pod autoscaler",
                serde_json::to_value(
                    create_replicator_vertical_pod_autoscaler_json(
                        &default_k8s_config(),
                        &prefix,
                        &identity,
                        &stateful_set_name,
                        ReplicatorAutoscalingUpdateMode::Off.as_k8s_value(),
                    )
                    .unwrap(),
                )
                .unwrap(),
            ),
            (
                "replicator stateful set",
                create_replicator_stateful_set_json(
                    &default_k8s_config(),
                    &prefix,
                    &identity,
                    &stateful_set_name,
                    replicator_image,
                    container_environment,
                    node_selector,
                    tolerations,
                    init_containers,
                    volumes,
                    volume_mounts,
                    &stateful_set_resources,
                ),
            ),
        ];

        for (resource_name, resource) in resources {
            assert_kubernetes_label_values_are_safe(resource_name, &resource);
            let expected_app_type = if resource_name == "ducklake maintenance" {
                DUCKLAKE_MAINTENANCE_APP_LABEL
            } else {
                REPLICATOR_APP_LABEL
            };
            assert_resource_identity_labels(
                resource_name,
                &resource,
                &replicator_app_name,
                expected_app_type,
                MAX_TENANT_ID,
                MAX_BIGINT_ID,
                MAX_BIGINT_ID,
            );
        }
    }

    #[test]
    fn configured_namespace_service_account_and_vector_image_are_propagated() {
        let mut k8s_config = default_k8s_config();
        k8s_config.replicator_namespace = "custom-data-plane".to_owned();
        k8s_config.replicator_service_account_name = "custom-replicator".to_owned();
        k8s_config.vector_image = "example.com/vector:custom".to_owned();

        let prefix = create_k8s_object_prefix(TENANT_ID, REPLICATOR_ID);
        let app_name = create_replicator_app_name(&prefix);
        let identity = pipeline_runtime_identity();
        let namespaced_resources = vec![
            create_postgres_secret_json(
                &k8s_config,
                "postgres-secret",
                &app_name,
                &identity,
                "secret",
            ),
            serde_json::to_value(create_clickhouse_password_secret(
                &k8s_config,
                "clickhouse-secret",
                &app_name,
                &identity,
                "secret",
            ))
            .unwrap(),
            create_snowflake_secret_json(
                &k8s_config,
                "snowflake-secret",
                &app_name,
                &identity,
                "secret",
                None,
            ),
            create_bq_service_account_key_secret_json(
                &k8s_config,
                "bigquery-secret",
                &app_name,
                &identity,
                "secret",
            ),
            create_iceberg_secret_json(
                &k8s_config,
                "iceberg-secret",
                &app_name,
                &identity,
                "secret",
                "secret",
                "secret",
            ),
            create_ducklake_secret_json(
                &k8s_config,
                "ducklake-secret",
                &app_name,
                &identity,
                "secret",
                "secret",
                "secret",
            ),
            create_replicator_config_map_json(
                &k8s_config,
                "replicator-config",
                &app_name,
                &identity,
                vec![],
            ),
            create_ducklake_maintenance_json(
                &k8s_config,
                &prefix,
                "ducklake-maintenance",
                &identity,
                DuckLakeMaintenanceResourceConfig {
                    image: "example.com/replicator:custom".to_owned(),
                    policy: DuckLakeMaintenancePolicy::default(),
                },
            ),
        ];
        for resource in namespaced_resources {
            assert_eq!(resource.pointer("/metadata/namespace"), Some(&json!("custom-data-plane")));
        }

        let environment = Environment::Staging;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();
        let container_environment = create_container_environment_json(
            &k8s_config,
            &prefix,
            &environment,
            "example.com/replicator:custom",
            DestinationType::Ducklake,
            Some(&DuckLakeMaintenanceConfig {
                min_inlined_bytes: 10,
                min_active_data_files: 20,
                ..DuckLakeMaintenanceConfig::default()
            }),
            LogLevel::Info,
        );
        assert!(container_environment.iter().any(|entry| {
            entry
                == &json!({
                    "name": "ETL_DUCKLAKE_MAINTENANCE_CR_NAMESPACE",
                    "value": "custom-data-plane"
                })
        }));

        let init_containers = create_init_containers_json(
            &k8s_config,
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        assert_eq!(init_containers.pointer("/0/image"), Some(&json!("example.com/vector:custom")));

        let stateful_set = create_replicator_stateful_set_json(
            &k8s_config,
            &prefix,
            &identity,
            &create_stateful_set_name(&prefix),
            "example.com/replicator:custom",
            container_environment,
            json!({}),
            json!([]),
            init_containers,
            create_volumes_json(&prefix, &environment),
            create_volume_mounts_json(&environment),
            &stateful_set_resources,
        );
        assert_eq!(stateful_set.pointer("/metadata/namespace"), Some(&json!("custom-data-plane")));
        assert_eq!(
            stateful_set.pointer("/spec/template/spec/serviceAccountName"),
            Some(&json!("custom-replicator"))
        );
    }

    #[test]
    fn replicator_workload_names_use_short_suffix_and_keep_legacy_lookup_names() {
        let prefix = create_k8s_object_prefix("tenant-1", 42);

        assert_eq!(create_stateful_set_name(&prefix), "tenant-1-42-replicator");
        assert_eq!(create_legacy_stateful_set_name(&prefix), "tenant-1-42-replicator-stateful-set");
        assert_eq!(
            stateful_set_names_for_lookup(&prefix),
            vec![
                "tenant-1-42-replicator".to_owned(),
                "tenant-1-42-replicator-stateful-set".to_owned(),
            ]
        );
        assert_eq!(
            pod_names_for_status(&prefix),
            vec![
                "tenant-1-42-replicator-0".to_owned(),
                "tenant-1-42-replicator-stateful-set-0".to_owned(),
            ]
        );
    }

    #[test]
    fn test_replicator_resources_prefer_pipeline_then_destination_then_global_defaults() {
        let overrides = ReplicatorResourcesConfig {
            cpu_request_millicores: Some(900),
            ..ReplicatorResourcesConfig::default()
        };
        let destinations = BTreeMap::from([(
            DestinationKind::Ducklake,
            DefaultReplicatorResourcesOverrideConfig {
                cpu_request_millicores: Some(600),
                memory_request_mib: Some(800),
            },
        )]);
        let k8s_config = K8sConfig {
            replicator_resources: DefaultReplicatorResourcesConfig {
                cpu_request_millicores: 300,
                memory_request_mib: 400,
                destinations,
            },
            vector_resources: DefaultVectorResourcesConfig {
                cpu_request_millicores: 80,
                memory_request_mib: 192,
            },
            ..default_k8s_config()
        };
        let default_replicator_resources =
            k8s_config.replicator_resources_for(DestinationKind::Ducklake);

        let stateful_set_resources = ReplicatorStatefulSetResourcesConfig::from_default_resources(
            &default_replicator_resources,
            &k8s_config.replicator_autoscaling,
            &k8s_config.vector_resources,
            Some(&overrides),
        )
        .unwrap();

        assert_eq!(stateful_set_resources.replicator_cpu_request, "900m");
        assert_eq!(stateful_set_resources.replicator_memory_request, "800Mi");
    }

    #[test]
    fn test_create_postgres_secret_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let secret_name = &create_postgres_secret_name(&prefix);
        let replicator_app_name = create_replicator_app_name(&prefix);
        let encoded_postgres_password = "dGVzdC1wYXNzd29yZA==";
        let identity = replicator_identity_with(TENANT_ID, 42, 42);

        let secret_json = create_postgres_secret_json(
            &default_k8s_config(),
            secret_name,
            &replicator_app_name,
            &identity,
            encoded_postgres_password,
        );

        assert_json_snapshot!(secret_json);

        let _secret: Secret = serde_json::from_value(secret_json).unwrap();
    }

    #[test]
    fn test_create_bq_service_account_key_secret_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let secret_name = &create_bq_secret_name(&prefix);
        let replicator_app_name = create_replicator_app_name(&prefix);
        let encoded_bq_service_account_key = "ewogICJrZXkiOiAidmFsdWUiCn0=";
        let identity = replicator_identity_with(TENANT_ID, 42, 42);

        let secret_json = create_bq_service_account_key_secret_json(
            &default_k8s_config(),
            secret_name,
            &replicator_app_name,
            &identity,
            encoded_bq_service_account_key,
        );

        assert_json_snapshot!(secret_json);

        let _secret: Secret = serde_json::from_value(secret_json).unwrap();
    }

    #[test]
    fn test_create_iceberg_secret_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let secret_name = &&create_iceberg_secret_name(&prefix);
        let replicator_app_name = create_replicator_app_name(&prefix);
        let encoded_catalog_token = "ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKRlV6STFOaUlzSW10cFpDSTZJakZrTnpGak1HRXlObUl4TURGak9EUTVaVGt4Wm1RMU5qZGpZakE1TlRKbUluMC5leUpsZUhBaU9qSXdOekEzTVRjeE5qQXNJbWxoZENJNk1UYzFOakUwTlRFMU1Dd2lhWE56SWpvaWMzVndZV0poYzJVaUxDSnlaV1lpT2lKaFltTmtaV1puYUdscWJHdHRibTl3Y1hKemRDSXNJbkp2YkdVaU9pSnpaWEoyYVdObFgzSnZiR1VpZlEuWWRUV2trSXZ3alNrWG90M05DMDd4eWpQakdXUU1OekxxNUVQenVtenJkTHp1SHJqLXp1ekktbmx5UXRRNVY3Z1phdXlzbS13R3dtcHp0UlhmUGMzQVE=";
        let encoded_s3_access_key_id = "Y2FlNGY0NjliNTY5MjJhMTNmMzNiNjM3YTNjMWU2ZjI=";
        let encoded_s3_secret_access_key = "NDUyOWE3ZmMwNzY2NDBjODRiZTgzZGJiNGMyNDI3MTNhOTk0MzE5OTBjYzJmMzIzMGM4MzVjOGJmZjAzYWE2ZQ==";
        let identity = replicator_identity_with(TENANT_ID, 42, 42);

        let secret_json = create_iceberg_secret_json(
            &default_k8s_config(),
            secret_name,
            &replicator_app_name,
            &identity,
            encoded_catalog_token,
            encoded_s3_access_key_id,
            encoded_s3_secret_access_key,
        );

        assert_json_snapshot!(secret_json);

        let _secret: Secret = serde_json::from_value(secret_json).unwrap();
    }

    #[test]
    fn test_create_replicator_config_map_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let replicator_config_map_name = create_replicator_config_map_name(&prefix);
        let replicator_app_name = create_replicator_app_name(&prefix);
        let environment = Environment::Prod;
        let base_config = "";
        let replicator_config = ReplicatorConfig {
            destination: DestinationConfig::BigQuery {
                project_id: "project-id".to_owned(),
                dataset_id: "dataset-id".to_owned(),
                service_account_key: "sa-key".into(),
                max_staleness_mins: None,
                connection_pool_size: 4,
                table_options: Default::default(),
            },
            pipeline: PipelineConfig {
                id: 42,
                publication_name: "all-pub".to_owned(),
                pg_connection: PgConnectionConfig {
                    host: "localhost".to_owned(),
                    hostaddr: Some("1a02:d034:3b7:f202:1803:84ed:98f8:131c".parse().unwrap()),
                    port: 5432,
                    name: "postgres".to_owned(),
                    username: "postgres".to_owned(),
                    password: Some("password".into()),
                    tls: TlsConfig::disabled(),
                    keepalive: TcpKeepaliveConfig::default(),
                },
                store_pg_connection: None,
                batch: BatchConfig {
                    max_fill_ms: 1_000,
                    memory_budget_ratio: 0.2,
                    max_bytes: 8 * 1024 * 1024,
                },
                table_error_retry_delay_ms: 500,
                table_error_retry_max_attempts: 3,
                max_table_sync_workers: 4,
                memory_refresh_interval_ms: 100,
                replication_lag_refresh_interval_ms: 10000,
                memory_backpressure: Some(MemoryBackpressureConfig {
                    activate_threshold: 1.0,
                    resume_threshold: 0.99,
                }),
                table_sync_copy: TableSyncCopyConfig::IncludeAllTables,
                invalidated_slot_behavior: InvalidatedSlotBehavior::Error,
                max_copy_connections_per_table: 2,
                run_source_migrations: true,
            },
            sentry: None,
            supabase: None,
        };
        let replicator_config_without_secrets: ReplicatorConfigWithoutSecrets =
            replicator_config.into();
        let env_config = serde_json::to_string(&replicator_config_without_secrets).unwrap();
        let identity = replicator_identity_with(TENANT_ID, 42, 42);

        let files = vec![
            ReplicatorConfigMapFile {
                filename: "base.json".to_owned(),
                content: base_config.to_owned(),
            },
            ReplicatorConfigMapFile {
                filename: format!("{environment}.json"),
                content: env_config,
            },
        ];

        let config_map_json = create_replicator_config_map_json(
            &default_k8s_config(),
            &replicator_config_map_name,
            &replicator_app_name,
            &identity,
            files,
        );

        assert_json_snapshot!(config_map_json);

        let _config_map: ConfigMap = serde_json::from_value(config_map_json).unwrap();
    }

    #[test]
    fn test_create_ducklake_maintenance_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let name = create_ducklake_maintenance_name(&prefix);

        let ducklake_maintenance_json = create_ducklake_maintenance_json(
            &default_k8s_config(),
            &prefix,
            &name,
            &replicator_identity_with(TENANT_ID, 24, 42),
            DuckLakeMaintenanceResourceConfig {
                image: "supabase/replicator:1.2.3".to_owned(),
                policy: DuckLakeMaintenancePolicy {
                    min_interval_seconds: 3600,
                    max_pause_seconds: 2700,
                    min_inlined_bytes: 10_000_000,
                    max_compacted_files: 40,
                    max_tables_per_run: 8,
                    target_file_size: "500MB".to_owned(),
                    delete_threshold: 0.5,
                    min_active_data_files: 40,
                    cpu_request_millicores: 1000,
                    memory_request_mib: 1024,
                    active_deadline_seconds: 1800,
                    operation_policy: Default::default(),
                },
            },
        );

        assert_snapshot!(serde_json::to_string_pretty(&ducklake_maintenance_json).unwrap());

        let _ducklake_maintenance: DynamicObject =
            serde_json::from_value(ducklake_maintenance_json).unwrap();
    }

    #[test]
    fn test_create_postgres_secret_env_var_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let postgres_secret_name = create_postgres_secret_name(&prefix);

        let postgres_env_var_json = create_postgres_secret_env_var_json(&postgres_secret_name);

        assert_json_snapshot!(postgres_env_var_json);
    }

    #[test]
    fn test_create_bq_secret_env_var_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let bq_secret_name = create_bq_secret_name(&prefix);

        let bq_env_var_json = create_bq_secret_env_var_json(&bq_secret_name);

        assert_json_snapshot!(bq_env_var_json);
    }

    #[test]
    fn test_create_iceberg_catlog_token_env_var_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let iceberg_secret_name = create_iceberg_secret_name(&prefix);

        let iceberg_catalog_token_env_var_json =
            create_iceberg_catlog_token_env_var_json(&iceberg_secret_name);

        assert_json_snapshot!(iceberg_catalog_token_env_var_json);
    }

    #[test]
    fn test_create_iceberg_s3_access_key_id_env_var_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let iceberg_secret_name = create_iceberg_secret_name(&prefix);

        let iceberg_s3_access_key_id_env_var_json =
            create_iceberg_s3_access_key_id_env_var_json(&iceberg_secret_name);

        assert_json_snapshot!(iceberg_s3_access_key_id_env_var_json);
    }

    #[test]
    fn test_create_iceberg_s3_secret_access_key_env_var_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let iceberg_secret_name = create_iceberg_secret_name(&prefix);

        let iceberg_s3_secret_access_key_env_var_json =
            create_iceberg_s3_secret_access_key_env_var_json(&iceberg_secret_name);

        assert_json_snapshot!(iceberg_s3_secret_access_key_env_var_json);
    }

    #[test]
    fn test_create_bq_container_environment() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        let environment = Environment::Dev;
        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            None,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);

        let environment = Environment::Staging;
        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            None,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);

        let environment = Environment::Prod;
        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            None,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);
    }

    #[test]
    fn test_create_iceberg_container_environment() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &Environment::Dev,
            replicator_image,
            DestinationType::Iceberg,
            None,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &Environment::Staging,
            replicator_image,
            DestinationType::Iceberg,
            None,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &Environment::Prod,
            replicator_image,
            DestinationType::Iceberg,
            None,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);
    }

    #[test]
    fn test_create_ducklake_container_environment() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &Environment::Dev,
            replicator_image,
            DestinationType::Ducklake,
            None,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &Environment::Staging,
            replicator_image,
            DestinationType::Ducklake,
            None,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &Environment::Prod,
            replicator_image,
            DestinationType::Ducklake,
            None,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);
    }

    #[test]
    fn clickhouse_with_password_references_password_secret() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &Environment::Dev,
            replicator_image,
            DestinationType::ClickHouse { password_secret_required: true },
            None,
            LogLevel::Info,
        );

        assert!(container_environment_has_var(
            &container_environment,
            "APP_PIPELINE__PG_CONNECTION__PASSWORD",
        ));
        assert!(container_environment_has_var(
            &container_environment,
            "APP_DESTINATION__CLICKHOUSE__PASSWORD",
        ));
    }

    #[test]
    fn passwordless_clickhouse_does_not_reference_missing_password_secret() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &Environment::Dev,
            replicator_image,
            DestinationType::ClickHouse { password_secret_required: false },
            None,
            LogLevel::Info,
        );

        assert!(container_environment_has_var(
            &container_environment,
            "APP_PIPELINE__PG_CONNECTION__PASSWORD",
        ));
        assert!(!container_environment_has_var(
            &container_environment,
            "APP_DESTINATION__CLICKHOUSE__PASSWORD",
        ));
    }

    #[test]
    fn snowflake_with_passphrase_references_both_secrets() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &Environment::Dev,
            replicator_image,
            DestinationType::Snowflake { passphrase_secret_required: true },
            None,
            LogLevel::Info,
        );

        assert!(container_environment_has_var(
            &container_environment,
            "APP_PIPELINE__PG_CONNECTION__PASSWORD",
        ));
        assert!(container_environment_has_var(
            &container_environment,
            "APP_DESTINATION__SNOWFLAKE__PRIVATE_KEY",
        ));
        assert!(container_environment_has_var(
            &container_environment,
            "APP_DESTINATION__SNOWFLAKE__PRIVATE_KEY_PASSPHRASE",
        ));
    }

    #[test]
    fn snowflake_without_passphrase_omits_passphrase_secret() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &Environment::Dev,
            replicator_image,
            DestinationType::Snowflake { passphrase_secret_required: false },
            None,
            LogLevel::Info,
        );

        assert!(container_environment_has_var(
            &container_environment,
            "APP_PIPELINE__PG_CONNECTION__PASSWORD",
        ));
        assert!(container_environment_has_var(
            &container_environment,
            "APP_DESTINATION__SNOWFLAKE__PRIVATE_KEY",
        ));
        assert!(!container_environment_has_var(
            &container_environment,
            "APP_DESTINATION__SNOWFLAKE__PRIVATE_KEY_PASSPHRASE",
        ));
    }

    #[test]
    fn snowflake_secret_contains_private_key() {
        let private_key = "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----";
        let encoded_private_key = BASE64_STANDARD.encode(private_key);
        let identity = replicator_identity_with("tenant", 42, 42);
        let snowflake_secret_json = create_snowflake_secret_json(
            &default_k8s_config(),
            "tenant-42-snowflake",
            "tenant-42-replicator-app",
            &identity,
            &encoded_private_key,
            None,
        );
        let secret: Secret = serde_json::from_value(snowflake_secret_json).unwrap();

        assert_eq!(secret.metadata.name.as_deref(), Some("tenant-42-snowflake"));
        assert_eq!(
            secret.metadata.namespace.as_deref(),
            Some(default_k8s_config().replicator_namespace.as_str())
        );
        assert_eq!(secret.type_.as_deref(), Some("Opaque"));

        let labels = secret.metadata.labels.as_ref().unwrap();
        assert_eq!(labels["etl.supabase.com/app-name"], "tenant-42-replicator-app");
        assert_eq!(labels["etl.supabase.com/app-type"], REPLICATOR_APP_LABEL);

        let data = secret.data.as_ref().unwrap();
        let stored_private_key = data.get(SNOWFLAKE_PRIVATE_KEY_NAME).unwrap();
        assert_eq!(stored_private_key.0, private_key.as_bytes());
        assert!(!data.contains_key(SNOWFLAKE_PRIVATE_KEY_PASSPHRASE_NAME));
    }

    #[test]
    fn snowflake_secret_contains_private_key_and_passphrase() {
        let private_key = "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----";
        let passphrase = "my-passphrase";
        let encoded_private_key = BASE64_STANDARD.encode(private_key);
        let encoded_passphrase = BASE64_STANDARD.encode(passphrase);
        let identity = replicator_identity_with("tenant", 42, 42);
        let snowflake_secret_json = create_snowflake_secret_json(
            &default_k8s_config(),
            "tenant-42-snowflake",
            "tenant-42-replicator-app",
            &identity,
            &encoded_private_key,
            Some(&encoded_passphrase),
        );
        let secret: Secret = serde_json::from_value(snowflake_secret_json).unwrap();

        let data = secret.data.as_ref().unwrap();
        let stored_private_key = data.get(SNOWFLAKE_PRIVATE_KEY_NAME).unwrap();
        let stored_passphrase = data.get(SNOWFLAKE_PRIVATE_KEY_PASSPHRASE_NAME).unwrap();
        assert_eq!(stored_private_key.0, private_key.as_bytes());
        assert_eq!(stored_passphrase.0, passphrase.as_bytes());
    }

    #[test]
    fn test_create_node_selector() {
        let node_selector = create_node_selector_json(&Environment::Dev);
        assert_json_snapshot!(node_selector);

        let node_selector = create_node_selector_json(&Environment::Staging);
        assert_json_snapshot!(node_selector);

        let node_selector = create_node_selector_json(&Environment::Prod);
        assert_json_snapshot!(node_selector);
    }

    #[test]
    fn replicator_stateful_set_applies_optional_scheduling_constraints() {
        let resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&Environment::Dev).unwrap();
        let identity = replicator_identity_with("tenant-1", PIPELINE_ID, REPLICATOR_ID);
        let create_stateful_set = |node_selector, tolerations| {
            create_replicator_stateful_set_json(
                &default_k8s_config(),
                "tenant-1-42",
                &identity,
                "tenant-1-42-replicator",
                "example.com/replicator:latest",
                Vec::new(),
                node_selector,
                tolerations,
                json!([]),
                Vec::new(),
                Vec::new(),
                &resources,
            )
        };

        let unpinned = create_stateful_set(json!({}), json!([]));
        assert_eq!(unpinned.pointer("/spec/template/spec/nodeSelector"), Some(&json!({})));
        assert_eq!(unpinned.pointer("/spec/template/spec/tolerations"), Some(&json!([])));

        let configured = create_stateful_set(
            node_selector_json(&[
                crate::config::NodeSelectorConfig {
                    key: "example.com/node-pool".to_owned(),
                    value: "data".to_owned(),
                },
                crate::config::NodeSelectorConfig {
                    key: "kubernetes.io/arch".to_owned(),
                    value: "arm64".to_owned(),
                },
            ]),
            tolerations_json(&[crate::config::TolerationConfig {
                key: "example.com/dedicated".to_owned(),
                value: "analytics".to_owned(),
                effect: "CustomEffect".to_owned(),
            }]),
        );
        assert_eq!(
            configured.pointer("/spec/template/spec/nodeSelector/example.com~1node-pool"),
            Some(&json!("data"))
        );
        assert_eq!(
            configured.pointer("/spec/template/spec/nodeSelector/kubernetes.io~1arch"),
            Some(&json!("arm64"))
        );
        assert_eq!(
            configured.pointer("/spec/template/spec/tolerations/0/key"),
            Some(&json!("example.com/dedicated"))
        );
        assert_eq!(
            configured.pointer("/spec/template/spec/tolerations/0/operator"),
            Some(&json!("Equal"))
        );
        assert_eq!(
            configured.pointer("/spec/template/spec/tolerations/0/value"),
            Some(&json!("analytics"))
        );
        assert_eq!(
            configured.pointer("/spec/template/spec/tolerations/0/effect"),
            Some(&json!("CustomEffect"))
        );
    }

    #[test]
    fn replicator_stateful_set_allows_in_place_cpu_and_memory_resize() {
        let resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&Environment::Dev).unwrap();
        let identity = replicator_identity_with("tenant-1", PIPELINE_ID, REPLICATOR_ID);
        let stateful_set = create_replicator_stateful_set_json(
            &default_k8s_config(),
            "tenant-1-42",
            &identity,
            "tenant-1-42-replicator",
            "example.com/replicator:latest",
            Vec::new(),
            json!({}),
            json!([]),
            json!([]),
            Vec::new(),
            Vec::new(),
            &resources,
        );

        assert_eq!(
            stateful_set.pointer("/spec/template/spec/containers/0/resizePolicy"),
            Some(&json!([
                {"resourceName": "cpu", "restartPolicy": "NotRequired"},
                {"resourceName": "memory", "restartPolicy": "NotRequired"}
            ]))
        );
        assert_eq!(
            stateful_set.pointer("/spec/updateStrategy/type"),
            Some(&json!("RollingUpdate"))
        );
    }

    #[test]
    fn replicator_vertical_pod_autoscaler_starts_in_recommendation_only_mode() {
        let identity = replicator_identity_with("tenant-1", PIPELINE_ID, REPLICATOR_ID);
        let autoscaler = create_replicator_vertical_pod_autoscaler_json(
            &default_k8s_config(),
            "tenant-1-42",
            &identity,
            "tenant-1-42-replicator",
            ReplicatorAutoscalingUpdateMode::Off.as_k8s_value(),
        )
        .unwrap();
        let autoscaler = serde_json::to_value(autoscaler).unwrap();

        assert_eq!(autoscaler.pointer("/spec/updatePolicy/updateMode"), Some(&json!("Off")));
        assert_eq!(autoscaler.pointer("/spec/updatePolicy/minReplicas"), Some(&json!(1)));
        assert_eq!(
            autoscaler.pointer("/spec/targetRef"),
            Some(&json!({
                "apiVersion": "apps/v1",
                "kind": "StatefulSet",
                "name": "tenant-1-42-replicator"
            }))
        );
        assert_eq!(
            autoscaler.pointer("/spec/resourcePolicy/containerPolicies/0/containerName"),
            Some(&json!("tenant-1-42-replicator"))
        );
        assert_eq!(
            autoscaler.pointer("/spec/resourcePolicy/containerPolicies/0/minAllowed"),
            Some(&json!({"cpu": "250m", "memory": "768Mi"}))
        );
        assert_eq!(
            autoscaler.pointer("/spec/resourcePolicy/containerPolicies/0/maxAllowed"),
            Some(&json!({"cpu": "2000m", "memory": "8192Mi"}))
        );
        assert_eq!(
            autoscaler.pointer("/spec/resourcePolicy/containerPolicies/0/controlledValues"),
            Some(&json!("RequestsAndLimits"))
        );
        assert_eq!(
            autoscaler.pointer("/spec/resourcePolicy/containerPolicies/1/mode"),
            Some(&json!("Off"))
        );
    }

    #[test]
    fn replicator_vertical_pod_autoscaler_updates_preserve_live_mode() {
        let identity = replicator_identity_with("tenant-1", PIPELINE_ID, REPLICATOR_ID);
        let live: DynamicObject = serde_json::from_value(json!({
            "apiVersion": "autoscaling.k8s.io/v1",
            "kind": "VerticalPodAutoscaler",
            "metadata": {"name": "tenant-1-42-replicator"},
            "spec": {"updatePolicy": {"updateMode": "InPlaceOrRecreate"}}
        }))
        .unwrap();
        let autoscaler = create_replicator_vertical_pod_autoscaler_json(
            &default_k8s_config(),
            "tenant-1-42",
            &identity,
            "tenant-1-42-replicator",
            vpa_update_mode(&live).unwrap(),
        )
        .unwrap();
        let autoscaler = serde_json::to_value(autoscaler).unwrap();

        assert_eq!(
            autoscaler.pointer("/spec/updatePolicy/updateMode"),
            Some(&json!("InPlaceOrRecreate"))
        );
        assert_eq!(autoscaler.pointer("/spec/updatePolicy/minReplicas"), Some(&json!(1)));
    }

    #[test]
    fn test_create_init_containers() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);

        let environment = Environment::Dev;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();
        let node_selector = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        assert_json_snapshot!(node_selector);

        let environment = Environment::Staging;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();
        let node_selector = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        assert_json_snapshot!(node_selector);

        let environment = Environment::Prod;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();
        let node_selector = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        assert_json_snapshot!(node_selector);
    }

    #[test]
    fn test_create_volumes() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);

        let environment = Environment::Dev;
        let volumes = create_volumes_json(&prefix, &environment);
        assert_json_snapshot!(volumes);

        let environment = Environment::Staging;
        let volumes = create_volumes_json(&prefix, &environment);
        assert_json_snapshot!(volumes);

        let environment = Environment::Prod;
        let volumes = create_volumes_json(&prefix, &environment);
        assert_json_snapshot!(volumes);
    }

    #[test]
    fn test_create_volume_mounts() {
        let environment = Environment::Dev;
        let volume_mounts = create_volume_mounts_json(&environment);
        assert_json_snapshot!(volume_mounts);

        let environment = Environment::Staging;
        let volume_mounts = create_volume_mounts_json(&environment);
        assert_json_snapshot!(volume_mounts);

        let environment = Environment::Prod;
        let volume_mounts = create_volume_mounts_json(&environment);
        assert_json_snapshot!(volume_mounts);
    }

    #[test]
    fn test_create_bq_replicator_stateful_set_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let identity = pipeline_runtime_identity();
        let stateful_set_name = create_stateful_set_name(&prefix);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        // Dev env
        let environment = Environment::Dev;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            None,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let tolerations = create_tolerations_json(&environment);
        let init_containers = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &default_k8s_config(),
            &prefix,
            &identity,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            tolerations,
            init_containers,
            volumes,
            volume_mounts,
            &stateful_set_resources,
        );

        assert_stateful_set_json_snapshot!(stateful_set_json);
        assert_stateful_set_has_identity_metadata_labels(
            &stateful_set_json,
            TENANT_ID,
            PIPELINE_ID,
            REPLICATOR_ID,
        );
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();

        // Staging env
        let environment = Environment::Staging;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            None,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let tolerations = create_tolerations_json(&environment);
        let init_containers = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &default_k8s_config(),
            &prefix,
            &identity,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            tolerations,
            init_containers,
            volumes,
            volume_mounts,
            &stateful_set_resources,
        );

        assert_stateful_set_json_snapshot!(stateful_set_json);
        assert_stateful_set_has_identity_metadata_labels(
            &stateful_set_json,
            TENANT_ID,
            PIPELINE_ID,
            REPLICATOR_ID,
        );
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();

        // Prod env
        let environment = Environment::Prod;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            None,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let tolerations = create_tolerations_json(&environment);
        let init_containers = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &default_k8s_config(),
            &prefix,
            &identity,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            tolerations,
            init_containers,
            volumes,
            volume_mounts,
            &stateful_set_resources,
        );

        assert_stateful_set_json_snapshot!(stateful_set_json);
        assert_stateful_set_has_identity_metadata_labels(
            &stateful_set_json,
            TENANT_ID,
            PIPELINE_ID,
            REPLICATOR_ID,
        );
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();
    }

    #[test]
    fn test_create_iceberg_replicator_stateful_set_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let identity = pipeline_runtime_identity();
        let stateful_set_name = create_stateful_set_name(&prefix);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        // Dev env
        let environment = Environment::Dev;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::Iceberg,
            None,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let tolerations = create_tolerations_json(&environment);
        let init_containers = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &default_k8s_config(),
            &prefix,
            &identity,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            tolerations,
            init_containers,
            volumes,
            volume_mounts,
            &stateful_set_resources,
        );

        assert_stateful_set_json_snapshot!(stateful_set_json);
        assert_stateful_set_has_identity_metadata_labels(
            &stateful_set_json,
            TENANT_ID,
            PIPELINE_ID,
            REPLICATOR_ID,
        );
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();

        // Staging env
        let environment = Environment::Staging;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::Iceberg,
            None,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let tolerations = create_tolerations_json(&environment);
        let init_containers = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &default_k8s_config(),
            &prefix,
            &identity,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            tolerations,
            init_containers,
            volumes,
            volume_mounts,
            &stateful_set_resources,
        );

        assert_stateful_set_json_snapshot!(stateful_set_json);
        assert_stateful_set_has_identity_metadata_labels(
            &stateful_set_json,
            TENANT_ID,
            PIPELINE_ID,
            REPLICATOR_ID,
        );
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();

        // Prod env
        let environment = Environment::Prod;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::Iceberg,
            None,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let tolerations = create_tolerations_json(&environment);
        let init_containers = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &default_k8s_config(),
            &prefix,
            &identity,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            tolerations,
            init_containers,
            volumes,
            volume_mounts,
            &stateful_set_resources,
        );

        assert_stateful_set_json_snapshot!(stateful_set_json);
        assert_stateful_set_has_identity_metadata_labels(
            &stateful_set_json,
            TENANT_ID,
            PIPELINE_ID,
            REPLICATOR_ID,
        );
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();
    }

    #[test]
    fn test_create_ducklake_replicator_stateful_set_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let identity = pipeline_runtime_identity();
        let stateful_set_name = create_stateful_set_name(&prefix);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        // Dev env
        let environment = Environment::Dev;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::Ducklake,
            None,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let tolerations = create_tolerations_json(&environment);
        let init_containers = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &default_k8s_config(),
            &prefix,
            &identity,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            tolerations,
            init_containers,
            volumes,
            volume_mounts,
            &stateful_set_resources,
        );

        assert_stateful_set_json_snapshot!(stateful_set_json);
        assert_stateful_set_has_identity_metadata_labels(
            &stateful_set_json,
            TENANT_ID,
            PIPELINE_ID,
            REPLICATOR_ID,
        );
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();

        // Staging env
        let environment = Environment::Staging;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::Ducklake,
            None,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let tolerations = create_tolerations_json(&environment);
        let init_containers = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &default_k8s_config(),
            &prefix,
            &identity,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            tolerations,
            init_containers,
            volumes,
            volume_mounts,
            &stateful_set_resources,
        );

        assert_stateful_set_json_snapshot!(stateful_set_json);
        assert_stateful_set_has_identity_metadata_labels(
            &stateful_set_json,
            TENANT_ID,
            PIPELINE_ID,
            REPLICATOR_ID,
        );
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();

        // Prod env
        let environment = Environment::Prod;
        let stateful_set_resources =
            ReplicatorStatefulSetResourcesConfig::for_environment(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            replicator_image,
            DestinationType::Ducklake,
            None,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let tolerations = create_tolerations_json(&environment);
        let init_containers = create_init_containers_json(
            &default_k8s_config(),
            &prefix,
            &environment,
            &stateful_set_resources,
        );
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &default_k8s_config(),
            &prefix,
            &identity,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            tolerations,
            init_containers,
            volumes,
            volume_mounts,
            &stateful_set_resources,
        );

        assert_stateful_set_json_snapshot!(stateful_set_json);
        assert_stateful_set_has_identity_metadata_labels(
            &stateful_set_json,
            TENANT_ID,
            PIPELINE_ID,
            REPLICATOR_ID,
        );
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();
    }
}
