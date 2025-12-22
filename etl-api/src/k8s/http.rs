use crate::configs::log::LogLevel;
use crate::k8s::{DestinationType, PodStatus, ReplicatorConfigMapFile};
use crate::k8s::{K8sClient, K8sError, PodPhase};
use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_STANDARD};
use chrono::Utc;
use etl_config::Environment;
use k8s_openapi::api::{
    apps::v1::StatefulSet,
    core::v1::{ConfigMap, Pod, Secret},
};
use kube::{
    Client,
    api::{Api, DeleteParams, Patch, PatchParams},
};
use serde_json::json;
use tracing::debug;

/// Secret name suffix for the BigQuery service account key.
const BQ_SECRET_NAME_SUFFIX: &str = "bq-service-account-key";
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
/// Secret name suffix for the Postgres password.
const POSTGRES_SECRET_NAME_SUFFIX: &str = "postgres-password";
/// ConfigMap name suffix for the replicator configuration files.
const REPLICATOR_CONFIG_MAP_NAME_SUFFIX: &str = "replicator-config";
/// StatefulSet name suffix for the replicator workload.
const REPLICATOR_STATEFUL_SET_SUFFIX: &str = "replicator-stateful-set";
/// Application label suffix used to group resources.
const REPLICATOR_APP_SUFFIX: &str = "replicator-app";
/// Container name suffix for the replicator container.
const REPLICATOR_CONTAINER_NAME_SUFFIX: &str = "replicator";
/// Container name suffix for the Vector sidecar.
const VECTOR_CONTAINER_NAME_SUFFIX: &str = "vector";
/// Namespace where data-plane resources are created.
const DATA_PLANE_NAMESPACE: &str = "etl-data-plane";
/// Secret storing the Logflare API key.
const LOGFLARE_SECRET_NAME: &str = "replicator-logflare-api-key";
/// Docker image used for the Vector sidecar.
const VECTOR_IMAGE_NAME: &str = "timberio/vector:0.46.1-distroless-libc";
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
/// ConfigMap name providing trusted root certificates.
pub const TRUSTED_ROOT_CERT_CONFIG_MAP_NAME: &str = "trusted-root-certs-config";
/// Key inside the trusted root certificates ConfigMap.
pub const TRUSTED_ROOT_CERT_KEY_NAME: &str = "trusted_root_certs";
/// Label used to identify replicator pods.
const REPLICATOR_APP_LABEL: &str = "etl-replicator-app";

/// Replicator memory request tuned for `c8gn.4xlarge` instances in prod.
const REPLICATOR_MEMORY_REQUEST_PROD: i32 = 2000;
/// Replicator CPU request tuned for `c8gn.4xlarge` instances in prod.
const REPLICATOR_CPU_REQUEST_PROD: i32 = 500;

/// Replicator memory request tuned for `c8gn.medium` instances in staging.
const REPLICATOR_MEMORY_REQUEST_STAGING: i32 = 250;
/// Replicator CPU request tuned for `c8gn.medium` instances in staging.
const REPLICATOR_CPU_REQUEST_STAGING: i32 = 125;

/// Vector memory request for init container.
const VECTOR_MEMORY_REQUEST: i32 = 100;
/// Vector CPU request for init container.
const VECTOR_CPU_REQUEST: i32 = 50;

/// Memory limit multiplier (request × 1.2 = limit).
///
/// We want to have 20% leeway in case of a memory usage spike.
const MEMORY_LIMIT_MULTIPLIER: f32 = 1.2;
/// CPU limit multiplier (request × 2.0 = limit).
///
/// CPU can be throttled, so the limits can be put higher.
const CPU_LIMIT_MULTIPLIER: f32 = 2.0;

/// Resource limits for a replicator pod.
struct ReplicatorResourceConfig {
    replicator_memory_limit: String,
    replicator_memory_request: String,
    replicator_cpu_limit: String,
    replicator_cpu_request: String,
    vector_memory_limit: String,
    vector_memory_request: String,
    vector_cpu_limit: String,
    vector_cpu_request: String,
}

impl ReplicatorResourceConfig {
    /// Loads the runtime limits for the current environment.
    ///
    /// Limits are computed from requests using multipliers:
    /// - Memory: request × 1.2 = limit
    /// - CPU: request × 2.0 = limit
    fn load(environment: &Environment) -> Result<Self, K8sError> {
        let (replicator_memory_request, replicator_cpu_request) = match environment {
            Environment::Prod => (REPLICATOR_MEMORY_REQUEST_PROD, REPLICATOR_CPU_REQUEST_PROD),
            _ => (
                REPLICATOR_MEMORY_REQUEST_STAGING,
                REPLICATOR_CPU_REQUEST_STAGING,
            ),
        };

        let replicator_memory_limit =
            ((replicator_memory_request as f32) * MEMORY_LIMIT_MULTIPLIER).round() as i32;
        let replicator_cpu_limit =
            ((replicator_cpu_request as f32) * CPU_LIMIT_MULTIPLIER).round() as i32;
        let vector_memory_limit =
            ((VECTOR_MEMORY_REQUEST as f32) * MEMORY_LIMIT_MULTIPLIER).round() as i32;
        let vector_cpu_limit = ((VECTOR_CPU_REQUEST as f32) * CPU_LIMIT_MULTIPLIER).round() as i32;

        Ok(Self {
            replicator_memory_limit: format!("{replicator_memory_limit}Mi"),
            replicator_memory_request: format!("{replicator_memory_request}Mi"),
            replicator_cpu_limit: format!("{replicator_cpu_limit}m"),
            replicator_cpu_request: format!("{replicator_cpu_request}m"),
            vector_memory_limit: format!("{vector_memory_limit}Mi"),
            vector_memory_request: format!("{VECTOR_MEMORY_REQUEST}Mi"),
            vector_cpu_limit: format!("{vector_cpu_limit}m"),
            vector_cpu_request: format!("{VECTOR_CPU_REQUEST}m"),
        })
    }
}

/// HTTP-based implementation of [`K8sClient`].
///
/// The client is namespaced to the data-plane namespace and uses server-side
/// apply to keep resources in sync.
#[derive(Debug)]
pub struct HttpK8sClient {
    secrets_api: Api<Secret>,
    config_maps_api: Api<ConfigMap>,
    stateful_sets_api: Api<StatefulSet>,
    pods_api: Api<Pod>,
}

impl HttpK8sClient {
    /// Creates a new [`HttpK8sClient`] using the ambient Kubernetes config.
    ///
    /// Prefers in-cluster configuration and falls back to the local kubeconfig
    /// when running outside the cluster.
    pub async fn new(client: Client) -> Result<HttpK8sClient, K8sError> {
        let secrets_api: Api<Secret> = Api::namespaced(client.clone(), DATA_PLANE_NAMESPACE);
        let config_maps_api: Api<ConfigMap> = Api::namespaced(client.clone(), DATA_PLANE_NAMESPACE);
        let stateful_sets_api: Api<StatefulSet> =
            Api::namespaced(client.clone(), DATA_PLANE_NAMESPACE);
        let pods_api: Api<Pod> = Api::namespaced(client, DATA_PLANE_NAMESPACE);

        Ok(HttpK8sClient {
            secrets_api,
            config_maps_api,
            stateful_sets_api,
            pods_api,
        })
    }

    /// Helper function to handle delete operations that should ignore 404 errors
    /// but propagate other errors.
    fn handle_delete_with_404_ignore<T>(
        delete_result: Result<T, kube::Error>,
    ) -> Result<(), K8sError> {
        match delete_result {
            Ok(_) => Ok(()),
            Err(kube::Error::Api(er)) if er.code == 404 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Returns true if the replicator container in the pod has terminated with error code
    fn has_replicator_container_error(pod: &Pod, replicator_container_name: &str) -> bool {
        // Find the replicator container status
        let container_status = pod.status.as_ref().and_then(|status| {
            status
                .container_statuses
                .as_ref()
                .and_then(|container_statuses| {
                    container_statuses
                        .iter()
                        .find(|cs| cs.name == replicator_container_name)
                        .cloned()
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

        // Waiting state, we want to distinguish normal waiting reasons from abnormal ones.
        if let Some(waiting) = &state.waiting {
            if let Some(reason) = &waiting.reason {
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
        }

        false
    }
}

#[async_trait]
impl K8sClient for HttpK8sClient {
    async fn create_or_update_postgres_secret(
        &self,
        prefix: &str,
        postgres_password: &str,
    ) -> Result<(), K8sError> {
        debug!("patching postgres secret");

        let encoded_postgres_password = BASE64_STANDARD.encode(postgres_password);
        let postgres_secret_name = create_postgres_secret_name(prefix);
        let replicator_app_name = create_replicator_app_name(prefix);
        let postgres_secret_json = create_postgres_secret_json(
            &postgres_secret_name,
            &replicator_app_name,
            &encoded_postgres_password,
        );
        let secret: Secret = serde_json::from_value(postgres_secret_json)?;

        // We are forcing the update since we are the field manager that should own the fields. If
        // there is an override (likely during an incident or SREs intervention), we want to override
        // their changes. The API database is the source of truth for credentials.
        let pp = PatchParams::apply(&postgres_secret_name).force();
        self.secrets_api
            .patch(&postgres_secret_name, &pp, &Patch::Apply(secret))
            .await?;

        Ok(())
    }

    async fn create_or_update_bigquery_secret(
        &self,
        prefix: &str,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError> {
        debug!("patching bq secret");

        let encoded_bq_service_account_key = BASE64_STANDARD.encode(bq_service_account_key);
        let bq_secret_name = create_bq_secret_name(prefix);
        let replicator_app_name = create_replicator_app_name(prefix);
        let bq_secret_json = create_bq_service_account_key_secret_json(
            &bq_secret_name,
            &replicator_app_name,
            &encoded_bq_service_account_key,
        );
        let secret: Secret = serde_json::from_value(bq_secret_json)?;

        // We are forcing the update since we are the field manager that should own the fields. If
        // there is an override (likely during an incident or SREs intervention), we want to override
        // their changes. The API database is the source of truth for credentials.
        let pp = PatchParams::apply(&bq_secret_name).force();
        self.secrets_api
            .patch(&bq_secret_name, &pp, &Patch::Apply(secret))
            .await?;

        Ok(())
    }

    async fn create_or_update_iceberg_secret(
        &self,
        prefix: &str,
        catalog_token: &str,
        s3_access_key_id: &str,
        s3_secret_access_key: &str,
    ) -> Result<(), K8sError> {
        debug!("patching iceberg secret");

        let encoded_catalog_token = BASE64_STANDARD.encode(catalog_token);
        let encoded_s3_access_key_id = BASE64_STANDARD.encode(s3_access_key_id);
        let encoded_s3_secret_access_key = BASE64_STANDARD.encode(s3_secret_access_key);

        let iceberg_secret_name = create_iceberg_secret_name(prefix);
        let replicator_app_name = create_replicator_app_name(prefix);
        let iceberg_secret_json = create_iceberg_secret_json(
            &iceberg_secret_name,
            &replicator_app_name,
            &encoded_catalog_token,
            &encoded_s3_access_key_id,
            &encoded_s3_secret_access_key,
        );
        let secret: Secret = serde_json::from_value(iceberg_secret_json)?;

        // We are forcing the update since we are the field manager that should own the fields. If
        // there is an override (likely during an incident or SREs intervention), we want to override
        // their changes. The API database is the source of truth for credentials.
        let pp = PatchParams::apply(&iceberg_secret_name).force();
        self.secrets_api
            .patch(&iceberg_secret_name, &pp, &Patch::Apply(secret))
            .await?;

        Ok(())
    }

    async fn delete_postgres_secret(&self, prefix: &str) -> Result<(), K8sError> {
        debug!("deleting postgres secret");

        let postgres_secret_name = create_postgres_secret_name(prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.secrets_api.delete(&postgres_secret_name, &dp).await,
        )?;

        Ok(())
    }

    async fn delete_bigquery_secret(&self, prefix: &str) -> Result<(), K8sError> {
        debug!("deleting bq secret");

        let bq_secret_name = create_bq_secret_name(prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(self.secrets_api.delete(&bq_secret_name, &dp).await)?;

        Ok(())
    }

    async fn delete_iceberg_secret(&self, prefix: &str) -> Result<(), K8sError> {
        debug!("deleting iceberg secret");

        let iceberg_secret_name = create_iceberg_secret_name(prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.secrets_api.delete(&iceberg_secret_name, &dp).await,
        )?;

        Ok(())
    }

    async fn get_config_map(&self, config_map_name: &str) -> Result<ConfigMap, K8sError> {
        debug!("getting config map");

        let config_map = match self.config_maps_api.get(config_map_name).await {
            Ok(config_map) => config_map,
            Err(e) => {
                return Err(e.into());
            }
        };

        Ok(config_map)
    }

    async fn create_or_update_replicator_config_map(
        &self,
        prefix: &str,
        files: Vec<ReplicatorConfigMapFile>,
    ) -> Result<(), K8sError> {
        debug!("patching config map");

        let replicator_config_map_name = create_replicator_config_map_name(prefix);
        let replicator_app_name = create_replicator_app_name(prefix);

        let config_map_json = create_replicator_config_map_json(
            &replicator_config_map_name,
            &replicator_app_name,
            files,
        );
        let config_map: ConfigMap = serde_json::from_value(config_map_json)?;

        // We are forcing the update since we are the field manager that should own the fields. If
        // there is an override (likely during an incident or SREs intervention), we want to override
        // their changes. The API database is the source of truth for configuration.
        let pp = PatchParams::apply(&replicator_config_map_name).force();
        self.config_maps_api
            .patch(&replicator_config_map_name, &pp, &Patch::Apply(config_map))
            .await?;

        Ok(())
    }

    async fn delete_replicator_config_map(&self, prefix: &str) -> Result<(), K8sError> {
        debug!("deleting config map");

        let replicator_config_map_name = create_replicator_config_map_name(prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.config_maps_api
                .delete(&replicator_config_map_name, &dp)
                .await,
        )?;

        Ok(())
    }

    async fn create_or_update_replicator_stateful_set(
        &self,
        prefix: &str,
        replicator_image: &str,
        environment: Environment,
        destination_type: DestinationType,
        log_level: LogLevel,
    ) -> Result<(), K8sError> {
        debug!("patching stateful set");

        let config = ReplicatorResourceConfig::load(&environment)?;

        let stateful_set_name = create_stateful_set_name(prefix);

        let container_environment = create_container_environment_json(
            prefix,
            &environment,
            replicator_image,
            destination_type,
            log_level,
        );

        let node_selector = create_node_selector_json(&environment);
        let init_containers = create_init_containers_json(prefix, &environment, &config);
        let volumes = create_volumes_json(prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            prefix,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            init_containers,
            volumes,
            volume_mounts,
            &config,
        );

        let stateful_set: StatefulSet = serde_json::from_value(stateful_set_json)?;

        // We are forcing the update since we are the field manager that should own the fields. If
        // there is an override (likely during an incident or SREs intervention), we want to override
        // their changes.
        let pp = PatchParams::apply(&stateful_set_name).force();
        self.stateful_sets_api
            .patch(&stateful_set_name, &pp, &Patch::Apply(stateful_set))
            .await?;

        Ok(())
    }

    async fn delete_replicator_stateful_set(&self, prefix: &str) -> Result<(), K8sError> {
        debug!("deleting stateful set");

        let stateful_set_name = create_stateful_set_name(prefix);
        let dp = DeleteParams::default();
        Self::handle_delete_with_404_ignore(
            self.stateful_sets_api.delete(&stateful_set_name, &dp).await,
        )?;

        Ok(())
    }

    async fn get_replicator_pod_status(&self, prefix: &str) -> Result<PodStatus, K8sError> {
        debug!("getting pod status");

        let pod_name = create_pod_name(prefix);
        let pod = match self.pods_api.get(&pod_name).await {
            Ok(pod) => pod,
            Err(kube::Error::Api(er)) if er.code == 404 => return Ok(PodStatus::Stopped),
            Err(e) => return Err(e.into()),
        };

        let replicator_container_name = create_replicator_container_name(prefix);

        if Self::has_replicator_container_error(&pod, &replicator_container_name) {
            return Ok(PodStatus::Failed);
        }

        if pod.metadata.deletion_timestamp.is_some() {
            return Ok(PodStatus::Stopping);
        }

        let phase = pod
            .status
            .map(|status| {
                let phase: PodPhase = status
                    .phase
                    .map(|phase| {
                        let phase: PodPhase = phase.as_str().into();
                        phase
                    })
                    .unwrap_or(PodPhase::Unknown);
                phase
            })
            .unwrap_or(PodPhase::Unknown);

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

fn create_replicator_config_map_name(prefix: &str) -> String {
    format!("{prefix}-{REPLICATOR_CONFIG_MAP_NAME_SUFFIX}")
}

fn create_stateful_set_name(prefix: &str) -> String {
    format!("{prefix}-{REPLICATOR_STATEFUL_SET_SUFFIX}")
}

fn create_pod_name(prefix: &str) -> String {
    format!("{prefix}-{REPLICATOR_STATEFUL_SET_SUFFIX}-0")
}

fn create_replicator_app_name(prefix: &str) -> String {
    format!("{prefix}-{REPLICATOR_APP_SUFFIX}")
}

fn create_replicator_container_name(prefix: &str) -> String {
    format!("{prefix}-{REPLICATOR_CONTAINER_NAME_SUFFIX}")
}

fn create_vector_container_name(prefix: &str) -> String {
    format!("{prefix}-{VECTOR_CONTAINER_NAME_SUFFIX}")
}

fn create_postgres_secret_json(
    secret_name: &str,
    replicator_app_name: &str,
    encoded_postgres_password: &str,
) -> serde_json::Value {
    json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": secret_name,
        "namespace": DATA_PLANE_NAMESPACE,
        "labels": {
          "etl.supabase.com/app-name": replicator_app_name,
          "etl.supabase.com/app-type": REPLICATOR_APP_LABEL,
        }
      },
      "type": "Opaque",
      "data": {
        "password": encoded_postgres_password,
      }
    })
}

fn create_bq_service_account_key_secret_json(
    secret_name: &str,
    replicator_app_name: &str,
    encoded_bq_service_account_key: &str,
) -> serde_json::Value {
    json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": secret_name,
        "namespace": DATA_PLANE_NAMESPACE,
        "labels": {
          "etl.supabase.com/app-name": replicator_app_name,
          "etl.supabase.com/app-type": REPLICATOR_APP_LABEL,
        }
      },
      "type": "Opaque",
      "data": {
        BQ_SERVICE_ACCOUNT_KEY_NAME: encoded_bq_service_account_key,
      }
    })
}

fn create_iceberg_secret_json(
    secret_name: &str,
    replicator_app_name: &str,
    encoded_catalog_token: &str,
    encoded_s3_access_key_id: &str,
    encoded_s3_secret_access_key: &str,
) -> serde_json::Value {
    json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": secret_name,
        "namespace": DATA_PLANE_NAMESPACE,
        "labels": {
          "etl.supabase.com/app-name": replicator_app_name,
          "etl.supabase.com/app-type": REPLICATOR_APP_LABEL,
        }
      },
      "type": "Opaque",
      "data": {
        ICEBERG_CATALOG_TOKEN_KEY_NAME: encoded_catalog_token,
        ICEBERG_S3_ACCESS_KEY_ID_KEY_NAME: encoded_s3_access_key_id,
        ICEBERG_S3_SECRET_ACCESS_KEY_KEY_NAME: encoded_s3_secret_access_key
      }
    })
}

fn create_replicator_config_map_json(
    config_map_name: &str,
    replicator_app_name: &str,
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
        "namespace": DATA_PLANE_NAMESPACE,
        "labels": {
          "etl.supabase.com/app-name": replicator_app_name,
          "etl.supabase.com/app-type": REPLICATOR_APP_LABEL,
        }
      },
      "data": data
    })
}

fn create_container_environment_json(
    prefix: &str,
    environment: &Environment,
    replicator_image: &str,
    destination_type: DestinationType,
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
        DestinationType::Memory => {}
        DestinationType::BigQuery => {
            let postgres_secret_name = create_postgres_secret_name(prefix);
            let postgres_secret_env_var_json =
                create_postgres_secret_env_var_json(&postgres_secret_name);
            container_environment.push(postgres_secret_env_var_json);

            let bq_secret_name = create_bq_secret_name(prefix);
            let bq_secret_env_var_json = create_bq_secret_env_var_json(&bq_secret_name);
            container_environment.push(bq_secret_env_var_json);
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
    }
    container_environment
}

fn create_node_selector_json(environment: &Environment) -> serde_json::Value {
    // In staging and prod, pin pods to workload pods.
    match environment {
        Environment::Dev => json!({}),
        Environment::Staging | Environment::Prod => json!({
            "etl.supabase.com/node-role": "workloads"
        }),
    }
}

fn create_init_containers_json(
    prefix: &str,
    environment: &Environment,
    config: &ReplicatorResourceConfig,
) -> serde_json::Value {
    let vector_container_name = create_vector_container_name(prefix);
    // In staging and prod, run vector init container to collect logs
    match environment {
        Environment::Dev => json!([]),
        Environment::Staging | Environment::Prod => json!([
          {
            "name": vector_container_name,
            "image": VECTOR_IMAGE_NAME,
            "restartPolicy": "Always",
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
                "memory": config.vector_memory_limit,
                "cpu": config.vector_cpu_limit,
              },
              "requests": {
                "memory": config.vector_memory_request,
                "cpu": config.vector_cpu_request,
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

#[expect(clippy::too_many_arguments)]
fn create_replicator_stateful_set_json(
    prefix: &str,
    stateful_set_name: &str,
    replicator_image: &str,
    container_environment: Vec<serde_json::Value>,
    node_selector: serde_json::Value,
    init_containers: serde_json::Value,
    volumes: Vec<serde_json::Value>,
    volume_mounts: Vec<serde_json::Value>,
    config: &ReplicatorResourceConfig,
) -> serde_json::Value {
    let replicator_app_name = create_replicator_app_name(prefix);
    let restarted_at_annotation = get_restarted_at_annotation_value();
    let replicator_container_name = create_replicator_container_name(prefix);

    json!({
      "apiVersion": "apps/v1",
      "kind": "StatefulSet",
      "metadata": {
        "name": stateful_set_name,
        "namespace": DATA_PLANE_NAMESPACE,
        "labels": {
          "etl.supabase.com/app-name": replicator_app_name,
          "etl.supabase.com/app-type": REPLICATOR_APP_LABEL
        },
      },
      "spec": {
        "replicas": 1,
        "selector": {
          "matchLabels": {
            "etl.supabase.com/app-name": replicator_app_name,
            "etl.supabase.com/app-type": REPLICATOR_APP_LABEL
          }
        },
        "template": {
          "metadata": {
            "labels": {
              "etl.supabase.com/app-name": replicator_app_name,
              "etl.supabase.com/app-type": REPLICATOR_APP_LABEL
            },
            "annotations": {
              // Attach template annotations (e.g., restart checksum) to trigger a rolling restart
              "etl.supabase.com/restarted-at": restarted_at_annotation,
            }
          },
          "spec": {
            "volumes": volumes,
            // Allow scheduling onto nodes tainted with the right node role.
            "tolerations": [
              {
                "key": "etl.supabase.com/node-role",
                "operator": "Equal",
                "value": "workloads",
                "effect": "NoSchedule"
              }
            ],
            "nodeSelector": node_selector,
            // We want to wait at most 5 minutes before K8S sends a `SIGKILL` to the containers,
            // this way we let the system finish any in-flight transaction, if there are any.
            "terminationGracePeriodSeconds": 300,
            "initContainers": init_containers,
            "containers": [
              {
                "name": replicator_container_name,
                "image": replicator_image,
                "ports": [
                  {
                    "name": "metrics",
                    "containerPort": 9000,
                    "protocol": "TCP"
                  }
                ],
                "env": container_environment,
                "volumeMounts": volume_mounts,
                "resources": {
                  "limits": {
                    "memory": config.replicator_memory_limit,
                    "cpu": config.replicator_cpu_limit,
                  },
                  "requests": {
                    "memory": config.replicator_memory_request,
                    "cpu": config.replicator_cpu_request,
                  }
                }
              }
            ]
          }
        }
      }
    })
}

fn get_restarted_at_annotation_value() -> String {
    let now = Utc::now();
    // We use nanoseconds to decrease the likelihood of generating the same annotation in sequence,
    // which would not result in a restart.
    now.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    use etl_config::shared::{
        BatchConfig, DestinationConfig, PgConnectionConfig, PipelineConfig, ReplicatorConfig,
        ReplicatorConfigWithoutSecrets, TlsConfig,
    };
    use insta::assert_json_snapshot;

    const TENANT_ID: &str = "abcdefghijklmnopqrst";

    fn create_k8s_object_prefix(tenant_id: &str, replicator_id: i64) -> String {
        format!("{tenant_id}-{replicator_id}")
    }

    #[test]
    fn test_create_postgres_secret_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let secret_name = &create_postgres_secret_name(&prefix);
        let replicator_app_name = create_replicator_app_name(&prefix);
        let encoded_postgres_password = "dGVzdC1wYXNzd29yZA==";

        let secret_json = create_postgres_secret_json(
            secret_name,
            &replicator_app_name,
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

        let secret_json = create_bq_service_account_key_secret_json(
            secret_name,
            &replicator_app_name,
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

        let secret_json = create_iceberg_secret_json(
            secret_name,
            &replicator_app_name,
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
                project_id: "project-id".to_string(),
                dataset_id: "dataset-id".to_string(),
                service_account_key: "sa-key".into(),
                max_staleness_mins: None,
                max_concurrent_streams: 4,
            },
            pipeline: PipelineConfig {
                id: 42,
                publication_name: "all-pub".to_string(),
                pg_connection: PgConnectionConfig {
                    host: "localhost".to_string(),
                    port: 5432,
                    name: "postgres".to_string(),
                    username: "postgres".to_string(),
                    password: Some("password".into()),
                    tls: TlsConfig::disabled(),
                    keepalive: None,
                },
                batch: BatchConfig {
                    max_size: 10_000,
                    max_fill_ms: 1_000,
                },
                table_error_retry_delay_ms: 500,
                table_error_retry_max_attempts: 3,
                max_table_sync_workers: 4,
            },
            sentry: None,
            supabase: None,
        };
        let replicator_config_without_secrets: ReplicatorConfigWithoutSecrets =
            replicator_config.into();
        let env_config = serde_json::to_string(&replicator_config_without_secrets).unwrap();

        let files = vec![
            ReplicatorConfigMapFile {
                filename: "base.json".to_string(),
                content: base_config.to_string(),
            },
            ReplicatorConfigMapFile {
                filename: format!("{environment}.json"),
                content: env_config,
            },
        ];

        let config_map_json = create_replicator_config_map_json(
            &replicator_config_map_name,
            &replicator_app_name,
            files,
        );

        assert_json_snapshot!(config_map_json);

        let _config_map: ConfigMap = serde_json::from_value(config_map_json).unwrap();
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
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);

        let environment = Environment::Staging;
        let container_environment = create_container_environment_json(
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);

        let environment = Environment::Prod;
        let container_environment = create_container_environment_json(
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);
    }

    #[test]
    fn test_create_iceberg_container_environment() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        let container_environment = create_container_environment_json(
            &prefix,
            &Environment::Dev,
            replicator_image,
            DestinationType::Iceberg,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);

        let container_environment = create_container_environment_json(
            &prefix,
            &Environment::Staging,
            replicator_image,
            DestinationType::Iceberg,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);

        let container_environment = create_container_environment_json(
            &prefix,
            &Environment::Prod,
            replicator_image,
            DestinationType::Iceberg,
            LogLevel::Info,
        );
        assert_json_snapshot!(container_environment);
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
    fn test_create_init_containers() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);

        let environment = Environment::Dev;
        let config = ReplicatorResourceConfig::load(&environment).unwrap();
        let node_selector = create_init_containers_json(&prefix, &environment, &config);
        assert_json_snapshot!(node_selector);

        let environment = Environment::Staging;
        let config = ReplicatorResourceConfig::load(&environment).unwrap();
        let node_selector = create_init_containers_json(&prefix, &environment, &config);
        assert_json_snapshot!(node_selector);

        let environment = Environment::Prod;
        let config = ReplicatorResourceConfig::load(&environment).unwrap();
        let node_selector = create_init_containers_json(&prefix, &environment, &config);
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
        let stateful_set_name = create_stateful_set_name(&prefix);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        // Dev env
        let environment = Environment::Dev;
        let config = ReplicatorResourceConfig::load(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let init_containers = create_init_containers_json(&prefix, &environment, &config);
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &prefix,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            init_containers,
            volumes,
            volume_mounts,
            &config,
        );

        assert_json_snapshot!(stateful_set_json, { ".spec.template.metadata.annotations[\"etl.supabase.com/restarted-at\"]" => "[timestamp]"});
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();

        // Staging env
        let environment = Environment::Staging;
        let config = ReplicatorResourceConfig::load(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let init_containers = create_init_containers_json(&prefix, &environment, &config);
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &prefix,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            init_containers,
            volumes,
            volume_mounts,
            &config,
        );

        assert_json_snapshot!(stateful_set_json, { ".spec.template.metadata.annotations[\"etl.supabase.com/restarted-at\"]" => "[timestamp]"});
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();

        // Prod env
        let environment = Environment::Prod;
        let config = ReplicatorResourceConfig::load(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &prefix,
            &environment,
            replicator_image,
            DestinationType::BigQuery,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let init_containers = create_init_containers_json(&prefix, &environment, &config);
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &prefix,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            init_containers,
            volumes,
            volume_mounts,
            &config,
        );

        assert_json_snapshot!(stateful_set_json, { ".spec.template.metadata.annotations[\"etl.supabase.com/restarted-at\"]" => "[timestamp]"});
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();
    }

    #[test]
    fn test_create_iceberg_replicator_stateful_set_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let stateful_set_name = create_stateful_set_name(&prefix);
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        // Dev env
        let environment = Environment::Dev;
        let config = ReplicatorResourceConfig::load(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &prefix,
            &environment,
            replicator_image,
            DestinationType::Iceberg,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let init_containers = create_init_containers_json(&prefix, &environment, &config);
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &prefix,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            init_containers,
            volumes,
            volume_mounts,
            &config,
        );

        assert_json_snapshot!(stateful_set_json, { ".spec.template.metadata.annotations[\"etl.supabase.com/restarted-at\"]" => "[timestamp]"});
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();

        // Staging env
        let environment = Environment::Staging;
        let config = ReplicatorResourceConfig::load(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &prefix,
            &environment,
            replicator_image,
            DestinationType::Iceberg,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let init_containers = create_init_containers_json(&prefix, &environment, &config);
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &prefix,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            init_containers,
            volumes,
            volume_mounts,
            &config,
        );

        assert_json_snapshot!(stateful_set_json, { ".spec.template.metadata.annotations[\"etl.supabase.com/restarted-at\"]" => "[timestamp]"});
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();

        // Prod env
        let environment = Environment::Prod;
        let config = ReplicatorResourceConfig::load(&environment).unwrap();

        let container_environment = create_container_environment_json(
            &prefix,
            &environment,
            replicator_image,
            DestinationType::Iceberg,
            LogLevel::Info,
        );

        let node_selector = create_node_selector_json(&environment);
        let init_containers = create_init_containers_json(&prefix, &environment, &config);
        let volumes = create_volumes_json(&prefix, &environment);
        let volume_mounts = create_volume_mounts_json(&environment);

        let stateful_set_json = create_replicator_stateful_set_json(
            &prefix,
            &stateful_set_name,
            replicator_image,
            container_environment,
            node_selector,
            init_containers,
            volumes,
            volume_mounts,
            &config,
        );

        assert_json_snapshot!(stateful_set_json, { ".spec.template.metadata.annotations[\"etl.supabase.com/restarted-at\"]" => "[timestamp]"});
        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();
    }
}
