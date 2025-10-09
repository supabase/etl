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
use tracing::info;

/// Secret name suffix for the BigQuery service account key.
const BQ_SECRET_NAME_SUFFIX: &str = "bq-service-account-key";
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
/// EmptyDir volume name used to share logs.
const LOGS_VOLUME_NAME: &str = "logs";
/// ConfigMap name providing trusted root certificates.
pub const TRUSTED_ROOT_CERT_CONFIG_MAP_NAME: &str = "trusted-root-certs-config";
/// Key inside the trusted root certificates ConfigMap.
pub const TRUSTED_ROOT_CERT_KEY_NAME: &str = "trusted_root_certs";
/// Pod template annotation used to trigger rolling restarts.
pub const RESTARTED_AT_ANNOTATION_KEY: &str = "etl.supabase.com/restarted-at";
/// Label used to identify replicator pods.
const REPLICATOR_APP_LABEL: &str = "etl-replicator-app";

/// Replicator memory limit tuned for `c6in.4xlarge` instances.
const REPLICATOR_MAX_MEMORY_PROD: &str = "500Mi";
/// Replicator CPU limit tuned for `c6in.4xlarge` instances.
const REPLICATOR_MAX_CPU_PROD: &str = "100m";
/// Replicator memory limit tuned for `t3.small` instances.
const REPLICATOR_MAX_MEMORY_STAGING: &str = "100Mi";
/// Replicator CPU limit tuned for `t3.small` instances.
const REPLICATOR_MAX_CPU_STAGING: &str = "100m";

/// Resource limits for a replicator pod.
struct ReplicatorResourceConfig {
    max_memory: &'static str,
    max_cpu: &'static str,
}

impl ReplicatorResourceConfig {
    /// Loads the runtime limits for the current environment.
    fn load(environment: &Environment) -> Result<Self, K8sError> {
        let config = match environment {
            Environment::Prod => Self {
                max_memory: REPLICATOR_MAX_MEMORY_PROD,
                max_cpu: REPLICATOR_MAX_CPU_PROD,
            },
            _ => Self {
                max_memory: REPLICATOR_MAX_MEMORY_STAGING,
                max_cpu: REPLICATOR_MAX_CPU_STAGING,
            },
        };

        Ok(config)
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
    pub async fn new() -> Result<HttpK8sClient, K8sError> {
        let client = Client::try_default().await?;

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
}

#[async_trait]
impl K8sClient for HttpK8sClient {
    async fn create_or_update_postgres_secret(
        &self,
        prefix: &str,
        postgres_password: &str,
    ) -> Result<(), K8sError> {
        info!("patching postgres secret");

        let encoded_postgres_password = BASE64_STANDARD.encode(postgres_password);
        let postgres_secret_name = create_postgres_secret_name(prefix);
        let secret_json =
            create_postgres_secret_json(&postgres_secret_name, &encoded_postgres_password);
        let secret: Secret = serde_json::from_value(secret_json)?;

        let pp = PatchParams::apply(&postgres_secret_name);
        self.secrets_api
            .patch(&postgres_secret_name, &pp, &Patch::Apply(secret))
            .await?;
        info!("patched postgres secret");

        Ok(())
    }

    async fn create_or_update_bq_secret(
        &self,
        prefix: &str,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError> {
        info!("patching bq secret");

        let encoded_bq_service_account_key = BASE64_STANDARD.encode(bq_service_account_key);
        let bq_secret_name = create_bq_secret_name(prefix);
        let secret_json = create_bq_service_account_key_secret_json(
            &bq_secret_name,
            &encoded_bq_service_account_key,
        );
        let secret: Secret = serde_json::from_value(secret_json)?;

        let pp = PatchParams::apply(&bq_secret_name);
        self.secrets_api
            .patch(&bq_secret_name, &pp, &Patch::Apply(secret))
            .await?;
        info!("patched bq secret");

        Ok(())
    }

    async fn delete_postgres_secret(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting postgres secret");
        let postgres_secret_name = create_postgres_secret_name(prefix);
        let dp = DeleteParams::default();
        match self.secrets_api.delete(&postgres_secret_name, &dp).await {
            Ok(_) => {}
            Err(e) => match e {
                kube::Error::Api(ref er) => {
                    if er.code != 404 {
                        return Err(e.into());
                    }
                }
                e => return Err(e.into()),
            },
        }
        info!("deleted postgres secret");
        Ok(())
    }

    async fn delete_bq_secret(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting bq secret");
        let bq_secret_name = create_bq_secret_name(prefix);
        let dp = DeleteParams::default();
        match self.secrets_api.delete(&bq_secret_name, &dp).await {
            Ok(_) => {}
            Err(e) => match e {
                kube::Error::Api(ref er) => {
                    if er.code != 404 {
                        return Err(e.into());
                    }
                }
                e => return Err(e.into()),
            },
        }
        info!("deleted bq secret");
        Ok(())
    }

    async fn get_config_map(&self, config_map_name: &str) -> Result<ConfigMap, K8sError> {
        info!("getting config map");
        let config_map = match self.config_maps_api.get(config_map_name).await {
            Ok(config_map) => config_map,
            Err(e) => {
                return Err(e.into());
            }
        };
        info!("got config map");
        Ok(config_map)
    }

    async fn create_or_update_config_map(
        &self,
        prefix: &str,
        base_config: &str,
        env_config: &str,
        environment: Environment,
    ) -> Result<(), K8sError> {
        info!("patching config map");

        let env_config_file = format!("{environment}.yaml");
        let replicator_config_map_name = create_replicator_config_map_name(prefix);
        let config_map_json = create_replicator_config_map_json(
            &replicator_config_map_name,
            base_config,
            &env_config_file,
            env_config,
        );
        let config_map: ConfigMap = serde_json::from_value(config_map_json)?;

        let pp = PatchParams::apply(&replicator_config_map_name);
        self.config_maps_api
            .patch(&replicator_config_map_name, &pp, &Patch::Apply(config_map))
            .await?;
        info!("patched config map");
        Ok(())
    }

    async fn delete_config_map(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting config map");
        let replicator_config_map_name = create_replicator_config_map_name(prefix);
        let dp = DeleteParams::default();
        match self
            .config_maps_api
            .delete(&replicator_config_map_name, &dp)
            .await
        {
            Ok(_) => {}
            Err(e) => match e {
                kube::Error::Api(ref er) => {
                    if er.code != 404 {
                        return Err(e.into());
                    }
                }
                e => return Err(e.into()),
            },
        }
        info!("deleted config map");
        Ok(())
    }

    async fn create_or_update_stateful_set(
        &self,
        prefix: &str,
        replicator_image: &str,
        environment: Environment,
    ) -> Result<(), K8sError> {
        info!("patching stateful set");

        let config = ReplicatorResourceConfig::load(&environment)?;

        let stateful_set_name = create_stateful_set_name(prefix);
        let replicator_app_name = format!("{prefix}-{REPLICATOR_APP_SUFFIX}");
        let restarted_at_annotation = get_restarted_at_annotation_value();
        let replicator_container_name = format!("{prefix}-{REPLICATOR_CONTAINER_NAME_SUFFIX}");
        let vector_container_name = format!("{prefix}-{VECTOR_CONTAINER_NAME_SUFFIX}");
        let postgres_secret_name = create_postgres_secret_name(prefix);
        let bq_secret_name = create_bq_secret_name(prefix);
        let replicator_config_map_name = create_replicator_config_map_name(prefix);

        let stateful_set_json = create_replicator_stateful_set_json(
            &stateful_set_name,
            &replicator_app_name,
            &restarted_at_annotation,
            &replicator_config_map_name,
            &vector_container_name,
            &config,
            &replicator_container_name,
            replicator_image,
            &environment,
            &postgres_secret_name,
            &bq_secret_name,
        );

        let stateful_set: StatefulSet = serde_json::from_value(stateful_set_json)?;

        let pp = PatchParams::apply(&stateful_set_name);
        self.stateful_sets_api
            .patch(&stateful_set_name, &pp, &Patch::Apply(stateful_set))
            .await?;

        info!("patched stateful set");

        Ok(())
    }

    async fn delete_stateful_set(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting stateful set");

        let stateful_set_name = create_stateful_set_name(prefix);
        let dp = DeleteParams::default();
        match self.stateful_sets_api.delete(&stateful_set_name, &dp).await {
            Ok(_) => {}
            Err(e) => match e {
                kube::Error::Api(ref er) => {
                    if er.code != 404 {
                        return Err(e.into());
                    }
                }
                e => return Err(e.into()),
            },
        }

        info!("deleted stateful set");

        Ok(())
    }

    async fn get_pod_phase(&self, prefix: &str) -> Result<PodPhase, K8sError> {
        info!("getting pod status");

        let pod_name = create_pod_name(prefix);
        let pod = match self.pods_api.get(&pod_name).await {
            Ok(pod) => pod,
            Err(e) => {
                return match e {
                    kube::Error::Api(ref er) => {
                        if er.code == 404 {
                            return Ok(PodPhase::Succeeded);
                        }

                        Err(e.into())
                    }
                    e => Err(e.into()),
                };
            }
        };

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

        Ok(phase)
    }

    async fn has_replicator_container_error(&self, prefix: &str) -> Result<bool, K8sError> {
        info!("checking for replicator container error");

        let pod_name = create_pod_name(prefix);
        let pod = match self.pods_api.get(&pod_name).await {
            Ok(pod) => pod,
            Err(e) => {
                return match e {
                    kube::Error::Api(ref er) => {
                        if er.code == 404 {
                            return Ok(false);
                        }
                        Err(e.into())
                    }
                    e => Err(e.into()),
                };
            }
        };

        let replicator_container_name = format!("{prefix}-{REPLICATOR_CONTAINER_NAME_SUFFIX}");

        // Find the replicator container status
        let container_status = pod.status.and_then(|status| {
            status.container_statuses.and_then(|container_statuses| {
                container_statuses
                    .iter()
                    .find(|cs| cs.name == replicator_container_name)
                    .cloned()
            })
        });

        let Some(container_status) = container_status else {
            return Ok(false);
        };

        // Check last terminated state for non-zero exit code
        if let Some(last_state) = &container_status.last_state
            && let Some(terminated) = &last_state.terminated
        {
            return Ok(terminated.exit_code != 0);
        }

        Ok(false)
    }
}

fn create_postgres_secret_name(prefix: &str) -> String {
    format!("{prefix}-{POSTGRES_SECRET_NAME_SUFFIX}")
}

fn create_bq_secret_name(prefix: &str) -> String {
    format!("{prefix}-{BQ_SECRET_NAME_SUFFIX}")
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

fn create_postgres_secret_json(
    secret_name: &str,
    encoded_postgres_password: &str,
) -> serde_json::Value {
    json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": secret_name,
        "namespace": DATA_PLANE_NAMESPACE,
      },
      "type": "Opaque",
      "data": {
        "password": encoded_postgres_password,
      }
    })
}

fn create_bq_service_account_key_secret_json(
    secret_name: &str,
    encoded_bq_service_account_key: &str,
) -> serde_json::Value {
    json!({
      "apiVersion": "v1",
      "kind": "Secret",
      "metadata": {
        "name": secret_name,
        "namespace": DATA_PLANE_NAMESPACE,
      },
      "type": "Opaque",
      "data": {
        "service-account-key": encoded_bq_service_account_key,
      }
    })
}

fn create_replicator_config_map_json(
    config_map_name: &str,
    base_config: &str,
    env_config_file: &str,
    env_config: &str,
) -> serde_json::Value {
    json!({
      "kind": "ConfigMap",
      "apiVersion": "v1",
      "metadata": {
        "name": config_map_name,
        "namespace": DATA_PLANE_NAMESPACE,
      },
      "data": {
        "base.yaml": base_config,
        env_config_file: env_config,
      }
    })
}

#[expect(clippy::too_many_arguments)]
fn create_replicator_stateful_set_json(
    stateful_set_name: &str,
    replicator_app_name: &str,
    restarted_at_annotation: &str,
    replicator_config_map_name: &str,
    vector_container_name: &str,
    config: &ReplicatorResourceConfig,
    replicator_container_name: &str,
    replicator_image: &str,
    environment: &Environment,
    postgres_secret_name: &str,
    bq_secret_name: &str,
) -> serde_json::Value {
    json!({
      "apiVersion": "apps/v1",
      "kind": "StatefulSet",
      "metadata": {
        "name": stateful_set_name,
        "namespace": DATA_PLANE_NAMESPACE,
      },
      "spec": {
        "replicas": 1,
        "selector": {
          "matchLabels": {
            "app-name": replicator_app_name,
          }
        },
        "template": {
          "metadata": {
            "labels": {
              "app-name": replicator_app_name,
              "app": REPLICATOR_APP_LABEL
            },
            "annotations": {
              // Attach template annotations (e.g., restart checksum) to trigger a rolling restart
              RESTARTED_AT_ANNOTATION_KEY: restarted_at_annotation,
            }
          },
          "spec": {
            "volumes": [
              {
                "name": REPLICATOR_CONFIG_FILE_VOLUME_NAME,
                "configMap": {
                  "name": replicator_config_map_name
                }
              },
              {
                "name": VECTOR_CONFIG_FILE_VOLUME_NAME,
                "configMap": {
                  "name": VECTOR_CONFIG_MAP_NAME
                }
              },
              {
                "name": LOGS_VOLUME_NAME,
                "emptyDir": {}
              }
            ],
            // Allow scheduling onto nodes tainted with `nodeType=workloads`.
            "tolerations": [
              {
                "key": "nodeType",
                "operator": "Equal",
                "value": "workloads",
                "effect": "NoSchedule"
              }
            ],
            // Pin pods to nodes labeled with `nodeType=workloads`.
            "nodeSelector": {
              "nodeType": "workloads"
            },
            // We want to wait at most 5 minutes before K8S sends a `SIGKILL` to the containers,
            // this way we let the system finish any in-flight transaction, if there are any.
            "terminationGracePeriodSeconds": 300,
            "initContainers": [
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
                    "memory": config.max_memory,
                    "cpu": config.max_cpu,
                  },
                  "requests": {
                    "memory": config.max_memory,
                    "cpu": config.max_cpu,
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
            ],
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
                "env": [
                  {
                    "name": "APP_ENVIRONMENT",
                    "value": environment.to_string()
                  },
                  {
                      "name": "APP_VERSION",
                      //TODO: set APP_VERSION to proper version instead of the replicator image name
                      "value": replicator_image
                  },
                  {
                    "name": "APP_SENTRY__DSN",
                    "valueFrom": {
                      "secretKeyRef": {
                        "name": SENTRY_DSN_SECRET_NAME,
                        "key": "dsn"
                      }
                    }
                  },
                  {
                    "name": "APP_PIPELINE__PG_CONNECTION__PASSWORD",
                    "valueFrom": {
                      "secretKeyRef": {
                        "name": postgres_secret_name,
                        "key": "password"
                      }
                    }
                  },
                  {
                    "name": "APP_DESTINATION__BIG_QUERY__SERVICE_ACCOUNT_KEY",
                    "valueFrom": {
                      "secretKeyRef": {
                        "name": bq_secret_name,
                        "key": "service-account-key"
                      }
                    }
                  }
                ],
                "volumeMounts": [
                  {
                    "name": REPLICATOR_CONFIG_FILE_VOLUME_NAME,
                    "mountPath": "/app/configuration"
                  },
                  {
                    "name": LOGS_VOLUME_NAME,
                    "mountPath": "/app/logs"
                  },
                ]
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
    use etl_config::{
        SerializableSecretString,
        shared::{
            BatchConfig, DestinationConfig, PgConnectionConfig, PipelineConfig, ReplicatorConfig,
            TlsConfig,
        },
    };
    use insta::assert_json_snapshot;

    const TENANT_ID: &str = "abcdefghijklmnopqrst";

    fn create_k8s_object_prefix(tenant_id: &str, replicator_id: i64) -> String {
        format!("{tenant_id}-{replicator_id}")
    }

    #[test]
    fn test_create_postgres_secret_json() {
        let secret_name = "test-secret";
        let encoded_postgres_password = "dGVzdC1wYXNzd29yZA==";

        let secret_json = create_postgres_secret_json(secret_name, encoded_postgres_password);

        assert_json_snapshot!(secret_json);

        let _secret: Secret = serde_json::from_value(secret_json).unwrap();
    }

    #[test]
    fn test_create_bq_service_account_key_secret_json() {
        let secret_name = "test-secret";
        let encoded_bq_service_account_key = "ewogICJrZXkiOiAidmFsdWUiCn0=";

        let secret_json =
            create_bq_service_account_key_secret_json(secret_name, encoded_bq_service_account_key);

        assert_json_snapshot!(secret_json);

        let _secret: Secret = serde_json::from_value(secret_json).unwrap();
    }

    #[test]
    fn test_create_replicator_config_map_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let replicator_config_map_name = create_replicator_config_map_name(&prefix);
        let environment = Environment::Prod;
        let env_config_file = format!("{environment}.yaml");
        let base_config = "";
        let replicator_config = ReplicatorConfig {
            destination: DestinationConfig::BigQuery {
                project_id: "project-id".to_string(),
                dataset_id: "dataset-id".to_string(),
                service_account_key: SerializableSecretString::from("sa-key".to_string()),
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
                    password: None,
                    tls: TlsConfig {
                        trusted_root_certs: "".to_string(),
                        enabled: false,
                    },
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
        let env_config = serde_json::to_string(&replicator_config).unwrap();

        let config_map_json = create_replicator_config_map_json(
            &replicator_config_map_name,
            base_config,
            &env_config_file,
            &env_config,
        );

        assert_json_snapshot!(config_map_json);

        let _config_map: ConfigMap = serde_json::from_value(config_map_json).unwrap();
    }

    #[test]
    fn test_create_replicator_stateful_set_json() {
        let prefix = create_k8s_object_prefix(TENANT_ID, 42);
        let stateful_set_name = create_stateful_set_name(&prefix);
        let replicator_app_name = format!("{prefix}-{REPLICATOR_APP_SUFFIX}");
        let restarted_at_annotation = "2025-10-09T16:02:24.127400000Z";
        let replicator_container_name = format!("{prefix}-{REPLICATOR_CONTAINER_NAME_SUFFIX}");
        let vector_container_name = format!("{prefix}-{VECTOR_CONTAINER_NAME_SUFFIX}");
        let postgres_secret_name = create_postgres_secret_name(&prefix);
        let bq_secret_name = create_bq_secret_name(&prefix);
        let replicator_config_map_name = create_replicator_config_map_name(&prefix);
        let environment = Environment::Prod;
        let config = ReplicatorResourceConfig::load(&environment).unwrap();
        let replicator_image = "ramsup/etl-replicator:2a41356af735f891de37d71c0e1a62864fe4630e";

        let stateful_set_json = create_replicator_stateful_set_json(
            &stateful_set_name,
            &replicator_app_name,
            restarted_at_annotation,
            &replicator_config_map_name,
            &vector_container_name,
            &config,
            &replicator_container_name,
            replicator_image,
            &environment,
            &postgres_secret_name,
            &bq_secret_name,
        );

        assert_json_snapshot!(stateful_set_json);

        let _stateful_set: StatefulSet = serde_json::from_value(stateful_set_json).unwrap();
    }
}
