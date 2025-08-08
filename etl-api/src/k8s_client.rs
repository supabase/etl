use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_STANDARD};
use k8s_openapi::api::{
    apps::v1::StatefulSet,
    core::v1::{ConfigMap, Pod, Secret},
};
use serde_json::json;
use thiserror::Error;
use tracing::info;

use kube::{
    Client,
    api::{Api, DeleteParams, Patch, PatchParams},
};

#[derive(Debug, Error)]
pub enum K8sError {
    #[error("serde_json error: {0}")]
    Serde(#[from] serde_json::error::Error),

    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
}

pub enum PodPhase {
    Pending,
    Running,
    Succeeded,
    Failed,
    Unknown,
}

impl From<&str> for PodPhase {
    fn from(value: &str) -> Self {
        match value {
            "Pending" => PodPhase::Pending,
            "Running" => PodPhase::Running,
            "Succeeded" => PodPhase::Succeeded,
            "Failed" => PodPhase::Failed,
            _ => PodPhase::Unknown,
        }
    }
}

#[async_trait]
pub trait K8sClient: Send + Sync {
    async fn create_or_update_postgres_secret(
        &self,
        prefix: &str,
        postgres_password: &str,
    ) -> Result<(), K8sError>;

    async fn create_or_update_bq_secret(
        &self,
        prefix: &str,
        bq_service_account_key: &str,
    ) -> Result<(), K8sError>;

    async fn delete_postgres_secret(&self, prefix: &str) -> Result<(), K8sError>;

    async fn delete_bq_secret(&self, prefix: &str) -> Result<(), K8sError>;

    async fn get_config_map(&self, config_map_name: &str) -> Result<ConfigMap, K8sError>;

    async fn create_or_update_config_map(
        &self,
        prefix: &str,
        base_config: &str,
        prod_config: &str,
    ) -> Result<(), K8sError>;

    async fn delete_config_map(&self, prefix: &str) -> Result<(), K8sError>;

    async fn create_or_update_stateful_set(
        &self,
        prefix: &str,
        replicator_image: &str,
    ) -> Result<(), K8sError>;

    async fn delete_stateful_set(&self, prefix: &str) -> Result<(), K8sError>;

    async fn get_pod_phase(&self, prefix: &str) -> Result<PodPhase, K8sError>;

    async fn has_replicator_container_error(&self, prefix: &str) -> Result<bool, K8sError>;

    async fn delete_pod(&self, prefix: &str) -> Result<(), K8sError>;
}

#[derive(Debug)]
pub struct HttpK8sClient {
    secrets_api: Api<Secret>,
    config_maps_api: Api<ConfigMap>,
    stateful_sets_api: Api<StatefulSet>,
    pods_api: Api<Pod>,
}

const BQ_SECRET_NAME_SUFFIX: &str = "bq-service-account-key";
const POSTGRES_SECRET_NAME_SUFFIX: &str = "postgres-password";
const REPLICATOR_CONFIG_MAP_NAME_SUFFIX: &str = "replicator-config";
const STATEFUL_SET_NAME_SUFFIX: &str = "replicator";
const REPLICATOR_CONTAINER_NAME_SUFFIX: &str = "replicator";
const VECTOR_CONTAINER_NAME_SUFFIX: &str = "vector";
const NAMESPACE_NAME: &str = "replicator-data-plane";
const LOGFLARE_SECRET_NAME: &str = "replicator-logflare-api-key";
const VECTOR_IMAGE_NAME: &str = "timberio/vector:0.46.1-distroless-libc";
const VECTOR_CONFIG_MAP_NAME: &str = "replicator-vector-config";
const REPLICATOR_CONFIG_FILE_VOLUME_NAME: &str = "replicator-config-file";
const VECTOR_CONFIG_FILE_VOLUME_NAME: &str = "vector-config-file";
const SENTRY_DSN_SECRET_NAME: &str = "replicator-sentry-dsn";
const LOGS_VOLUME_NAME: &str = "logs";
pub const TRUSTED_ROOT_CERT_CONFIG_MAP_NAME: &str = "trusted-root-certs-config";
pub const TRUSTED_ROOT_CERT_KEY_NAME: &str = "trusted_root_certs";
const PG_PASSWORD_ENV_VAR_NAME: &str = "APP_PIPELINE__PG_CONNECTION__PASSWORD";
const BIG_QUERY_SA_KEY_ENV_VAR_NAME: &str = "APP_DESTINATION__BIG_QUERY__SERVICE_ACCOUNT_KEY";

impl HttpK8sClient {
    pub async fn new() -> Result<HttpK8sClient, K8sError> {
        let client = Client::try_default().await?;

        let secrets_api: Api<Secret> = Api::namespaced(client.clone(), NAMESPACE_NAME);
        let config_maps_api: Api<ConfigMap> = Api::namespaced(client.clone(), NAMESPACE_NAME);
        let stateful_sets_api: Api<StatefulSet> = Api::namespaced(client.clone(), NAMESPACE_NAME);
        let pods_api: Api<Pod> = Api::namespaced(client, NAMESPACE_NAME);

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
        let secret_name = format!("{prefix}-{POSTGRES_SECRET_NAME_SUFFIX}");
        let secret_json = json!({
          "apiVersion": "v1",
          "kind": "Secret",
          "metadata": {
            "name": secret_name,
            "namespace": NAMESPACE_NAME,
          },
          "type": "Opaque",
          "data": {
            "password": encoded_postgres_password,
          }
        });
        let secret: Secret = serde_json::from_value(secret_json)?;

        let pp = PatchParams::apply(&secret_name);
        self.secrets_api
            .patch(&secret_name, &pp, &Patch::Apply(secret))
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
        let secret_name = format!("{prefix}-{BQ_SECRET_NAME_SUFFIX}");
        let secret_json = json!({
          "apiVersion": "v1",
          "kind": "Secret",
          "metadata": {
            "name": secret_name,
            "namespace": NAMESPACE_NAME,
          },
          "type": "Opaque",
          "data": {
            "service-account-key": encoded_bq_service_account_key,
          }
        });
        let secret: Secret = serde_json::from_value(secret_json)?;

        let pp = PatchParams::apply(&secret_name);
        self.secrets_api
            .patch(&secret_name, &pp, &Patch::Apply(secret))
            .await?;
        info!("patched bq secret");

        Ok(())
    }

    async fn delete_postgres_secret(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting postgres secret");
        let secret_name = format!("{prefix}-{POSTGRES_SECRET_NAME_SUFFIX}");
        let dp = DeleteParams::default();
        match self.secrets_api.delete(&secret_name, &dp).await {
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
        let secret_name = format!("{prefix}-{BQ_SECRET_NAME_SUFFIX}");
        let dp = DeleteParams::default();
        match self.secrets_api.delete(&secret_name, &dp).await {
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
        prod_config: &str,
    ) -> Result<(), K8sError> {
        info!("patching config map");

        let config_map_name = format!("{prefix}-{REPLICATOR_CONFIG_MAP_NAME_SUFFIX}");
        let config_map_json = json!({
          "kind": "ConfigMap",
          "apiVersion": "v1",
          "metadata": {
            "name": config_map_name,
            "namespace": NAMESPACE_NAME,
          },
          "data": {
            "base.yaml": base_config,
            "prod.yaml": prod_config,
          }
        });
        // TODO: for consistency we might want to use `serde_yaml` since writing a `.yaml` as JSON.
        let config_map: ConfigMap = serde_json::from_value(config_map_json)?;

        let pp = PatchParams::apply(&config_map_name);
        self.config_maps_api
            .patch(&config_map_name, &pp, &Patch::Apply(config_map))
            .await?;
        info!("patched config map");
        Ok(())
    }

    async fn delete_config_map(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting config map");
        let config_map_name = format!("{prefix}-{REPLICATOR_CONFIG_MAP_NAME_SUFFIX}");
        let dp = DeleteParams::default();
        match self.config_maps_api.delete(&config_map_name, &dp).await {
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
    ) -> Result<(), K8sError> {
        info!("patching stateful set");

        let stateful_set_name = format!("{prefix}-{STATEFUL_SET_NAME_SUFFIX}");
        let replicator_container_name = format!("{prefix}-{REPLICATOR_CONTAINER_NAME_SUFFIX}");
        let vector_container_name = format!("{prefix}-{VECTOR_CONTAINER_NAME_SUFFIX}");
        let postgres_secret_name = format!("{prefix}-{POSTGRES_SECRET_NAME_SUFFIX}");
        let bq_secret_name = format!("{prefix}-{BQ_SECRET_NAME_SUFFIX}");
        let replicator_config_map_name = format!("{prefix}-{REPLICATOR_CONFIG_MAP_NAME_SUFFIX}");

        let stateful_set_json = json!({
          "apiVersion": "apps/v1",
          "kind": "StatefulSet",
          "metadata": {
            "name": stateful_set_name,
            "namespace": NAMESPACE_NAME,
          },
          "spec": {
            "replicas": 1,
            "selector": {
              "matchLabels": {
                "app": stateful_set_name
              }
            },
            "template": {
              "metadata": {
                "labels": {
                  "app": stateful_set_name
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
                // We want to wait at most 60 seconds before K8S sends a `SIGKILL` to the containers.
                "terminationGracePeriodSeconds": 60,
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
                        "memory": "200Mi",
                      },
                      "requests": {
                        "memory": "200Mi",
                        "cpu": "100m"
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
                    "env": [
                      {
                        "name": "APP_ENVIRONMENT",
                        "value": "prod"
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
                        "name": PG_PASSWORD_ENV_VAR_NAME,
                        "valueFrom": {
                          "secretKeyRef": {
                            "name": postgres_secret_name,
                            "key": "password"
                          }
                        }
                      },
                      {
                        "name": BIG_QUERY_SA_KEY_ENV_VAR_NAME,
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
        });

        let stateful_set: StatefulSet = serde_json::from_value(stateful_set_json)?;

        let pp = PatchParams::apply(&stateful_set_name);
        self.stateful_sets_api
            .patch(&stateful_set_name, &pp, &Patch::Apply(stateful_set))
            .await?;

        self.delete_pod(prefix).await?;

        info!("patched stateful set");

        Ok(())
    }

    async fn delete_stateful_set(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting stateful set");

        let stateful_set_name = format!("{prefix}-{STATEFUL_SET_NAME_SUFFIX}");
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
        self.delete_pod(prefix).await?;
        info!("deleted stateful set");

        Ok(())
    }

    async fn get_pod_phase(&self, prefix: &str) -> Result<PodPhase, K8sError> {
        info!("getting pod status");

        let pod_name = format!("{prefix}-{STATEFUL_SET_NAME_SUFFIX}-0");
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

        let pod_name = format!("{prefix}-{STATEFUL_SET_NAME_SUFFIX}-0");
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

    async fn delete_pod(&self, prefix: &str) -> Result<(), K8sError> {
        info!("deleting pod");

        let pod_name = format!("{prefix}-{STATEFUL_SET_NAME_SUFFIX}-0");
        let dp = DeleteParams::default();
        match self.pods_api.delete(&pod_name, &dp).await {
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
        info!("deleted pod");

        Ok(())
    }
}
