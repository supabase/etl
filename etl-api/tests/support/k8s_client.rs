#![allow(dead_code)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use etl_api::configs::log::LogLevel;
use etl_api::k8s::http::{TRUSTED_ROOT_CERT_CONFIG_MAP_NAME, TRUSTED_ROOT_CERT_KEY_NAME};
use etl_api::k8s::{DestinationType, K8sClient, K8sError, PodStatus, ReplicatorConfigMapFile};
use etl_config::Environment;
use k8s_openapi::api::core::v1::ConfigMap;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct MockK8sState {
    pod_status: Arc<RwLock<PodStatus>>,
    create_calls: Arc<AtomicUsize>,
}

impl Default for MockK8sState {
    fn default() -> Self {
        Self {
            pod_status: Arc::new(RwLock::new(PodStatus::Started)),
            create_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl MockK8sState {
    pub async fn set_pod_status(&self, pod_status: PodStatus) {
        *self.pod_status.write().await = pod_status;
    }

    pub fn create_calls(&self) -> usize {
        self.create_calls.load(Ordering::Relaxed)
    }
}

pub struct MockK8sClient {
    state: MockK8sState,
}

impl MockK8sClient {
    pub fn new(state: MockK8sState) -> Self {
        Self { state }
    }

    fn record_create_call(&self) {
        self.state.create_calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl K8sClient for MockK8sClient {
    async fn create_or_update_postgres_secret(
        &self,
        _prefix: &str,
        _postgres_password: &str,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn create_or_update_bigquery_secret(
        &self,
        _prefix: &str,
        _bq_service_account_key: &str,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn create_or_update_iceberg_secret(
        &self,
        _prefix: &str,
        _catalog_token: &str,
        _s3_access_key_id: &str,
        _s3_secret_access_key: &str,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn create_or_update_ducklake_secret(
        &self,
        _prefix: &str,
        _s3_access_key_id: &str,
        _s3_secret_access_key: &str,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_postgres_secret(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_bigquery_secret(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_iceberg_secret(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_ducklake_secret(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn get_config_map(&self, config_map_name: &str) -> Result<ConfigMap, K8sError> {
        // For tests to pass the TRUSTED_ROOT_CERT_CONFIG_MAP_NAME config map expects
        // a key named TRUSTED_ROOT_CERT_KEY_NAME to be present with non-empty certs
        if config_map_name == TRUSTED_ROOT_CERT_CONFIG_MAP_NAME {
            let mut map = BTreeMap::new();
            // Use a dummy certificate for tests - the actual TLS connection won't use this
            // since test postgres doesn't have TLS enabled
            map.insert(
                TRUSTED_ROOT_CERT_KEY_NAME.to_string(),
                "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----".to_string(),
            );
            let cm = ConfigMap {
                data: Some(map),
                ..ConfigMap::default()
            };
            Ok(cm)
        } else {
            Ok(ConfigMap::default())
        }
    }

    async fn create_or_update_replicator_config_map(
        &self,
        _prefix: &str,
        _files: Vec<ReplicatorConfigMapFile>,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn delete_replicator_config_map(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn create_or_update_replicator_stateful_set(
        &self,
        _prefix: &str,
        _replicator_image: &str,
        _environment: Environment,
        _destination_type: DestinationType,
        _log_level: LogLevel,
    ) -> Result<(), K8sError> {
        self.record_create_call();
        Ok(())
    }

    async fn delete_replicator_stateful_set(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn get_replicator_pod_status(&self, _prefix: &str) -> Result<PodStatus, K8sError> {
        Ok(*self.state.pod_status.read().await)
    }
}
