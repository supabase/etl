use std::collections::BTreeMap;

use async_trait::async_trait;
use etl_api::k8s_client::{
    K8sClient, K8sError, PodPhase, TRUSTED_ROOT_CERT_CONFIG_MAP_NAME, TRUSTED_ROOT_CERT_KEY_NAME,
};
use k8s_openapi::api::core::v1::ConfigMap;

pub struct MockK8sClient;

#[async_trait]
impl K8sClient for MockK8sClient {
    async fn create_or_update_postgres_secret(
        &self,
        _prefix: &str,
        _postgres_password: &str,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn create_or_update_bq_secret(
        &self,
        _prefix: &str,
        _bq_service_account_key: &str,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_postgres_secret(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_bq_secret(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn get_config_map(&self, config_map_name: &str) -> Result<ConfigMap, K8sError> {
        // For tests to pass the TRUSTED_ROOT_CERT_CONFIG_MAP_NAME config map expects
        // a key named TRUSTED_ROOT_CERT_KEY_NAME to be present
        if config_map_name == TRUSTED_ROOT_CERT_CONFIG_MAP_NAME {
            let mut map = BTreeMap::new();
            map.insert(TRUSTED_ROOT_CERT_KEY_NAME.to_string(), String::new());
            let cm = ConfigMap {
                data: Some(map),
                ..ConfigMap::default()
            };
            Ok(cm)
        } else {
            Ok(ConfigMap::default())
        }
    }

    async fn create_or_update_config_map(
        &self,
        _prefix: &str,
        _base_config: &str,
        _prod_config: &str,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_config_map(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn create_or_update_stateful_set(
        &self,
        _prefix: &str,
        _replicator_image: &str,
    ) -> Result<(), K8sError> {
        Ok(())
    }

    async fn delete_stateful_set(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }

    async fn get_pod_phase(&self, _prefix: &str) -> Result<PodPhase, K8sError> {
        Ok(PodPhase::Running)
    }

    async fn has_replicator_container_error(&self, _prefix: &str) -> Result<bool, K8sError> {
        Ok(false)
    }

    async fn delete_pod(&self, _prefix: &str) -> Result<(), K8sError> {
        Ok(())
    }
}
