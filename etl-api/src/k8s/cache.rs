use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::RwLock;
use tracing::debug;

use etl_config::shared::TlsConfig;

use crate::k8s::http::{TRUSTED_ROOT_CERT_CONFIG_MAP_NAME, TRUSTED_ROOT_CERT_KEY_NAME};
use crate::k8s::{K8sClient, K8sError};

/// Errors that can occur when fetching trusted root certificates.
#[derive(Debug, Error)]
pub enum TrustedRootCertsError {
    /// The ConfigMap data field is missing.
    #[error("trusted root certificates ConfigMap data is missing")]
    ConfigMapDataMissing,
    /// The trusted root certificates key is missing from the ConfigMap.
    #[error("trusted root certificates key is missing from ConfigMap")]
    CertsKeyMissing,
    /// The trusted root certificates value is empty.
    #[error("trusted root certificates are empty")]
    CertsEmpty,
    /// A Kubernetes API error occurred.
    #[error("Kubernetes error: {0}")]
    K8s(#[from] K8sError),
}

/// Cached trusted root certificates with expiration tracking.
struct CachedCerts {
    certs: String,
    fetched_at: Instant,
}

/// In-memory cache for trusted root certificates from Kubernetes.
///
/// Fetches and caches the trusted root certificates ConfigMap from Kubernetes,
/// refreshing the cache when it expires. The cache uses a 1-day TTL.
pub struct TrustedRootCertsCache {
    k8s_client: Arc<dyn K8sClient>,
    cache: RwLock<Option<CachedCerts>>,
}

impl TrustedRootCertsCache {
    /// Cache time-to-live duration (1 day).
    const TTL: Duration = Duration::from_secs(24 * 60 * 60);

    /// Creates a new cache backed by the provided Kubernetes client.
    pub fn new(k8s_client: Arc<dyn K8sClient>) -> Self {
        Self {
            k8s_client,
            cache: RwLock::new(None),
        }
    }

    /// Returns a [`TlsConfig`] based on whether TLS is enabled.
    ///
    /// When `tls_enabled` is `true`, fetches trusted root certificates from the cache
    /// and returns an enabled TLS config. When `false`, returns a disabled TLS config
    /// without fetching certificates.
    pub async fn get_tls_config(
        &self,
        tls_enabled: bool,
    ) -> Result<TlsConfig, TrustedRootCertsError> {
        if tls_enabled {
            let trusted_root_certs = self.get().await?;
            Ok(TlsConfig {
                enabled: true,
                trusted_root_certs,
            })
        } else {
            Ok(TlsConfig::disabled())
        }
    }

    /// Returns the cached trusted root certificates, fetching from Kubernetes if needed.
    ///
    /// The cache is refreshed when:
    /// - No cached value exists
    /// - The cached value has expired (older than 1 day)
    ///
    /// Returns an error if the Kubernetes fetch fails or the ConfigMap is malformed.
    pub async fn get(&self) -> Result<String, TrustedRootCertsError> {
        // Fast path: check if we have a valid cached value
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.as_ref() {
                if cached.fetched_at.elapsed() < Self::TTL {
                    debug!("returning cached trusted root certificates");
                    return Ok(cached.certs.clone());
                }
            }
        }

        // Slow path: fetch from Kubernetes and update cache
        debug!("fetching trusted root certificates from kubernetes");
        let certs = self.fetch_from_k8s().await?;

        let mut cache = self.cache.write().await;
        *cache = Some(CachedCerts {
            certs: certs.clone(),
            fetched_at: Instant::now(),
        });

        Ok(certs)
    }

    /// Fetches the trusted root certificates from the Kubernetes ConfigMap.
    async fn fetch_from_k8s(&self) -> Result<String, TrustedRootCertsError> {
        let config_map = self
            .k8s_client
            .get_config_map(TRUSTED_ROOT_CERT_CONFIG_MAP_NAME)
            .await?;

        let data = config_map
            .data
            .ok_or(TrustedRootCertsError::ConfigMapDataMissing)?;

        let certs = data
            .get(TRUSTED_ROOT_CERT_KEY_NAME)
            .ok_or(TrustedRootCertsError::CertsKeyMissing)?
            .clone();

        if certs.is_empty() {
            return Err(TrustedRootCertsError::CertsEmpty);
        }

        Ok(certs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use etl_config::Environment;
    use k8s_openapi::api::core::v1::ConfigMap;

    use crate::configs::log::LogLevel;
    use crate::k8s::{DestinationType, PodStatus, ReplicatorConfigMapFile};

    struct MockK8sClient {
        certs: String,
    }

    #[async_trait]
    impl K8sClient for MockK8sClient {
        async fn create_or_update_postgres_secret(
            &self,
            _prefix: &str,
            _postgres_password: &str,
        ) -> Result<(), K8sError> {
            Ok(())
        }

        async fn create_or_update_bigquery_secret(
            &self,
            _prefix: &str,
            _bq_service_account_key: &str,
        ) -> Result<(), K8sError> {
            Ok(())
        }

        async fn create_or_update_iceberg_secret(
            &self,
            _prefix: &str,
            _catalog_token: &str,
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

        async fn get_config_map(&self, config_map_name: &str) -> Result<ConfigMap, K8sError> {
            if config_map_name == TRUSTED_ROOT_CERT_CONFIG_MAP_NAME {
                let mut map = BTreeMap::new();
                map.insert(TRUSTED_ROOT_CERT_KEY_NAME.to_string(), self.certs.clone());
                Ok(ConfigMap {
                    data: Some(map),
                    ..ConfigMap::default()
                })
            } else {
                Ok(ConfigMap::default())
            }
        }

        async fn create_or_update_replicator_config_map(
            &self,
            _prefix: &str,
            _files: Vec<ReplicatorConfigMapFile>,
        ) -> Result<(), K8sError> {
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
            Ok(())
        }

        async fn delete_replicator_stateful_set(&self, _prefix: &str) -> Result<(), K8sError> {
            Ok(())
        }

        async fn get_replicator_pod_status(&self, _prefix: &str) -> Result<PodStatus, K8sError> {
            Ok(PodStatus::Started)
        }
    }

    #[tokio::test]
    async fn test_cache_returns_certs() {
        let expected_certs = "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----";
        let client = Arc::new(MockK8sClient {
            certs: expected_certs.to_string(),
        });
        let cache = TrustedRootCertsCache::new(client);

        let result = cache.get().await.unwrap();
        assert_eq!(result, expected_certs);
    }

    #[tokio::test]
    async fn test_cache_returns_cached_value() {
        let expected_certs = "cached-certs";
        let client = Arc::new(MockK8sClient {
            certs: expected_certs.to_string(),
        });
        let cache = TrustedRootCertsCache::new(client);

        // First call fetches from K8s
        let result1 = cache.get().await.unwrap();
        assert_eq!(result1, expected_certs);

        // Second call should return cached value
        let result2 = cache.get().await.unwrap();
        assert_eq!(result2, expected_certs);
    }
}
