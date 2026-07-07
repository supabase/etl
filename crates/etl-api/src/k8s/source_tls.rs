use etl_config::shared::TlsConfig;
use thiserror::Error;

/// Errors resolving the source TLS configuration at startup.
#[derive(Debug, Error)]
pub enum SourceTlsConfigError {
    /// TLS is enabled but no trusted root certificate was supplied.
    #[error(
        "source TLS is enabled but no trusted root certificate was supplied; set \
         `source.tls.trusted_root_certs` (e.g. via APP_SOURCE__TLS__TRUSTED_ROOT_CERTS) or \
         disable source TLS"
    )]
    MissingTrustedRootCerts,
}

/// Provides the [`TlsConfig`] used for customer/tenant *source* Postgres
/// connections.
#[derive(Debug, Clone)]
pub struct SourceTlsConfig {
    tls_config: TlsConfig,
}

impl SourceTlsConfig {
    /// Resolves the source TLS configuration.
    ///
    /// Fails if `tls.enabled` is `true` but `tls.trusted_root_certs` is
    /// empty, since a TLS connection could never be established in that
    /// state.
    pub fn new(tls: TlsConfig) -> Result<Self, SourceTlsConfigError> {
        if tls.enabled && tls.trusted_root_certs.trim().is_empty() {
            return Err(SourceTlsConfigError::MissingTrustedRootCerts);
        }

        Ok(Self { tls_config: tls })
    }

    /// Returns the resolved [`TlsConfig`] for source Postgres connections.
    pub fn get_tls_config(&self) -> TlsConfig {
        self.tls_config.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_tls_does_not_require_certs() {
        let config = SourceTlsConfig::new(TlsConfig::disabled()).unwrap();
        assert!(!config.get_tls_config().enabled);
    }

    #[test]
    fn enabled_tls_with_certs_succeeds() {
        let tls = TlsConfig { enabled: true, trusted_root_certs: "cert-data".to_owned() };
        let config = SourceTlsConfig::new(tls).unwrap();
        let tls_config = config.get_tls_config();
        assert!(tls_config.enabled);
        assert_eq!(tls_config.trusted_root_certs, "cert-data");
    }

    #[test]
    fn enabled_tls_without_certs_fails() {
        let tls = TlsConfig { enabled: true, trusted_root_certs: "   ".to_owned() };
        let result = SourceTlsConfig::new(tls);
        assert!(matches!(result, Err(SourceTlsConfigError::MissingTrustedRootCerts)));
    }
}
