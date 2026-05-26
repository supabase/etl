use std::path::{Path, PathBuf};

use etl_config::shared::TlsConfig;

const DEFAULT_DATABASE_TLS_ROOT_CERT: &str = "target/postgres-tls/root.crt";

/// Returns whether test database clients should require TLS.
#[must_use]
pub fn test_tls_enabled_from_env() -> bool {
    std::env::var("TESTS_DATABASE_TLS_ENABLED")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
        .unwrap_or(false)
}

/// Returns TLS settings for local test database clients.
#[must_use]
pub fn local_tls_config_from_env() -> TlsConfig {
    if !test_tls_enabled_from_env() {
        return TlsConfig::disabled();
    }

    let root_cert_path = test_tls_root_cert_path();
    let trusted_root_certs = std::fs::read_to_string(&root_cert_path).unwrap_or_else(|_| {
        panic!("Failed to read TESTS_DATABASE_TLS_ROOT_CERT at {}", root_cert_path.display())
    });

    TlsConfig { trusted_root_certs, enabled: true }
}

/// Returns the trusted root certificate path for local test TLS.
#[must_use]
pub fn test_tls_root_cert_path() -> PathBuf {
    std::env::var_os("TESTS_DATABASE_TLS_ROOT_CERT")
        .map_or_else(|| workspace_root().join(DEFAULT_DATABASE_TLS_ROOT_CERT), PathBuf::from)
}

/// Returns trusted root certificates for local test TLS when enabled.
#[must_use]
pub fn test_tls_root_certs_from_env() -> Option<String> {
    test_tls_enabled_from_env().then(|| local_tls_config_from_env().trusted_root_certs)
}

/// Returns the repository workspace root from this crate's manifest path.
fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("etl-postgres should live under crates/")
}
