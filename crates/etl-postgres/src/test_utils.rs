use std::path::{Path, PathBuf};

use etl_config::shared::TlsConfig;

const DEFAULT_DATABASE_TLS_ROOT_CERT: &str = "target/postgres-tls/root.crt";
/// Port of the first local source Postgres cluster.
const DEFAULT_DATABASE_PORT: u16 = 5430;
/// Number of local Postgres clusters shared by concurrently running tests.
const TESTS_DATABASE_SHARDS: &str = "TESTS_DATABASE_SHARDS";
/// Nextest's compact index for the running test, unique among running tests.
const NEXTEST_TEST_GLOBAL_SLOT: &str = "NEXTEST_TEST_GLOBAL_SLOT";

/// Returns the index of the local cluster assigned to the running test.
///
/// Without `TESTS_DATABASE_SHARDS`, every test uses the first cluster. With
/// it, `cargo xtask nextest` runs at most that many Postgres tests at once, and
/// nextest gives each running test a distinct `NEXTEST_TEST_GLOBAL_SLOT` below
/// that limit. Using the slot as the cluster index gives every running test a
/// cluster of its own.
///
/// # Panics
///
/// Panics when sharding is enabled outside nextest or when nextest runs more
/// tests at once than there are clusters.
fn test_database_shard() -> u16 {
    let Ok(shards) = std::env::var(TESTS_DATABASE_SHARDS) else {
        return 0;
    };
    let shards: u16 = shards.parse().expect("TESTS_DATABASE_SHARDS must be a positive integer");
    let slot: u16 = std::env::var(NEXTEST_TEST_GLOBAL_SLOT)
        .expect("TESTS_DATABASE_SHARDS requires running under nextest")
        .parse()
        .expect("NEXTEST_TEST_GLOBAL_SLOT must be a non-negative integer");
    assert!(
        slot < shards,
        "nextest slot {slot} has no Postgres shard; run at most {shards} tests at once"
    );

    slot
}

/// Offsets the first cluster's port by the running test's shard.
fn shard_port(first_port: u16) -> u16 {
    first_port
        .checked_add(test_database_shard())
        .expect("test database shard port should fit in the valid port range")
}

/// Returns the source Postgres port assigned to the running test.
///
/// `TESTS_DATABASE_PORT` names the first local cluster and defaults to
/// `5430`. When `TESTS_DATABASE_SHARDS` is set, the running test uses the
/// cluster at `NEXTEST_TEST_GLOBAL_SLOT` ports above the first, so no two
/// running tests share a cluster.
///
/// # Panics
///
/// Panics on a malformed port, when sharding is enabled outside nextest, or
/// when nextest runs more tests at once than there are clusters.
pub fn test_database_port_from_env() -> u16 {
    let port = std::env::var("TESTS_DATABASE_PORT").map_or(DEFAULT_DATABASE_PORT, |port| {
        port.parse().expect("TESTS_DATABASE_PORT must be a valid port number")
    });

    shard_port(port)
}

/// Returns the read-replica port assigned to the running test, if configured.
///
/// `TESTS_DATABASE_REPLICA_PORT` names the first local replica and is offset
/// like [`test_database_port_from_env`]. Callers derive a default from the
/// source port when it is unset.
///
/// # Panics
///
/// Panics under the same conditions as [`test_database_port_from_env`].
pub fn test_database_replica_port_from_env() -> Option<u16> {
    let port = std::env::var("TESTS_DATABASE_REPLICA_PORT").ok()?;
    let port = port.parse().expect("TESTS_DATABASE_REPLICA_PORT must be a valid port number");

    Some(shard_port(port))
}

/// Returns whether test database clients should require TLS.
pub fn test_tls_enabled_from_env() -> bool {
    std::env::var("TESTS_DATABASE_TLS_ENABLED")
        .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "on"))
}

/// Returns TLS settings for local test database clients.
pub fn local_tls_config_from_env() -> TlsConfig {
    if !test_tls_enabled_from_env() {
        return TlsConfig::disabled();
    }

    let root_cert_path = test_tls_root_cert_path();
    let trusted_root_certs = std::fs::read_to_string(&root_cert_path)
        .expect("Failed to read TESTS_DATABASE_TLS_ROOT_CERT");

    TlsConfig { trusted_root_certs, enabled: true }
}

/// Returns the trusted root certificate path for local test TLS.
pub fn test_tls_root_cert_path() -> PathBuf {
    std::env::var_os("TESTS_DATABASE_TLS_ROOT_CERT")
        .map_or_else(|| workspace_root().join(DEFAULT_DATABASE_TLS_ROOT_CERT), PathBuf::from)
}

/// Returns trusted root certificates for local test TLS when enabled.
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
