#[cfg(test)]
mod tests {
    use etl_postgres::{
        test_utils::{test_tls_enabled_from_env, test_tls_root_cert_path},
        tokio::tls::MakeRustlsConnect,
    };
    use rustls::{
        ClientConfig, RootCertStore,
        pki_types::{CertificateDer, pem::PemObject},
    };
    use tokio_postgres::config::SslMode;

    #[tokio::test]
    async fn connects_with_rustls_when_test_tls_is_enabled() {
        if !test_tls_enabled_from_env() {
            return;
        }

        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let host = std::env::var("TESTS_DATABASE_HOST").unwrap_or_else(|_| "localhost".to_owned());
        let port = std::env::var("TESTS_DATABASE_PORT")
            .unwrap_or_else(|_| "5430".to_owned())
            .parse()
            .expect("TESTS_DATABASE_PORT must be a valid port number");
        let user =
            std::env::var("TESTS_DATABASE_USERNAME").unwrap_or_else(|_| "postgres".to_owned());
        let password =
            std::env::var("TESTS_DATABASE_PASSWORD").unwrap_or_else(|_| "postgres".to_owned());
        let root_cert_path = test_tls_root_cert_path();
        let trusted_root_certs = std::fs::read_to_string(&root_cert_path).unwrap_or_else(|_| {
            panic!("Failed to read TESTS_DATABASE_TLS_ROOT_CERT at {}", root_cert_path.display())
        });

        let mut root_store = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(trusted_root_certs.as_bytes()) {
            root_store.add(cert.expect("failed to parse root certificate")).unwrap();
        }

        let tls_config =
            ClientConfig::builder().with_root_certificates(root_store).with_no_client_auth();
        let tls = MakeRustlsConnect::new(tls_config);

        let mut config = tokio_postgres::Config::new();
        config.host(host).port(port).user(user).password(password).ssl_mode(SslMode::VerifyFull);

        let (client, conn) = config.connect(tls).await.expect("connect");

        tokio::task::spawn(async move { conn.await.map_err(|e| panic!("{e:?}")) });

        let stmt = client.prepare("select 1").await.expect("prepare");
        let _ = client.query(&stmt, &[]).await.expect("query");
    }
}
