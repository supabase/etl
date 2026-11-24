//! Classification at source connection establishment, before issuing queries.

use std::{error::Error as _, io};

use sqlx::postgres::PgConnectOptions;
use tokio_postgres::{Config, config::Host};

use crate::error::{ErrorKind, EtlError};

/// Checks the socket errors that can indicate a stale or missing TCP address.
fn unreachable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::TimedOut
    ) || (error.raw_os_error().is_none()
        // std::net's getaddrinfo error has no portable ErrorKind or errno.
        && error.to_string().starts_with("failed to lookup address information:"))
        || (error.kind() == io::ErrorKind::InvalidInput
            && error.to_string() == "could not resolve any addresses")
}

/// Classifies a failed replication or snapshot connection.
pub(super) fn replication_connect_error(config: &Config, error: tokio_postgres::Error) -> EtlError {
    // The pinned driver exposes Kind::Connect only through this fixed label.
    // TLS, authentication and protocol I/O can also have an io::Error source.
    let tcp = !config.get_hostaddrs().is_empty()
        || matches!(config.get_hosts(), [Host::Tcp(host)] if !host.is_empty());
    let failed = tcp
        && error.source().and_then(|e| e.downcast_ref::<io::Error>()).is_some_and(unreachable)
        && error.to_string() == "error connecting to server";
    EtlError::from(error).with_source_network_failure(failed)
}

/// Preserves the migration's normal error handling for all other causes.
pub(super) fn migration_connect_error(
    options: &PgConnectOptions,
    error: sqlx::Error,
    fallback: ErrorKind,
    description: &'static str,
) -> EtlError {
    let failed = options.get_socket().is_none()
        && !options.get_host().is_empty()
        && !options.get_host().starts_with('/')
        && matches!(&error, sqlx::Error::Io(io) if unreachable(io));
    EtlError::from((fallback, description)).with_source(error).with_source_network_failure(failed)
}

#[cfg(test)]
mod tests {
    use sqlx::{
        Connection, PgConnection,
        postgres::{PgConnectOptions, PgSslMode},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    use tokio_postgres::{Config, NoTls, config::SslMode};

    use super::*;

    /// Both drivers reached the server and received a rejection during TLS
    /// negotiation.
    #[tokio::test]
    async fn source_network_failure_excludes_tls_negotiation() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let reject_tls = async || {
            let (mut socket, _) = listener.accept().await.unwrap();
            assert_eq!(socket.read_u32().await.unwrap(), 8);
            assert_eq!(socket.read_u32().await.unwrap(), 80877103);
            socket.write_all(b"N").await.unwrap();
        };
        let mut config = Config::new();
        config.host("127.0.0.1").port(port).user("test").ssl_mode(SslMode::Require);
        let (connection, ()) = tokio::join!(config.connect(NoTls), reject_tls());
        let error = replication_connect_error(&config, connection.err().unwrap());
        assert!(!error.is_source_network_failure());
        assert_eq!(error.kind(), ErrorKind::SourceConnectionFailed);

        let options = PgConnectOptions::new()
            .host("127.0.0.1")
            .port(port)
            .username("test")
            .ssl_mode(PgSslMode::Require);
        let (connection, ()) = tokio::join!(PgConnection::connect_with(&options), reject_tls());
        let error = migration_connect_error(
            &options,
            connection.unwrap_err(),
            ErrorKind::SourceError,
            "Test migration",
        );
        assert_eq!(error.kind(), ErrorKind::SourceError);
        assert!(!error.is_source_network_failure());
    }

    #[test]
    fn classifies_only_dial_errors() {
        for error in [
            io::ErrorKind::HostUnreachable.into(),
            io::ErrorKind::NetworkUnreachable.into(),
            io::ErrorKind::TimedOut.into(),
            io::Error::other("failed to lookup address information: unknown hostname"),
        ] {
            assert!(unreachable(&error));
        }
        for error in [
            sqlx::Error::PoolTimedOut,
            sqlx::Error::Tls(io::Error::from(io::ErrorKind::TimedOut).into()),
            sqlx::Error::Io(io::ErrorKind::InvalidData.into()),
            sqlx::Error::Protocol("test protocol error".into()),
        ] {
            let options = PgConnectOptions::new().host("localhost");
            let error =
                migration_connect_error(&options, error, ErrorKind::SourceError, "Test migration");
            assert_eq!(error.kind(), ErrorKind::SourceError);
            assert!(!error.is_source_network_failure());
        }
        let protocol = tokio_postgres::Error::parse(io::ErrorKind::TimedOut.into());
        assert!(
            !replication_connect_error(Config::new().host("localhost"), protocol)
                .is_source_network_failure()
        );
        let socket = PgConnectOptions::new().host("/example/socket");
        let refused = sqlx::Error::Io(io::ErrorKind::ConnectionRefused.into());
        assert!(
            !migration_connect_error(&socket, refused, ErrorKind::SourceError, "Test migration")
                .is_source_network_failure()
        );
    }
}
