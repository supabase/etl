//! Classification at source connection establishment, before issuing queries.

use std::{error::Error as _, io, time::Duration};

use sqlx::{error::TcpConnectTimeout, postgres::PgConnectOptions};
use tokio::time::timeout;
use tokio_postgres::{Client, Config, Connection, Socket, config::Host, tls::MakeTlsConnect};

use crate::{
    error::{ErrorKind, EtlError, EtlResult},
    etl_error,
};

/// Bounds DNS, socket establishment, TLS negotiation, and authentication
/// together.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

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

/// Observes when the driver starts PostgreSQL or TLS negotiation on any
/// candidate socket.
struct ObserveConnectedSocket<'a, T> {
    connected: &'a mut bool,
    tls: T,
}

impl<T: MakeTlsConnect<Socket>> MakeTlsConnect<Socket> for ObserveConnectedSocket<'_, T> {
    type Stream = T::Stream;
    type TlsConnect = T::TlsConnect;
    type Error = T::Error;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        // tokio-postgres invokes the factory after TCP connects, even for
        // NoTls.
        *self.connected = true;
        self.tls.make_tls_connect(domain)
    }
}

/// Connects a replication or snapshot client, preserving the driver's address
/// fallback.
pub(super) async fn connect_replication<T: MakeTlsConnect<Socket>>(
    config: &Config,
    tls: T,
) -> EtlResult<(Client, Connection<Socket, T::Stream>)> {
    let mut connected = false;
    let result = timeout(
        CONNECT_TIMEOUT,
        config.connect(ObserveConnectedSocket { connected: &mut connected, tls }),
    )
    .await;

    let hosts = config.get_hosts();
    let tcp_dial = !connected
        && (!config.get_hostaddrs().is_empty()
            || (!hosts.is_empty()
                && hosts.iter().all(|h| matches!(h, Host::Tcp(host) if !host.is_empty()))));

    match result {
        Ok(Ok(connection)) => Ok(connection),
        Ok(Err(error)) => {
            // A later failed dial can mask an earlier authentication or TLS
            // rejection.
            let failed = tcp_dial
                && error
                    .source()
                    .and_then(|e| e.downcast_ref::<io::Error>())
                    .is_some_and(unreachable);
            Err(EtlError::from(error).with_postgres_dial_failure(failed))
        }
        Err(elapsed) => {
            Err(etl_error!(ErrorKind::SourceConnectionFailed, "Postgres connection timed out")
                .with_source(elapsed)
                .with_postgres_dial_failure(tcp_dial))
        }
    }
}

/// Preserves the migration's normal error handling for all other causes.
pub(super) fn migration_connect_error(
    options: &PgConnectOptions,
    error: sqlx::Error,
    fallback: ErrorKind,
    description: &'static str,
) -> EtlError {
    let dial_error = match &error {
        sqlx::Error::Io(io) if io.kind() == io::ErrorKind::TimedOut => {
            matches!(io.get_ref(), Some(cause) if cause.is::<TcpConnectTimeout>())
        }
        sqlx::Error::Io(io) => unreachable(io),
        _ => false,
    };
    let failed = options.get_socket().is_none()
        && !options.get_host().is_empty()
        && !options.get_host().starts_with('/')
        && dial_error;
    EtlError::from((fallback, description)).with_source(error).with_postgres_dial_failure(failed)
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;
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

    /// Occupying the resolver's blocking thread keeps the DNS lookup pending
    /// without depending on a particular network or DNS server.
    #[test]
    fn stalled_hostname_lookup_is_a_dial_failure() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();

        runtime.block_on(async {
            let (release, wait) = std::sync::mpsc::channel::<()>();
            let (started, ready) = tokio::sync::oneshot::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                let _ = wait.recv();
            });
            ready.await.unwrap();

            let mut config = Config::new();
            config.host("localhost").user("test");

            tokio::time::pause();
            let connection = connect_replication(&config, NoTls);
            tokio::pin!(connection);
            assert!(connection.as_mut().now_or_never().is_none());
            tokio::time::advance(Duration::from_secs(31)).await;
            let result = connection.as_mut().now_or_never();

            drop(release);
            blocker.await.unwrap();

            let Some(Err(error)) = result else {
                panic!("expected the connection deadline to expire during DNS lookup");
            };
            assert_eq!(error.kind(), ErrorKind::SourceConnectionFailed);
            assert!(error.is_postgres_dial_failure());
        });
    }

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
        let (connection, ()) = tokio::join!(connect_replication(&config, NoTls), reject_tls());
        let error = connection.err().unwrap();
        assert!(!error.is_postgres_dial_failure());
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
        assert!(!error.is_postgres_dial_failure());
    }

    /// The last refused dial must not hide that the first server rejected the
    /// credentials.
    #[tokio::test]
    async fn refused_fallback_after_auth_rejection_is_not_a_dial_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = Config::new();
        config
            .host("localhost")
            .host("127.0.0.1")
            .port(listener.local_addr().unwrap().port())
            .user("test");
        let reject_auth = async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            drop(listener);
            let len = socket.read_u32().await.unwrap();
            let mut startup = vec![0; (len - 4) as usize];
            socket.read_exact(&mut startup).await.unwrap();
            assert_eq!(&startup[..4], &[0, 3, 0, 0]);
            let fields = b"SFATAL\0C28P01\0Mtest authentication rejected\0\0";
            socket.write_u8(b'E').await.unwrap();
            socket.write_u32(u32::try_from(fields.len() + 4).unwrap()).await.unwrap();
            socket.write_all(fields).await.unwrap();
        };

        let (result, ()) = tokio::join!(connect_replication(&config, NoTls), reject_auth);
        let error = result.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::SourceConnectionFailed);
        assert!(!error.is_postgres_dial_failure());

        let error = connect_replication(&config, NoTls).await.err().unwrap();
        assert_eq!(error.kind(), ErrorKind::SourceConnectionFailed);
        assert!(error.is_postgres_dial_failure());
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
            sqlx::Error::Io(io::ErrorKind::TimedOut.into()),
            sqlx::Error::Io(io::Error::new(io::ErrorKind::TimedOut, "Startup read timed out")),
            sqlx::Error::Io(io::Error::new(io::ErrorKind::TimedOut, "TCP connection timed out")),
            sqlx::Error::Tls(io::Error::from(io::ErrorKind::TimedOut).into()),
            sqlx::Error::Io(io::ErrorKind::InvalidData.into()),
            sqlx::Error::Protocol("test protocol error".into()),
        ] {
            let options = PgConnectOptions::new().host("localhost");
            let error =
                migration_connect_error(&options, error, ErrorKind::SourceError, "Test migration");
            assert_eq!(error.kind(), ErrorKind::SourceError);
            assert!(!error.is_postgres_dial_failure());
        }
        let socket = PgConnectOptions::new().host("/example/socket");
        let refused = sqlx::Error::Io(io::ErrorKind::ConnectionRefused.into());
        assert!(
            !migration_connect_error(&socket, refused, ErrorKind::SourceError, "Test migration")
                .is_postgres_dial_failure()
        );
    }
}
