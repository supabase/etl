// This code is copied from the tokio-postgres-rustls library (https://github.com/jbg/tokio-postgres-rustls),
// available under the MIT License, which provides Rustls-based TLS support for
// secure asynchronous Postgres connections using the tokio-postgres client.

use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use aws_lc_rs::digest;
use const_oid::db::{
    rfc5912::{
        ECDSA_WITH_SHA_256, ECDSA_WITH_SHA_384, ID_SHA_1, ID_SHA_256, ID_SHA_384, ID_SHA_512,
        SHA_1_WITH_RSA_ENCRYPTION, SHA_256_WITH_RSA_ENCRYPTION, SHA_384_WITH_RSA_ENCRYPTION,
        SHA_512_WITH_RSA_ENCRYPTION,
    },
    rfc8410::ID_ED_25519,
};
use futures::FutureExt;
use rustls::{ClientConfig, pki_types::ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect};
use tokio_rustls::{TlsConnector, client::TlsStream};
use x509_cert::{TbsCertificate, der::Decode};

/// A [`MakeTlsConnect`] implementation using Rustls.
///
/// This is shared by source-side clients that use [`tokio_postgres`].
#[derive(Clone)]
pub struct MakeRustlsConnect {
    config: Arc<ClientConfig>,
}

impl MakeRustlsConnect {
    /// Creates a new [`MakeRustlsConnect`] from the provided [`ClientConfig`].
    #[must_use]
    pub fn new(config: ClientConfig) -> Self {
        Self { config: Arc::new(config) }
    }
}

impl<S> MakeTlsConnect<S> for MakeRustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsStream<S>;
    type TlsConnect = RustlsConnect;
    type Error = rustls::pki_types::InvalidDnsNameError;

    fn make_tls_connect(&mut self, hostname: &str) -> Result<Self::TlsConnect, Self::Error> {
        ServerName::try_from(hostname).map(|dns_name| {
            RustlsConnect(RustlsConnectData {
                hostname: dns_name.to_owned(),
                connector: Arc::clone(&self.config).into(),
            })
        })
    }
}

/// Future returned while establishing a Rustls connection.
pub struct TlsConnectFuture<S> {
    inner: tokio_rustls::Connect<S>,
}

impl<S> Future for TlsConnectFuture<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    type Output = io::Result<RustlsStream<S>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().inner.poll_unpin(cx).map_ok(RustlsStream)
    }
}

/// Per-connection Rustls connector.
pub struct RustlsConnect(RustlsConnectData);

/// Data needed to establish one Rustls connection.
pub struct RustlsConnectData {
    hostname: ServerName<'static>,
    connector: TlsConnector,
}

impl<S> TlsConnect<S> for RustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsStream<S>;
    type Error = io::Error;
    type Future = TlsConnectFuture<S>;

    fn connect(self, stream: S) -> Self::Future {
        TlsConnectFuture { inner: self.0.connector.connect(self.0.hostname, stream) }
    }
}

/// Rustls stream wrapper implementing [`tokio_postgres::tls::TlsStream`].
pub struct RustlsStream<S>(TlsStream<S>);

impl<S> RustlsStream<S>
where
    S: Unpin,
{
    /// Projects to the wrapped TLS stream.
    fn project_stream(self: Pin<&mut Self>) -> Pin<&mut TlsStream<S>> {
        Pin::new(&mut self.get_mut().0)
    }
}

impl<S> tokio_postgres::tls::TlsStream for RustlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn channel_binding(&self) -> ChannelBinding {
        let (_, session) = self.0.get_ref();
        match session.peer_certificates() {
            Some(certs) if !certs.is_empty() => TbsCertificate::from_der(&certs[0])
                .ok()
                .and_then(|cert| {
                    let digest = match cert.signature.oid {
                        // SHA1 is upgraded to SHA256 per RFC 5929 section 4.1.
                        ID_SHA_1
                        | ID_SHA_256
                        | SHA_1_WITH_RSA_ENCRYPTION
                        | SHA_256_WITH_RSA_ENCRYPTION
                        | ECDSA_WITH_SHA_256 => &digest::SHA256,
                        ID_SHA_384 | SHA_384_WITH_RSA_ENCRYPTION | ECDSA_WITH_SHA_384 => {
                            &digest::SHA384
                        }
                        ID_SHA_512 | SHA_512_WITH_RSA_ENCRYPTION | ID_ED_25519 => &digest::SHA512,
                        _ => return None,
                    };

                    Some(digest)
                })
                .map_or_else(ChannelBinding::none, |algorithm| {
                    let hash = digest::digest(algorithm, certs[0].as_ref());
                    ChannelBinding::tls_server_end_point(hash.as_ref().into())
                }),
            _ => ChannelBinding::none(),
        }
    }
}

impl<S> AsyncRead for RustlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.project_stream().poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for RustlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project_stream().poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project_stream().poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project_stream().poll_shutdown(cx)
    }
}
