//! Rustls TLS integration for `tokio-postgres`.
//!
//! This code is adapted from the `tokio-postgres-rustls` library
//! (<https://github.com/jbg/tokio-postgres-rustls>), available under the MIT
//! License.

use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use aws_lc_rs::digest;
use const_oid::db::rfc5912::{
    ECDSA_WITH_SHA_256, ECDSA_WITH_SHA_384, ID_SHA_1, ID_SHA_256, ID_SHA_384, ID_SHA_512,
    SHA_1_WITH_RSA_ENCRYPTION, SHA_256_WITH_RSA_ENCRYPTION, SHA_384_WITH_RSA_ENCRYPTION,
    SHA_512_WITH_RSA_ENCRYPTION,
};
use futures::FutureExt;
use rustls::{ClientConfig, pki_types::ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_postgres::tls::{ChannelBinding, MakeTlsConnect, TlsConnect};
use tokio_rustls::{TlsConnector, client::TlsStream};
use x509_cert::{Certificate, der::Decode};

/// A `MakeTlsConnect` implementation using `rustls`.
///
/// That way you can connect to Postgres using `rustls` as the TLS stack.
#[derive(Clone)]
pub struct MakeRustlsConnect {
    config: Arc<ClientConfig>,
}

impl MakeRustlsConnect {
    /// Creates a new `MakeRustlsConnect` from the provided `ClientConfig`.
    pub fn new(config: ClientConfig) -> Self {
        Self::from_shared_config(Arc::new(config))
    }

    /// Creates a new `MakeRustlsConnect` from a shared `ClientConfig`.
    pub fn from_shared_config(config: Arc<ClientConfig>) -> Self {
        Self { config }
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

/// Future returned while establishing a rustls-backed Postgres TLS stream.
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

/// Connector returned by [`MakeRustlsConnect`] for a single host name.
pub struct RustlsConnect(RustlsConnectData);

struct RustlsConnectData {
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

/// Rustls stream wrapper implementing `tokio-postgres` TLS traits.
pub struct RustlsStream<S>(TlsStream<S>);

impl<S> RustlsStream<S>
where
    S: Unpin,
{
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
        session
            .peer_certificates()
            .and_then(|certificates| certificates.first())
            .and_then(|certificate| tls_server_end_point(certificate.as_ref()))
            .map_or_else(ChannelBinding::none, ChannelBinding::tls_server_end_point)
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

/// Computes `tls-server-end-point` binding data from the complete server
/// certificate.
///
/// Returns `None` when the certificate cannot be decoded or its signature
/// algorithm has no supported channel-binding digest. Certificate validation
/// remains the responsibility of rustls.
fn tls_server_end_point(certificate_der: &[u8]) -> Option<Vec<u8>> {
    let certificate = Certificate::from_der(certificate_der).ok()?;
    let algorithm = match certificate.signature_algorithm.oid {
        // RFC 5929 section 4.1 upgrades SHA-1 signatures to SHA-256.
        ID_SHA_1
        | ID_SHA_256
        | SHA_1_WITH_RSA_ENCRYPTION
        | SHA_256_WITH_RSA_ENCRYPTION
        | ECDSA_WITH_SHA_256 => &digest::SHA256,
        ID_SHA_384 | SHA_384_WITH_RSA_ENCRYPTION | ECDSA_WITH_SHA_384 => &digest::SHA384,
        ID_SHA_512 | SHA_512_WITH_RSA_ENCRYPTION => &digest::SHA512,
        // Do not infer a digest for unsupported algorithms, including Ed25519.
        _ => return None,
    };

    Some(digest::digest(algorithm, certificate_der).as_ref().to_vec())
}

#[cfg(test)]
mod tests {
    use const_oid::{ObjectIdentifier, db::rfc8410::ID_ED_25519};
    use x509_cert::{
        Certificate,
        der::{Decode, Encode},
    };

    use crate::tokio::tls::tls_server_end_point;

    /// Checks the complete DER hashes against independently computed OpenSSL
    /// digests.
    #[test]
    fn channel_binding_hashes_full_certificates() {
        let cases: &[(&[u8], &[u8])] = &[
            (
                include_bytes!("fixtures/rsa-sha1.der"),
                &[
                    0x51, 0xbf, 0x68, 0x33, 0xe2, 0xda, 0x1e, 0xdd, 0x56, 0x64, 0x88, 0x42, 0x62,
                    0x23, 0x55, 0x98, 0xc7, 0xd8, 0xe5, 0x7c, 0x2b, 0xfd, 0xf8, 0x8c, 0x81, 0x65,
                    0xb3, 0xed, 0x46, 0xa6, 0x66, 0x9f,
                ],
            ),
            (
                include_bytes!("fixtures/rsa-sha256.der"),
                &[
                    0x78, 0x13, 0x28, 0x57, 0x6d, 0xa5, 0x01, 0x70, 0x86, 0x7d, 0x92, 0xb8, 0xd0,
                    0x0f, 0xe2, 0x2d, 0xef, 0x06, 0x6a, 0x05, 0xca, 0x5e, 0xec, 0x34, 0xc3, 0xe3,
                    0x66, 0xf3, 0x97, 0xba, 0x8b, 0x79,
                ],
            ),
            (
                include_bytes!("fixtures/rsa-sha512.der"),
                &[
                    0xf7, 0x53, 0x64, 0x71, 0x8d, 0x66, 0x5b, 0x5e, 0x41, 0xce, 0x9d, 0x71, 0x53,
                    0xc5, 0x1e, 0xff, 0x36, 0x79, 0x13, 0x7a, 0x94, 0x5e, 0x60, 0xf1, 0x13, 0x8d,
                    0xea, 0x08, 0xd3, 0xf9, 0x8a, 0x33, 0xeb, 0xfd, 0x42, 0xd5, 0x03, 0x62, 0xbd,
                    0x79, 0xbc, 0x35, 0x9d, 0x52, 0x6e, 0x33, 0x9c, 0x31, 0x50, 0x53, 0xfb, 0x36,
                    0xe4, 0x25, 0x1b, 0x73, 0xa2, 0x89, 0x95, 0xaa, 0xd9, 0x11, 0xe4, 0x7b,
                ],
            ),
            (
                include_bytes!("fixtures/ecdsa-sha256.der"),
                &[
                    0xe2, 0xdf, 0xc4, 0xc2, 0x72, 0x60, 0x8c, 0x24, 0xc7, 0x7b, 0xf2, 0x30, 0x73,
                    0x5e, 0x0a, 0x83, 0xf0, 0x23, 0x8d, 0x25, 0x02, 0xcd, 0x5d, 0x96, 0x44, 0xfc,
                    0x97, 0x25, 0x02, 0x45, 0x0b, 0x8d,
                ],
            ),
            (
                include_bytes!("fixtures/ecdsa-sha384.der"),
                &[
                    0x40, 0xa3, 0xf1, 0x63, 0x06, 0x49, 0xd5, 0xac, 0xa7, 0xfb, 0xed, 0x36, 0xd9,
                    0x0b, 0xa7, 0xa9, 0xb3, 0xa8, 0x14, 0xf7, 0xe1, 0x38, 0x05, 0xb0, 0xb1, 0xa9,
                    0x2c, 0x33, 0xc1, 0xb7, 0x73, 0x95, 0xb0, 0x71, 0xaa, 0xa6, 0xec, 0xd3, 0x4c,
                    0x06, 0x13, 0x91, 0x0d, 0xcc, 0x69, 0x82, 0x26, 0x2f,
                ],
            ),
        ];

        for (certificate, expected) in cases {
            assert_eq!(tls_server_end_point(certificate).unwrap(), *expected);
        }
    }

    /// Rejects malformed DER, including truncated certificates and trailing
    /// bytes.
    #[test]
    fn channel_binding_rejects_malformed_certificates() {
        let certificate = include_bytes!("fixtures/rsa-sha256.der");
        for length in 0..certificate.len() {
            assert!(tls_server_end_point(&certificate[..length]).is_none());
        }

        let mut trailing_bytes = certificate.to_vec();
        trailing_bytes.push(0);
        assert!(tls_server_end_point(&trailing_bytes).is_none());
        assert!(tls_server_end_point(b"not a certificate").is_none());
    }

    /// Unsupported signature algorithms must not advertise guessed binding
    /// data.
    #[test]
    fn channel_binding_rejects_unsupported_signature_algorithms() {
        let mut certificate =
            Certificate::from_der(include_bytes!("fixtures/rsa-sha256.der")).unwrap();
        for oid in [ID_ED_25519, ObjectIdentifier::new_unwrap("1.2.3.4")] {
            // Only digest selection is under test; rustls validates signatures
            // first.
            certificate.signature_algorithm.oid = oid;
            certificate.tbs_certificate.signature.oid = oid;
            assert!(tls_server_end_point(&certificate.to_der().unwrap()).is_none());
        }
    }
}
