use std::{
    future::Future,
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use clickhouse::Client;
use etl::{
    error::{ErrorKind, EtlResult},
    etl_error,
};
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::{
    client::legacy::{
        Client as HyperClient,
        connect::{
            HttpConnector,
            dns::{GaiFuture, GaiResolver, Name},
        },
    },
    rt::TokioExecutor,
};
use tower_service::Service;
use url::{Host, Url};

/// TCP keepalive interval used by the ClickHouse HTTP connector.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

/// Maximum time an idle ClickHouse connection remains pooled.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// DNS resolver that returns only fully validated public address sets.
#[derive(Clone, Debug)]
struct PublicDnsResolver {
    /// Hyper's default system resolver, backed by blocking `getaddrinfo` calls.
    inner: GaiResolver,
}

impl PublicDnsResolver {
    /// Creates a public-address DNS resolver.
    fn new() -> Self {
        Self { inner: GaiResolver::new() }
    }
}

impl Service<Name> for PublicDnsResolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = io::Error;
    type Future = PublicDnsFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, name: Name) -> Self::Future {
        PublicDnsFuture { inner: self.inner.call(name) }
    }
}

/// Pending system DNS lookup whose complete result must pass the public-address
/// policy.
struct PublicDnsFuture {
    /// Pending system resolver lookup.
    inner: GaiFuture,
}

impl Future for PublicDnsFuture {
    type Output = io::Result<std::vec::IntoIter<SocketAddr>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx).map(|result| {
            result.and_then(|addresses| {
                let addresses = addresses.collect::<Vec<_>>();
                ensure_public_socket_addresses(&addresses)?;
                Ok(addresses.into_iter())
            })
        })
    }
}

/// Builds a ClickHouse client whose HTTPS connector can reach only public
/// addresses.
pub(super) async fn new_public_client(
    url: &Url,
    resolution_timeout: Duration,
) -> EtlResult<Client> {
    ensure_public_https_url(url, resolution_timeout).await?;

    let mut connector = HttpConnector::new_with_resolver(PublicDnsResolver::new());
    connector.set_keepalive(Some(TCP_KEEPALIVE));

    // The outer connector enforces HTTPS and passes its URI through this connector.
    connector.enforce_http(false);

    let connector = HttpsConnectorBuilder::new()
        .with_provider_and_webpki_roots(rustls::crypto::aws_lc_rs::default_provider())
        .map_err(|error| {
            etl_error!(
                ErrorKind::ConfigError,
                "ClickHouse TLS client configuration failed",
                source: error
            )
        })?
        .https_only()
        .enable_http1()
        .wrap_connector(connector);
    let http_client = HyperClient::builder(TokioExecutor::new())
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .build(connector);

    Ok(Client::with_http_client(http_client))
}

/// Verifies that a URL uses HTTPS and resolves only to public addresses.
async fn ensure_public_https_url(url: &Url, resolution_timeout: Duration) -> EtlResult<()> {
    if url.scheme() != "https" {
        return Err(etl_error!(ErrorKind::ConfigError, "ClickHouse URL must use HTTPS"));
    }

    let Some(host) = url.host() else {
        return Err(etl_error!(ErrorKind::ConfigError, "ClickHouse URL must include a host"));
    };

    match host {
        Host::Ipv4(address) => ensure_public_ip(IpAddr::V4(address)).map_err(|error| {
            etl_error!(
                ErrorKind::ConfigError,
                "ClickHouse URL host is not publicly routable",
                source: error
            )
        }),
        Host::Ipv6(address) => ensure_public_ip(IpAddr::V6(address)).map_err(|error| {
            etl_error!(
                ErrorKind::ConfigError,
                "ClickHouse URL host is not publicly routable",
                source: error
            )
        }),
        Host::Domain(domain) => {
            let name = domain.parse::<Name>().map_err(|error| {
                etl_error!(
                    ErrorKind::ConfigError,
                    "ClickHouse URL host is invalid",
                    source: error
                )
            })?;
            let mut resolver = PublicDnsResolver::new();
            match tokio::time::timeout(resolution_timeout, resolver.call(name)).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(error)) => Err(etl_error!(
                    ErrorKind::ConfigError,
                    "ClickHouse URL host could not be resolved safely",
                    source: error
                )),
                Err(error) => Err(etl_error!(
                    ErrorKind::ConfigError,
                    "ClickHouse URL host resolution timed out",
                    source: error
                )),
            }
        }
    }
}

/// Rejects an empty DNS response or any response containing a non-public
/// address.
fn ensure_public_socket_addresses(addresses: &[SocketAddr]) -> io::Result<()> {
    if addresses.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "ClickHouse URL host did not resolve to an IP address",
        ));
    }

    if addresses.iter().all(|address| is_public_ip(address.ip())) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "ClickHouse URL host resolved to a non-public IP address",
        ))
    }
}

/// Rejects one non-public IP address.
fn ensure_public_ip(address: IpAddr) -> io::Result<()> {
    if is_public_ip(address) {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "ClickHouse URL host is not a public IP address",
        ))
    }
}

/// Returns whether an address is globally routable unicast traffic.
fn is_public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

/// Returns whether an IPv4 address is publicly routable.
///
/// The exclusions follow the IANA IPv4 Special-Purpose Address Registry.
fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, d] = address.octets();
    let is_shared = a == 100 && (64..=127).contains(&b);
    let is_protocol_assignment = a == 192 && b == 0 && c == 0 && !matches!(d, 9 | 10);
    let is_documentation = address.is_documentation();
    let is_deprecated_6to4 = (a, b, c) == (192, 88, 99);
    let is_benchmarking = a == 198 && matches!(b, 18 | 19);

    !(a == 0
        || address.is_private()
        || is_shared
        || address.is_loopback()
        || address.is_link_local()
        || is_protocol_assignment
        || is_documentation
        || is_deprecated_6to4
        || is_benchmarking
        || address.is_multicast()
        || a >= 240)
}

/// Returns whether an IPv6 address is publicly routable.
///
/// The exclusions follow the IANA IPv6 Special-Purpose Address Registry.
fn is_public_ipv6(address: Ipv6Addr) -> bool {
    let octets = address.octets();
    let segments = address.segments();
    if matches!(segments, [0x64, 0xff9b, 0, 0, 0, 0, _, _]) {
        let embedded = Ipv4Addr::new(octets[12], octets[13], octets[14], octets[15]);
        return is_public_ipv4(embedded);
    }

    let value = u128::from_be_bytes(octets);
    let is_global_unicast = segments[0] & 0xe000 == 0x2000;
    let is_ietf_assignment = segments[0] == 0x2001 && segments[1] < 0x0200;
    let is_ietf_global_exception =
        matches!(
            value,
            0x2001_0001_0000_0000_0000_0000_0000_0001..=0x2001_0001_0000_0000_0000_0000_0000_0003
        ) || matches!(segments, [0x2001, 3 | 0x20..=0x3f, ..] | [0x2001, 4, 0x0112, ..]);
    let is_documentation =
        matches!(segments, [0x2001, 0x0db8, ..]) || (segments[0] == 0x3fff && segments[1] < 0x1000);
    let is_6to4 = segments[0] == 0x2002;
    let is_ipv4_mapped = matches!(segments, [0, 0, 0, 0, 0, 0xffff, _, _]);

    is_global_unicast
        && !(is_ipv4_mapped
            || (is_ietf_assignment && !is_ietf_global_exception)
            || is_documentation
            || is_6to4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_public_ipv4_addresses() {
        let cases = [
            ("8.8.8.8", true),
            ("100.63.255.255", true),
            ("100.128.0.0", true),
            ("192.0.0.9", true),
            ("192.0.0.10", true),
            ("198.17.255.255", true),
            ("198.20.0.0", true),
            ("0.0.0.0", false),
            ("10.0.0.1", false),
            ("100.64.0.0", false),
            ("100.127.255.255", false),
            ("127.0.0.1", false),
            ("169.254.169.254", false),
            ("172.16.0.1", false),
            ("192.168.0.1", false),
            ("192.0.0.8", false),
            ("192.0.0.11", false),
            ("192.0.2.1", false),
            ("192.88.99.1", false),
            ("198.18.0.0", false),
            ("198.19.255.255", false),
            ("198.51.100.1", false),
            ("203.0.113.1", false),
            ("224.0.0.1", false),
            ("240.0.0.1", false),
            ("255.255.255.255", false),
        ];

        for (input, expected) in cases {
            let address = input.parse().unwrap();
            assert_eq!(is_public_ip(address), expected, "{input}");
        }
    }

    #[test]
    fn classifies_public_ipv6_addresses() {
        let cases = [
            ("2606:4700:4700::1111", true),
            ("2001:1::1", true),
            ("2001:1::2", true),
            ("2001:1::3", true),
            ("2001:3:ffff:ffff:ffff:ffff:ffff:ffff", true),
            ("2001:4:112:ffff:ffff:ffff:ffff:ffff", true),
            ("2001:20::1", true),
            ("2001:2f:ffff:ffff:ffff:ffff:ffff:ffff", true),
            ("2001:30::1", true),
            ("2001:3f:ffff:ffff:ffff:ffff:ffff:ffff", true),
            ("::", false),
            ("::1", false),
            ("::ffff:8.8.8.8", false),
            ("64:ff9b::808:808", true),
            ("64:ff9b::7f00:1", false),
            ("64:ff9b:1::1", false),
            ("100::1", false),
            ("2001::1", false),
            ("2001:1::", false),
            ("2001:1::4", false),
            ("2001:1f:ffff:ffff:ffff:ffff:ffff:ffff", false),
            ("2001:2::1", false),
            ("2001:4::1", false),
            ("2001:4:111:ffff:ffff:ffff:ffff:ffff", false),
            ("2001:4:113::1", false),
            ("2001:40::1", false),
            ("2001:db8::1", false),
            ("2002::1", false),
            ("3fff::1", false),
            ("5f00::1", false),
            ("fc00::1", false),
            ("fe80::1", false),
            ("ff02::1", false),
        ];

        for (input, expected) in cases {
            let address = input.parse().unwrap();
            assert_eq!(is_public_ip(address), expected, "{input}");
        }
    }

    #[test]
    fn rejects_empty_and_mixed_dns_answers() {
        let public = "8.8.8.8:443".parse().unwrap();
        let private = "127.0.0.1:443".parse().unwrap();

        assert_eq!(
            ensure_public_socket_addresses(&[]).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert!(ensure_public_socket_addresses(&[public]).is_ok());
        assert_eq!(
            ensure_public_socket_addresses(&[public, private]).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
    }

    #[tokio::test]
    async fn guarded_client_rejects_private_literal() {
        let url = Url::parse("https://127.0.0.1:8443").unwrap();
        let result = new_public_client(&url, Duration::from_secs(1)).await;

        assert_eq!(result.err().unwrap().kind(), ErrorKind::ConfigError);
    }

    #[tokio::test]
    async fn guarded_client_accepts_public_literal() {
        let url = Url::parse("https://8.8.8.8:8443").unwrap();

        assert!(new_public_client(&url, Duration::from_secs(1)).await.is_ok());
    }

    #[tokio::test]
    async fn guarded_client_rejects_localhost_dns_answer() {
        let url = Url::parse("https://localhost:8443").unwrap();
        let result = new_public_client(&url, Duration::from_secs(5)).await;

        assert_eq!(result.err().unwrap().kind(), ErrorKind::ConfigError);
    }
}
