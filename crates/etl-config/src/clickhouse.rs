//! ClickHouse-specific configuration types.

use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

/// Errors returned when a value is not an acceptable [`ClickHouseUrl`].
///
/// None of the variants echo the rejected value or any part of it, since it
/// may contain the credentials that made it unacceptable.
#[derive(Debug, Error)]
pub enum ParseClickHouseUrlError {
    /// Parsing the value as a URL failed.
    #[error("Invalid ClickHouse URL")]
    Parse(#[source] url::ParseError),

    /// The URL does not use the HTTP interface.
    #[error("ClickHouse URL must use the http:// or https:// scheme")]
    UnsupportedScheme,

    /// The URL carries a user name or password in its userinfo.
    #[error(
        "ClickHouse URL must not embed credentials; configure `user` and `password` separately"
    )]
    EmbeddedCredentials,

    /// The URL has a query string or fragment.
    #[error(
        "ClickHouse URL must not have a query string or fragment; configure `user`, `password`, \
         and `database` separately"
    )]
    QueryOrFragment,
}

/// ClickHouse HTTP(S) endpoint URL that carries no credentials.
///
/// Credentials for ClickHouse live in the separate `user` and `password`
/// configuration fields, where the password is a secret. A URL such as
/// `https://alice:secret@clickhouse.example:8443` or
/// `https://clickhouse.example?password=secret` would move the secret into a
/// field every secret-free projection, log line, and `Debug` output treats as
/// plain data. Constructing this type rejects such values, so holding one
/// guarantees the URL is safe to serialize.
///
/// Any query string or fragment is rejected, not only credential parameters:
/// the ClickHouse client replaces the query with its own settings on every
/// request, so a query here could never take effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Url", into = "Url")]
pub struct ClickHouseUrl(Url);

impl ClickHouseUrl {
    /// Returns the underlying URL.
    pub fn as_url(&self) -> &Url {
        &self.0
    }

    /// Returns the URL scheme, either `http` or `https`.
    pub fn scheme(&self) -> &str {
        self.0.scheme()
    }
}

impl TryFrom<Url> for ClickHouseUrl {
    type Error = ParseClickHouseUrlError;

    fn try_from(url: Url) -> Result<Self, Self::Error> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(ParseClickHouseUrlError::UnsupportedScheme);
        }

        if !url.username().is_empty() || url.password().is_some() {
            return Err(ParseClickHouseUrlError::EmbeddedCredentials);
        }

        if url.query().is_some() || url.fragment().is_some() {
            return Err(ParseClickHouseUrlError::QueryOrFragment);
        }

        Ok(Self(url))
    }
}

impl FromStr for ClickHouseUrl {
    type Err = ParseClickHouseUrlError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Url::parse(value).map_err(ParseClickHouseUrlError::Parse)?.try_into()
    }
}

impl From<ClickHouseUrl> for Url {
    fn from(url: ClickHouseUrl) -> Self {
        url.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Asserts that `value` is rejected as `expected` without echoing any part
    /// of the placeholder credentials.
    fn assert_rejected(value: &str, expected: fn(&ParseClickHouseUrlError) -> bool) {
        let err = value.parse::<ClickHouseUrl>().unwrap_err();
        assert!(expected(&err), "{value}: {err:?}");

        let message = err.to_string();
        assert!(!message.contains("alice"), "{value}");
        assert!(!message.contains("s3cr3t"), "{value}");
    }

    #[test]
    fn accepts_http_and_https_endpoints() {
        for value in
            ["http://localhost:8123", "https://clickhouse.example:8443/proxy", "http://[::1]:8123"]
        {
            assert!(value.parse::<ClickHouseUrl>().is_ok(), "{value}");
        }
    }

    #[test]
    fn rejects_userinfo() {
        for value in [
            "https://alice:s3cr3t@clickhouse.example:8443",
            "https://alice@clickhouse.example:8443",
            "https://:s3cr3t@clickhouse.example:8443",
        ] {
            assert_rejected(value, |err| {
                matches!(err, ParseClickHouseUrlError::EmbeddedCredentials)
            });
        }
    }

    #[test]
    fn rejects_any_query_or_fragment() {
        for value in [
            "https://clickhouse.example:8443/?password=s3cr3t",
            "https://clickhouse.example:8443?user=alice",
            "https://clickhouse.example:8443/?database=analytics",
            "https://clickhouse.example:8443/?",
            "https://clickhouse.example:8443/#s3cr3t",
        ] {
            assert_rejected(value, |err| matches!(err, ParseClickHouseUrlError::QueryOrFragment));
        }
    }

    #[test]
    fn rejects_non_http_schemes_without_echoing_them() {
        // Without `//`, the text before the first `:` parses as the scheme, so
        // a scheme-less value with userinfo must not have its scheme echoed.
        for value in [
            "tcp://clickhouse.example:9000",
            "clickhouse.example:8123",
            "alice:s3cr3t@clickhouse.example:8443",
        ] {
            assert_rejected(value, |err| matches!(err, ParseClickHouseUrlError::UnsupportedScheme));
        }
    }

    #[test]
    fn rejects_malformed_values() {
        for value in ["", "http://", "http://exa mple"] {
            assert_rejected(value, |err| matches!(err, ParseClickHouseUrlError::Parse(_)));
        }
    }
}
