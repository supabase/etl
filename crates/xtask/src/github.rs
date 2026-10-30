//! Shared GitHub REST transport for xtask commands.
//!
//! [`GitHubClient`] only sends authenticated requests and reports what came
//! back. It does not interpret status codes, so callers keep the rate-limit,
//! retry and pagination headers that budgeted collectors need. Endpoint
//! helpers live with the commands that use them.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::{Context, Result, bail};
use reqwest::{StatusCode, Url, header::HeaderMap};
use secrecy::{ExposeSecret, SecretString};
use serde::de::DeserializeOwned;

/// Base URL of the GitHub REST API.
const GITHUB_API_BASE: &str = "https://api.github.com";
/// Host that may receive the token.
const GITHUB_API_HOST: &str = "api.github.com";
/// Pinned REST API version.
const GITHUB_API_VERSION: &str = "2022-11-28";
/// Media type of GitHub REST responses.
const GITHUB_ACCEPT: &str = "application/vnd.github+json";

/// Authenticated GitHub REST client scoped to one repository.
pub(crate) struct GitHubClient {
    client: reqwest::Client,
    token: SecretString,
    repo: String,
    requests_sent: AtomicU64,
}

/// One GitHub response with the headers budgeted callers depend on.
pub(crate) struct GitHubResponse {
    status: StatusCode,
    rate_limit_remaining: Option<u64>,
    retry_after: Option<Duration>,
    has_next_page: bool,
    body: Vec<u8>,
    truncated: bool,
}

impl GitHubClient {
    /// Creates a client for `repo` (`owner/name`).
    pub(crate) fn new(token: SecretString, repo: String, user_agent: &'static str) -> Result<Self> {
        let client = reqwest::Client::builder()
            .user_agent(user_agent)
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(60))
            .build()
            .context("Failed to build GitHub HTTP client")?;
        Ok(Self { client, token, repo, requests_sent: AtomicU64::new(0) })
    }

    /// Returns how many requests this client has sent.
    pub(crate) fn requests_sent(&self) -> u64 {
        self.requests_sent.load(Ordering::Relaxed)
    }

    /// Sends `GET /repos/{repo}/{path}` with `query` and reads the whole body.
    pub(crate) async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<GitHubResponse> {
        let url = format!("{GITHUB_API_BASE}/repos/{}/{path}", self.repo);
        self.send(&url, query, None).await
    }

    /// Sends `GET` to an absolute GitHub URL, such as an artifact download.
    ///
    /// Reads at most `max_body_bytes`; a longer body is marked truncated rather
    /// than read into memory. Redirects to other hosts drop the authorization
    /// header, so artifact downloads never send the token to blob storage.
    pub(crate) async fn get_url(&self, url: &str, max_body_bytes: usize) -> Result<GitHubResponse> {
        if !is_github_api_url(url) {
            bail!("Refusing to send GitHub credentials to a non-API URL");
        }
        self.send(url, &[], Some(max_body_bytes)).await
    }

    /// Sends one authenticated request and reads its body, up to
    /// `max_body_bytes` when given.
    async fn send(
        &self,
        url: &str,
        query: &[(&str, String)],
        max_body_bytes: Option<usize>,
    ) -> Result<GitHubResponse> {
        self.requests_sent.fetch_add(1, Ordering::Relaxed);
        let mut response = self
            .client
            .get(url)
            .query(query)
            .bearer_auth(self.token.expose_secret())
            .header("Accept", GITHUB_ACCEPT)
            .header("X-GitHub-Api-Version", GITHUB_API_VERSION)
            .send()
            .await
            // Errors carry the request URL, which after a redirect is a signed
            // blob-storage URL. Drop it so callers can log errors safely.
            .map_err(reqwest::Error::without_url)
            .context("GitHub request failed")?;

        let status = response.status();
        let headers = response.headers();
        let rate_limit_remaining = header_u64(headers, "x-ratelimit-remaining");
        let retry_after = header_u64(headers, "retry-after").map(Duration::from_secs);
        let has_next_page = headers
            .get("link")
            .and_then(|value| value.to_str().ok())
            .is_some_and(|link| link.contains("rel=\"next\""));

        let mut body = Vec::new();
        let mut truncated = false;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(reqwest::Error::without_url)
            .context("Failed to read GitHub response")?
        {
            if let Some(max) = max_body_bytes
                && body.len() + chunk.len() > max
            {
                truncated = true;
                break;
            }
            body.extend_from_slice(&chunk);
        }

        Ok(GitHubResponse {
            status,
            rate_limit_remaining,
            retry_after,
            has_next_page,
            body,
            truncated,
        })
    }
}

impl GitHubResponse {
    /// Returns the HTTP status.
    pub(crate) fn status(&self) -> StatusCode {
        self.status
    }

    /// Returns `x-ratelimit-remaining`, when present.
    pub(crate) fn rate_limit_remaining(&self) -> Option<u64> {
        self.rate_limit_remaining
    }

    /// Returns `retry-after`, when present.
    pub(crate) fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Returns whether the `link` header names a next page.
    pub(crate) fn has_next_page(&self) -> bool {
        self.has_next_page
    }

    /// Returns whether the body exceeded the requested limit.
    pub(crate) fn truncated(&self) -> bool {
        self.truncated
    }

    /// Returns whether GitHub rejected the request for rate limiting.
    pub(crate) fn is_rate_limited(&self) -> bool {
        self.status == StatusCode::TOO_MANY_REQUESTS
            || (self.status == StatusCode::FORBIDDEN
                && (self.retry_after.is_some() || self.rate_limit_remaining == Some(0)))
    }

    /// Fails unless the status is successful, naming `operation`.
    pub(crate) fn error_for_status(self, operation: &str) -> Result<Self> {
        if !self.status.is_success() {
            bail!("{operation} failed with HTTP status {}", self.status);
        }
        Ok(self)
    }

    /// Parses the body as JSON.
    pub(crate) fn json<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_slice(&self.body).context("Failed to parse GitHub response")
    }

    /// Returns the body bytes.
    pub(crate) fn into_body(self) -> Vec<u8> {
        self.body
    }
}

/// Parses a numeric response header.
fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers.get(name)?.to_str().ok()?.trim().parse().ok()
}

/// Returns whether `url` targets the GitHub REST API over HTTPS on the default
/// port, the only destination allowed to receive the token.
fn is_github_api_url(url: &str) -> bool {
    Url::parse(url).is_ok_and(|url| {
        url.scheme() == "https"
            && url.host_str() == Some(GITHUB_API_HOST)
            && url.port().is_none()
            && url.username().is_empty()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_api_urls_receive_credentials() {
        assert!(is_github_api_url(
            "https://api.github.com/repos/supabase/etl/actions/artifacts/1/zip"
        ));
        for url in [
            "https://api.github.com.example.org/repos/x",
            "https://api.github.com@example.org/repos/x",
            "https://user@api.github.com/repos/x",
            "http://api.github.com/repos/x",
            "https://api.github.com:8443/repos/x",
            "https://example.org/https://api.github.com/",
            "",
            "not a url",
        ] {
            assert!(!is_github_api_url(url), "{url}");
        }
    }
}
