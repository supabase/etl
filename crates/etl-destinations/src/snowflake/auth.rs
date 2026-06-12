//! Key-pair JWT authentication for the Snowflake SQL and Streaming APIs.
//!
//! Flow:
//!   1. Load RSA private key from config, derive a public-key fingerprint
//!   2. Sign a short-lived JWT (1 h) identifying the account and user
//!   3. Exchange the JWT at `POST {account_url}/oauth/token` for a bearer token
//!      (~10 min TTL)
//!   4. Cache the bearer token; refresh proactively when < 60 s remain
//!
//! The `scope` parameter is intentionally omitted from the token exchange so
//! the resulting token is accepted by both the SQL REST API and the Snowpipe
//! Streaming REST API. Setting `scope=<ingest_host>` would restrict the token
//! to a single ingest host.
//!
//! Ref: <https://docs.snowflake.com/en/user-guide/key-pair-auth>
//! Ref: <https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-rest-api>

use std::{future::Future, time::Duration};

use aws_lc_rs::{encoding::AsDer, signature::KeyPair as _};
use base64::{Engine as _, engine::general_purpose as base64_engine};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use reqwest::StatusCode;
use secrecy::ExposeSecret as _;
use sha2::Digest as _;
use tokio::{sync::Mutex, time::Instant};

use crate::snowflake::{
    Config, Error, Result,
    config::{HTTP_CONNECT_TIMEOUT, HTTP_REQUEST_TIMEOUT},
};

/// Self-signed JWT validity window (Snowflake rejects JWTs older than this).
const TOKEN_LIFETIME_SECS: u64 = 3600;

/// Refresh the scoped token this far before it expires to avoid mid-request
/// expiry.
const TOKEN_REFRESH_BUFFER: Duration = Duration::from_secs(60);

/// Token produced by Snowflake's `/oauth/token` endpoint.
///
/// "Scoped" in Snowflake's terminology refers to the token being a short-lived,
/// restricted derivative of the raw JWT, not the `scope` request parameter
/// (which controls host restriction and is optional; see module docs).
pub struct ScopedToken {
    pub(crate) access_token: String,
    pub(crate) expires_at: Instant,
}

/// Abstracts the HTTP call that exchanges a self-signed JWT for a scoped token.
pub trait TokenExchanger: Send + Sync {
    fn exchange(
        &self,
        account_url: &str,
        jwt: &str,
    ) -> impl Future<Output = Result<ScopedToken>> + Send;
}

/// Provides a valid bearer token and supports invalidation.
pub trait TokenProvider: Send + Sync {
    /// Return a valid bearer token, refreshing it if necessary.
    fn get_token(&self) -> impl Future<Output = Result<String>> + Send;

    /// Invalidate any cached token, forcing a fresh exchange on the next
    /// [`get_token`](Self::get_token) call.
    fn invalidate_token(&self) -> impl Future<Output = ()> + Send;
}

/// Production implementation that calls Snowflake's OAuth endpoint over HTTP.
pub struct HttpExchanger {
    http: reqwest::Client,
}

impl HttpExchanger {
    pub fn new(http: reqwest::Client) -> Self {
        Self { http }
    }
}

impl TokenExchanger for HttpExchanger {
    async fn exchange(&self, account_url: &str, jwt: &str) -> Result<ScopedToken> {
        let url = format!("{account_url}/oauth/token");
        let body =
            format!("grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion={jwt}");

        let response = self
            .http
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;

        let status = response.status();
        let body_text = response.text().await?;

        // Likely wrong key, non-retriable.
        if status == StatusCode::UNAUTHORIZED {
            return Err(Error::Auth(format!("token exchange rejected (401): {body_text}")));
        }

        // Transient failure, probably retriable error.
        if status.is_client_error() || status.is_server_error() {
            return Err(Error::HttpStatus { status, body: body_text });
        }

        // Snowflake returns the scoped token as a raw JWT (RFC 7519).
        let expires_at = decode_token_expiry(&body_text)?;

        Ok(ScopedToken { access_token: body_text, expires_at })
    }
}

/// Manages key-pair authentication and scoped-token lifecycle.
///
/// Signs a JWT locally, exchanges it for a short-lived scoped token via
/// Snowflake's OAuth endpoint, and caches the result.
pub struct AuthManager<E = HttpExchanger> {
    config: Config,
    account: String,
    user: String,
    encoding_key: EncodingKey,
    key_fingerprint: String,
    cached_token: Mutex<Option<ScopedToken>>,
    exchanger: E,
}

impl AuthManager<HttpExchanger> {
    /// Build an `AuthManager` from Snowflake configuration.
    ///
    /// Creates its own internal `reqwest::Client` via `HttpExchanger`.
    pub fn new(config: Config) -> Result<Self> {
        Self::with_exchanger(
            config,
            HttpExchanger::new(
                reqwest::Client::builder()
                    .connect_timeout(HTTP_CONNECT_TIMEOUT)
                    .timeout(HTTP_REQUEST_TIMEOUT)
                    .build()
                    .expect("failed to build HTTP client"),
            ),
        )
    }
}

impl<E: TokenExchanger> AuthManager<E> {
    /// Build an `AuthManager` from Snowflake configuration.
    ///
    /// The public-key fingerprint is derived and reused for every JWT.
    /// Accepts PKCS#8 (encrypted or plain) and PKCS#1 PEM formats.
    pub fn with_exchanger(mut config: Config, exchanger: E) -> Result<Self> {
        let account = config.account_id.to_uppercase();
        let user = config.username.to_uppercase();
        let (private_key, passphrase) = config.take_credentials()?;
        let (pkcs1_der, key_pair) =
            decode_and_load_rsa_key(private_key.expose_secret(), passphrase.as_ref())?;

        // Snowflake identifies keys by SHA-256(DER-encoded public key).
        let pub_der = key_pair
            .public_key()
            .as_der()
            .map_err(|_| Error::Auth("failed to encode public key to DER".into()))?;
        let hash = sha2::Sha256::digest(pub_der.as_ref());
        let b64 = base64_engine::STANDARD.encode(hash);
        let key_fingerprint = format!("SHA256:{b64}");

        // jsonwebtoken's aws_lc_rs backend passes these bytes to
        // RsaKeyPair::from_der(), which expects PKCS#1 (raw RSA) DER.
        let encoding_key = EncodingKey::from_rsa_der(&pkcs1_der);

        Ok(Self {
            config,
            account,
            user,
            encoding_key,
            key_fingerprint,
            cached_token: tokio::sync::Mutex::new(None),
            exchanger,
        })
    }

    /// Return sanitized Snowflake configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Create a short-lived JWT (1 hour) used to request a scoped OAuth token.
    fn generate_jwt(&self) -> Result<String> {
        #[derive(serde::Serialize)]
        struct JwtClaims {
            iss: String,
            sub: String,
            iat: u64,
            exp: u64,
        }

        let iat = jsonwebtoken::get_current_timestamp();
        let claims = JwtClaims {
            iss: format!("{}.{}.{}", self.account, self.user, self.key_fingerprint),
            sub: format!("{}.{}", self.account, self.user),
            iat,
            exp: iat + TOKEN_LIFETIME_SECS,
        };

        jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &self.encoding_key)
            .map_err(|e| Error::Auth(format!("failed to sign JWT: {e}")))
    }
}

impl<E: TokenExchanger> TokenProvider for AuthManager<E> {
    /// Return a valid scoped token, refreshing it if necessary.
    async fn get_token(&self) -> Result<String> {
        let mut cached = self.cached_token.lock().await;

        if let Some(ref token) = *cached
            && token.expires_at > Instant::now() + TOKEN_REFRESH_BUFFER
        {
            return Ok(token.access_token.clone());
        }

        tracing::debug!("refreshing Snowflake scoped token");

        let jwt = self.generate_jwt()?;
        let scoped = self.exchanger.exchange(self.config.account_url(), &jwt).await?;
        let access_token = scoped.access_token.clone();
        *cached = Some(scoped);

        Ok(access_token)
    }

    /// Invalidate the cached scoped token.
    ///
    /// The next call to [`TokenProvider::get_token`] will perform a fresh
    /// token exchange.
    async fn invalidate_token(&self) {
        *self.cached_token.lock().await = None;
    }
}

/// Decode a PEM-encoded RSA private key, returning the PKCS#1 DER bytes
/// (for jsonwebtoken) and a loaded `KeyPair` (for fingerprint derivation).
///
/// Supports encrypted PKCS#8, plain PKCS#8, and PKCS#1 PEM formats.
fn decode_and_load_rsa_key(
    pem_text: &str,
    passphrase: Option<&secrecy::SecretString>,
) -> Result<(Vec<u8>, aws_lc_rs::rsa::KeyPair)> {
    use pkcs8::der::{Decode as _, SecretDocument};

    let der_bytes = if let Some(pass) = passphrase {
        let (_, doc) = SecretDocument::from_pem(pem_text)
            .map_err(|e| Error::Auth(format!("failed to parse encrypted PEM: {e}")))?;
        let enc = pkcs8::EncryptedPrivateKeyInfoRef::try_from(doc.as_bytes())
            .map_err(|e| Error::Auth(format!("failed to parse encrypted key: {e}")))?;
        enc.decrypt(pass.expose_secret())
            .map_err(|e| Error::Auth(format!("failed to decrypt private key: {e}")))?
            .as_bytes()
            .to_vec()
    } else {
        let (_, doc) = SecretDocument::from_pem(pem_text)
            .map_err(|e| Error::Auth(format!("failed to parse private key PEM: {e}")))?;
        doc.as_bytes().to_vec()
    };

    // Try PKCS#8: extract the inner RSA key for jsonwebtoken, load via from_pkcs8.
    if let Ok(pki) = pkcs8::PrivateKeyInfoRef::from_der(&der_bytes) {
        let key_pair = aws_lc_rs::rsa::KeyPair::from_pkcs8(&der_bytes)
            .map_err(|e| Error::Auth(format!("failed to load RSA key: {e}")))?;
        return Ok((pki.private_key.as_bytes().to_vec(), key_pair));
    }

    // Raw PKCS#1 DER: use directly.
    let key_pair = aws_lc_rs::rsa::KeyPair::from_der(&der_bytes)
        .map_err(|e| Error::Auth(format!("failed to load RSA key: {e}")))?;
    Ok((der_bytes, key_pair))
}

/// Extract `exp` from a raw JWT (RFC 7519) and convert to monotonic Instant.
///
/// Returned value is adjusted accordingly to how much time remains for original
/// expiry to happen.
fn decode_token_expiry(token: &str) -> Result<Instant> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| Error::Auth("scoped token is not a valid JWT (missing payload)".into()))?;

    let bytes = base64_engine::URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|e| Error::Auth(format!("scoped token payload is not valid base64: {e}")))?;

    let claims: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| Error::Auth(format!("scoped token payload is not valid JSON: {e}")))?;

    let exp = claims["exp"]
        .as_u64()
        .ok_or_else(|| Error::Auth("scoped token is missing the `exp` claim".into()))?;

    let remaining = exp.saturating_sub(jsonwebtoken::get_current_timestamp());

    Ok(Instant::now() + Duration::from_secs(remaining))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Non-secret RSA key generated only for hermetic auth unit tests.
    const TEST_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDOiGnVXNdtwyJr
jOJHno9nS+oVBl7PHU+64VvVL7EHZ5hzyL+ZM6bdM+CDjKmaJ0g8TiCPcHTHXnRY
fMcuaZKk5VOsR1xi81biezJj/Q/TGb+Vja4TD7f4DnTmxG0xnQXsceya5fzQM8lB
D9FzIOk0rR4UHw/zIvB4FhpAqaHWxFcgIMDKGv3puYmooXtTOhepgAlATZuKrfBQ
AqdbaMWDtucGcfQru02GOXqn/JeggDKSQ5xlCguhocUPEVxI9ZwHh+yCVtxurAtc
6z0gyQ2dlbZHG91KParLCaDU2Ff9Pf8ubE/HVv+8RtLoqYISWOPGE7W1V8pbprQ7
scg6J9iVAgMBAAECggEAIorcMIw7l6cITbadbd8OGveuad/L4ZYEbLweUNSOJi/k
ZpEPwn7KDLsNdNME1rx1L2jdtz/WuDWK/fW4loGfviaAzRKOWBpc0LpMHj8H84Wd
7lRo5dU+LqW0VZhKrv6VLAuNyAZpNyVCJriPjlLVzjKaEkFzuHWChIMl1uTIJZQa
F88mP4YpbJfpSU3b94eUFwPsYL8dCQCujLpriehMnBN+3qLK39JMbQk4xSzcQ8r6
+L6ZVTcmKaXMWWH6e4Tm08SvX6SgAfxZm/v5LlByEQRlxfergGlLA8+4lYM8JbXi
eut7EOjMloA/eE+3edJubvPmWmu+30lL0x1Fkcz2QQKBgQDml5T2Xvj/khna6mqy
HEG42RThWdapXg3OcxjXzcYfxQd9Uke34fwgo7LAsegONJgakmFdqVrvMUiU+sio
s8Ds4iHw3yqOqDel06PYTkn10J8vWgyP4Z1KT6wuSMdAYqnze4EQr4+36lGFpEll
QvvX/rjvydqKo2pxtVhOf7S4xQKBgQDlSi5eAn2MZXc7zi+QE/iK8TWdpKkeaDBR
AhiV7XGKwq0yBWdGYv11cVSKNpjJ9gCNcAYSFOoNNz6532Y/2DQvhAvfvkP4m08D
jU5HpPw5GEYUtXySbCJ1DkEPrX4lqkXA5Uw4DcMWmmA5YONgJ6BN0bd2zqSSoY2g
wTn/53d9kQKBgQDWmbvIjhqtvwrQ8djaafHAVkdYcoOUnDO9LuCv9pGsf3G48BpO
x8Idnjt9mhSdI9Vq5VA4GqTGdtdVzw9v8dpamxl7UjYJDgS8D3ssk6/BVabQKr4G
KbJ4ti1H5fOJuEjykL5NCRZ301qLRZoI443+NtFmWDVLUUp/CIZmh/NpAQKBgB5g
7LHB7LZsPxbqY3zYWIa4HJ1tUobX0Qb6mx1KH0/+KQpGkv9NYD1uLYA+aZHgiQQ0
Qmmk4bmshyADTD3LPGbLPPOA9up6UUasMyHk5xH9eFOIFCAmOY5+u/oCx4LgA2vi
NW37zMwy2ergPl/gACovTfpsuHtA8k3JLBEOrtMxAoGBAMXOs1j0+p1Y7gm7lppF
FXYtPt7sn+eROS9ACov4lrhXqIjgiLxBiQbO34dSy8X3sb18W8vIckeRrHE7zcLE
G/8wosvgIJ5zIqMZSeT3WFS82C9dk7ad2L1LEMbL/AHvXxgIr/sjNuReIBXFmp/t
P23pIjtEtEPNpGkXj0aB1RDq
-----END PRIVATE KEY-----"#;

    struct TestExchanger;

    impl TokenExchanger for TestExchanger {
        async fn exchange(&self, _account_url: &str, _jwt: &str) -> Result<ScopedToken> {
            Ok(ScopedToken {
                access_token: "fresh-token-from-exchange".to_owned(),
                expires_at: Instant::now() + Duration::from_secs(3600),
            })
        }
    }

    impl<E: TokenExchanger> AuthManager<E> {
        fn inject_token_for_test(&self, access_token: String, ttl: std::time::Duration) {
            *self.cached_token.try_lock().unwrap() =
                Some(ScopedToken { access_token, expires_at: Instant::now() + ttl });
        }

        fn fingerprint(&self) -> &str {
            &self.key_fingerprint
        }
    }

    fn make_test_config(account_id: &str, username: &str) -> Config {
        Config::new(account_id, username, "TEST_DB", "PUBLIC")
            .expect("valid config")
            .with_private_key(TEST_PRIVATE_KEY, None)
    }

    fn make_test_manager() -> AuthManager<TestExchanger> {
        AuthManager::with_exchanger(
            make_test_config("TESTORG-TESTACCOUNT", "TESTUSER"),
            TestExchanger,
        )
        .expect("AuthManager::with_exchanger")
    }

    fn make_test_manager_with_account(
        account_id: &str,
        username: &str,
    ) -> AuthManager<TestExchanger> {
        AuthManager::with_exchanger(make_test_config(account_id, username), TestExchanger)
            .expect("AuthManager::with_exchanger")
    }

    #[test]
    fn jwt_claims() {
        let cases = [
            // (input_account, input_user, expected_account, expected_user).
            ("TESTORG-TESTACCOUNT", "TESTUSER", "TESTORG-TESTACCOUNT", "TESTUSER"),
            ("org-account", "my_user", "ORG-ACCOUNT", "MY_USER"),
        ];

        fn decode_jwt_claims(jwt: &str) -> serde_json::Value {
            let parts: Vec<&str> = jwt.split('.').collect();
            assert_eq!(parts.len(), 3, "JWT must have 3 dot-separated parts");
            let payload_bytes =
                base64_engine::URL_SAFE_NO_PAD.decode(parts[1]).expect("base64 decode payload");
            serde_json::from_slice(&payload_bytes).expect("json parse claims")
        }

        for (account, user, expect_account, expect_user) in cases {
            let manager = make_test_manager_with_account(account, user);
            let jwt = manager.generate_jwt().expect("generate_jwt");
            let claims = decode_jwt_claims(&jwt);

            // Issuer must be ACCOUNT.USER.FINGERPRINT.
            let iss = claims["iss"].as_str().expect("iss");
            assert!(
                iss.starts_with(&format!("{expect_account}.{expect_user}.SHA256:")),
                "unexpected iss: {iss}"
            );

            // Subject must be ACCOUNT.USER.
            assert_eq!(claims["sub"], format!("{expect_account}.{expect_user}"));

            // Token must expire exactly TOKEN_LIFETIME_SECS after issuance.
            let iat = claims["iat"].as_u64().expect("iat");
            let exp = claims["exp"].as_u64().expect("exp");
            assert_eq!(exp, iat + TOKEN_LIFETIME_SECS);
        }
    }

    #[test]
    fn key_fingerprint_format() {
        let manager = make_test_manager();

        // Auth sanitized config, by consuming PK and secret passphrase.
        assert!(manager.config().private_key().is_none());
        assert!(manager.config().private_key_passphrase().is_none());

        let fp = manager.fingerprint();

        // Fingerprint must use the SHA256: prefix per Snowflake convention.
        assert!(fp.starts_with("SHA256:"), "fingerprint missing SHA256 prefix");

        // The base64 payload must decode to exactly 32 bytes (SHA-256 digest).
        let b64_part = &fp["SHA256:".len()..];
        let decoded = base64_engine::STANDARD.decode(b64_part).expect("base64 decode fingerprint");
        assert_eq!(decoded.len(), 32, "SHA-256 digest must be 32 bytes");
    }

    #[test]
    fn config_derives_account_url() {
        let config = Config::new("ORG-ACCT", "USER", "TEST_DB", "PUBLIC").expect("valid config");
        assert_eq!(config.account_url(), "https://ORG-ACCT.snowflakecomputing.com");
    }

    #[tokio::test]
    async fn token_cache_hit() {
        let manager = make_test_manager();
        manager.inject_token_for_test("cached-token-123".to_owned(), Duration::from_secs(120));

        // A non-expired token should be returned directly from cache.
        let token = manager.get_token().await.expect("get_token");
        assert_eq!(token, "cached-token-123");
    }

    #[tokio::test]
    async fn token_cache_expired_triggers_refresh() {
        let manager = make_test_manager();

        // Inject a token with 30s TTL, which is below the 60s refresh buffer.
        manager.inject_token_for_test("stale-token".to_owned(), Duration::from_secs(30));

        // get_token should detect the stale cache and call the exchanger.
        let token = manager.get_token().await.expect("get_token");

        // The fresh token from FakeExchanger must be returned
        assert_ne!(token, "stale-token", "still getting stale token");
        assert_eq!(token, "fresh-token-from-exchange", "unexpected refreshed token");

        // The cache must now hold the fresh token.
        {
            let cached = manager.cached_token.try_lock().unwrap();
            let entry = cached.as_ref().expect("cache should not be empty");
            assert_eq!(entry.access_token, "fresh-token-from-exchange");
        }

        // Second call should return the cached fresh token without re-exchanging.
        let token = manager.get_token().await.expect("get_token");
        assert_eq!(token, "fresh-token-from-exchange");
    }
}
