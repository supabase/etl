use std::{future::Future, time::Duration};

use base64::{Engine as _, engine::general_purpose as base64_engine};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::{
    pkcs1::{DecodeRsaPrivateKey, EncodeRsaPrivateKey},
    pkcs8::{DecodePrivateKey, EncodePublicKey},
};
use secrecy::ExposeSecret as _;
use sha2::Digest as _;
use tokio::{sync::Mutex, time::Instant};

use crate::snowflake::{Error, Result};

/// Self-signed JWT validity window (Snowflake rejects JWTs older than this).
const TOKEN_LIFETIME_SECS: u64 = 3600;

/// Refresh the scoped token this far before it expires to avoid mid-request
/// expiry.
const TOKEN_REFRESH_BUFFER: Duration = Duration::from_secs(60);

/// Token produced by Snowflake.
///
/// Scoped tokens are short-lived, limited-privilege credentials.
pub struct ScopedToken {
    pub access_token: String,
    pub expires_at: Instant,
}

/// Abstracts the HTTP call that exchanges a self-signed JWT for a scoped token.
pub trait TokenExchanger: Send + Sync {
    fn exchange(&self, host: &str, jwt: &str) -> impl Future<Output = Result<ScopedToken>> + Send;
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
    async fn exchange(&self, host: &str, jwt: &str) -> Result<ScopedToken> {
        let url = format!("https://{host}/oauth/token");
        let body =
            format!("grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion={jwt}");

        let response = self
            .http
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;

        let status = response.status().as_u16();
        let body_text = response.text().await?;

        // Likely wrong key, non-retriable.
        if status == 401 {
            return Err(Error::Auth(format!("token exchange rejected (401): {body_text}")));
        }

        // Transient failure, probably retriable error.
        if status >= 400 {
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
pub struct AuthManager<E: TokenExchanger = HttpExchanger> {
    host: String,
    account: String,
    user: String,
    encoding_key: EncodingKey,
    key_fingerprint: String,
    cached_token: Mutex<Option<ScopedToken>>,
    exchanger: E,
}

impl AuthManager<HttpExchanger> {
    /// Build an `AuthManager` from an RSA private key on disk.
    ///
    /// Creates its own internal `reqwest::Client` via `HttpExchanger`.
    pub fn new(
        account: &str,
        user: &str,
        private_key_path: &str,
        passphrase: Option<&secrecy::SecretString>,
        host: Option<&str>,
    ) -> Result<Self> {
        Self::with_exchanger(
            account,
            user,
            private_key_path,
            passphrase,
            host,
            HttpExchanger::new(reqwest::Client::new()),
        )
    }
}

impl<E: TokenExchanger> AuthManager<E> {
    /// Build an `AuthManager` with a custom token exchanger.
    ///
    /// The public-key fingerprint is derived and reused for every JWT.
    /// Accepts PKCS#8 (encrypted or plain) and PKCS#1 PEM formats.
    pub fn with_exchanger(
        account: &str,
        user: &str,
        private_key_path: &str,
        passphrase: Option<&secrecy::SecretString>,
        host: Option<&str>,
        exchanger: E,
    ) -> Result<Self> {
        let pem = std::fs::read_to_string(private_key_path)
            .map_err(|e| Error::Config(format!("failed to read private key file: {e}")))?;

        // Try encrypted PKCS#8 first, then plain PKCS#8, then PKCS#1.
        let private_key = if let Some(pass) = passphrase {
            rsa::RsaPrivateKey::from_pkcs8_encrypted_pem(&pem, pass.expose_secret())
                .map_err(|e| Error::Auth(format!("failed to parse encrypted private key: {e}")))?
        } else {
            rsa::RsaPrivateKey::from_pkcs8_pem(&pem)
                .or_else(|_| rsa::RsaPrivateKey::from_pkcs1_pem(&pem))
                .map_err(|e| Error::Auth(format!("failed to parse private key: {e}")))?
        };

        // Snowflake identifies keys by SHA-256(DER-encoded public key).
        let public_key = rsa::RsaPublicKey::from(&private_key);
        let der = public_key
            .to_public_key_der()
            .map_err(|e| Error::Auth(format!("failed to encode public key to DER: {e}")))?;
        let hash = sha2::Sha256::digest(der.as_bytes());
        let b64 = base64_engine::STANDARD.encode(hash);
        let key_fingerprint = format!("SHA256:{b64}");

        // jsonwebtoken requires PKCS#1 DER for RSA signing.
        let pkcs1_der = private_key
            .to_pkcs1_der()
            .map_err(|e| Error::Auth(format!("failed to convert key to PKCS1 DER: {e}")))?;
        let encoding_key = EncodingKey::from_rsa_der(pkcs1_der.as_bytes());

        // Configure Snowflake endpoint used to obtain scoped token.
        let account = account.to_uppercase();
        let host =
            host.map(String::from).unwrap_or_else(|| format!("{account}.snowflakecomputing.com"));

        Ok(Self {
            host,
            account,
            user: user.to_uppercase(),
            encoding_key,
            key_fingerprint,
            cached_token: tokio::sync::Mutex::new(None),
            exchanger,
        })
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

    /// Return a valid scoped token.
    ///
    /// The token is refreshed, if necessary.
    pub async fn get_token(&self) -> Result<String> {
        let mut cached = self.cached_token.lock().await;

        if let Some(ref token) = *cached {
            if token.expires_at > Instant::now() + TOKEN_REFRESH_BUFFER {
                return Ok(token.access_token.clone());
            }
        }

        tracing::debug!("refreshing Snowflake scoped token");

        let jwt = self.generate_jwt()?;
        let scoped = self.exchanger.exchange(&self.host, &jwt).await?;
        let access_token = scoped.access_token.clone();
        *cached = Some(scoped);

        Ok(access_token)
    }
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

    struct TestExchanger;

    impl TokenExchanger for TestExchanger {
        async fn exchange(&self, _host: &str, _jwt: &str) -> Result<ScopedToken> {
            Ok(ScopedToken {
                access_token: "fresh-token-from-exchange".to_string(),
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

    const TEST_KEY_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/testdata/test_key.pem");

    fn make_test_manager() -> AuthManager<TestExchanger> {
        make_test_manager_with_account("ORG-ACCT", "USER")
    }

    fn make_test_manager_with_account(account: &str, user: &str) -> AuthManager<TestExchanger> {
        AuthManager::with_exchanger(account, user, TEST_KEY_PATH, None, None, TestExchanger)
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
                "iss={iss} for account={account}"
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
        let fp = manager.fingerprint();

        // Fingerprint must use the SHA256: prefix per Snowflake convention.
        assert!(fp.starts_with("SHA256:"), "fingerprint: {fp}");

        // The base64 payload must decode to exactly 32 bytes (SHA-256 digest).
        let b64_part = &fp["SHA256:".len()..];
        let decoded = base64_engine::STANDARD.decode(b64_part).expect("base64 decode fingerprint");
        assert_eq!(decoded.len(), 32, "SHA-256 digest must be 32 bytes");
    }

    #[tokio::test]
    async fn token_cache_hit() {
        let manager = make_test_manager();
        manager.inject_token_for_test("cached-token-123".to_string(), Duration::from_secs(120));

        // A non-expired token should be returned directly from cache.
        let token = manager.get_token().await.expect("get_token");
        assert_eq!(token, "cached-token-123");
    }

    #[tokio::test]
    async fn token_cache_expired_triggers_refresh() {
        let manager = make_test_manager();

        // Inject a token with 30s TTL, which is below the 60s refresh buffer.
        manager.inject_token_for_test("stale-token".to_string(), Duration::from_secs(30));

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
