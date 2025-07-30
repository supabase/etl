use base64::{Engine, prelude::BASE64_STANDARD};
use etl_config::shared::{PgConnectionConfig, SentryConfig};
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, de};
use std::fmt;
use thiserror::Error;

/// The length in bytes required for a valid API key.
const API_KEY_LENGTH_IN_BYTES: usize = 32;

/// Top-level configuration for the API service.
///
/// Contains database, application, encryption, API key, and optional Sentry settings.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    /// Database connection configuration.
    pub database: PgConnectionConfig,
    /// Application server settings.
    pub application: ApplicationSettings,
    /// Encryption key configuration.
    pub encryption_key: EncryptionKey,
    /// Base64-encoded API key string.
    pub api_key: String,
    /// Optional Sentry configuration for error tracking.
    pub sentry: Option<SentryConfig>,
}

/// Network and server settings for the API.
#[derive(Debug, Clone, Deserialize)]
pub struct ApplicationSettings {
    /// Host address the API listens on.
    pub host: String,
    /// Port number the API listens on.
    pub port: u16,
}

impl fmt::Display for ApplicationSettings {
    /// Formats the [`ApplicationSettings`] for display, showing host and port.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "    host: {}", self.host)?;
        writeln!(f, "    port: {}", self.port)
    }
}

/// Configuration for an encryption key.
#[derive(Debug, Clone, Deserialize)]
pub struct EncryptionKey {
    /// Unique identifier for the key.
    pub id: u32,
    /// Base64-encoded key material.
    pub key: String,
}

/// Errors that can occur when converting a string to an [`ApiKey`].
#[derive(Debug, Error)]
pub enum ApiKeyConversionError {
    /// The API key is not valid base64.
    #[error("api key is not base64 encoded")]
    NotBase64Encoded,

    /// The API key does not have the expected length of 32 bytes.
    #[error("expected length of api key is 32, but actual length is {0}")]
    LengthNot32IBytes(usize),
}

/// A validated API key, represented as a fixed-size byte array.
#[derive(Debug)]
pub struct ApiKey {
    /// The 32-byte decoded API key.
    pub key: [u8; API_KEY_LENGTH_IN_BYTES],
}

impl TryFrom<&str> for ApiKey {
    type Error = ApiKeyConversionError;

    /// Attempts to decode a base64-encoded string into an [`ApiKey`].
    ///
    /// Returns an error if the string is not valid base64 or does not decode to 32 bytes.
    ///
    /// # Panics
    ///
    /// Panics if the decoded key cannot be converted to a 32-byte array, which should never occur if the length check passes.
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let key = BASE64_STANDARD
            .decode(value)
            .map_err(|_| ApiKeyConversionError::NotBase64Encoded)?;

        if key.len() != API_KEY_LENGTH_IN_BYTES {
            return Err(ApiKeyConversionError::LengthNot32IBytes(key.len()));
        }

        Ok(ApiKey {
            key: key
                .try_into()
                .expect("failed to convert api key into array"),
        })
    }
}

impl<'de> Deserialize<'de> for ApiKey {
    /// Deserializes an [`ApiKey`] from a struct with a base64-encoded `key` field.
    ///
    /// Returns an error if the field is missing, duplicated, not base64, or not 32 bytes.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Key,
        }

        struct ApiKeyVisitor;

        impl<'de> Visitor<'de> for ApiKeyVisitor {
            type Value = ApiKey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct ApiKey")
            }

            fn visit_map<V>(self, mut map: V) -> Result<ApiKey, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut key: Option<&str> = None;
                while let Some(map_key) = map.next_key()? {
                    match map_key {
                        Field::Key => {
                            if key.is_some() {
                                return Err(de::Error::duplicate_field("key"));
                            }
                            key = Some(map.next_value()?);
                        }
                    }
                }
                let key_str = key.ok_or_else(|| de::Error::missing_field("key"))?;
                let key = key_str.try_into().map_err(|_| {
                    de::Error::invalid_value(
                        de::Unexpected::Str(key_str),
                        &"base64 encoded 32 bytes",
                    )
                })?;
                Ok(key)
            }
        }

        const FIELDS: &[&str] = &["key"];
        deserializer.deserialize_struct("ApiKey", FIELDS, ApiKeyVisitor)
    }
}
