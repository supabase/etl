use etl_config::SerializableSecretString;
use etl_config::shared::{PgConnectionConfig, TlsConfig};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::configs::encryption::{
    Decrypt, DecryptionError, Encrypt, EncryptedValue, EncryptionError, EncryptionKey,
    decrypt_text, encrypt_text,
};
use crate::configs::store::Store;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct FullApiSourceConfig {
    #[schema(example = "localhost")]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub host: String,
    #[schema(example = 5432)]
    pub port: u16,
    #[schema(example = "mydb")]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub name: String,
    #[schema(example = "postgres")]
    #[serde(deserialize_with = "crate::utils::trim_string")]
    pub username: String,
    #[schema(example = "secret123")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<SerializableSecretString>,
}

impl From<StoredSourceConfig> for FullApiSourceConfig {
    fn from(stored_source_config: StoredSourceConfig) -> Self {
        Self {
            host: stored_source_config.host,
            port: stored_source_config.port,
            name: stored_source_config.name,
            username: stored_source_config.username,
            password: stored_source_config.password,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct StrippedApiSourceConfig {
    #[schema(example = "localhost")]
    pub host: String,
    #[schema(example = 5432)]
    pub port: u16,
    #[schema(example = "mydb")]
    pub name: String,
    #[schema(example = "postgres")]
    pub username: String,
}

impl From<StoredSourceConfig> for StrippedApiSourceConfig {
    fn from(source: StoredSourceConfig) -> Self {
        Self {
            host: source.host,
            port: source.port,
            name: source.name,
            username: source.username,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StoredSourceConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub username: String,
    pub password: Option<SerializableSecretString>,
}

impl StoredSourceConfig {
    /// Converts the stored source config into a Postgres connection config with TLS settings.
    pub fn into_connection_config(self, tls_config: TlsConfig) -> PgConnectionConfig {
        PgConnectionConfig {
            host: self.host,
            port: self.port,
            name: self.name,
            username: self.username,
            password: self.password.map(|p| p.into()),
            tls: tls_config,
            keepalive: None,
        }
    }
}

impl From<FullApiSourceConfig> for StoredSourceConfig {
    fn from(full_api_source_config: FullApiSourceConfig) -> Self {
        Self {
            host: full_api_source_config.host,
            port: full_api_source_config.port,
            name: full_api_source_config.name,
            username: full_api_source_config.username,
            password: full_api_source_config.password,
        }
    }
}

impl Encrypt<EncryptedStoredSourceConfig> for StoredSourceConfig {
    fn encrypt(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<EncryptedStoredSourceConfig, EncryptionError> {
        let mut encrypted_password = None;
        if let Some(password) = self.password {
            encrypted_password = Some(encrypt_text(
                password.expose_secret().to_owned(),
                encryption_key,
            )?);
        }

        Ok(EncryptedStoredSourceConfig {
            host: self.host,
            port: self.port,
            name: self.name,
            username: self.username,
            password: encrypted_password,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedStoredSourceConfig {
    host: String,
    port: u16,
    name: String,
    username: String,
    password: Option<EncryptedValue>,
}

impl Store for EncryptedStoredSourceConfig {}

impl Decrypt<StoredSourceConfig> for EncryptedStoredSourceConfig {
    fn decrypt(
        self,
        encryption_key: &EncryptionKey,
    ) -> Result<StoredSourceConfig, DecryptionError> {
        let mut decrypted_password = None;
        if let Some(password) = self.password {
            let pwd = decrypt_text(password, encryption_key)?;
            decrypted_password = Some(SerializableSecretString::from(pwd));
        }

        Ok(StoredSourceConfig {
            host: self.host,
            port: self.port,
            name: self.name,
            username: self.username,
            password: decrypted_password,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::encryption::{EncryptionKey, generate_random_key};

    #[test]
    fn test_stored_source_config_encryption_decryption() {
        let config = StoredSourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "testdb".to_string(),
            username: "user".to_string(),
            password: Some(SerializableSecretString::from("password".to_string())),
        };

        let key = EncryptionKey {
            id: 1,
            key: generate_random_key::<32>().unwrap(),
        };

        let encrypted = config.clone().encrypt(&key).unwrap();
        let decrypted = encrypted.decrypt(&key).unwrap();

        assert_eq!(config.host, decrypted.host);
        assert_eq!(config.port, decrypted.port);
        assert_eq!(config.name, decrypted.name);
        assert_eq!(config.username, decrypted.username);

        // Assert that password was encrypted and decrypted correctly
        match (config.password, decrypted.password) {
            (Some(original), Some(decrypted_pwd)) => {
                assert_eq!(original.expose_secret(), decrypted_pwd.expose_secret());
            }
            (None, None) => {}
            _ => panic!("Password encryption/decryption failed"),
        }
    }

    #[test]
    fn test_full_api_source_config_conversion() {
        let full_config = FullApiSourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "testdb".to_string(),
            username: "user".to_string(),
            password: Some(SerializableSecretString::from("password".to_string())),
        };

        let stored: StoredSourceConfig = full_config.clone().into();
        let back_to_full: FullApiSourceConfig = stored.into();

        assert_eq!(full_config.host, back_to_full.host);
        assert_eq!(full_config.port, back_to_full.port);
        assert_eq!(full_config.name, back_to_full.name);
        assert_eq!(full_config.username, back_to_full.username);
    }
}
