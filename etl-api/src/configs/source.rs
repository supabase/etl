use etl_config::SerializableSecretString;
use etl_config::shared::{PgConnectionConfig, TlsConfig};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::configs::encryption::{
    Decrypt, DecryptionError, Encrypt, EncryptedValue, EncryptionError, EncryptionKey,
    decrypt_text, encrypt_text,
};

const DEFAULT_TLS_TRUSTED_ROOT_CERTS: &str = "";
const DEFAULT_TLS_ENABLED: bool = false;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct FullApiSourceConfig {
    #[schema(example = "localhost")]
    pub host: String,
    #[schema(example = 5432)]
    pub port: u16,
    #[schema(example = "mydb")]
    pub name: String,
    #[schema(example = "postgres")]
    pub username: String,
    #[schema(example = "secret123")]
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StoredSourceConfig {
    pub host: String,
    pub port: u16,
    pub name: String,
    pub username: String,
    pub password: Option<SerializableSecretString>,
}

impl StoredSourceConfig {
    pub fn into_connection_config(self) -> PgConnectionConfig {
        PgConnectionConfig {
            host: self.host,
            port: self.port,
            name: self.name,
            username: self.username,
            password: self.password,
            // TODO: enable TLS
            tls: TlsConfig {
                trusted_root_certs: DEFAULT_TLS_TRUSTED_ROOT_CERTS.to_string(),
                enabled: DEFAULT_TLS_ENABLED,
            },
        }
    }

    pub fn into_connection_config_with_tls(self, tls: TlsConfig) -> PgConnectionConfig {
        PgConnectionConfig {
            host: self.host,
            port: self.port,
            name: self.name,
            username: self.username,
            password: self.password,
            tls,
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
    fn test_stored_source_config_serialization() {
        let config = StoredSourceConfig {
            host: "localhost".to_string(),
            port: 5432,
            name: "testdb".to_string(),
            username: "user".to_string(),
            password: Some(SerializableSecretString::from("password".to_string())),
        };

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: StoredSourceConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(config.host, deserialized.host);
        assert_eq!(config.port, deserialized.port);
        assert_eq!(config.name, deserialized.name);
        assert_eq!(config.username, deserialized.username);
    }

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
