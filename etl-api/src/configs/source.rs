use etl_config::SerializableSecretString;
use etl_config::shared::{PgConnectionConfig, TlsConfig};
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::configs::encryption::{
    Decrypt, DecryptionError, Encrypt, EncryptedValue, EncryptionError, EncryptionKey,
    decrypt_text, encrypt_text,
};

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
                trusted_root_certs: String::new(),
                enabled: false,
            },
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
