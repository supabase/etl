use crate::encryption::{
    Decryptable, DecryptionError, Encryptable, EncryptionError, EncryptionKey,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum DbSerializationError {
    #[error("Error while serializing data to the db: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("An error occurred while encrypting data for the db representation: {0}")]
    Encryption(#[from] EncryptionError),
}

#[derive(Debug, Error)]
pub enum DbDeserializationError {
    #[error("Error while deserializing data from the db: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("An error occurred while decrypting data from the db representation: {0}")]
    Decryption(#[from] DecryptionError),
}

pub fn serialize<S>(value: S) -> Result<serde_json::Value, DbSerializationError>
where
    S: Serialize,
{
    let serialized_value = serde_json::to_value(value)?;

    Ok(serialized_value)
}

pub fn encrypt_and_serialize<T, S>(
    value: T,
    encryption_key: &EncryptionKey,
) -> Result<serde_json::Value, DbSerializationError>
where
    T: Encryptable<S>,
    S: Serialize,
{
    let value = value.encrypt(encryption_key)?;
    let serialized_value = serde_json::to_value(value)?;

    Ok(serialized_value)
}

pub fn deserialize_from_value<S>(value: serde_json::Value) -> Result<S, DbDeserializationError>
where
    S: DeserializeOwned,
{
    let deserialized_value = serde_json::from_value(value)?;

    Ok(deserialized_value)
}

pub fn decrypt_and_deserialize_from_value<T, S>(
    value: serde_json::Value,
    encryption_key: &EncryptionKey,
) -> Result<S, DbDeserializationError>
where
    T: Decryptable<S>,
    T: DeserializeOwned,
{
    let deserialized_value: T = serde_json::from_value(value)?;
    let value = deserialized_value.decrypt(encryption_key)?;

    Ok(value)
}
