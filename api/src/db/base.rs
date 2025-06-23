use crate::encryption::{DecryptionError, EncryptionError, EncryptionKey};
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

// TODO: remove and use encryption nd decryption error types.
#[derive(Debug, Error)]
pub enum ToDbError {
    #[error("An encryption error happened while converting to the db variant: {0}")]
    Encryption(#[from] EncryptionError),
}

#[derive(Debug, Error)]
pub enum ToMemoryError {
    #[error("A decryption error happened while converting to the memory variant: {0}")]
    Decryption(#[from] DecryptionError),
}

pub trait Encryptable<T> {
    fn encrypt(self, encryption_key: &EncryptionKey) -> Result<T, ToDbError>;
}

pub trait Decryptable<T> {
    fn decrypt(self, encryption_key: &EncryptionKey) -> Result<T, ToMemoryError>;
}

#[derive(Debug, Error)]
pub enum DbSerializationError {
    #[error("Error while serializing data to the db: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("An error occurred while converting to the db representation: {0}")]
    ToDb(#[from] ToDbError),
}

#[derive(Debug, Error)]
pub enum DbDeserializationError {
    #[error("Error while deserializing data from the db: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("An error occurred while converting to the memory representation: {0}")]
    ToMemory(#[from] ToMemoryError),
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

pub fn deserialize_from_str<S>(value: &str) -> Result<S, DbDeserializationError>
where
    S: DeserializeOwned,
{
    let deserialized_value = serde_json::from_str(value)?;

    Ok(deserialized_value)
}

pub fn decrypt_and_deserialize_from_str<T, S>(
    value: &str,
    encryption_key: &EncryptionKey,
) -> Result<S, DbDeserializationError>
where
    T: Decryptable<S>,
    T: DeserializeOwned,
{
    let deserialized_value: T = serde_json::from_str(value)?;
    let value = deserialized_value.decrypt(encryption_key)?;

    Ok(value)
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
