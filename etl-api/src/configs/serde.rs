use serde::Serialize;
use thiserror::Error;

use crate::configs::encryption::{
    Decrypt, DecryptionError, Encrypt, EncryptionError, EncryptionKey,
};
use crate::configs::store::Store;

/// Errors that can occur during serialization or encryption for database storage.
#[derive(Debug, Error)]
pub enum DbSerializationError {
    /// Error occurred while serializing data to JSON for the database.
    #[error("Error while serializing data to the db: {0}")]
    Serde(#[from] serde_json::Error),

    /// Error occurred while encrypting data for the database representation.
    #[error("An error occurred while encrypting data for the db representation: {0}")]
    Encryption(#[from] EncryptionError),
}

/// Errors that can occur during deserialization or decryption from database values.
#[derive(Debug, Error)]
pub enum DbDeserializationError {
    /// Error occurred while deserializing data from JSON from the database.
    #[error("Error while deserializing data from the db: {0}")]
    Serde(#[from] serde_json::Error),

    /// Error occurred while decrypting data from the database representation.
    #[error("An error occurred while decrypting data from the db representation: {0}")]
    Decryption(#[from] DecryptionError),
}

/// Serializes a value to a [`serde_json::Value`] for database storage.
///
/// Returns an error if serialization fails.
pub fn serialize<S>(value: S) -> Result<serde_json::Value, DbSerializationError>
where
    S: Store,
{
    let serialized_value = serde_json::to_value(value)?;

    Ok(serialized_value)
}

/// Encrypts a value and serializes it to a [`serde_json::Value`] for database storage.
///
/// The value is first encrypted using the provided [`EncryptionKey`], then serialized.
/// Returns an error if encryption or serialization fails.
pub fn encrypt_and_serialize<T, S>(
    value: T,
    encryption_key: &EncryptionKey,
) -> Result<serde_json::Value, DbSerializationError>
where
    T: Encrypt<S>,
    S: Serialize,
{
    let value = value.encrypt(encryption_key)?;
    let serialized_value = serde_json::to_value(value)?;

    Ok(serialized_value)
}

/// Deserializes a [`serde_json::Value`] into a value of type `S`.
///
/// Returns an error if deserialization fails.
pub fn deserialize_from_value<S>(value: serde_json::Value) -> Result<S, DbDeserializationError>
where
    S: Store,
{
    let deserialized_value = serde_json::from_value(value)?;

    Ok(deserialized_value)
}

/// Deserializes and decrypts a [`serde_json::Value`] into a value of type `S`.
///
/// The value is first deserialized into a type implementing [`Decrypt`], then decrypted using
/// the provided [`EncryptionKey`].
///
/// Returns an error if deserialization or decryption fails.
pub fn decrypt_and_deserialize_from_value<T, S>(
    value: serde_json::Value,
    encryption_key: &EncryptionKey,
) -> Result<S, DbDeserializationError>
where
    T: Decrypt<S>,
    T: Store,
{
    let deserialized_value: T = serde_json::from_value(value)?;
    let value = deserialized_value.decrypt(encryption_key)?;

    Ok(value)
}
