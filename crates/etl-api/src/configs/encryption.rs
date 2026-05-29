use std::{collections::BTreeMap, string};

use aws_lc_rs::{
    aead::{AES_256_GCM, Aad, Nonce, RandomizedNonceKey},
    rand::fill,
};
use base64::{Engine, prelude::BASE64_STANDARD};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that occur during data encryption.
#[derive(Debug, Error)]
pub enum EncryptionError {
    /// An unspecified error occurred while encrypting data.
    #[error("An unspecified error occurred while encrypting data")]
    Unspecified(#[from] aws_lc_rs::error::Unspecified),
}

/// Errors that occur during data decryption.
#[derive(Debug, Error)]
pub enum DecryptionError {
    /// An unspecified error occurred while decrypting data.
    #[error("An unspecified error occurred while decrypting data")]
    Unspecified(#[from] aws_lc_rs::error::Unspecified),

    /// Failed to decode base64 data during decryption.
    #[error("An error occurred while decoding BASE64 data for decryption: {0}")]
    Decode(#[from] base64::DecodeError),

    /// Failed to convert decrypted bytes to UTF-8 string.
    #[error("An error occurred while converting bytes to UTF-8 for decryption: {0}")]
    FromUtf8(#[from] string::FromUtf8Error),

    /// No configured encryption key matched the encrypted value's key ID.
    #[error("Unknown encryption key id while decrypting data: {0}")]
    UnknownKeyId(u32),
}

/// Errors that occur while building an encryption keyring.
#[derive(Debug, Error)]
pub enum EncryptionKeyringError {
    /// The keyring did not contain any keys.
    #[error("At least one encryption key must be configured")]
    Empty,

    /// Multiple keys used the same identifier.
    #[error("Duplicate encryption key id in configuration: {0}")]
    DuplicateKeyId(u32),
}

/// Trait for types that can be encrypted.
pub trait Encrypt<T> {
    /// Encrypts this value using the provided encryption keyring.
    fn encrypt(self, encryption_keyring: &EncryptionKeyring) -> Result<T, EncryptionError>;
}

/// Trait for types that can be decrypted.
pub trait Decrypt<T> {
    /// Decrypts this value using the provided encryption keyring.
    fn decrypt(self, encryption_keyring: &EncryptionKeyring) -> Result<T, DecryptionError>;
}

/// Encryption key with identifier for key management.
pub struct EncryptionKey {
    /// Unique identifier for the key.
    pub id: u32,
    /// The key material used for encryption and decryption.
    pub key: RandomizedNonceKey,
}

/// Collection of encryption keys used for reads and writes.
pub struct EncryptionKeyring {
    /// Keys indexed by their stored key identifier.
    keys: BTreeMap<u32, RandomizedNonceKey>,
}

impl EncryptionKeyring {
    /// Creates a keyring from one or more encryption keys.
    pub fn new(keys: Vec<EncryptionKey>) -> Result<Self, EncryptionKeyringError> {
        if keys.is_empty() {
            return Err(EncryptionKeyringError::Empty);
        }

        let mut keyring_keys = BTreeMap::new();
        for key in keys {
            if keyring_keys.insert(key.id, key.key).is_some() {
                return Err(EncryptionKeyringError::DuplicateKeyId(key.id));
            }
        }

        Ok(Self { keys: keyring_keys })
    }

    /// Returns the key id that will be used for new encrypted values.
    pub fn latest_key_id(&self) -> u32 {
        self.keys
            .last_key_value()
            .map(|(id, _)| *id)
            .expect("encryption keyring should contain at least one key")
    }

    /// Returns the key used for new encrypted values.
    fn latest_key(&self) -> (u32, &RandomizedNonceKey) {
        self.keys
            .last_key_value()
            .map(|(id, key)| (*id, key))
            .expect("encryption keyring should contain at least one key")
    }

    /// Returns the key matching a stored encrypted value.
    fn key_for_decryption(&self, key_id: u32) -> Result<&RandomizedNonceKey, DecryptionError> {
        self.keys.get(&key_id).ok_or(DecryptionError::UnknownKeyId(key_id))
    }
}

impl From<EncryptionKey> for EncryptionKeyring {
    fn from(key: EncryptionKey) -> Self {
        Self::new(vec![key]).expect("one-key encryption keyring should be valid")
    }
}

/// Encrypted value with metadata for decryption.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedValue {
    /// Identifier of the key used for encryption.
    pub id: u32,
    /// Base64-encoded nonce used during encryption.
    pub nonce: String,
    /// Base64-encoded encrypted value.
    pub value: String,
}

/// Encrypts a string using AES-256-GCM encryption.
///
/// Returns an [`EncryptedValue`] containing the key ID, nonce, and encrypted
/// data, all base64-encoded for safe storage and transmission.
pub fn encrypt_text(
    value: String,
    encryption_keyring: &EncryptionKeyring,
) -> Result<EncryptedValue, EncryptionError> {
    let (key_id, key) = encryption_keyring.latest_key();
    let (encrypted_password, nonce) = encrypt(value.as_bytes(), key)?;
    let encoded_encrypted_password = BASE64_STANDARD.encode(encrypted_password);
    let encoded_nonce = BASE64_STANDARD.encode(nonce.as_ref());

    Ok(EncryptedValue { id: key_id, nonce: encoded_nonce, value: encoded_encrypted_password })
}

/// Decrypts an [`EncryptedValue`] back to the original string.
///
/// Selects the configured key matching the stored key ID before decryption.
/// Returns the original plaintext string if decryption succeeds.
pub fn decrypt_text(
    encrypted_value: EncryptedValue,
    encryption_keyring: &EncryptionKeyring,
) -> Result<String, DecryptionError> {
    let encrypted_value_bytes = BASE64_STANDARD.decode(encrypted_value.value)?;
    let nonce = Nonce::try_assume_unique_for_key(&BASE64_STANDARD.decode(encrypted_value.nonce)?)?;
    let key = encryption_keyring.key_for_decryption(encrypted_value.id)?;

    let decrypted_value_bytes = decrypt(encrypted_value_bytes, nonce, key)?;

    let decrypted_value = String::from_utf8(decrypted_value_bytes)?;

    Ok(decrypted_value)
}

/// Encrypts bytes using AES-256-GCM with a randomized nonce.
///
/// Returns the encrypted data and the nonce used for encryption.
fn encrypt(
    plaintext: &[u8],
    key: &RandomizedNonceKey,
) -> Result<(Vec<u8>, Nonce), aws_lc_rs::error::Unspecified> {
    let mut in_out = plaintext.to_vec();
    let nonce = key.seal_in_place_append_tag(Aad::empty(), &mut in_out)?;

    Ok((in_out, nonce))
}

/// Decrypts AES-256-GCM encrypted data using the key and nonce.
///
/// Returns the original plaintext bytes.
fn decrypt(
    mut ciphertext: Vec<u8>,
    nonce: Nonce,
    key: &RandomizedNonceKey,
) -> Result<Vec<u8>, aws_lc_rs::error::Unspecified> {
    let plaintext = key.open_in_place(nonce, Aad::empty(), &mut ciphertext)?;

    Ok(plaintext.to_vec())
}

/// Generates a cryptographically secure random encryption key.
///
/// Creates a new [`RandomizedNonceKey`] for AES-256-GCM encryption using
/// secure random bytes.
///
/// # Panics
/// Panics if `T` doesn't match the required key length for AES-256-GCM.
pub fn generate_random_key<const T: usize>()
-> Result<RandomizedNonceKey, aws_lc_rs::error::Unspecified> {
    let mut key_bytes = [0u8; T];
    fill(&mut key_bytes)?;

    let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)?;

    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key(id: u32, byte: u8) -> EncryptionKey {
        let key_bytes = [byte; 32];
        let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes).unwrap();

        EncryptionKey { id, key }
    }

    #[test]
    fn encrypt_text_uses_highest_configured_key_id() {
        let keyring =
            EncryptionKeyring::new(vec![test_key(1, 1), test_key(3, 3), test_key(2, 2)]).unwrap();

        let encrypted = encrypt_text("secret".to_owned(), &keyring).unwrap();
        let decrypted = decrypt_text(encrypted.clone(), &keyring).unwrap();

        assert_eq!(encrypted.id, 3);
        assert_eq!(decrypted, "secret");
    }

    #[test]
    fn decrypt_text_uses_stored_key_id() {
        let old_keyring = EncryptionKeyring::from(test_key(1, 1));
        let encrypted = encrypt_text("secret".to_owned(), &old_keyring).unwrap();
        let new_keyring = EncryptionKeyring::new(vec![test_key(1, 1), test_key(2, 2)]).unwrap();

        let decrypted = decrypt_text(encrypted, &new_keyring).unwrap();

        assert_eq!(decrypted, "secret");
    }

    #[test]
    fn decrypt_text_rejects_unknown_key_id() {
        let old_keyring = EncryptionKeyring::from(test_key(1, 1));
        let encrypted = encrypt_text("secret".to_owned(), &old_keyring).unwrap();
        let new_keyring = EncryptionKeyring::from(test_key(2, 2));

        let error = decrypt_text(encrypted, &new_keyring).unwrap_err();

        assert!(matches!(error, DecryptionError::UnknownKeyId(1)));
    }

    #[test]
    fn keyring_rejects_duplicate_key_ids() {
        let Err(error) = EncryptionKeyring::new(vec![test_key(1, 1), test_key(1, 2)]) else {
            panic!("duplicate key id should fail");
        };

        assert!(matches!(error, EncryptionKeyringError::DuplicateKeyId(1)));
    }
}
