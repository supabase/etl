use std::string;

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

    /// The key ID in the encrypted data did not match the expected key ID.
    #[error("There was a mismatch in the key id while decrypting data (got: {0}, expected: {1})")]
    MismatchedKeyId(u32, u32),
}

/// Trait for types that can be encrypted.
pub trait Encrypt<T> {
    /// Encrypts this value using the provided encryption key.
    fn encrypt(self, encryption_key: &EncryptionKey) -> Result<T, EncryptionError>;
}

/// Trait for types that can be decrypted.
pub trait Decrypt<T> {
    /// Decrypts this value using the provided encryption key.
    fn decrypt(self, encryption_key: &EncryptionKey) -> Result<T, DecryptionError>;
}

/// Encryption key with identifier for key management.
pub struct EncryptionKey {
    /// Unique identifier for the key.
    pub id: u32,
    /// The key material used for encryption and decryption.
    pub key: RandomizedNonceKey,
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
/// Returns an [`EncryptedValue`] containing the key ID, nonce, and encrypted data,
/// all base64-encoded for safe storage and transmission.
pub fn encrypt_text(
    value: String,
    encryption_key: &EncryptionKey,
) -> Result<EncryptedValue, EncryptionError> {
    let (encrypted_password, nonce) = encrypt(value.as_bytes(), &encryption_key.key)?;
    let encoded_encrypted_password = BASE64_STANDARD.encode(encrypted_password);
    let encoded_nonce = BASE64_STANDARD.encode(nonce.as_ref());

    Ok(EncryptedValue {
        id: encryption_key.id,
        nonce: encoded_nonce,
        value: encoded_encrypted_password,
    })
}

/// Decrypts an [`EncryptedValue`] back to the original string.
///
/// Validates the key ID matches before attempting decryption. Returns the original
/// plaintext string if decryption succeeds.
pub fn decrypt_text(
    encrypted_value: EncryptedValue,
    encryption_key: &EncryptionKey,
) -> Result<String, DecryptionError> {
    if encrypted_value.id != encryption_key.id {
        return Err(DecryptionError::MismatchedKeyId(
            encrypted_value.id,
            encryption_key.id,
        ));
    }

    let encrypted_value_bytes = BASE64_STANDARD.decode(encrypted_value.value)?;
    let nonce = Nonce::try_assume_unique_for_key(&BASE64_STANDARD.decode(encrypted_value.nonce)?)?;

    let decrypted_value_bytes = decrypt(encrypted_value_bytes, nonce, &encryption_key.key)?;

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
