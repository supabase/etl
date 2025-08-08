use std::string;

use aws_lc_rs::{
    aead::{AES_256_GCM, Aad, Nonce, RandomizedNonceKey},
    rand::fill,
};
use base64::{Engine, prelude::BASE64_STANDARD};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Errors that can occur during encryption operations.
#[derive(Debug, Error)]
pub enum EncryptionError {
    /// An unspecified error occurred while encrypting data.
    #[error("An unspecified error occurred while encrypting data")]
    Unspecified(#[from] aws_lc_rs::error::Unspecified),
}

/// Errors that can occur during decryption operations.
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

/// Trait for types that can be encrypted into another type.
pub trait Encrypt<T> {
    /// Encrypts `self` using the provided [`EncryptionKey`].
    fn encrypt(self, encryption_key: &EncryptionKey) -> Result<T, EncryptionError>;
}

/// Trait for types that can be decrypted into another type.
pub trait Decrypt<T> {
    /// Decrypts `self` using the provided [`EncryptionKey`].
    fn decrypt(self, encryption_key: &EncryptionKey) -> Result<T, DecryptionError>;
}

/// Holds an encryption key and its identifier.
pub struct EncryptionKey {
    /// Unique identifier for the key.
    pub id: u32,
    /// The key material used for encryption and decryption.
    pub key: RandomizedNonceKey,
}

/// Represents an encrypted value with its key ID and nonce.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedValue {
    /// Identifier of the key used for encryption.
    pub id: u32,
    /// Base64-encoded nonce used during encryption.
    pub nonce: String,
    /// Base64-encoded encrypted value.
    pub value: String,
}

/// Encrypts a string value using the provided [`EncryptionKey`].
///
/// The result is an [`EncryptedValue`] containing the key ID, base64-encoded nonce,
/// and base64-encoded ciphertext.
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

/// Decrypts an [`EncryptedValue`] using the provided [`EncryptionKey`].
///
/// Returns the original string if decryption succeeds. Fails if the key ID does not match or if
/// decoding or decryption fails.
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

/// Encrypts a byte slice using the given [`RandomizedNonceKey`].
///
/// Returns the ciphertext and the nonce used for encryption.
fn encrypt(
    plaintext: &[u8],
    key: &RandomizedNonceKey,
) -> Result<(Vec<u8>, Nonce), aws_lc_rs::error::Unspecified> {
    let mut in_out = plaintext.to_vec();
    let nonce = key.seal_in_place_append_tag(Aad::empty(), &mut in_out)?;

    Ok((in_out, nonce))
}

/// Decrypts a ciphertext using the given [`RandomizedNonceKey`] and [`Nonce`].
///
/// Returns the decrypted plaintext bytes.
fn decrypt(
    mut ciphertext: Vec<u8>,
    nonce: Nonce,
    key: &RandomizedNonceKey,
) -> Result<Vec<u8>, aws_lc_rs::error::Unspecified> {
    let plaintext = key.open_in_place(nonce, Aad::empty(), &mut ciphertext)?;

    Ok(plaintext.to_vec())
}

/// Generates a random [`RandomizedNonceKey`] of length `T` bytes for use with AES-256-GCM.
///
/// The key is filled with cryptographically secure random bytes.
///
/// # Panics
///
/// Panics if `T` does not match the required key length for the cipher.
pub fn generate_random_key<const T: usize>()
-> Result<RandomizedNonceKey, aws_lc_rs::error::Unspecified> {
    let mut key_bytes = [0u8; T];
    fill(&mut key_bytes)?;

    let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)?;

    Ok(key)
}
