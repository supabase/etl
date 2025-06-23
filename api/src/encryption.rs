use aws_lc_rs::{
    aead::{Aad, Nonce, RandomizedNonceKey, AES_256_GCM},
    rand::fill,
};
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::string;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EncryptionError {
    #[error("An unspecified error occurred while encrypting data")]
    Unspecified(#[from] aws_lc_rs::error::Unspecified),
}

#[derive(Debug, Error)]
pub enum DecryptionError {
    #[error("An unspecified error occurred while decrypting data")]
    Unspecified(#[from] aws_lc_rs::error::Unspecified),

    #[error("An error occurred while decoding BASE64 data for decryption: {0}")]
    Decode(#[from] base64::DecodeError),

    #[error("An error occurred while converting bytes to UTF-8 for decryption: {0}")]
    FromUtf8(#[from] string::FromUtf8Error),

    #[error("There was a mismatch in the key id while decrypting data (got: {0}, expected: {1})")]
    MismatchedKeyId(u32, u32),
}

pub trait Encryptable<T> {
    fn encrypt(self, encryption_key: &EncryptionKey) -> Result<T, EncryptionError>;
}

pub trait Decryptable<T> {
    fn decrypt(self, encryption_key: &EncryptionKey) -> Result<T, DecryptionError>;
}

pub struct EncryptionKey {
    pub id: u32,
    pub key: RandomizedNonceKey,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EncryptedValue {
    pub id: u32,
    pub nonce: String,
    pub value: String,
}

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

pub fn encrypt(
    plaintext: &[u8],
    key: &RandomizedNonceKey,
) -> Result<(Vec<u8>, Nonce), aws_lc_rs::error::Unspecified> {
    let mut in_out = plaintext.to_vec();
    let nonce = key.seal_in_place_append_tag(Aad::empty(), &mut in_out)?;

    Ok((in_out, nonce))
}

pub fn decrypt(
    mut ciphertext: Vec<u8>,
    nonce: Nonce,
    key: &RandomizedNonceKey,
) -> Result<Vec<u8>, aws_lc_rs::error::Unspecified> {
    let plaintext = key.open_in_place(nonce, Aad::empty(), &mut ciphertext)?;

    Ok(plaintext.to_vec())
}

/// Returns a `T` byte long random key encoded as base64
pub fn generate_random_key<const T: usize>(
) -> Result<RandomizedNonceKey, aws_lc_rs::error::Unspecified> {
    let mut key_bytes = [0u8; T];
    fill(&mut key_bytes)?;

    let key = RandomizedNonceKey::new(&AES_256_GCM, &key_bytes)?;

    Ok(key)
}
