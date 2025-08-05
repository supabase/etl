use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::Deref;
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Wrapper around [`Secret<String>`] that implements [`Serialize`] and [`Deserialize`].
#[derive(Clone, Debug)]
#[cfg_attr(feature = "utoipa", derive(ToSchema), schema(value_type = String, example = "secret123"))]
pub struct SerializableSecretString(SecretString);

impl Deref for SerializableSecretString {
    type Target = SecretString;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for SerializableSecretString {
    fn from(value: String) -> Self {
        Self(value.into())
    }
}

impl From<SecretString> for SerializableSecretString {
    fn from(value: SecretString) -> Self {
        Self(value)
    }
}

impl From<SerializableSecretString> for SecretString {
    fn from(value: SerializableSecretString) -> Self {
        value.0
    }
}

impl Serialize for SerializableSecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.0.expose_secret())
    }
}

impl<'de> Deserialize<'de> for SerializableSecretString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        Ok(Self(string.into()))
    }
}
