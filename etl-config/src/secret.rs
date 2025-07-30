use secrecy::{ExposeSecret, Secret};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::Deref;

/// Wrapper around [`Secret<String>`] that implements [`Serialize`] and [`Deserialize`].
#[derive(Clone)]
pub struct SerializableSecretString(Secret<String>);

impl Deref for SerializableSecretString {
    type Target = Secret<String>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for SerializableSecretString {
    fn from(value: String) -> Self {
        Self(Secret::new(value))
    }
}

impl From<Secret<String>> for SerializableSecretString {
    fn from(value: Secret<String>) -> Self {
        Self(value)
    }
}

impl From<SerializableSecretString> for Secret<String> {
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
        let secret = Secret::new(string);

        Ok(Self(secret))
    }
}

impl fmt::Debug for SerializableSecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
