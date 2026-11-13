//! Opaque string pipeline identity.

use std::{convert::Infallible, fmt, str::FromStr, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Opaque string identity used by the ETL runtime.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipelineId(Arc<str>);

impl PipelineId {
    /// Returns the exact pipeline identity as text.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<&PipelineId> for PipelineId {
    fn from(value: &PipelineId) -> Self {
        value.clone()
    }
}

impl From<String> for PipelineId {
    fn from(value: String) -> Self {
        Self(Arc::from(value))
    }
}

impl From<&str> for PipelineId {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

impl FromStr for PipelineId {
    type Err = Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(value.into())
    }
}

impl fmt::Display for PipelineId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for PipelineId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for PipelineId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::from)
    }
}

impl From<&String> for PipelineId {
    fn from(value: &String) -> Self {
        value.as_str().into()
    }
}

impl From<PipelineId> for String {
    fn from(value: PipelineId) -> Self {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::PipelineId;

    #[test]
    fn string_identity_is_preserved() {
        for value in ["150", "00150", "03dg2o5teb2eucxr9ggh3uie6", "pipeline-example"] {
            let id: PipelineId = value.parse().unwrap();
            assert_eq!(id.as_str(), value);
            let json = serde_json::json!(value);
            assert_eq!(serde_json::from_value::<PipelineId>(json.clone()).unwrap(), id);
            assert_eq!(serde_json::to_value(&id).unwrap(), json);
        }

        assert!(serde_json::from_str::<PipelineId>("150").is_err());
    }
}
