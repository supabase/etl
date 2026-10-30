//! Canonical pipeline identity and compatibility with numeric adapters.

use std::{convert::Infallible, fmt, str::FromStr, sync::Arc};

use etl_config::shared::{HybridPipelineId, ValidationError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Numeric identity retained by adapters that do not yet support string IDs.
pub type LegacyPipelineId = u64;

/// Opaque string identity used by the ETL runtime.
///
/// Configuration representations are normalized to text at the runtime
/// boundary. Numeric adapters explicitly convert to [`LegacyPipelineId`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PipelineId(Arc<str>);

impl PipelineId {
    /// Returns the exact pipeline identity as text.
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<HybridPipelineId> for PipelineId {
    fn from(value: HybridPipelineId) -> Self {
        match value {
            HybridPipelineId::Integer(value) => value.into(),
            HybridPipelineId::String(value) => value.into(),
        }
    }
}

impl From<&HybridPipelineId> for PipelineId {
    fn from(value: &HybridPipelineId) -> Self {
        match value {
            HybridPipelineId::Integer(value) => (*value).into(),
            HybridPipelineId::String(value) => value.as_str().into(),
        }
    }
}

impl From<PipelineId> for HybridPipelineId {
    fn from(value: PipelineId) -> Self {
        Self::String(value.to_string())
    }
}

impl From<LegacyPipelineId> for PipelineId {
    fn from(value: LegacyPipelineId) -> Self {
        Self(Arc::from(value.to_string()))
    }
}

impl From<&PipelineId> for PipelineId {
    fn from(value: &PipelineId) -> Self {
        value.clone()
    }
}

impl TryFrom<&PipelineId> for LegacyPipelineId {
    type Error = ValidationError;

    fn try_from(value: &PipelineId) -> Result<Self, Self::Error> {
        value
            .as_str()
            .parse::<Self>()
            .ok()
            .filter(|id| id.to_string() == value.as_str())
            .ok_or_else(|| ValidationError::InvalidFieldValue {
                field: "pipeline.id".to_owned(),
                constraint: "must remain numeric while legacy pipeline interfaces are in use"
                    .to_owned(),
            })
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

#[cfg(test)]
mod tests {
    use etl_config::shared::HybridPipelineId;

    use crate::pipeline::{LegacyPipelineId, PipelineId};

    #[test]
    fn canonical_identity_converts_only_at_legacy_boundaries() {
        for json in ["150", "\"150\""] {
            let input: HybridPipelineId = serde_json::from_str(json).unwrap();
            let id = PipelineId::from(input);
            assert_eq!(id.as_str(), "150");
            assert_eq!(LegacyPipelineId::try_from(&id).unwrap(), 150);
            assert_eq!(serde_json::to_string(&id).unwrap(), "\"150\"");
        }
        assert!(serde_json::from_str::<PipelineId>("150").is_err());
        let id = PipelineId::from(u64::MAX);
        assert_eq!(LegacyPipelineId::try_from(&id).unwrap(), u64::MAX);
        for value in [
            "",
            "00150",
            "PIPELINE",
            "é",
            "pipeline-with-a-different-format",
            "03dg2o5teb2eucxr9ggh3uie6",
            "0000000000000000000000001",
        ] {
            let id: PipelineId = value.parse().unwrap();
            assert_eq!(id.as_str(), value);
            let json = serde_json::json!(value);
            assert_eq!(serde_json::from_value::<PipelineId>(json.clone()).unwrap(), id);
            assert_eq!(serde_json::to_value(&id).unwrap(), json);
            assert!(LegacyPipelineId::try_from(&id).is_err());
        }
    }
}
