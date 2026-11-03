//! Configuration compatibility for numeric and string pipeline identities.

use std::{convert::Infallible, fmt, str::FromStr};

use serde::{Deserialize, Serialize};

/// Pipeline identity accepted by configuration while numeric callers remain.
///
/// Serialization preserves the integer or string representation supplied by
/// the caller. String identities retain their exact contents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum HybridPipelineId {
    /// Numeric identity supplied by an existing caller.
    Integer(u64),
    /// String identity supplied by an upgraded caller.
    String(String),
}

impl FromStr for HybridPipelineId {
    type Err = Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(value.into())
    }
}

impl From<String> for HybridPipelineId {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for HybridPipelineId {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<u64> for HybridPipelineId {
    fn from(value: u64) -> Self {
        Self::Integer(value)
    }
}

impl fmt::Display for HybridPipelineId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(value) => value.fmt(formatter),
            Self::String(value) => formatter.write_str(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::shared::{HybridPipelineId, PipelineConfig, PipelineConfigWithoutSecrets};

    #[test]
    fn configuration_forms_preserve_identity() {
        for json in ["150", "\"150\""] {
            let id: HybridPipelineId = serde_json::from_str(json).unwrap();
            assert_eq!(id.to_string(), "150");
            assert_eq!(serde_json::to_string(&id).unwrap(), json);
            let config: PipelineConfig = serde_json::from_value(serde_json::json!({
                "id": serde_json::from_str::<serde_json::Value>(json).unwrap(),
                "publication_name": "example_publication",
                "pg_connection": {
                    "host": "127.0.0.1", "port": 5432, "name": "postgres",
                    "username": "postgres", "password": null,
                    "tls": { "enabled": false, "trusted_root_certs": "" }
                }
            }))
            .unwrap();
            let public = PipelineConfigWithoutSecrets::from(config);
            assert_eq!(
                serde_json::to_value(public).unwrap()["id"],
                serde_json::from_str::<serde_json::Value>(json).unwrap()
            );
        }
        let json = serde_json::to_string(&u64::MAX).unwrap();
        assert_eq!(serde_json::from_str::<HybridPipelineId>(&json).unwrap(), u64::MAX.into());
        for value in [
            "",
            "-1",
            "00150",
            "150_1",
            "PIPELINE",
            "é",
            "150.0",
            "pipeline-with-a-different-format",
            "03dg2o5teb2eucxr9ggh3uie6",
        ] {
            let id: HybridPipelineId = value.parse().unwrap();
            assert_eq!(id.to_string(), value);
            let json = serde_json::json!(value);
            assert_eq!(serde_json::from_value::<HybridPipelineId>(json.clone()).unwrap(), id);
            assert_eq!(serde_json::to_value(id).unwrap(), json);
        }
    }

    #[test]
    fn unsupported_configuration_types_are_rejected() {
        for json in ["-1", "1.5", "true", "null", "[]", "{}", "18446744073709551616"] {
            assert!(serde_json::from_str::<HybridPipelineId>(json).is_err());
        }
    }
}
