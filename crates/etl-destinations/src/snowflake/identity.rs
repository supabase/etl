//! Temporary compatibility with numeric Snowflake ownership and retry
//! identities.

use serde::Serialize;

/// Identity encoding understood by existing Snowflake resources.
#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum LegacyCompatiblePipelineId<'a> {
    /// Preserve the numeric encoding used by existing pipelines.
    Integer(u64),
    /// Preserve string identities without numeric normalization.
    String(&'a str),
}

/// Preserves legacy Snowflake identities. Remove this compatibility layer once
/// numeric ownership markers and in-flight truncate attempts no longer need to
/// be recognized.
///
/// Only canonical decimal strings use the old encoding, so distinct identities
/// such as `"150"` and `"00150"` cannot become aliases.
pub(super) fn legacy_compatible_pipeline_id(pipeline_id: &str) -> LegacyCompatiblePipelineId<'_> {
    match pipeline_id.parse::<u64>() {
        Ok(id) if id.to_string() == pipeline_id => LegacyCompatiblePipelineId::Integer(id),
        _ => LegacyCompatiblePipelineId::String(pipeline_id),
    }
}

#[cfg(test)]
mod tests {
    use crate::snowflake::identity::legacy_compatible_pipeline_id;

    #[test]
    fn compatibility_preserves_numeric_encoding_without_aliasing_strings() {
        for (id, expected) in [
            ("150", serde_json::json!(150)),
            ("18446744073709551615", serde_json::json!(u64::MAX)),
            ("00150", serde_json::json!("00150")),
            ("18446744073709551616", serde_json::json!("18446744073709551616")),
            ("03dg2o5teb2eucxr9ggh3uie6", serde_json::json!("03dg2o5teb2eucxr9ggh3uie6")),
        ] {
            assert_eq!(serde_json::to_value(legacy_compatible_pipeline_id(id)).unwrap(), expected);
        }
    }
}
