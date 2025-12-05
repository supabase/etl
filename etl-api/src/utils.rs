use rand::Rng;
use serde::{Deserialize, Deserializer};

/// Deserializes a string and trims leading and trailing whitespace.
pub fn trim_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.trim().to_string())
}

/// Deserializes an optional string and trims leading and trailing whitespace if present.
pub fn trim_option_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt.map(|s| s.trim().to_string()))
}

/// Generates a random alphabetic string of length `len`.
pub fn generate_random_alpha_str(len: usize) -> String {
    let chars = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];
    let mut rng = rand::rng();
    (0..len)
        .map(|_| chars[rng.random_range(0..chars.len())])
        .collect()
}

/// Parses a Docker image reference to extract the tag to be used as a version name.
///
/// Expected formats: `HOST[:PORT]/NAMESPACE/REPOSITORY[:TAG][@DIGEST]`.
/// - If a tag is present, returns it (ignoring any trailing digest part).
/// - If no tag is present and also no digest, defaults to `latest`.
/// - If parsing fails or only a digest is present, returns `unavailable`.
pub fn parse_docker_image_tag(image: &str) -> String {
    // Work on the last path segment only
    let last_slash = image.rfind('/').map(|i| i + 1).unwrap_or(0);
    let segment = &image[last_slash..];

    // Identify optional digest marker within the segment
    let at_pos = segment.find('@');

    // Search for ':' in the segment, but if a digest '@' exists, ignore ':' that occur after it
    let colon_pos_in_segment = match at_pos {
        Some(at_idx) => segment[..at_idx].find(':'),
        None => segment.find(':'),
    };

    if let Some(col_idx) = colon_pos_in_segment {
        // Extract tag between ':' and optional '@'
        let after_colon = &segment[col_idx + 1..];
        let tag = match at_pos {
            Some(at_idx) => &segment[col_idx + 1..at_idx],
            None => after_colon,
        };

        if tag.is_empty() {
            return "unavailable".to_string();
        }

        return tag.to_string();
    }

    // No tag in the segment. If there's a digest in the segment, we can't infer a tag.
    if at_pos.is_some() {
        return "unavailable".to_string();
    }

    // No tag and no digest in the segment -> default docker tag is latest
    "latest".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::parse_docker_image_tag;

    #[test]
    fn parse_with_tag() {
        assert_eq!(parse_docker_image_tag("supabase/replicator:1.2.3"), "1.2.3");
        assert_eq!(
            parse_docker_image_tag("example.com:5000/team/my-app:2.0"),
            "2.0"
        );
        assert_eq!(
            parse_docker_image_tag("ghcr.io/dockersamples/example-app:pr-311"),
            "pr-311"
        );
    }

    #[test]
    fn parse_with_tag_and_digest() {
        assert_eq!(
            parse_docker_image_tag("example.com:5000/team/my-app:2.0@sha256:abcdef0123456789"),
            "2.0"
        );
    }

    #[test]
    fn parse_without_tag_defaults_to_latest() {
        assert_eq!(parse_docker_image_tag("alpine"), "latest");
        assert_eq!(parse_docker_image_tag("library/alpine"), "latest");
        assert_eq!(parse_docker_image_tag("docker.io/library/alpine"), "latest");
    }

    #[test]
    fn parse_with_only_digest_unavailable() {
        assert_eq!(
            parse_docker_image_tag("repo/name@sha256:abcdef0123456789"),
            "unavailable"
        );
    }

    #[test]
    fn test_trim_string_with_leading_and_trailing_whitespace() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(deserialize_with = "trim_string")]
            value: String,
        }

        let json = r#"{"value": "  hello world  "}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, "hello world");
    }

    #[test]
    fn test_trim_string_without_whitespace() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(deserialize_with = "trim_string")]
            value: String,
        }

        let json = r#"{"value": "no_whitespace"}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, "no_whitespace");
    }

    #[test]
    fn test_trim_string_with_tabs_and_newlines() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(deserialize_with = "trim_string")]
            value: String,
        }

        let json = r#"{"value": "\t\n  trimmed  \n\t"}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, "trimmed");
    }

    #[test]
    fn test_trim_string_empty_string() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(deserialize_with = "trim_string")]
            value: String,
        }

        let json = r#"{"value": ""}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, "");
    }

    #[test]
    fn test_trim_string_only_whitespace_becomes_empty() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(deserialize_with = "trim_string")]
            value: String,
        }

        let json = r#"{"value": "   \t\n   "}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, "");
    }

    #[test]
    fn test_trim_string_preserves_internal_whitespace() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(deserialize_with = "trim_string")]
            value: String,
        }

        let json = r#"{"value": "  hello   world  "}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, "hello   world");
    }

    #[test]
    fn test_trim_string_leading_only() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(deserialize_with = "trim_string")]
            value: String,
        }

        let json = r#"{"value": "   leading"}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, "leading");
    }

    #[test]
    fn test_trim_string_trailing_only() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(deserialize_with = "trim_string")]
            value: String,
        }

        let json = r#"{"value": "trailing   "}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, "trailing");
    }

    #[test]
    fn test_trim_option_string_with_whitespace() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(default, deserialize_with = "trim_option_string")]
            value: Option<String>,
        }

        let json = r#"{"value": "  hello world  "}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, Some("hello world".to_string()));
    }

    #[test]
    fn test_trim_option_string_with_null() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(default, deserialize_with = "trim_option_string")]
            value: Option<String>,
        }

        let json = r#"{"value": null}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, None);
    }

    #[test]
    fn test_trim_option_string_with_missing_field() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(default, deserialize_with = "trim_option_string")]
            value: Option<String>,
        }

        let json = r#"{}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, None);
    }

    #[test]
    fn test_trim_option_string_with_empty_string() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(default, deserialize_with = "trim_option_string")]
            value: Option<String>,
        }

        let json = r#"{"value": ""}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, Some("".to_string()));
    }

    #[test]
    fn test_trim_option_string_whitespace_only_becomes_empty() {
        #[derive(Deserialize)]
        struct TestStruct {
            #[serde(default, deserialize_with = "trim_option_string")]
            value: Option<String>,
        }

        let json = r#"{"value": "   "}"#;
        let result: TestStruct = serde_json::from_str(json).unwrap();
        assert_eq!(result.value, Some("".to_string()));
    }
}
