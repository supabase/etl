use etl_config::SerializableSecretString;
use rand::Rng;
use serde::{Deserialize, Deserializer, de::Error as _};
use url::Url;

/// Deserializes a string and trims leading and trailing whitespace.
pub fn trim_string<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s.trim().to_owned())
}

/// Deserializes an optional string and trims leading and trailing whitespace if
/// present.
pub fn trim_option_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt.map(|s| s.trim().to_owned()))
}

/// Deserializes an optional secret string and trims leading and trailing
/// whitespace if present.
pub fn trim_option_secret_string<'de, D>(
    deserializer: D,
) -> Result<Option<SerializableSecretString>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    Ok(opt.map(|s| SerializableSecretString::from(s.trim().to_owned())))
}

/// Deserializes an HTTP(S) URL string, trimming whitespace.
pub fn trim_http_url<'de, D>(deserializer: D) -> Result<Url, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    let url = Url::parse(s.trim()).map_err(D::Error::custom)?;

    match url.scheme() {
        "http" | "https" => Ok(url),
        scheme => Err(D::Error::custom(format!("url must use http or https scheme, got {scheme}"))),
    }
}

/// Deserializes and trims a Snowflake account identifier.
///
/// Values that are not strict account identifiers are rejected.
///
/// The account id is interpolated into the Snowflake account URL, so rejecting
/// non-identifier characters here prevents host takeover (SSRF) and surfaces a
/// clean, field-specific error at the API boundary.
pub fn trim_snowflake_account_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?.trim().to_owned();
    etl_config::shared::validate_snowflake_account_id(&s).map_err(D::Error::custom)?;
    Ok(s)
}

/// Deserializes and trims a Supabase project ref.
///
/// Values that are not strict project refs are rejected.
/// The project ref is interpolated into the Iceberg catalog URL.
pub fn trim_supabase_project_ref<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?.trim().to_owned();
    etl_config::shared::validate_supabase_project_ref(&s).map_err(D::Error::custom)?;
    Ok(s)
}

/// Generates a random alphabetic string of length `len`.
pub fn generate_random_alpha_str(len: usize) -> String {
    let chars = [
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r',
        's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
    ];
    let mut rng = rand::rng();
    (0..len).map(|_| chars[rng.random_range(0..chars.len())]).collect()
}

/// Parses a Docker image reference to extract the tag to be used as a version
/// name.
///
/// Expected formats: `HOST[:PORT]/NAMESPACE/REPOSITORY[:TAG][@DIGEST]`.
/// - If a tag is present, returns it (ignoring any trailing digest part).
/// - If no tag is present and also no digest, defaults to `latest`.
/// - If parsing fails or only a digest is present, returns `unavailable`.
pub fn parse_docker_image_tag(image: &str) -> String {
    // Work on the last path segment only
    let last_slash = image.rfind('/').map_or(0, |i| i + 1);
    let segment = &image[last_slash..];

    // Identify optional digest marker within the segment
    let at_pos = segment.find('@');

    // Search for ':' in the segment, but if a digest '@' exists, ignore ':' that
    // occur after it
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
            return "unavailable".to_owned();
        }

        return tag.to_owned();
    }

    // No tag in the segment. If there's a digest in the segment, we can't infer a
    // tag.
    if at_pos.is_some() {
        return "unavailable".to_owned();
    }

    // No tag and no digest in the segment -> default docker tag is latest
    "latest".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::parse_docker_image_tag;

    #[test]
    fn docker_image_tag_parsing() {
        let cases: &[(&str, &str)] = &[
            // With explicit tag.
            ("supabase/replicator:1.2.3", "1.2.3"),
            ("example.com:5000/team/my-app:2.0", "2.0"),
            ("ghcr.io/dockersamples/example-app:pr-311", "pr-311"),
            // Tag with digest -- tag wins.
            ("example.com:5000/team/my-app:2.0@sha256:abcdef0123456789", "2.0"),
            // No tag defaults to "latest".
            ("alpine", "latest"),
            ("library/alpine", "latest"),
            ("docker.io/library/alpine", "latest"),
            // Digest only -- unavailable.
            ("repo/name@sha256:abcdef0123456789", "unavailable"),
        ];

        for (input, expected) in cases {
            assert_eq!(parse_docker_image_tag(input), *expected, "image {input:?}");
        }
    }

    #[test]
    fn trim_string_deserializer() {
        #[derive(Deserialize)]
        struct T {
            #[serde(deserialize_with = "trim_string")]
            value: String,
        }

        let cases: &[(&str, &str)] = &[
            (r#"{"value": "  hello world  "}"#, "hello world"),
            (r#"{"value": "no_whitespace"}"#, "no_whitespace"),
            (r#"{"value": "\t\n  trimmed  \n\t"}"#, "trimmed"),
            (r#"{"value": ""}"#, ""),
            (r#"{"value": "   \t\n   "}"#, ""),
            (r#"{"value": "  hello   world  "}"#, "hello   world"),
            (r#"{"value": "   leading"}"#, "leading"),
            (r#"{"value": "trailing   "}"#, "trailing"),
        ];

        for (json, expected) in cases {
            let result: T = serde_json::from_str(json).unwrap();
            assert_eq!(result.value, *expected, "json {json}");
        }
    }

    #[test]
    fn trim_option_string_deserializer() {
        #[derive(Deserialize)]
        struct T {
            #[serde(default, deserialize_with = "trim_option_string")]
            value: Option<String>,
        }

        let cases: &[(&str, Option<&str>)] = &[
            (r#"{"value": "  hello world  "}"#, Some("hello world")),
            (r#"{"value": null}"#, None),
            (r#"{}"#, None),
            (r#"{"value": ""}"#, Some("")),
            (r#"{"value": "   "}"#, Some("")),
        ];

        for (json, expected) in cases {
            let result: T = serde_json::from_str(json).unwrap();
            assert_eq!(result.value.as_deref(), *expected, "json {json}");
        }
    }

    #[test]
    fn trim_http_url_deserializer() {
        #[derive(Debug, Deserialize)]
        struct T {
            #[serde(rename = "value", deserialize_with = "trim_http_url")]
            url: Url,
        }

        // (input, Some(parsed_url) for valid, None for rejection)
        let cases: &[(&str, Option<&str>)] = &[
            ("  https://example.com:8443/path  ", Some("https://example.com:8443/path")),
            ("ftp://example.com/data", None),
        ];

        for (input, expected) in cases {
            let json = format!(r#"{{"value": "{input}"}}"#);
            match expected {
                Some(url) => {
                    let result: T = serde_json::from_str(&json).unwrap();
                    assert_eq!(result.url.as_str(), *url, "input {input:?}");
                }
                None => {
                    assert!(serde_json::from_str::<T>(&json).is_err(), "should reject {input:?}");
                }
            }
        }
    }

    #[test]
    fn trim_snowflake_account_id_deserializer() {
        #[derive(Debug, Deserialize)]
        struct T {
            #[serde(rename = "value", deserialize_with = "trim_snowflake_account_id")]
            value: String,
        }

        // (input, Some(trimmed) for valid, None for rejection)
        let cases: &[(&str, Option<&str>)] = &[
            ("  myorg-myaccount  ", Some("myorg-myaccount")),
            ("org-account", Some("org-account")),
            ("127.0.0.1:8443/x", None),
            ("attacker.example/foo", None),
            ("169.254.169.254#", None),
        ];

        for (input, expected) in cases {
            let json = format!(r#"{{"value": "{input}"}}"#);
            match expected {
                Some(val) => {
                    let result: T = serde_json::from_str(&json).unwrap();
                    assert_eq!(result.value, *val, "input {input:?}");
                }
                None => {
                    assert!(serde_json::from_str::<T>(&json).is_err(), "should reject {input:?}");
                }
            }
        }
    }

    #[test]
    fn trim_supabase_project_ref_deserializer() {
        #[derive(Debug, Deserialize)]
        struct T {
            #[serde(rename = "value", deserialize_with = "trim_supabase_project_ref")]
            value: String,
        }

        // (input, Some(trimmed) for valid, None for rejection)
        let cases: &[(&str, Option<&str>)] = &[
            ("  abcdefghijklmnopqrst  ", Some("abcdefghijklmnopqrst")),
            ("attacker.example/foo", None),
            ("169.254.169.254#", None),
            ("tooshort", None),
        ];

        for (input, expected) in cases {
            let json = format!(r#"{{"value": "{input}"}}"#);
            match expected {
                Some(val) => {
                    let result: T = serde_json::from_str(&json).unwrap();
                    assert_eq!(result.value, *val, "input {input:?}");
                }
                None => {
                    assert!(serde_json::from_str::<T>(&json).is_err(), "should reject {input:?}");
                }
            }
        }
    }
}
