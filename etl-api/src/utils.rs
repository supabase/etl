use rand::Rng;

/// Generates a random alphabetic string of length `len`
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
}
