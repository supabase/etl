use std::num::NonZeroI32;

/// Extracts the PostgreSQL server version from a version string.
#[must_use]
pub fn extract_server_version(server_version_str: impl AsRef<str>) -> Option<NonZeroI32> {
    let version_part = server_version_str.as_ref().split_whitespace().next().unwrap_or("0.0");
    let version_components = version_part.split('.').collect::<Vec<_>>();

    let major = version_components.first().and_then(|v| v.parse::<i32>().ok()).unwrap_or(0);
    let minor = version_components.get(1).and_then(|v| v.parse::<i32>().ok()).unwrap_or(0);
    let patch = version_components.get(2).and_then(|v| v.parse::<i32>().ok()).unwrap_or(0);

    NonZeroI32::new(major * 10000 + minor * 100 + patch)
}
