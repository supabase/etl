use serde::{Deserialize, Serialize};
use std::fmt;

/// Supabase integration configuration.
///
/// Contains Supabase-specific settings for ETL applications that
/// integrate with Supabase services.
#[derive(Clone, Serialize, Deserialize)]
pub struct SupabaseConfig {
    /// Supabase project reference identifier.
    pub project_ref: String,
}

impl fmt::Debug for SupabaseConfig {
    /// Formats the config for debugging with redacted project reference.
    ///
    /// The project reference is redacted in debug output for security.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupabaseConfig")
            .field("project_ref", &"REDACTED")
            .finish()
    }
}
