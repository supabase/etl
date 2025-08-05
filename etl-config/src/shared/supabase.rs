use serde::{Deserialize, Serialize};
use std::fmt;

/// Configuration options for Supabase related information.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SupabaseConfig {
    /// The project to which this replicator belongs.
    pub project_ref: String,
}

impl fmt::Debug for SupabaseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SupabaseConfig")
            .field("project_ref", &"REDACTED")
            .finish()
    }
}
