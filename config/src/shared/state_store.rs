use serde::{Deserialize, Serialize};

/// Configurations options for the state store.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateStoreConfig {
    /// The in-memory state store.
    Memory,
}

impl Default for StateStoreConfig {
    fn default() -> Self {
        Self::Memory
    }
}
