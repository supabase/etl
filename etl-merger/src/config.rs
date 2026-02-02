//! Configuration for the merger process.

/// Configuration for the merger process.
#[derive(Debug, Clone)]
pub struct MergerConfig {
    /// Maximum number of events to process in a single batch.
    pub batch_size: usize,

    /// Namespace where changelog tables reside.
    pub changelog_namespace: String,

    /// Namespace where mirror tables will be created.
    pub mirror_namespace: String,

    /// Initial capacity for the secondary index (optimization).
    pub index_initial_capacity: usize,

    /// Starting checkpoint (sequence number to start processing from).
    /// If None, processes from the beginning.
    pub checkpoint: Option<String>,
}

impl Default for MergerConfig {
    fn default() -> Self {
        Self {
            batch_size: 10_000,
            changelog_namespace: "default".to_string(),
            mirror_namespace: "mirror".to_string(),
            index_initial_capacity: 100_000,
            checkpoint: None,
        }
    }
}

impl MergerConfig {
    /// Creates a new configuration with the given namespaces.
    pub fn new(changelog_namespace: String, mirror_namespace: String) -> Self {
        Self {
            changelog_namespace,
            mirror_namespace,
            ..Default::default()
        }
    }

    /// Sets the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets the initial index capacity.
    pub fn with_index_capacity(mut self, capacity: usize) -> Self {
        self.index_initial_capacity = capacity;
        self
    }

    /// Sets the starting checkpoint.
    pub fn with_checkpoint(mut self, checkpoint: Option<String>) -> Self {
        self.checkpoint = checkpoint;
        self
    }
}
