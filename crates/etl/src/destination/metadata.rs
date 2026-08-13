use crate::schema::{ReplicationMask, SnapshotId};

/// Schema metadata for a table at a destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DestinationTableSchema {
    /// The destination table is being created for the first time.
    Creating {
        /// The source schema snapshot being created.
        snapshot_id: SnapshotId,
        /// The columns replicated from the snapshot.
        replication_mask: ReplicationMask,
    },
    /// A schema change from a known previous schema is being applied.
    Applying {
        /// The target source schema snapshot.
        snapshot_id: SnapshotId,
        /// The columns replicated from the target snapshot.
        replication_mask: ReplicationMask,
        /// The source schema snapshot before the change.
        previous_snapshot_id: SnapshotId,
        /// The columns replicated from the previous snapshot.
        previous_replication_mask: ReplicationMask,
    },
    /// The current schema has been successfully applied.
    Applied {
        /// The applied source schema snapshot.
        snapshot_id: SnapshotId,
        /// The columns replicated from the applied snapshot.
        replication_mask: ReplicationMask,
    },
}

impl DestinationTableSchema {
    /// Returns the current or target source schema snapshot.
    pub fn snapshot_id(&self) -> SnapshotId {
        match self {
            Self::Creating { snapshot_id, .. }
            | Self::Applying { snapshot_id, .. }
            | Self::Applied { snapshot_id, .. } => *snapshot_id,
        }
    }

    /// Returns the columns replicated from the current or target snapshot.
    pub fn replication_mask(&self) -> &ReplicationMask {
        match self {
            Self::Creating { replication_mask, .. }
            | Self::Applying { replication_mask, .. }
            | Self::Applied { replication_mask, .. } => replication_mask,
        }
    }
}

/// Unified metadata for a table at a destination.
///
/// The schema variant contains every field required by its state, making
/// incomplete recovery metadata unrepresentable in memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationTableMetadata {
    /// The name or identifier of the table in the destination system.
    destination_table_id: String,
    /// The schema state stored for the destination table.
    schema: DestinationTableSchema,
}

impl DestinationTableMetadata {
    /// Creates metadata for a table being created at the destination.
    pub fn new_creating(
        destination_table_id: String,
        snapshot_id: SnapshotId,
        replication_mask: ReplicationMask,
    ) -> Self {
        Self {
            destination_table_id,
            schema: DestinationTableSchema::Creating { snapshot_id, replication_mask },
        }
    }

    /// Creates metadata for a table whose schema has been applied.
    pub fn new_applied(
        destination_table_id: String,
        snapshot_id: SnapshotId,
        replication_mask: ReplicationMask,
    ) -> Self {
        Self {
            destination_table_id,
            schema: DestinationTableSchema::Applied { snapshot_id, replication_mask },
        }
    }

    /// Returns the destination-specific table identifier.
    pub fn destination_table_id(&self) -> &str {
        &self.destination_table_id
    }

    /// Returns the destination table schema metadata.
    pub fn schema(&self) -> &DestinationTableSchema {
        &self.schema
    }

    /// Returns the current or target source schema snapshot.
    pub fn snapshot_id(&self) -> SnapshotId {
        self.schema.snapshot_id()
    }

    /// Returns the columns replicated from the current or target snapshot.
    pub fn replication_mask(&self) -> &ReplicationMask {
        self.schema.replication_mask()
    }

    /// Returns true if the destination table is being created.
    pub fn is_creating(&self) -> bool {
        matches!(self.schema, DestinationTableSchema::Creating { .. })
    }

    /// Returns true if a schema change from a previous schema is in progress.
    pub fn is_applying(&self) -> bool {
        matches!(self.schema, DestinationTableSchema::Applying { .. })
    }

    /// Returns true if destination setup or a schema change is incomplete.
    pub fn is_pending(&self) -> bool {
        !self.is_applied()
    }

    /// Returns true if the current schema has been applied.
    pub fn is_applied(&self) -> bool {
        matches!(self.schema, DestinationTableSchema::Applied { .. })
    }

    /// Transitions this metadata to applied state.
    pub fn to_applied(mut self) -> Self {
        let (snapshot_id, replication_mask) = match self.schema {
            DestinationTableSchema::Creating { snapshot_id, replication_mask }
            | DestinationTableSchema::Applying { snapshot_id, replication_mask, .. }
            | DestinationTableSchema::Applied { snapshot_id, replication_mask } => {
                (snapshot_id, replication_mask)
            }
        };

        self.schema = DestinationTableSchema::Applied { snapshot_id, replication_mask };

        self
    }

    /// Starts a schema change from the current schema to a new schema.
    pub fn with_schema_change(
        mut self,
        snapshot_id: SnapshotId,
        replication_mask: ReplicationMask,
    ) -> Self {
        let (previous_snapshot_id, previous_replication_mask) = match self.schema {
            DestinationTableSchema::Creating { snapshot_id, replication_mask }
            | DestinationTableSchema::Applying { snapshot_id, replication_mask, .. }
            | DestinationTableSchema::Applied { snapshot_id, replication_mask } => {
                (snapshot_id, replication_mask)
            }
        };

        self.schema = DestinationTableSchema::Applying {
            snapshot_id,
            replication_mask,
            previous_snapshot_id,
            previous_replication_mask,
        };

        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::PgLsn;

    #[test]
    fn creating_metadata_contains_only_the_target_schema() {
        let snapshot_id = SnapshotId::initial();
        let replication_mask = ReplicationMask::from_bytes(vec![1]);
        let metadata = DestinationTableMetadata::new_creating(
            "users".to_owned(),
            snapshot_id,
            replication_mask.clone(),
        );

        assert_eq!(
            metadata.schema(),
            &DestinationTableSchema::Creating { snapshot_id, replication_mask }
        );
    }

    #[test]
    fn applying_metadata_contains_both_schemas() {
        let snapshot_id = SnapshotId::new(PgLsn::from(20), PgLsn::from(21));
        let replication_mask = ReplicationMask::from_bytes(vec![1, 1, 1]);
        let previous_snapshot_id = SnapshotId::new(PgLsn::from(10), PgLsn::from(11));
        let previous_replication_mask = ReplicationMask::from_bytes(vec![1, 0, 1]);

        let metadata = DestinationTableMetadata::new_applied(
            "users".to_owned(),
            previous_snapshot_id,
            previous_replication_mask.clone(),
        )
        .with_schema_change(snapshot_id, replication_mask.clone());

        assert_eq!(
            metadata.schema(),
            &DestinationTableSchema::Applying {
                snapshot_id,
                replication_mask,
                previous_snapshot_id,
                previous_replication_mask,
            }
        );
    }

    #[test]
    fn applied_metadata_discards_the_previous_schema() {
        let snapshot_id = SnapshotId::new(PgLsn::from(20), PgLsn::from(21));
        let replication_mask = ReplicationMask::from_bytes(vec![1, 1]);
        let metadata = DestinationTableMetadata::new_applied(
            "users".to_owned(),
            SnapshotId::initial(),
            ReplicationMask::from_bytes(vec![1, 0]),
        )
        .with_schema_change(snapshot_id, replication_mask.clone())
        .to_applied();

        assert_eq!(
            metadata.schema(),
            &DestinationTableSchema::Applied { snapshot_id, replication_mask }
        );
    }
}
