//! ETL schema projection and replication schema model.
//!
//! This module owns the schema view that ETL exposes to destinations and event
//! consumers. Base Postgres schema identifiers are shared from `etl-postgres`,
//! while replication masks and projected schemas live here with the ETL domain.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt,
    sync::Arc,
};

pub use etl_postgres::{
    default_expression::{DefaultExpression, parse_default_expression},
    schema::{
        ColumnSchema, NumericModifiers, SchemaError, SnapshotId, TableId, TableName, TableSchema,
        numeric_modifiers,
    },
    type_utils::is_array_type,
};
pub use tokio_postgres::types::{PgLsn, Type};
use tracing::warn;

/// Prefix reserved for generated cycle-breaking column names.
const DDL_TEMPORARY_COLUMN_PREFIX: &str = "supabase_etl_ddl_tmp_column_";

/// Validates that all named columns exist in the supplied [`TableSchema`].
///
/// # Errors
///
/// Returns [`SchemaError::UnknownReplicatedColumns`] if any provided column
/// name does not exist in the table schema.
fn validate_mask_column_names(
    table_schema: &TableSchema,
    column_names: &HashSet<String>,
) -> Result<(), SchemaError> {
    let schema_column_names: HashSet<&str> = table_schema
        .column_schemas
        .iter()
        .map(|column_schema| column_schema.name.as_str())
        .collect();

    let unknown_columns: Vec<String> = column_names
        .iter()
        .filter(|name| !schema_column_names.contains(name.as_str()))
        .cloned()
        .collect();

    if !unknown_columns.is_empty() {
        return Err(SchemaError::UnknownReplicatedColumns(unknown_columns));
    }

    Ok(())
}

/// Builds raw mask bytes from schema order and a validated set of column
/// names.
fn build_mask_bytes(table_schema: &TableSchema, column_names: &HashSet<String>) -> Vec<u8> {
    table_schema
        .column_schemas
        .iter()
        .map(|column_schema| u8::from(column_names.contains(&column_schema.name)))
        .collect()
}

/// A bitmask indicating which columns are being replicated.
///
/// Each element is either 0 (not replicated) or 1 (replicated), with indices
/// corresponding to the columns in the table schema. Wrapped in [`Arc`] for
/// efficient sharing across multiple events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationMask(Arc<Vec<u8>>);

impl fmt::Display for ReplicationMask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, &v) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{v}")?;
        }
        write!(f, ")")
    }
}

impl ReplicationMask {
    /// Tries to create a new [`ReplicationMask`] from a table schema and column
    /// names.
    ///
    /// The mask is constructed by checking which column names from the schema
    /// are present in the provided set of replicated column names.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::UnknownReplicatedColumns`] if any column in
    /// `replicated_column_names` does not exist in the table schema.
    ///
    /// The column validation occurs because we have to make sure that the
    /// stored table schema is always up to date, if not, it's a critical
    /// problem.
    pub fn try_build(
        table_schema: &TableSchema,
        replicated_column_names: &HashSet<String>,
    ) -> Result<Self, SchemaError> {
        // This check ensures all replicated columns are present in the schema.
        //
        // Limitation: If a column exists in the schema but is absent from the
        // replicated columns, we assume publication-level column filtering is
        // enabled. However, this is indistinguishable from an invalid state
        // where the schema has diverged, we cannot detect the difference.
        //
        // How schema divergence occurs: When progress tracking fails and the system
        // restarts, we may receive a `Relation` message reflecting the
        // *current* table schema rather than the schema at the time the
        // in-flight events were emitted. This is how Postgres handles
        // initial `Relation` messages on reconnection. It's not the wrong behavior
        // since the data has the columns that it announces, but it conflicts
        // with our schema management logic. TODO: We are still debugging this
        // case to validate when it happens, since it's hard to  reproduce.
        // Nonetheless, the error should be raised.
        //
        // Invariant: Our schema management assumes the schema in `Relation` messages is
        // consistent with the schema under which the corresponding row events
        // were produced.
        //
        // In the future we might want to implement a system to go around this edge
        // case.
        validate_mask_column_names(table_schema, replicated_column_names)?;

        Ok(Self(Arc::new(build_mask_bytes(table_schema, replicated_column_names))))
    }

    /// Creates a new [`ReplicationMask`] from a table schema and column names,
    /// falling back to an all-replicated mask if validation fails.
    ///
    /// This method attempts to validate that all replicated column names exist
    /// in the schema. If validation succeeds, it builds a mask based on
    /// matching columns. If validation fails (unknown columns are present),
    /// it returns a mask with all columns marked as replicated.
    ///
    /// This fallback behavior handles the case where Postgres sends a
    /// `Relation` message on reconnection with the current schema, but the
    /// stored schema is from an earlier point before DDL changes. Rather
    /// than failing, we enable all columns and let the system converge when
    /// the actual DDL message is replayed.
    pub fn build_or_all(
        table_schema: &TableSchema,
        replicated_column_names: &HashSet<String>,
    ) -> Self {
        match Self::try_build(table_schema, replicated_column_names) {
            Ok(mask) => mask,
            Err(_) => Self::all(table_schema),
        }
    }

    /// Creates a new [`ReplicationMask`] from a table schema and column names.
    pub fn build(table_schema: &TableSchema, replicated_column_names: &HashSet<String>) -> Self {
        Self(Arc::new(build_mask_bytes(table_schema, replicated_column_names)))
    }

    /// Creates a [`ReplicationMask`] with all columns marked as replicated.
    pub fn all(table_schema: &TableSchema) -> Self {
        let mask = vec![1; table_schema.column_schemas.len()];
        Self(Arc::new(mask))
    }

    /// Creates a [`ReplicationMask`] from raw bytes.
    ///
    /// Used for deserializing a mask from storage.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(Arc::new(bytes))
    }

    /// Returns the underlying mask as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Returns the underlying mask as a vector of bytes.
    ///
    /// Used for serializing the mask to storage.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.as_ref().clone()
    }

    /// Returns the number of columns in the mask.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the mask is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the number of replicated columns (count of 1s in the mask).
    pub fn replicated_count(&self) -> usize {
        self.0.iter().filter(|&&m| m == 1).count()
    }
}

/// A bitmask indicating which replicated columns belong to the replica
/// identity.
///
/// Unlike [`ReplicationMask`], this type is only used for runtime row-identity
/// semantics. It therefore exposes a smaller API surface: callers can build it
/// from schema metadata or raw bytes, then inspect the resulting bit pattern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityMask(Arc<Vec<u8>>);

impl IdentityMask {
    /// Tries to create a new [`IdentityMask`] from a table schema and column
    /// names.
    ///
    /// The mask is constructed by checking which schema columns are present in
    /// the provided set of identity column names.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::UnknownReplicatedColumns`] if any provided column
    /// name does not exist in the supplied table schema.
    pub fn try_build(
        table_schema: &TableSchema,
        identity_column_names: &HashSet<String>,
    ) -> Result<Self, SchemaError> {
        validate_mask_column_names(table_schema, identity_column_names)?;

        Ok(Self(Arc::new(build_mask_bytes(table_schema, identity_column_names))))
    }

    /// Creates an [`IdentityMask`] from raw bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(Arc::new(bytes))
    }

    /// Returns the underlying mask as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Returns the underlying mask as a vector of bytes.
    ///
    /// Used for serializing the mask into durable `SyncDone` decoding state.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.as_ref().clone()
    }

    /// Returns the number of columns in the mask.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the mask is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Semantic classification of the replica identity used for row events.
///
/// This captures the meaning of the runtime identity, not just the raw
/// identity-column mask:
/// - [`IdentityType::PrimaryKey`] means row identity matches the table primary
///   key, whether that came from `REPLICA IDENTITY DEFAULT` or `USING INDEX`
///   pointing at the primary-key index.
/// - [`IdentityType::AlternativeKey`] means row identity comes from a distinct
///   unique index.
/// - [`IdentityType::Full`] means the whole replicated row is the old-row key.
/// - [`IdentityType::Missing`] means updates and deletes do not have a usable
///   row identity.
///
/// Equivalence is established structurally from the current replicated schema
/// columns, not from the raw PostgreSQL mode byte or from an index OID. In
/// practice that means a `USING INDEX` identity is treated as
/// [`IdentityType::PrimaryKey`] whenever it resolves to the same current
/// columns as the primary key. This is the semantic question destinations care
/// about, and it remains stable across supported DDL evolution because ETL
/// keeps rebuilding the runtime schema from schema-change messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityType {
    /// The full replicated row is the row identity.
    Full,
    /// The replica identity matches the table primary key.
    PrimaryKey,
    /// The replica identity comes from a non-primary-key unique index.
    AlternativeKey,
    /// No usable replica identity is available.
    Missing,
}

impl fmt::Display for IdentityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            IdentityType::Full => "full",
            IdentityType::PrimaryKey => "primary_key",
            IdentityType::AlternativeKey => "alternative_key",
            IdentityType::Missing => "missing",
        };

        f.write_str(value)
    }
}

/// An iterator wrapper that provides an exact size even when the inner iterator
/// doesn't know its length.
///
/// This is useful for iterators like `FilterMap` where the exact count is not
/// known upfront, but can be pre-computed. The wrapper stores the pre-computed
/// length and implements [`ExactSizeIterator`].
#[derive(Clone)]
pub struct SizedIterator<I> {
    inner: I,
    len: usize,
}

impl<I> SizedIterator<I> {
    /// Creates a new [`SizedIterator`] with a pre-computed length.
    pub fn new(inner: I, len: usize) -> Self {
        Self { inner, len }
    }
}

impl<I: Iterator> Iterator for SizedIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next();
        if item.is_some() {
            self.len = self.len.saturating_sub(1);
        }
        item
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<I: Iterator> ExactSizeIterator for SizedIterator<I> {
    fn len(&self) -> usize {
        self.len
    }
}

/// Maps exact PostgreSQL column names into a destination namespace.
///
/// The mapping is idempotent and defines both the physical destination name
/// and which source names collide there. Logical columns remain identified by
/// their PostgreSQL ordinal positions. A rename with the same mapped name
/// remains in the logical diff but requires no physical operation.
///
/// A mapping is an integration contract with a destination's identifier
/// semantics, not a property discovered from each table. Initial creation,
/// data writes, schema planning, and recovery must use the same mapping. If a
/// destination release or configuration change alters those semantics,
/// existing tables may need compatibility handling or a resync before this
/// mapping can change safely. Otherwise the planner could reject a valid
/// schema, miss a collision, or classify a required rename as a no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnNameMapping {
    /// Preserves the source name exactly.
    Identity,
    /// Converts the source name to Unicode lowercase.
    UnicodeLowercase,
    /// Converts ASCII uppercase letters in the source name to lowercase.
    AsciiLowercase,
}

impl ColumnNameMapping {
    /// Returns whether two names identify the same destination column.
    pub fn equivalent(self, left: &str, right: &str) -> bool {
        self.map_name(left) == self.map_name(right)
    }

    /// Maps an exact PostgreSQL name to its physical destination name.
    pub fn map_name(self, name: &str) -> String {
        match self {
            Self::Identity => name.to_owned(),
            Self::UnicodeLowercase => name.to_lowercase(),
            Self::AsciiLowercase => name.to_ascii_lowercase(),
        }
    }

    /// Clones a source column with its name mapped for the destination.
    pub fn map_column_schema(self, column_schema: &ColumnSchema) -> ColumnSchema {
        let mut destination_column_schema = column_schema.clone();
        destination_column_schema.name = self.map_name(&column_schema.name);
        destination_column_schema
    }
}

impl fmt::Display for ColumnNameMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity => write!(f, "identity"),
            Self::UnicodeLowercase => write!(f, "Unicode lowercase"),
            Self::AsciiLowercase => write!(f, "ASCII lowercase"),
        }
    }
}

/// A wrapper around [`TableSchema`] that tracks replicated and identity
/// columns.
///
/// This struct holds a reference to the underlying table schema, a
/// [`ReplicationMask`] indicating which columns are included in replication,
/// and an [`IdentityMask`] indicating which replicated columns participate in
/// row identity for logical replication events.
#[derive(Debug, Clone)]
pub struct ReplicatedTableSchema {
    /// The underlying table schema.
    table_schema: Arc<TableSchema>,
    /// A bitmask where 1 indicates the column at that index is replicated.
    replication_mask: ReplicationMask,
    /// Cached number of replicated columns.
    replicated_column_count: usize,
    /// A bitmask where 1 indicates the column at that index is a replicated
    /// row identity column used by logical replication.
    identity_mask: IdentityMask,
    /// Cached number of replicated identity columns.
    identity_column_count: usize,
    /// Cached number of replicated primary-key columns.
    primary_key_column_count: usize,
    /// Semantic classification of the replica identity for this runtime schema.
    identity_type: IdentityType,
}

impl ReplicatedTableSchema {
    /// Creates a [`ReplicatedTableSchema`] from a schema and pre-computed
    /// masks, inferring the identity type from the mask shape.
    ///
    /// Both masks are expressed in full table-schema width. The identity mask
    /// must be a subset of the replication mask because row-event decoding can
    /// only consume key columns that PostgreSQL includes in the relation
    /// payload.
    ///
    /// ETL stores runtime identity, not raw source catalog identity. Initial
    /// copy follows streaming relation-message semantics by marking only
    /// replicated columns as identity columns. Update/delete replication relies
    /// on PostgreSQL validating that the source identity is covered;
    /// insert-only publications do not need identity data.
    ///
    /// This constructor infers the semantic identity type from the table
    /// schema and supplied masks, and caches the derived column counts needed
    /// by the iterator accessors.
    pub fn from_masks(
        table_schema: Arc<TableSchema>,
        replication_mask: ReplicationMask,
        identity_mask: IdentityMask,
    ) -> Self {
        let identity_type =
            Self::infer_identity_type(&table_schema, &replication_mask, &identity_mask);

        debug_assert_eq!(
            table_schema.column_schemas.len(),
            replication_mask.len(),
            "mask length must match column count"
        );

        debug_assert_eq!(
            table_schema.column_schemas.len(),
            identity_mask.len(),
            "identity mask length must match column count"
        );

        for ((column_schema, &replicated), &identity) in table_schema
            .column_schemas
            .iter()
            .zip(replication_mask.as_slice().iter())
            .zip(identity_mask.as_slice().iter())
        {
            if identity == 1 && replicated == 0 {
                warn!(
                    table_id = %table_schema.id,
                    table_name = %table_schema.name,
                    column_name = %column_schema.name,
                    "replica identity column is not replicated"
                );
            }
        }

        // We pre-compute counts to avoid computing them each time since they are needed
        // for the exact size iterators.
        let replicated_column_count = replication_mask.replicated_count();
        let identity_column_count = replication_mask
            .as_slice()
            .iter()
            .zip(identity_mask.as_slice().iter())
            .filter(|(replicated, identity)| **replicated == 1 && **identity == 1)
            .count();
        let primary_key_column_count = table_schema
            .column_schemas
            .iter()
            .zip(replication_mask.as_slice().iter())
            .filter(|(column_schema, replicated)| **replicated == 1 && column_schema.primary_key())
            .count();

        Self {
            table_schema,
            replication_mask,
            replicated_column_count,
            identity_mask,
            identity_column_count,
            primary_key_column_count,
            identity_type,
        }
    }

    /// Creates a [`ReplicatedTableSchema`] from a schema and a pre-computed
    /// replication mask.
    ///
    /// The identity mask is derived from replicated primary-key membership.
    /// This is a convenient fallback for code paths that only need replicated
    /// columns or when the source schema and identity are known to match
    /// primary-key semantics.
    pub fn from_mask(table_schema: Arc<TableSchema>, replication_mask: ReplicationMask) -> Self {
        let identity_mask = Self::primary_key_identity_mask(&table_schema, &replication_mask);
        Self::from_masks(table_schema, replication_mask, identity_mask)
    }

    /// Creates a [`ReplicatedTableSchema`] where all columns are replicated.
    pub fn all(table_schema: Arc<TableSchema>) -> Self {
        let replication_mask = ReplicationMask::all(&table_schema);
        Self::from_mask(table_schema, replication_mask)
    }

    /// Returns the table ID.
    pub fn id(&self) -> TableId {
        self.table_schema.id
    }

    /// Returns the table name.
    pub fn name(&self) -> &TableName {
        &self.table_schema.name
    }

    /// Returns the underlying table schema.
    pub fn inner(&self) -> &TableSchema {
        &self.table_schema
    }

    /// Returns the replication mask.
    pub fn replication_mask(&self) -> &ReplicationMask {
        &self.replication_mask
    }

    /// Returns the identity mask.
    pub fn identity_mask(&self) -> &IdentityMask {
        &self.identity_mask
    }

    /// Returns the semantic replica-identity classification for this schema.
    pub fn identity_type(&self) -> IdentityType {
        self.identity_type
    }

    /// Returns an iterator over only the column schemas that are being
    /// replicated.
    ///
    /// This filters the columns based on the mask, returning only those where
    /// the corresponding mask value is 1. The returned iterator implements
    /// [`ExactSizeIterator`].
    pub fn column_schemas(&self) -> impl ExactSizeIterator<Item = &ColumnSchema> + Clone + '_ {
        // Assuming that the schema is created via the constructor, we can safely assume
        // that the column schemas and replication mask are of the same length.
        debug_assert!(
            self.replication_mask.len() == self.table_schema.column_schemas.len(),
            "the replication mask columns have a different len from the table schema columns, \
             they should be the same"
        );

        let inner = self
            .table_schema
            .column_schemas
            .iter()
            .zip(self.replication_mask.as_slice().iter())
            .filter_map(|(cs, &m)| if m == 1 { Some(cs) } else { None });

        SizedIterator::new(inner, self.replicated_column_count)
    }

    /// Returns an iterator over only the column schemas that are part of the
    /// row identity, preserving replicated table-column order.
    pub fn identity_column_schemas(
        &self,
    ) -> impl ExactSizeIterator<Item = &ColumnSchema> + Clone + '_ {
        // Key tuples from PostgreSQL should only use columns present in the
        // relation payload. Check both masks here so tuple decoding only sees
        // columns that are both identity columns and actually replicated.
        let inner = self
            .table_schema
            .column_schemas
            .iter()
            .zip(self.replication_mask.as_slice().iter().zip(self.identity_mask.as_slice().iter()))
            .filter_map(
                |(column_schema, (&replicated, &identity))| {
                    if replicated == 1 && identity == 1 { Some(column_schema) } else { None }
                },
            );

        SizedIterator::new(inner, self.identity_column_count)
    }

    /// Returns an iterator over only the replicated primary-key columns,
    /// preserving replicated table-column order.
    pub fn primary_key_column_schemas(
        &self,
    ) -> impl ExactSizeIterator<Item = &ColumnSchema> + Clone + '_ {
        let inner = self
            .table_schema
            .column_schemas
            .iter()
            .zip(self.replication_mask.as_slice().iter())
            .filter_map(|(column_schema, &replicated)| {
                if replicated == 1 && column_schema.primary_key() {
                    Some(column_schema)
                } else {
                    None
                }
            });

        SizedIterator::new(inner, self.primary_key_column_count)
    }

    /// Returns whether every source primary-key column is replicated.
    ///
    /// Destinations that match rows by the source primary key need this check
    /// in addition to runtime identity checks, because replicated primary-key
    /// iterators intentionally expose only the replicated subset.
    pub fn all_primary_key_columns_replicated(&self) -> bool {
        self.unreplicated_primary_key_column_schemas().next().is_none()
    }

    /// Returns source primary-key columns omitted from replication.
    pub fn unreplicated_primary_key_column_schemas(
        &self,
    ) -> impl Iterator<Item = &ColumnSchema> + Clone + '_ {
        self.table_schema
            .column_schemas
            .iter()
            .zip(self.replication_mask.as_slice().iter())
            .filter_map(|(column_schema, &replicated)| {
                if column_schema.primary_key() && replicated == 0 {
                    Some(column_schema)
                } else {
                    None
                }
            })
    }

    /// Computes the diff between this schema (old) and another schema (new).
    ///
    /// Only consider replicated columns. Uses ordinal positions to track
    /// columns:
    /// - Columns in the same position with different names are renamed.
    /// - Type or type-modifier changes in the same position are retained for
    ///   fail-closed planning.
    /// - Nullability and default changes in the same position are modified.
    /// - Positions in old but not in new are columns to drop.
    /// - Positions in new but not in old are columns to add.
    ///
    /// Each DDL message stores a complete post-statement table snapshot, but it
    /// does not materialize a schema change at the destination. `pgoutput`
    /// emits relation metadata lazily before the next DML event, and that
    /// [`crate::event::RelationEvent`] advances the destination from its last
    /// applied schema to the newest stored snapshot. Several DDL statements
    /// without intervening DML can therefore appear here as one endpoint diff
    /// containing any combination of additions, drops, renames, and metadata
    /// changes, including rename chains or cycles that one `ALTER TABLE`
    /// statement could not express.
    ///
    /// Diffing catalog snapshots is intentional. Row decoding still requires
    /// immutable schemas at their WAL positions, so capturing operations would
    /// not replace snapshots: ETL would first have to reproduce PostgreSQL's
    /// `attnum` allocation, command ordering, dependencies, and implicit
    /// catalog effects to rebuild them. The post-DDL catalog already provides
    /// that canonical result.
    ///
    /// Source snapshots contain only PostgreSQL catalog states used to decode
    /// WAL rows. Planner-generated temporary names exist only while destination
    /// DDL is running and are never stored as source schema versions. The
    /// planner emits only the operations needed to reach the schema used by the
    /// next row event.
    pub fn diff(&self, new_schema: &ReplicatedTableSchema) -> SchemaDiff {
        let old_table_ordinals: HashSet<_> =
            self.inner().column_schemas.iter().map(|column| column.ordinal_position).collect();
        let new_table_ordinals: HashSet<_> = new_schema
            .inner()
            .column_schemas
            .iter()
            .map(|column| column.ordinal_position)
            .collect();
        let mut old_columns: Vec<_> = self.column_schemas().collect();
        let mut new_columns: Vec<_> = new_schema.column_schemas().collect();

        // PostgreSQL snapshots arrive in `attnum` order. Preserve correctness
        // for schemas assembled through the public constructors as well,
        // without paying for sorting on the production path.
        if !old_columns.is_sorted_by_key(|column| column.ordinal_position) {
            old_columns.sort_unstable_by_key(|column| column.ordinal_position);
        }
        if !new_columns.is_sorted_by_key(|column| column.ordinal_position) {
            new_columns.sort_unstable_by_key(|column| column.ordinal_position);
        }

        // Once ordered by `attnum`, one linear merge classifies the endpoint
        // difference. Planning happens afterward because a rename target may
        // still be occupied by a higher-`attnum` column that is itself renamed
        // or dropped by the same endpoint transition.
        let mut old_index = 0;
        let mut new_index = 0;
        let mut columns_to_add = Vec::new();
        let mut columns_to_alter = Vec::new();
        let mut columns_to_drop = Vec::new();

        while let (Some(&old_column), Some(&new_column)) =
            (old_columns.get(old_index), new_columns.get(new_index))
        {
            match old_column.ordinal_position.cmp(&new_column.ordinal_position) {
                Ordering::Less => {
                    let reason = if new_table_ordinals.contains(&old_column.ordinal_position) {
                        ColumnPresenceChangeReason::ReplicationMask
                    } else {
                        ColumnPresenceChangeReason::TableSchema
                    };

                    columns_to_drop
                        .push(ColumnPresenceChange { column_schema: old_column.clone(), reason });

                    old_index += 1;
                }
                Ordering::Greater => {
                    let reason = if old_table_ordinals.contains(&new_column.ordinal_position) {
                        ColumnPresenceChangeReason::ReplicationMask
                    } else {
                        ColumnPresenceChangeReason::TableSchema
                    };

                    columns_to_add
                        .push(ColumnPresenceChange { column_schema: new_column.clone(), reason });

                    new_index += 1;
                }
                Ordering::Equal => {
                    // Equal `attnum` means the same logical column. A rename and its metadata
                    // changes therefore stay grouped even when the endpoint name changed.
                    if let Some(change) = ColumnMetadataChange::between(old_column, new_column) {
                        columns_to_alter.push(change);
                    }

                    old_index += 1;
                    new_index += 1;
                }
            }
        }

        columns_to_drop.extend(old_columns[old_index..].iter().map(|column| {
            let reason = if new_table_ordinals.contains(&column.ordinal_position) {
                ColumnPresenceChangeReason::ReplicationMask
            } else {
                ColumnPresenceChangeReason::TableSchema
            };

            ColumnPresenceChange { column_schema: (**column).clone(), reason }
        }));
        columns_to_add.extend(new_columns[new_index..].iter().map(|column| {
            let reason = if old_table_ordinals.contains(&column.ordinal_position) {
                ColumnPresenceChangeReason::ReplicationMask
            } else {
                ColumnPresenceChangeReason::TableSchema
            };

            ColumnPresenceChange { column_schema: (**column).clone(), reason }
        }));

        SchemaDiff::from_column_changes(columns_to_add, columns_to_drop, columns_to_alter)
    }

    /// Computes the exact endpoint diff and plans it for a destination.
    ///
    /// This is the preferred entrypoint whenever both replicated endpoint
    /// schemas are available. Diffing remains independent of destination
    /// identifier behavior and retains exact PostgreSQL names. Planning maps
    /// both endpoint namespaces, validates them, and maps every emitted
    /// operation before ordering it in the destination namespace. Consumers of
    /// [`SchemaPlan::ordered_operations`] must therefore execute those names as
    /// provided rather than mapping them again.
    ///
    /// `column_name_mapping` is assumed to describe both the currently applied
    /// destination schema and the destination semantics used during execution.
    /// Changing that interpretation without migrating or resyncing the applied
    /// table can invalidate no-op and name-occupancy decisions.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaPlanError`] when the destination cannot represent an
    /// endpoint namespace or the derived diff cannot be planned safely.
    pub fn plan_schema_change(
        &self,
        new_schema: &ReplicatedTableSchema,
        column_name_mapping: ColumnNameMapping,
    ) -> Result<SchemaPlan, SchemaPlanError> {
        SchemaPlan::from_schemas(self.diff(new_schema), self, new_schema, column_name_mapping)
    }

    /// Validates that replicated column names are unique for a destination.
    ///
    /// Exact PostgreSQL names remain unchanged. This method only checks that
    /// no two names map to the same destination column. Use it
    /// for initial table setup when no schema plan is built;
    /// [`ReplicatedTableSchema::plan_schema_change`] performs the same check
    /// for both schema-change endpoints.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaPlanError::DestinationColumnNameCollision`] when two
    /// replicated columns would occupy the same destination namespace entry.
    pub fn validate_destination_column_names(
        &self,
        column_name_mapping: ColumnNameMapping,
    ) -> Result<(), SchemaPlanError> {
        let column_names: Vec<_> =
            self.column_schemas().map(|column| column.name.clone()).collect();
        collect_unique_column_names(&column_names, column_name_mapping, SchemaEndpoint::Target)?;
        Ok(())
    }

    /// Returns replicated columns with destination-mapped names.
    ///
    /// Source schema metadata remains unchanged; callers should use these
    /// owned columns whenever names cross a destination boundary.
    pub fn destination_column_schemas(
        &self,
        column_name_mapping: ColumnNameMapping,
    ) -> impl ExactSizeIterator<Item = ColumnSchema> + '_ {
        self.column_schemas().map(move |column| column_name_mapping.map_column_schema(column))
    }

    /// Builds the primary-key identity mask within the replicated schema
    /// width.
    fn primary_key_identity_mask(
        table_schema: &TableSchema,
        replication_mask: &ReplicationMask,
    ) -> IdentityMask {
        IdentityMask::from_bytes(
            table_schema
                .column_schemas
                .iter()
                .zip(replication_mask.as_slice().iter())
                .map(|(column_schema, &replicated)| {
                    u8::from(replicated == 1 && column_schema.primary_key())
                })
                .collect(),
        )
    }

    /// Infers the identity type from a schema and mask pair.
    ///
    /// This is used only for fallback constructors that do not receive the
    /// explicit PostgreSQL identity mode.
    ///
    /// In the case when a primary key is made up of all the table columns, the
    /// identity will be marked as [`IdentityType::PrimaryKey`].
    ///
    /// The inference is structural: if the identity mask selects the same
    /// current replicated columns as the primary key mask, the result is
    /// [`IdentityType::PrimaryKey`] even if the original source mode might
    /// have been `USING INDEX`.
    fn infer_identity_type(
        table_schema: &TableSchema,
        replication_mask: &ReplicationMask,
        identity_mask: &IdentityMask,
    ) -> IdentityType {
        let mut has_identity = false;
        let mut matches_primary_key = true;
        let mut matches_full = true;

        for ((column_schema, &replicated), &identity) in table_schema
            .column_schemas
            .iter()
            .zip(replication_mask.as_slice().iter())
            .zip(identity_mask.as_slice().iter())
        {
            has_identity |= identity == 1;

            if identity != u8::from(replicated == 1 && column_schema.primary_key()) {
                matches_primary_key = false;
            }

            if identity != replicated {
                matches_full = false;
            }
        }

        if !has_identity {
            IdentityType::Missing
        } else if matches_primary_key {
            IdentityType::PrimaryKey
        } else if matches_full {
            IdentityType::Full
        } else {
            IdentityType::AlternativeKey
        }
    }
}

/// One endpoint-to-endpoint metadata diff for an existing logical column.
///
/// The endpoint schemas have the same PostgreSQL ordinal position, which is
/// the logical-column identity used by diffing. Their exact source names and
/// metadata remain unmodified by destination identifier rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnMetadataChange {
    /// The previous endpoint column schema.
    previous_column_schema: ColumnSchema,
    /// The target endpoint column schema.
    target_column_schema: ColumnSchema,
}

impl ColumnMetadataChange {
    /// Builds a change from two endpoint states of one logical column.
    pub fn between(
        previous_column_schema: &ColumnSchema,
        target_column_schema: &ColumnSchema,
    ) -> Option<Self> {
        let change = Self {
            previous_column_schema: previous_column_schema.clone(),
            target_column_schema: target_column_schema.clone(),
        };

        change.has_changes().then_some(change)
    }

    /// Returns the previous endpoint column schema.
    pub fn previous_column_schema(&self) -> &ColumnSchema {
        &self.previous_column_schema
    }

    /// Returns the target endpoint column schema.
    pub fn target_column_schema(&self) -> &ColumnSchema {
        &self.target_column_schema
    }

    /// Returns whether the column name changed.
    pub fn name_changed(&self) -> bool {
        self.previous_column_schema.name != self.target_column_schema.name
    }

    /// Returns whether the column type or type modifier changed.
    pub fn data_type_changed(&self) -> bool {
        self.previous_column_schema.typ != self.target_column_schema.typ
            || self.previous_column_schema.modifier != self.target_column_schema.modifier
    }

    /// Returns whether the column nullability changed.
    pub fn nullability_changed(&self) -> bool {
        self.previous_column_schema.nullable != self.target_column_schema.nullable
    }

    /// Returns whether the column default expression changed.
    pub fn default_changed(&self) -> bool {
        self.previous_column_schema.default_expression
            != self.target_column_schema.default_expression
    }

    /// Returns whether primary-key membership or order changed.
    pub fn primary_key_changed(&self) -> bool {
        self.previous_column_schema.primary_key_ordinal_position
            != self.target_column_schema.primary_key_ordinal_position
    }

    /// Returns whether any classified metadata field changed.
    fn has_changes(&self) -> bool {
        self.name_changed()
            || self.data_type_changed()
            || self.nullability_changed()
            || self.default_changed()
            || self.primary_key_changed()
    }
}

/// Identifies why a column entered or left the replicated destination schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnPresenceChangeReason {
    /// The column was added to or removed from the physical PostgreSQL table
    /// schema.
    TableSchema,
    /// The physical column exists in both endpoint schemas, but its publication
    /// column-list membership changed.
    ReplicationMask,
}

/// One column added to or removed from the replicated destination schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPresenceChange {
    /// The column schema at the endpoint where the column is present.
    pub column_schema: ColumnSchema,
    /// Why the column is present in only one replicated endpoint.
    pub reason: ColumnPresenceChangeReason,
}

/// One executable metadata alteration in a destination schema transition.
///
/// Alter-column operations carry the expected column state immediately before
/// one alteration. The planner advances this state after every operation, so a
/// later operation reflects all preceding planned changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnAlteration {
    /// Renames the column.
    Rename {
        /// The new destination-mapped column name.
        new_name: String,
    },
    /// Changes the column type or type modifier.
    Type {
        /// The previous PostgreSQL column type.
        previous_type: Type,
        /// The previous PostgreSQL type modifier.
        previous_modifier: i32,
        /// The target PostgreSQL column type.
        target_type: Type,
        /// The target PostgreSQL type modifier.
        target_modifier: i32,
    },
    /// Changes whether the column accepts null values.
    Nullability {
        /// The previous nullability.
        previous_nullable: bool,
        /// The target nullability.
        target_nullable: bool,
    },
    /// Changes the source default expression.
    Default {
        /// The previous source default expression.
        previous_default_expression: Option<String>,
        /// The target source default expression.
        target_default_expression: Option<String>,
    },
}

impl ColumnAlteration {
    /// Applies this alteration to an expected column state.
    pub fn apply_to(&self, column_schema: &ColumnSchema) -> ColumnSchema {
        let mut target_column_schema = column_schema.clone();

        match self {
            Self::Rename { new_name } => target_column_schema.name.clone_from(new_name),
            Self::Type { target_type, target_modifier, .. } => {
                target_column_schema.typ.clone_from(target_type);
                target_column_schema.modifier = *target_modifier;
            }
            Self::Nullability { target_nullable, .. } => {
                target_column_schema.nullable = *target_nullable;
            }
            Self::Default { target_default_expression, .. } => {
                target_column_schema.default_expression.clone_from(target_default_expression);
            }
        }

        target_column_schema
    }
}

/// One ordered operation in a destination schema transition.
///
/// Destinations preserve this order and decide whether each alteration is
/// supported. Built-in destinations that execute schema plans currently warn
/// and skip unsupported type alterations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaOperation {
    /// Drop a column that is absent from the new endpoint schema.
    DropColumn {
        /// The old endpoint column schema.
        column_schema: ColumnSchema,
        /// Why the column left the replicated endpoint.
        reason: ColumnPresenceChangeReason,
    },
    /// Add a new logical column.
    AddColumn {
        /// The new endpoint column schema.
        column_schema: ColumnSchema,
        /// Why the column entered the replicated endpoint.
        reason: ColumnPresenceChangeReason,
    },
    /// Alter one field of an existing logical column.
    AlterColumn {
        /// The expected state immediately before this operation.
        column_schema: ColumnSchema,
        /// The single metadata alteration to apply.
        alteration: ColumnAlteration,
    },
}

/// Represents differences between two schema versions.
///
/// This type contains only exact facts from the PostgreSQL endpoint snapshots.
/// It does not apply destination identifier semantics or contain generated
/// temporary renames. Use [`ReplicatedTableSchema::plan_schema_change`] to
/// validate destination name compatibility and produce an executable
/// [`SchemaPlan`]. Type and type-modifier changes are planned explicitly even
/// though built-in destinations that execute schema plans currently warn and
/// skip their execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDiff {
    /// Columns that need to be added to the destination.
    pub columns_to_add: Vec<ColumnPresenceChange>,
    /// Columns that need to be dropped from the destination.
    pub columns_to_drop: Vec<ColumnPresenceChange>,
    /// Existing columns whose endpoint metadata must be altered.
    pub columns_to_alter: Vec<ColumnMetadataChange>,
}

impl SchemaDiff {
    /// Builds a diff from explicit physical table-schema operations.
    ///
    /// Prefer [`ReplicatedTableSchema::diff`] when both endpoint schemas are
    /// available so additions and removals can be distinguished from
    /// replication-mask changes. Additions and removals supplied here are
    /// classified as [`ColumnPresenceChangeReason::TableSchema`]. The caller is
    /// responsible for supplying endpoint facts produced by the same rules as
    /// [`ReplicatedTableSchema::diff`].
    pub fn new(
        columns_to_add: Vec<ColumnSchema>,
        columns_to_drop: Vec<ColumnSchema>,
        columns_to_alter: Vec<ColumnMetadataChange>,
    ) -> Self {
        let columns_to_add = columns_to_add
            .into_iter()
            .map(|column_schema| ColumnPresenceChange {
                column_schema,
                reason: ColumnPresenceChangeReason::TableSchema,
            })
            .collect();
        let columns_to_drop = columns_to_drop
            .into_iter()
            .map(|column_schema| ColumnPresenceChange {
                column_schema,
                reason: ColumnPresenceChangeReason::TableSchema,
            })
            .collect();

        Self { columns_to_add, columns_to_drop, columns_to_alter }
    }

    /// Builds a diff whose add and drop reasons were derived from both full
    /// table-schema endpoints.
    fn from_column_changes(
        columns_to_add: Vec<ColumnPresenceChange>,
        columns_to_drop: Vec<ColumnPresenceChange>,
        columns_to_alter: Vec<ColumnMetadataChange>,
    ) -> Self {
        Self { columns_to_add, columns_to_drop, columns_to_alter }
    }

    /// Plans an explicit diff when complete replicated schemas are unavailable.
    ///
    /// This lower-level entrypoint is intended for destination recovery paths
    /// that know both physical endpoint namespaces but cannot reconstruct a
    /// complete current [`ReplicatedTableSchema`]. Prefer
    /// [`ReplicatedTableSchema::plan_schema_change`] otherwise.
    ///
    /// A rename cycle receives one temporary rename that avoids both endpoint
    /// namespaces. Exact source names remain in [`SchemaPlan::diff`], while
    /// executable operations use mapped destination names. Endpoint names are
    /// mapped before validation and ordering; recovery callers may pass names
    /// already read from the destination because every [`ColumnNameMapping`] is
    /// idempotent.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaPlanError`] if either endpoint maps two source columns
    /// to the same destination name or rename ordering cannot make progress.
    pub fn plan_for_column_names(
        self,
        current_column_names: Vec<String>,
        target_column_names: Vec<String>,
        column_name_mapping: ColumnNameMapping,
    ) -> Result<SchemaPlan, SchemaPlanError> {
        SchemaPlan::from_column_names(
            self,
            current_column_names,
            target_column_names,
            column_name_mapping,
        )
    }

    /// Returns `true` if there are no schema changes.
    pub fn is_empty(&self) -> bool {
        self.columns_to_add.is_empty()
            && self.columns_to_drop.is_empty()
            && self.columns_to_alter.is_empty()
    }
}

/// Identifies an endpoint while validating destination column names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaEndpoint {
    /// The currently applied schema.
    Current,
    /// The desired schema after applying the diff.
    Target,
}

impl fmt::Display for SchemaEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Current => write!(f, "current"),
            Self::Target => write!(f, "target"),
        }
    }
}

/// Errors produced while validating or ordering a destination schema plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaPlanError {
    /// Two exact source names occupy the same destination namespace entry.
    DestinationColumnNameCollision {
        /// The endpoint containing the collision.
        endpoint: SchemaEndpoint,
        /// The destination name mapping that produced the collision.
        column_name_mapping: ColumnNameMapping,
        /// The first exact source column name.
        first_column_name: String,
        /// The second exact source column name.
        second_column_name: String,
    },
    /// A blocked rename target is not owned by another pending rename.
    BlockedRenameTarget {
        /// The exact current column name.
        current_column_name: String,
        /// The exact requested target name.
        target_column_name: String,
    },
    /// A ready rename no longer exists in the pending rename set.
    ReadyRenameNotPending {
        /// PostgreSQL ordinal position identifying the logical column.
        ordinal_position: i32,
    },
}

impl fmt::Display for SchemaPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DestinationColumnNameCollision {
                endpoint,
                column_name_mapping,
                first_column_name,
                second_column_name,
            } => write!(
                f,
                "Destination column names '{first_column_name}' and '{second_column_name}' \
                 collide in the {endpoint} schema under {column_name_mapping} mapping"
            ),
            Self::BlockedRenameTarget { current_column_name, target_column_name } => write!(
                f,
                "Rename from '{current_column_name}' to '{target_column_name}' is blocked by a \
                 column outside the pending rename set"
            ),
            Self::ReadyRenameNotPending { ordinal_position } => write!(
                f,
                "Ready rename at ordinal {ordinal_position} is absent from the pending rename set"
            ),
        }
    }
}

impl std::error::Error for SchemaPlanError {}

/// A destination-validated schema transition with physical operations in safe order.
///
/// The exact endpoint facts remain available through [`SchemaPlan::diff`]. The
/// generated operation sequence uses mapped destination names and may contain
/// temporary renames used to break cycles in that destination namespace. Its
/// operations are ready for destination execution and must not be mapped a
/// second time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaPlan {
    /// The exact source endpoint difference.
    diff: SchemaDiff,
    /// Operations in destination-safe execution order.
    ordered_operations: Vec<SchemaOperation>,
    /// Destination identifier rules used to produce the operation sequence.
    column_name_mapping: ColumnNameMapping,
}

impl SchemaPlan {
    /// Plans an exact diff using its authoritative replicated endpoints.
    fn from_schemas(
        diff: SchemaDiff,
        current_schema: &ReplicatedTableSchema,
        target_schema: &ReplicatedTableSchema,
        column_name_mapping: ColumnNameMapping,
    ) -> Result<Self, SchemaPlanError> {
        let current_column_names: Vec<_> =
            current_schema.column_schemas().map(|column| column.name.clone()).collect();
        let target_column_names: Vec<_> =
            target_schema.column_schemas().map(|column| column.name.clone()).collect();

        Self::from_column_names(
            diff,
            current_column_names,
            target_column_names,
            column_name_mapping,
        )
    }

    /// Plans a trusted diff using explicit endpoint name sets.
    fn from_column_names(
        diff: SchemaDiff,
        current_column_names: Vec<String>,
        target_column_names: Vec<String>,
        column_name_mapping: ColumnNameMapping,
    ) -> Result<Self, SchemaPlanError> {
        // Map the endpoint names first so validation and ordering model the
        // same physical namespace in which the DDL will execute.
        let namespaces = SchemaPlanNamespaces::from_column_names(
            &current_column_names,
            &target_column_names,
            column_name_mapping,
        )?;

        // The diff keeps exact source names. The operation planner maps each
        // column as it emits the destination-executable sequence.
        let ordered_operations =
            SchemaOperationPlanner::new(namespaces, column_name_mapping).plan(&diff)?;

        Ok(Self { diff, ordered_operations, column_name_mapping })
    }

    /// Returns the exact endpoint diff represented by this plan.
    pub fn diff(&self) -> &SchemaDiff {
        &self.diff
    }

    /// Returns `true` if the plan contains no operations.
    pub fn is_empty(&self) -> bool {
        self.ordered_operations.is_empty()
    }

    /// Returns the only destination-executable schema operations.
    ///
    /// Every column name is already mapped for the destination. The mapping
    /// must match the one used to create the currently applied schema.
    ///
    /// Drops precede renames, followed by additions and type, nullability, and
    /// default alterations. Destinations must preserve this exact order and
    /// must not independently apply the classified diff fields.
    pub fn ordered_operations(&self) -> &[SchemaOperation] {
        &self.ordered_operations
    }

    /// Returns whether the plan contains a rename cycle.
    ///
    /// Each cycle requires one planner-generated rename in addition to its
    /// endpoint renames.
    pub fn has_rename_cycles(&self) -> bool {
        let endpoint_rename_count = self
            .diff
            .columns_to_alter
            .iter()
            .filter(|change| {
                change.name_changed()
                    && !self.column_name_mapping.equivalent(
                        &change.previous_column_schema().name,
                        &change.target_column_schema().name,
                    )
            })
            .count();
        let planned_rename_count = self
            .ordered_operations
            .iter()
            .filter(|operation| {
                matches!(
                    operation,
                    SchemaOperation::AlterColumn {
                        alteration: ColumnAlteration::Rename { .. },
                        ..
                    }
                )
            })
            .count();

        planned_rename_count > endpoint_rename_count
    }
}

/// Collects exact source names by mapped destination name and rejects
/// collisions.
fn collect_unique_column_names(
    column_names: &[String],
    column_name_mapping: ColumnNameMapping,
    endpoint: SchemaEndpoint,
) -> Result<HashMap<String, String>, SchemaPlanError> {
    let mut source_name_by_destination_name = HashMap::with_capacity(column_names.len());

    for column_name in column_names {
        let destination_name = column_name_mapping.map_name(column_name);
        if let Some(first_column_name) =
            source_name_by_destination_name.insert(destination_name, column_name.clone())
        {
            return Err(SchemaPlanError::DestinationColumnNameCollision {
                endpoint,
                column_name_mapping,
                first_column_name,
                second_column_name: column_name.clone(),
            });
        }
    }

    Ok(source_name_by_destination_name)
}

/// Validated destination namespace state needed to order physical operations.
#[derive(Debug)]
struct SchemaPlanNamespaces {
    /// Names occupied at the current endpoint.
    occupied_destination_names: HashSet<String>,
    /// Names reserved by either endpoint.
    reserved_destination_names: HashSet<String>,
}

impl SchemaPlanNamespaces {
    /// Validates both endpoints and collects their occupied name keys.
    fn from_column_names(
        current_column_names: &[String],
        target_column_names: &[String],
        column_name_mapping: ColumnNameMapping,
    ) -> Result<Self, SchemaPlanError> {
        let current_names = collect_unique_column_names(
            current_column_names,
            column_name_mapping,
            SchemaEndpoint::Current,
        )?;
        let target_names = collect_unique_column_names(
            target_column_names,
            column_name_mapping,
            SchemaEndpoint::Target,
        )?;

        Ok(Self {
            occupied_destination_names: current_names.keys().cloned().collect(),
            reserved_destination_names: current_names
                .keys()
                .chain(target_names.keys())
                .cloned()
                .collect(),
        })
    }
}

/// A pending logical rename tracked by ordinal identity.
#[derive(Debug)]
struct PendingRename {
    /// The current state of the logical column.
    current_column_schema: ColumnSchema,
    /// The final target name from the new endpoint.
    target_name: String,
}

/// Maps and orders a validated diff into destination-executable phases.
struct SchemaOperationPlanner {
    /// Operations accumulated in execution order.
    operations: Vec<SchemaOperation>,
    /// Names occupied at the current point in the operation sequence.
    occupied_destination_names: HashSet<String>,
    /// Endpoint and generated names unavailable for temporary renames.
    reserved_destination_names: HashSet<String>,
    /// Destination rules used to map exact diff names into physical names.
    column_name_mapping: ColumnNameMapping,
}

impl SchemaOperationPlanner {
    /// Creates a planner from already validated endpoint namespaces.
    fn new(namespaces: SchemaPlanNamespaces, column_name_mapping: ColumnNameMapping) -> Self {
        Self {
            operations: Vec::new(),
            occupied_destination_names: namespaces.occupied_destination_names,
            reserved_destination_names: namespaces.reserved_destination_names,
            column_name_mapping,
        }
    }

    /// Maps operations while building the four required physical phases.
    fn plan(mut self, diff: &SchemaDiff) -> Result<Vec<SchemaOperation>, SchemaPlanError> {
        self.plan_drops(&diff.columns_to_drop);
        self.plan_renames(&diff.columns_to_alter)?;
        self.plan_additions(&diff.columns_to_add);
        self.plan_alterations(&diff.columns_to_alter);

        Ok(self.operations)
    }

    /// Phase 1: emits drops and frees every reusable current name.
    fn plan_drops(&mut self, columns_to_drop: &[ColumnPresenceChange]) {
        for change in columns_to_drop {
            let column = self.column_name_mapping.map_column_schema(&change.column_schema);
            self.occupied_destination_names.remove(&column.name);
            self.operations
                .push(SchemaOperation::DropColumn { column_schema: column, reason: change.reason });
        }
    }

    /// Phase 2: emits rename chains and breaks each cycle once.
    fn plan_renames(
        &mut self,
        columns_to_alter: &[ColumnMetadataChange],
    ) -> Result<(), SchemaPlanError> {
        // Pending work is keyed by ordinal identity. The ordered map makes
        // cycle-root selection deterministic.
        let mut pending_renames = BTreeMap::new();

        // This reverse lookup identifies the rename unblocked when another
        // rename releases its target. Validated target names are unique.
        let mut waiting_ordinal_by_target_name = HashMap::new();
        for change in columns_to_alter {
            if !change.name_changed()
                || self.column_name_mapping.equivalent(
                    &change.previous_column_schema().name,
                    &change.target_column_schema().name,
                )
            {
                continue;
            }

            let ordinal_position = change.previous_column_schema().ordinal_position;
            let current_column_schema =
                self.column_name_mapping.map_column_schema(change.previous_column_schema());
            let target_name =
                self.column_name_mapping.map_name(&change.target_column_schema().name);
            waiting_ordinal_by_target_name.insert(target_name.clone(), ordinal_position);
            pending_renames
                .insert(ordinal_position, PendingRename { current_column_schema, target_name });
        }

        // Free targets form the initial ready frontier. Ordinal ordering keeps
        // independent rename components deterministic.
        let mut ready_renames: BTreeSet<i32> = pending_renames
            .iter()
            .filter_map(|(&ordinal_position, rename)| {
                (!self.occupied_destination_names.contains(&rename.target_name))
                    .then_some(ordinal_position)
            })
            .collect();
        let mut temporary_name_sequence = 0_u64;

        while !pending_renames.is_empty() {
            if let Some(ordinal_position) = ready_renames.pop_first() {
                let Some(rename) = pending_renames.remove(&ordinal_position) else {
                    return Err(SchemaPlanError::ReadyRenameNotPending { ordinal_position });
                };

                // Consume the free target, release the current name, and wake
                // the rename waiting for that released name.
                let current_destination_name = rename.current_column_schema.name.clone();
                self.occupied_destination_names.remove(&current_destination_name);
                self.occupied_destination_names.insert(rename.target_name.clone());
                if let Some(waiting_ordinal) =
                    waiting_ordinal_by_target_name.get(&current_destination_name)
                    && pending_renames.contains_key(waiting_ordinal)
                {
                    ready_renames.insert(*waiting_ordinal);
                }

                let alteration = ColumnAlteration::Rename { new_name: rename.target_name };
                self.operations.push(SchemaOperation::AlterColumn {
                    column_schema: rename.current_column_schema,
                    alteration,
                });
                continue;
            }

            // With no free target, every valid remaining component is a cycle.
            // Break the smallest-ordinal component with one reserved temporary
            // name; ordinary ready processing then unwinds the cycle.
            let Some((&ordinal_position, rename)) = pending_renames.first_key_value() else {
                break;
            };
            if !pending_renames.values().any(|pending_rename| {
                pending_rename.current_column_schema.name == rename.target_name
            }) {
                return Err(SchemaPlanError::BlockedRenameTarget {
                    current_column_name: rename.current_column_schema.name.clone(),
                    target_column_name: rename.target_name.clone(),
                });
            }

            let temporary_name = loop {
                let candidate = format!(
                    "{DDL_TEMPORARY_COLUMN_PREFIX}{ordinal_position}_{temporary_name_sequence}"
                );
                temporary_name_sequence += 1;
                if self.reserved_destination_names.insert(candidate.clone()) {
                    break candidate;
                }
            };

            let old_column_schema = rename.current_column_schema.clone();
            let mut current_column_schema = old_column_schema.clone();
            current_column_schema.name = temporary_name.clone();
            let target_name = rename.target_name.clone();
            let old_destination_name = old_column_schema.name.clone();
            self.occupied_destination_names.remove(&old_destination_name);
            self.occupied_destination_names.insert(temporary_name.clone());
            if let Some(waiting_ordinal) = waiting_ordinal_by_target_name.get(&old_destination_name)
            {
                ready_renames.insert(*waiting_ordinal);
            }
            self.operations.push(SchemaOperation::AlterColumn {
                column_schema: old_column_schema,
                alteration: ColumnAlteration::Rename { new_name: temporary_name },
            });
            pending_renames
                .insert(ordinal_position, PendingRename { current_column_schema, target_name });
        }

        Ok(())
    }

    /// Phase 3: emits additions after every reusable name is available.
    fn plan_additions(&mut self, columns_to_add: &[ColumnPresenceChange]) {
        for change in columns_to_add {
            self.operations.push(SchemaOperation::AddColumn {
                column_schema: self.column_name_mapping.map_column_schema(&change.column_schema),
                reason: change.reason,
            });
        }
    }

    /// Phase 4: emits metadata changes against final endpoint names.
    fn plan_alterations(&mut self, columns_to_alter: &[ColumnMetadataChange]) {
        for change in columns_to_alter {
            let mut applied_column_schema = self.column_schema_after_renames(change);
            let target_column_schema =
                self.column_name_mapping.map_column_schema(change.target_column_schema());

            if change.data_type_changed() {
                let alteration = ColumnAlteration::Type {
                    previous_type: applied_column_schema.typ.clone(),
                    previous_modifier: applied_column_schema.modifier,
                    target_type: target_column_schema.typ.clone(),
                    target_modifier: target_column_schema.modifier,
                };
                self.push_alteration(&mut applied_column_schema, alteration);
            }

            if change.nullability_changed() {
                let alteration = ColumnAlteration::Nullability {
                    previous_nullable: applied_column_schema.nullable,
                    target_nullable: target_column_schema.nullable,
                };
                self.push_alteration(&mut applied_column_schema, alteration);
            }

            if change.default_changed() {
                let alteration = ColumnAlteration::Default {
                    previous_default_expression: applied_column_schema.default_expression.clone(),
                    target_default_expression: target_column_schema.default_expression.clone(),
                };
                self.push_alteration(&mut applied_column_schema, alteration);
            }
        }
    }

    /// Returns the expected column state after the rename phase.
    fn column_schema_after_renames(&self, change: &ColumnMetadataChange) -> ColumnSchema {
        let mut column_schema =
            self.column_name_mapping.map_column_schema(change.previous_column_schema());
        column_schema.name = self.column_name_mapping.map_name(&change.target_column_schema().name);
        column_schema
    }

    /// Emits one alteration and advances the expected column state.
    fn push_alteration(&mut self, column_schema: &mut ColumnSchema, alteration: ColumnAlteration) {
        let target_column_schema = alteration.apply_to(column_schema);
        let previous_column_schema = std::mem::replace(column_schema, target_column_schema);
        self.operations.push(SchemaOperation::AlterColumn {
            column_schema: previous_column_schema,
            alteration,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_table_schema() -> TableSchema {
        TableSchema::new(
            TableId::new(123),
            TableName::new("public".to_owned(), "test_table".to_owned()),
            vec![
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
                ColumnSchema::new("age".to_owned(), Type::INT4, -1, 3, true),
            ],
        )
    }
    #[test]
    fn replication_mask_try_build_all_columns_replicated() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> =
            ["id", "name", "age"].into_iter().map(String::from).collect();

        let mask = ReplicationMask::try_build(&schema, &replicated_columns).unwrap();

        assert_eq!(mask.as_slice(), &[1, 1, 1]);
    }

    #[test]
    fn replication_mask_try_build_partial_columns_replicated() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> =
            ["id", "age"].into_iter().map(String::from).collect();

        let mask = ReplicationMask::try_build(&schema, &replicated_columns).unwrap();

        assert_eq!(mask.as_slice(), &[1, 0, 1]);
    }

    #[test]
    fn replication_mask_try_build_no_columns_replicated() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> = HashSet::new();

        let mask = ReplicationMask::try_build(&schema, &replicated_columns).unwrap();

        assert_eq!(mask.as_slice(), &[0, 0, 0]);
    }

    #[test]
    fn replication_mask_try_build_unknown_column_error() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> =
            ["id", "unknown_column"].into_iter().map(String::from).collect();

        let result = ReplicationMask::try_build(&schema, &replicated_columns);

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            SchemaError::UnknownReplicatedColumns(columns) => {
                assert_eq!(columns, vec!["unknown_column".to_owned()]);
            }
            _ => panic!("expected UnknownReplicatedColumns error"),
        }
    }

    #[test]
    fn replication_mask_try_build_multiple_unknown_columns_error() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> =
            ["id", "foo", "bar"].into_iter().map(String::from).collect();

        let result = ReplicationMask::try_build(&schema, &replicated_columns);

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            SchemaError::UnknownReplicatedColumns(mut columns) => {
                columns.sort();
                assert_eq!(columns, vec!["bar".to_owned(), "foo".to_owned()]);
            }
            _ => panic!("expected UnknownReplicatedColumns error"),
        }
    }

    #[test]
    fn replication_mask_build_or_all_success() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> =
            ["id", "age"].into_iter().map(String::from).collect();

        let mask = ReplicationMask::build_or_all(&schema, &replicated_columns);

        assert_eq!(mask.as_slice(), &[1, 0, 1]);
    }

    #[test]
    fn replication_mask_build_or_all_falls_back_to_all() {
        let schema = create_test_table_schema();
        let replicated_columns: HashSet<String> =
            ["id", "unknown_column"].into_iter().map(String::from).collect();

        let mask = ReplicationMask::build_or_all(&schema, &replicated_columns);

        // Falls back to all columns being replicated.
        assert_eq!(mask.as_slice(), &[1, 1, 1]);
    }

    #[test]
    fn replication_mask_all() {
        let schema = create_test_table_schema();
        let mask = ReplicationMask::all(&schema);

        assert_eq!(mask.as_slice(), &[1, 1, 1]);
    }

    fn create_replicated_schema(columns: Vec<ColumnSchema>) -> ReplicatedTableSchema {
        let column_names: HashSet<String> = columns.iter().map(|c| c.name.clone()).collect();
        let table_schema = Arc::new(TableSchema::new(
            TableId::new(123),
            TableName::new("public".to_owned(), "test_table".to_owned()),
            columns,
        ));
        let mask = ReplicationMask::build(&table_schema, &column_names);
        ReplicatedTableSchema::from_mask(table_schema, mask)
    }

    fn text_column(name: &str, ordinal_position: i32) -> ColumnSchema {
        ColumnSchema::new(name.to_owned(), Type::TEXT, -1, ordinal_position, true)
    }

    fn plan_schema_change(
        old_schema: &ReplicatedTableSchema,
        new_schema: &ReplicatedTableSchema,
        column_name_mapping: ColumnNameMapping,
    ) -> SchemaPlan {
        old_schema.plan_schema_change(new_schema, column_name_mapping).unwrap()
    }

    fn operation_names(plan: &SchemaPlan) -> Vec<String> {
        plan.ordered_operations()
            .iter()
            .map(|operation| match operation {
                SchemaOperation::DropColumn { column_schema, .. } => {
                    format!("drop:{}", column_schema.name)
                }
                SchemaOperation::AddColumn { column_schema, .. } => {
                    format!("add:{}", column_schema.name)
                }
                SchemaOperation::AlterColumn {
                    column_schema,
                    alteration: ColumnAlteration::Rename { new_name },
                } => {
                    format!("rename:{}->{new_name}", column_schema.name)
                }
                SchemaOperation::AlterColumn {
                    column_schema,
                    alteration: ColumnAlteration::Type { .. },
                } => {
                    format!("modify-type:{}", column_schema.name)
                }
                SchemaOperation::AlterColumn {
                    column_schema,
                    alteration: ColumnAlteration::Nullability { .. },
                } => {
                    format!("modify-nullability:{}", column_schema.name)
                }
                SchemaOperation::AlterColumn {
                    column_schema,
                    alteration: ColumnAlteration::Default { .. },
                } => {
                    format!("modify-default:{}", column_schema.name)
                }
            })
            .collect()
    }

    fn assert_operations_converge(
        old_schema: &ReplicatedTableSchema,
        new_schema: &ReplicatedTableSchema,
    ) {
        assert_operations_converge_with_mapping(
            old_schema,
            new_schema,
            ColumnNameMapping::Identity,
        );
    }

    fn assert_operations_converge_with_mapping(
        old_schema: &ReplicatedTableSchema,
        new_schema: &ReplicatedTableSchema,
        column_name_mapping: ColumnNameMapping,
    ) {
        let plan = plan_schema_change(old_schema, new_schema, column_name_mapping);
        let mut columns_by_ordinal: BTreeMap<i32, ColumnSchema> = old_schema
            .destination_column_schemas(column_name_mapping)
            .map(|column| (column.ordinal_position, column))
            .collect();
        let mut occupied_names: HashMap<String, i32> = old_schema
            .destination_column_schemas(column_name_mapping)
            .map(|column| (column.name, column.ordinal_position))
            .collect();

        for operation in plan.ordered_operations() {
            match operation {
                SchemaOperation::DropColumn { column_schema, .. } => {
                    assert_eq!(
                        columns_by_ordinal.remove(&column_schema.ordinal_position),
                        Some(column_schema.clone())
                    );
                    assert_eq!(
                        occupied_names.remove(&column_name_mapping.map_name(&column_schema.name)),
                        Some(column_schema.ordinal_position)
                    );
                }
                SchemaOperation::AddColumn { column_schema, .. } => {
                    assert_eq!(
                        columns_by_ordinal
                            .insert(column_schema.ordinal_position, column_schema.clone()),
                        None
                    );
                    assert_eq!(
                        occupied_names.insert(
                            column_name_mapping.map_name(&column_schema.name),
                            column_schema.ordinal_position,
                        ),
                        None
                    );
                }
                SchemaOperation::AlterColumn { column_schema, alteration } => {
                    assert_eq!(
                        columns_by_ordinal.get(&column_schema.ordinal_position),
                        Some(column_schema)
                    );

                    let target_column_schema = alteration.apply_to(column_schema);
                    if let ColumnAlteration::Rename { .. } = alteration {
                        assert_eq!(
                            occupied_names.remove(&column_schema.name),
                            Some(column_schema.ordinal_position)
                        );
                        assert_eq!(
                            occupied_names.insert(
                                target_column_schema.name.clone(),
                                target_column_schema.ordinal_position,
                            ),
                            None
                        );
                    }
                    columns_by_ordinal
                        .insert(target_column_schema.ordinal_position, target_column_schema);
                }
            }
        }

        let expected: BTreeMap<i32, ColumnSchema> = new_schema
            .destination_column_schemas(column_name_mapping)
            .map(|column| (column.ordinal_position, column))
            .collect();
        assert_eq!(columns_by_ordinal, expected);
    }

    fn permutations(values: &mut [String], start: usize, output: &mut Vec<Vec<String>>) {
        if start == values.len() {
            output.push(values.to_vec());
            return;
        }

        for index in start..values.len() {
            values.swap(start, index);
            permutations(values, start + 1, output);
            values.swap(start, index);
        }
    }

    fn partial_permutations(
        values: &mut [String],
        start: usize,
        length: usize,
        output: &mut Vec<Vec<String>>,
    ) {
        if start == length {
            output.push(values[..length].to_vec());
            return;
        }

        for index in start..values.len() {
            values.swap(start, index);
            partial_permutations(values, start + 1, length, output);
            values.swap(start, index);
        }
    }

    #[test]
    fn identity_type_primary_key() {
        let schema = Arc::new(create_test_table_schema());
        let replication_mask = ReplicationMask::all(&schema);
        let replicated_table_schema = ReplicatedTableSchema::from_mask(schema, replication_mask);

        assert_eq!(replicated_table_schema.identity_type(), IdentityType::PrimaryKey);
    }

    #[test]
    fn identity_type_alternative_key() {
        let schema = Arc::new(create_test_table_schema());
        let replication_mask = ReplicationMask::all(&schema);
        let identity_mask = IdentityMask::from_bytes(vec![0, 1, 1]);
        let replicated_table_schema =
            ReplicatedTableSchema::from_masks(schema, replication_mask, identity_mask);

        assert_eq!(replicated_table_schema.identity_type(), IdentityType::AlternativeKey);
    }

    #[test]
    fn identity_type_full() {
        let schema = Arc::new(create_test_table_schema());
        let replication_mask = ReplicationMask::all(&schema);
        let identity_mask = IdentityMask::from_bytes(vec![1, 1, 1]);
        let replicated_table_schema =
            ReplicatedTableSchema::from_masks(schema, replication_mask, identity_mask);

        assert_eq!(replicated_table_schema.identity_type(), IdentityType::Full);
    }

    #[test]
    fn identity_type_missing() {
        let schema = Arc::new(create_test_table_schema());
        let replication_mask = ReplicationMask::all(&schema);
        let identity_mask = IdentityMask::from_bytes(vec![0, 0, 0]);
        let replicated_table_schema =
            ReplicatedTableSchema::from_masks(schema, replication_mask, identity_mask);

        assert_eq!(replicated_table_schema.identity_type(), IdentityType::Missing);
    }

    #[test]
    fn all_primary_key_columns_replicated_returns_true_for_complete_primary_key() {
        let schema = Arc::new(create_test_table_schema());
        let replication_mask = ReplicationMask::all(&schema);
        let replicated_table_schema = ReplicatedTableSchema::from_mask(schema, replication_mask);

        assert!(replicated_table_schema.all_primary_key_columns_replicated());
        assert_eq!(replicated_table_schema.unreplicated_primary_key_column_schemas().count(), 0);
    }

    #[test]
    fn all_primary_key_columns_replicated_returns_false_for_partial_primary_key() {
        let schema = Arc::new(TableSchema::new(
            TableId::new(123),
            TableName::new("public".to_owned(), "test_table".to_owned()),
            vec![
                ColumnSchema::new("tenant_id".to_owned(), Type::INT4, -1, 1, false)
                    .with_primary_key(1),
                ColumnSchema::new("id".to_owned(), Type::INT4, -1, 2, false).with_primary_key(2),
                ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 3, true),
            ],
        ));
        let replication_mask = ReplicationMask::from_bytes(vec![0, 1, 1]);
        let identity_mask = IdentityMask::from_bytes(vec![0, 1, 0]);
        let replicated_table_schema =
            ReplicatedTableSchema::from_masks(schema, replication_mask, identity_mask);

        let omitted_columns = replicated_table_schema
            .unreplicated_primary_key_column_schemas()
            .map(|column_schema| column_schema.name.as_str())
            .collect::<Vec<_>>();

        assert!(!replicated_table_schema.all_primary_key_columns_replicated());
        assert_eq!(omitted_columns, ["tenant_id"]);
    }

    #[test]
    fn schema_diff_no_changes() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(diff.is_empty());
        assert!(diff.columns_to_add.is_empty());
        assert!(diff.columns_to_drop.is_empty());
        assert!(diff.columns_to_alter.is_empty());
    }

    #[test]
    fn schema_diff_column_added() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, true),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(!diff.is_empty());
        assert_eq!(diff.columns_to_add.len(), 1);
        assert_eq!(diff.columns_to_add[0].column_schema.name, "email");
        assert_eq!(diff.columns_to_add[0].column_schema.ordinal_position, 3);
        assert_eq!(diff.columns_to_add[0].reason, ColumnPresenceChangeReason::TableSchema);
        assert!(diff.columns_to_drop.is_empty());
        assert!(diff.columns_to_alter.is_empty());
    }

    #[test]
    fn schema_diff_column_dropped() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("age".to_owned(), Type::INT4, -1, 3, true),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(!diff.is_empty());
        assert!(diff.columns_to_add.is_empty());
        assert_eq!(diff.columns_to_drop.len(), 1);
        assert_eq!(diff.columns_to_drop[0].column_schema.name, "age");
        assert_eq!(diff.columns_to_drop[0].column_schema.ordinal_position, 3);
        assert_eq!(diff.columns_to_drop[0].reason, ColumnPresenceChangeReason::TableSchema);
        assert!(diff.columns_to_alter.is_empty());
    }

    #[test]
    fn schema_diff_classifies_publication_mask_addition_and_drop() {
        let table_schema = Arc::new(create_test_table_schema());
        let without_age = ReplicatedTableSchema::from_mask(
            Arc::clone(&table_schema),
            ReplicationMask::from_bytes(vec![1, 1, 0]),
        );
        let with_age = ReplicatedTableSchema::from_mask(
            table_schema,
            ReplicationMask::from_bytes(vec![1, 1, 1]),
        );

        let addition = without_age.diff(&with_age);
        assert_eq!(addition.columns_to_add.len(), 1);
        assert_eq!(addition.columns_to_add[0].column_schema.name, "age");
        assert_eq!(addition.columns_to_add[0].reason, ColumnPresenceChangeReason::ReplicationMask);
        assert!(addition.columns_to_drop.is_empty());
        assert_eq!(
            without_age
                .plan_schema_change(&with_age, ColumnNameMapping::Identity)
                .unwrap()
                .ordered_operations(),
            &[SchemaOperation::AddColumn {
                column_schema: create_test_table_schema().column_schemas[2].clone(),
                reason: ColumnPresenceChangeReason::ReplicationMask,
            }]
        );

        let removal = with_age.diff(&without_age);
        assert_eq!(removal.columns_to_drop.len(), 1);
        assert_eq!(removal.columns_to_drop[0].column_schema.name, "age");
        assert_eq!(removal.columns_to_drop[0].reason, ColumnPresenceChangeReason::ReplicationMask);
        assert!(removal.columns_to_add.is_empty());
        assert_eq!(
            with_age
                .plan_schema_change(&without_age, ColumnNameMapping::Identity)
                .unwrap()
                .ordered_operations(),
            &[SchemaOperation::DropColumn {
                column_schema: create_test_table_schema().column_schemas[2].clone(),
                reason: ColumnPresenceChangeReason::ReplicationMask,
            }]
        );
    }

    #[test]
    fn schema_diff_column_renamed() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("full_name".to_owned(), Type::TEXT, -1, 2, true),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(!diff.is_empty());
        assert!(diff.columns_to_add.is_empty());
        assert!(diff.columns_to_drop.is_empty());
        assert_eq!(diff.columns_to_alter.len(), 1);
        let change = &diff.columns_to_alter[0];
        assert_eq!(change.previous_column_schema().ordinal_position, 2);
        assert_eq!(change.previous_column_schema().name, "name");
        assert_eq!(change.target_column_schema().name, "full_name");
        assert!(change.name_changed());
        assert!(!change.data_type_changed());
        assert!(!change.nullability_changed());
        assert!(!change.default_changed());
    }

    #[test]
    fn schema_diff_column_default_changed() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 2, true),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 2, true)
                .with_default_expression("'pending'::text".to_owned()),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(!diff.is_empty());
        assert!(diff.columns_to_add.is_empty());
        assert!(diff.columns_to_drop.is_empty());
        assert_eq!(diff.columns_to_alter.len(), 1);
        let change = &diff.columns_to_alter[0];
        assert_eq!(change.target_column_schema().name, "status");
        assert!(change.default_changed());
        assert_eq!(change.previous_column_schema().default_expression, None);
        assert_eq!(
            change.target_column_schema().default_expression.as_deref(),
            Some("'pending'::text")
        );
    }

    #[test]
    fn schema_diff_ignores_unchanged_column_default() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 2, true)
                .with_default_expression("'pending'::text".to_owned()),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 2, true)
                .with_default_expression("'pending'::text".to_owned()),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(diff.is_empty());
    }

    #[test]
    fn schema_diff_column_nullability_changed() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 2, false),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 2, true),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(!diff.is_empty());
        assert!(diff.columns_to_add.is_empty());
        assert!(diff.columns_to_drop.is_empty());
        assert_eq!(diff.columns_to_alter.len(), 1);
        let change = &diff.columns_to_alter[0];
        assert_eq!(change.target_column_schema().name, "email");
        assert!(change.nullability_changed());
        assert!(!change.previous_column_schema().nullable);
        assert!(change.target_column_schema().nullable);
    }

    #[test]
    fn schema_diff_records_primary_key_metadata_change() {
        let previous_schema = create_replicated_schema(vec![ColumnSchema::new(
            "id".to_owned(),
            Type::INT4,
            -1,
            1,
            false,
        )]);
        let target_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
        ]);

        let diff = previous_schema.diff(&target_schema);

        assert_eq!(diff.columns_to_alter.len(), 1);
        let change = &diff.columns_to_alter[0];
        assert!(change.primary_key_changed());
        assert!(!change.name_changed());
        assert!(!change.data_type_changed());
        assert!(!change.nullability_changed());
        assert!(!change.default_changed());
    }

    #[test]
    fn schema_plan_emits_column_type_changes() {
        for (previous_column, target_column, expected_operation_names) in [
            (
                ColumnSchema::new("value".to_owned(), Type::INT4, -1, 1, true),
                ColumnSchema::new("value".to_owned(), Type::INT8, -1, 1, true),
                vec!["modify-type:value"],
            ),
            (
                ColumnSchema::new("value".to_owned(), Type::VARCHAR, 14, 1, true),
                ColumnSchema::new("value".to_owned(), Type::VARCHAR, 24, 1, true),
                vec!["modify-type:value"],
            ),
            (
                ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 1, false)
                    .with_default_expression("'pending'::text".to_owned()),
                ColumnSchema::new("state".to_owned(), Type::VARCHAR, 24, 1, true)
                    .with_default_expression("'queued'::text".to_owned()),
                vec![
                    "rename:status->state",
                    "modify-type:state",
                    "modify-nullability:state",
                    "modify-default:state",
                ],
            ),
        ] {
            let previous_schema = create_replicated_schema(vec![previous_column.clone()]);
            let target_schema = create_replicated_schema(vec![target_column.clone()]);

            let plan = previous_schema
                .plan_schema_change(&target_schema, ColumnNameMapping::Identity)
                .unwrap();

            assert_eq!(plan.diff().columns_to_alter.len(), 1);
            assert!(plan.diff().columns_to_alter[0].data_type_changed());
            assert_eq!(operation_names(&plan), expected_operation_names);
            assert_operations_converge(&previous_schema, &target_schema);
        }
    }

    #[test]
    fn schema_diff_groups_multiple_changes_for_same_column() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("status".to_owned(), Type::TEXT, -1, 2, false)
                .with_default_expression("'pending'::text".to_owned()),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("state".to_owned(), Type::TEXT, -1, 2, true)
                .with_default_expression("'queued'::text".to_owned()),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(!diff.is_empty());
        assert!(diff.columns_to_add.is_empty());
        assert!(diff.columns_to_drop.is_empty());
        assert_eq!(diff.columns_to_alter.len(), 1);
        let change = &diff.columns_to_alter[0];
        assert_eq!(change.previous_column_schema().name, "status");
        assert_eq!(change.target_column_schema().name, "state");
        assert!(change.name_changed());
        assert!(!change.data_type_changed());
        assert!(change.nullability_changed());
        assert!(change.default_changed());

        let plan = old_schema.plan_schema_change(&new_schema, ColumnNameMapping::Identity).unwrap();
        assert_eq!(
            operation_names(&plan),
            ["rename:status->state", "modify-nullability:state", "modify-default:state"]
        );
    }

    #[test]
    fn schema_diff_mixed_operations() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("age".to_owned(), Type::INT4, -1, 3, true),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("full_name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 4, true),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(!diff.is_empty());

        assert_eq!(diff.columns_to_add.len(), 1);
        assert_eq!(diff.columns_to_add[0].column_schema.name, "email");

        assert_eq!(diff.columns_to_drop.len(), 1);
        assert_eq!(diff.columns_to_drop[0].column_schema.name, "age");

        assert_eq!(diff.columns_to_alter.len(), 1);
        assert_eq!(diff.columns_to_alter[0].previous_column_schema().name, "name");
        assert_eq!(diff.columns_to_alter[0].target_column_schema().name, "full_name");
        assert!(diff.columns_to_alter[0].name_changed());
    }

    #[test]
    fn schema_diff_multiple_additions() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, true),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert_eq!(diff.columns_to_add.len(), 2);
        let added_names: HashSet<&str> =
            diff.columns_to_add.iter().map(|change| change.column_schema.name.as_str()).collect();
        assert!(added_names.contains("name"));
        assert!(added_names.contains("email"));
        assert!(diff.columns_to_drop.is_empty());
        assert!(diff.columns_to_alter.is_empty());
    }

    #[test]
    fn schema_diff_multiple_drops() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
            ColumnSchema::new("name".to_owned(), Type::TEXT, -1, 2, true),
            ColumnSchema::new("email".to_owned(), Type::TEXT, -1, 3, true),
        ]);
        let new_schema = create_replicated_schema(vec![
            ColumnSchema::new("id".to_owned(), Type::INT4, -1, 1, false).with_primary_key(1),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(diff.columns_to_add.is_empty());
        assert_eq!(diff.columns_to_drop.len(), 2);
        let dropped_names: HashSet<&str> =
            diff.columns_to_drop.iter().map(|change| change.column_schema.name.as_str()).collect();
        assert!(dropped_names.contains("name"));
        assert!(dropped_names.contains("email"));
        assert!(diff.columns_to_alter.is_empty());
    }

    #[test]
    fn schema_plan_orders_name_reuse_transitions() {
        for (old_columns, new_columns, expected_operations) in [
            (
                vec![text_column("value", 1)],
                vec![text_column("value", 2)],
                vec!["drop:value", "add:value"],
            ),
            (
                vec![text_column("a", 1), text_column("b", 2)],
                vec![text_column("b", 1), text_column("c", 2)],
                vec!["rename:b->c", "rename:a->b"],
            ),
            (
                vec![text_column("a", 1)],
                vec![text_column("b", 1), text_column("a", 2)],
                vec!["rename:a->b", "add:a"],
            ),
            (
                vec![text_column("a", 1), text_column("b", 2)],
                vec![text_column("b", 1)],
                vec!["drop:b", "rename:a->b"],
            ),
        ] {
            let old_schema = create_replicated_schema(old_columns);
            let new_schema = create_replicated_schema(new_columns);
            let plan = plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::Identity);

            assert_eq!(operation_names(&plan), expected_operations);
            assert_operations_converge(&old_schema, &new_schema);
        }
    }

    #[test]
    fn schema_plan_plans_only_final_state_after_ddl_compression() {
        // The source may have renamed `a` through one or more transient names,
        // and may have dropped and recreated `value` repeatedly. With no DML
        // between those states, DDL snapshotting exposes only these endpoints.
        let old_schema =
            create_replicated_schema(vec![text_column("a", 1), text_column("value", 2)]);
        let new_schema =
            create_replicated_schema(vec![text_column("final_a", 1), text_column("value", 3)]);

        let plan = plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::Identity);

        assert_eq!(operation_names(&plan), ["drop:value", "rename:a->final_a", "add:value"]);
        assert_operations_converge(&old_schema, &new_schema);
    }

    #[test]
    fn schema_plan_uses_one_temporary_name_for_rename_cycle() {
        // PostgreSQL can produce this endpoint transition through staged
        // renames such as `a -> swap`, `b -> a`, and `swap -> b`. Without DML
        // between those statements, pgoutput exposes only the final relation
        // schema to the destination.
        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("b", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1), text_column("a", 2)]);

        let plan = plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::Identity);

        assert!(plan.has_rename_cycles());
        assert_eq!(
            operation_names(&plan),
            [
                "rename:a->supabase_etl_ddl_tmp_column_1_0",
                "rename:b->a",
                "rename:supabase_etl_ddl_tmp_column_1_0->b",
            ]
        );
        assert_operations_converge(&old_schema, &new_schema);
    }

    #[test]
    fn schema_plan_deterministically_breaks_each_disjoint_rename_cycle_once() {
        let old_schema = create_replicated_schema(vec![
            text_column("a", 1),
            text_column("b", 2),
            text_column("c", 3),
            text_column("d", 4),
        ]);
        let new_schema = create_replicated_schema(vec![
            text_column("b", 1),
            text_column("a", 2),
            text_column("d", 3),
            text_column("c", 4),
        ]);
        let expected = [
            "rename:a->supabase_etl_ddl_tmp_column_1_0",
            "rename:b->a",
            "rename:supabase_etl_ddl_tmp_column_1_0->b",
            "rename:c->supabase_etl_ddl_tmp_column_3_1",
            "rename:d->c",
            "rename:supabase_etl_ddl_tmp_column_3_1->d",
        ];

        let plan = plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::Identity);

        assert!(plan.has_rename_cycles());
        assert_eq!(operation_names(&plan), expected);
        assert_operations_converge(&old_schema, &new_schema);
    }

    #[test]
    fn schema_plan_temporary_name_avoids_endpoint_columns() {
        let old_schema = create_replicated_schema(vec![
            text_column("a", 1),
            text_column("b", 2),
            text_column("SUPABASE_ETL_DDL_TMP_COLUMN_1_0", 3),
        ]);
        let new_schema = create_replicated_schema(vec![
            text_column("b", 1),
            text_column("a", 2),
            text_column("SUPABASE_ETL_DDL_TMP_COLUMN_1_0", 3),
        ]);

        let plan = plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::AsciiLowercase);

        assert_eq!(
            operation_names(&plan),
            [
                "rename:a->supabase_etl_ddl_tmp_column_1_1",
                "rename:b->a",
                "rename:supabase_etl_ddl_tmp_column_1_1->b",
            ]
        );
    }

    #[test]
    fn schema_plan_respects_destination_column_name_mapping() {
        let old_schema = create_replicated_schema(vec![text_column("A", 1), text_column("b", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("B", 1), text_column("A", 2)]);

        let identity_plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::Identity);
        assert_eq!(operation_names(&identity_plan), ["rename:A->B", "rename:b->A"]);
        assert_operations_converge_with_mapping(
            &old_schema,
            &new_schema,
            ColumnNameMapping::Identity,
        );

        let lowercase_plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::AsciiLowercase);
        assert_eq!(
            operation_names(&lowercase_plan),
            [
                "rename:a->supabase_etl_ddl_tmp_column_1_0",
                "rename:b->a",
                "rename:supabase_etl_ddl_tmp_column_1_0->b",
            ]
        );
        assert_operations_converge_with_mapping(
            &old_schema,
            &new_schema,
            ColumnNameMapping::AsciiLowercase,
        );

        let old_schema = create_replicated_schema(vec![text_column("Ä", 1), text_column("b", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("B", 1), text_column("ä", 2)]);
        let unicode_lowercase_plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::UnicodeLowercase);
        assert_eq!(
            operation_names(&unicode_lowercase_plan),
            [
                "rename:ä->supabase_etl_ddl_tmp_column_1_0",
                "rename:b->ä",
                "rename:supabase_etl_ddl_tmp_column_1_0->b",
            ]
        );
        assert_operations_converge_with_mapping(
            &old_schema,
            &new_schema,
            ColumnNameMapping::UnicodeLowercase,
        );
    }

    #[test]
    fn destination_column_schemas_map_owned_copies_without_changing_source_schema() {
        let schema = create_replicated_schema(vec![text_column("Display_Name", 1)]);

        let destination_columns = schema
            .destination_column_schemas(ColumnNameMapping::AsciiLowercase)
            .collect::<Vec<_>>();

        assert_eq!(destination_columns[0].name, "display_name");
        assert_eq!(schema.column_schemas().next().unwrap().name, "Display_Name");
    }

    #[test]
    fn schema_plan_rejects_destination_equivalent_target_columns() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("c", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("a", 1), text_column("A", 2)]);

        // Diffing records the exact PostgreSQL rename independently of the
        // destination namespace in which it will eventually be planned.
        let diff = old_schema.diff(&new_schema);
        assert_eq!(diff.columns_to_alter.len(), 1);
        assert_eq!(diff.columns_to_alter[0].previous_column_schema().name, "c");
        assert_eq!(diff.columns_to_alter[0].target_column_schema().name, "A");

        for column_name_mapping in
            [ColumnNameMapping::AsciiLowercase, ColumnNameMapping::UnicodeLowercase]
        {
            let error =
                old_schema.plan_schema_change(&new_schema, column_name_mapping).unwrap_err();

            assert_eq!(
                error,
                SchemaPlanError::DestinationColumnNameCollision {
                    endpoint: SchemaEndpoint::Target,
                    column_name_mapping,
                    first_column_name: "a".to_owned(),
                    second_column_name: "A".to_owned(),
                }
            );
        }

        let identity_plan =
            old_schema.plan_schema_change(&new_schema, ColumnNameMapping::Identity).unwrap();
        assert_eq!(operation_names(&identity_plan), ["rename:c->A"]);
    }

    #[test]
    fn schema_plan_rejects_destination_equivalent_current_columns() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("A", 2)]);
        let new_schema = old_schema.clone();

        old_schema.validate_destination_column_names(ColumnNameMapping::Identity).unwrap();

        let error = old_schema
            .plan_schema_change(&new_schema, ColumnNameMapping::AsciiLowercase)
            .unwrap_err();

        assert_eq!(
            error,
            SchemaPlanError::DestinationColumnNameCollision {
                endpoint: SchemaEndpoint::Current,
                column_name_mapping: ColumnNameMapping::AsciiLowercase,
                first_column_name: "a".to_owned(),
                second_column_name: "A".to_owned(),
            }
        );
    }

    #[test]
    fn schema_plan_distinguishes_ascii_and_unicode_lowercase_mappings() {
        let old_schema = create_replicated_schema(vec![text_column("x", 1)]);
        let new_schema = create_replicated_schema(vec![
            text_column("x", 1),
            text_column("Ä", 2),
            text_column("ä", 3),
        ]);

        old_schema.plan_schema_change(&new_schema, ColumnNameMapping::AsciiLowercase).unwrap();
        let error = old_schema
            .plan_schema_change(&new_schema, ColumnNameMapping::UnicodeLowercase)
            .unwrap_err();

        assert_eq!(
            error,
            SchemaPlanError::DestinationColumnNameCollision {
                endpoint: SchemaEndpoint::Target,
                column_name_mapping: ColumnNameMapping::UnicodeLowercase,
                first_column_name: "Ä".to_owned(),
                second_column_name: "ä".to_owned(),
            }
        );
    }

    #[test]
    fn schema_plan_skips_equivalent_renames() {
        for (old_column_name, new_column_name) in [("a", "A"), ("A", "a")] {
            let old_schema = create_replicated_schema(vec![text_column(old_column_name, 1)]);
            let new_schema = create_replicated_schema(vec![text_column(new_column_name, 1)]);
            let plan = old_schema
                .plan_schema_change(&new_schema, ColumnNameMapping::AsciiLowercase)
                .unwrap();
            assert!(plan.diff().columns_to_alter[0].name_changed());
            assert!(plan.is_empty());
            assert!(plan.ordered_operations().is_empty());
            assert_operations_converge_with_mapping(
                &old_schema,
                &new_schema,
                ColumnNameMapping::AsciiLowercase,
            );
        }
    }

    #[test]
    fn schema_plan_applies_metadata_changes_after_skipping_an_equivalent_rename() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("A".to_owned(), Type::TEXT, -1, 1, false)
                .with_default_expression("'old'::text".to_owned()),
        ]);
        let new_schema = create_replicated_schema(vec![
            text_column("a", 1).with_default_expression("'new'::text".to_owned()),
        ]);

        let plan = plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::AsciiLowercase);

        let change = &plan.diff().columns_to_alter[0];
        assert!(change.name_changed());
        assert!(change.nullability_changed());
        assert!(change.default_changed());
        assert!(matches!(
            plan.ordered_operations(),
            [
                SchemaOperation::AlterColumn {
                    column_schema: nullability_column,
                    alteration: ColumnAlteration::Nullability { .. },
                },
                SchemaOperation::AlterColumn {
                    column_schema: default_column,
                    alteration: ColumnAlteration::Default { .. },
                },
            ] if nullability_column.name == "a" && default_column.name == "a"
        ));
        assert_operations_converge_with_mapping(
            &old_schema,
            &new_schema,
            ColumnNameMapping::AsciiLowercase,
        );
    }

    #[test]
    fn schema_plan_exhaustively_validates_case_insensitive_endpoint_transitions() {
        let old_schema = create_replicated_schema(vec![
            text_column("a", 1),
            text_column("b", 2),
            text_column("Ä", 3),
        ]);

        for column_name_mapping in
            [ColumnNameMapping::AsciiLowercase, ColumnNameMapping::UnicodeLowercase]
        {
            // Every subset of the three old and two possible new ordinals,
            // combined with every exact-name assignment, covers drops, adds,
            // chains, cycles, case-only renames, name reuse, and collisions.
            for ordinal_mask in 0_u8..(1 << 5) {
                let retained_ordinals: Vec<i32> = (1..=5)
                    .filter(|ordinal_position| ordinal_mask & (1 << (ordinal_position - 1)) != 0)
                    .collect();
                let mut available_names = vec![
                    "a".to_owned(),
                    "A".to_owned(),
                    "b".to_owned(),
                    "B".to_owned(),
                    "Ä".to_owned(),
                    "ä".to_owned(),
                ];
                let mut name_assignments = Vec::new();
                partial_permutations(
                    &mut available_names,
                    0,
                    retained_ordinals.len(),
                    &mut name_assignments,
                );

                for names in name_assignments {
                    let new_schema = create_replicated_schema(
                        retained_ordinals
                            .iter()
                            .zip(&names)
                            .map(|(&ordinal_position, name)| text_column(name, ordinal_position))
                            .collect(),
                    );
                    let unique_destination_name_count = names
                        .iter()
                        .map(|name| column_name_mapping.map_name(name))
                        .collect::<HashSet<_>>()
                        .len();
                    let result = old_schema.plan_schema_change(&new_schema, column_name_mapping);

                    if unique_destination_name_count != names.len() {
                        assert!(matches!(
                            result,
                            Err(SchemaPlanError::DestinationColumnNameCollision {
                                endpoint: SchemaEndpoint::Target,
                                column_name_mapping: error_mapping,
                                ..
                            }) if error_mapping == column_name_mapping
                        ));
                    } else {
                        let plan = result.unwrap();
                        assert_eq!(
                            plan,
                            plan_schema_change(&old_schema, &new_schema, column_name_mapping,)
                        );
                        assert_operations_converge_with_mapping(
                            &old_schema,
                            &new_schema,
                            column_name_mapping,
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn schema_plan_deterministically_converges_for_mixed_endpoint_schemas() {
        let old_schema = create_replicated_schema(vec![
            text_column("a", 1),
            text_column("b", 2),
            text_column("c", 3),
        ]);
        let mut names =
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned(), "d".to_owned(), "e".to_owned()];
        let mut name_permutations = Vec::new();
        permutations(&mut names, 0, &mut name_permutations);

        // Retaining or removing ordinals 1-3 and adding ordinals 4-5 covers
        // mixed rename, drop, add, name reuse, chain, and cycle transitions.
        for ordinal_mask in 0_u8..(1 << 5) {
            let retained_ordinals: Vec<i32> = (1..=5)
                .filter(|ordinal_position| ordinal_mask & (1 << (ordinal_position - 1)) != 0)
                .collect();

            for names in &name_permutations {
                let new_schema = create_replicated_schema(
                    retained_ordinals
                        .iter()
                        .zip(names)
                        .map(|(&ordinal_position, name)| text_column(name, ordinal_position))
                        .collect(),
                );
                let plan =
                    plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::Identity);

                assert_eq!(
                    plan,
                    plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::Identity)
                );
                assert_operations_converge(&old_schema, &new_schema);
            }
        }
    }

    #[test]
    fn schema_plan_applies_structural_operations_before_modifying_a_renamed_column() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("a".to_owned(), Type::TEXT, -1, 1, false)
                .with_default_expression("'old'::text".to_owned()),
            text_column("b", 2),
            text_column("unused", 3),
        ]);
        let new_schema = create_replicated_schema(vec![
            text_column("b", 1).with_default_expression("'new'::text".to_owned()),
            text_column("a", 4),
        ]);

        let plan = plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::Identity);

        assert!(matches!(
            plan.ordered_operations(),
            [
                SchemaOperation::DropColumn {
                    column_schema: blocking_drop,
                    reason: ColumnPresenceChangeReason::TableSchema,
                },
                SchemaOperation::DropColumn {
                    column_schema: unrelated_drop,
                    reason: ColumnPresenceChangeReason::TableSchema,
                },
                SchemaOperation::AlterColumn {
                    column_schema: rename_column_schema,
                    alteration: ColumnAlteration::Rename { new_name },
                },
                SchemaOperation::AddColumn {
                    column_schema: addition,
                    reason: ColumnPresenceChangeReason::TableSchema,
                },
                SchemaOperation::AlterColumn {
                    column_schema: nullability_column_schema,
                    alteration: ColumnAlteration::Nullability { .. },
                },
                SchemaOperation::AlterColumn {
                    column_schema: default_column_schema,
                    alteration: ColumnAlteration::Default { .. },
                },
            ] if blocking_drop.name == "b"
                && unrelated_drop.name == "unused"
                && rename_column_schema.name == "a"
                && new_name == "b"
                && addition.name == "a"
                && nullability_column_schema.name == "b"
                && default_column_schema.name == "b"
        ));
        assert_operations_converge(&old_schema, &new_schema);
    }

    #[test]
    fn schema_plan_modifies_cycle_member_after_its_final_rename() {
        let old_schema = create_replicated_schema(vec![
            ColumnSchema::new("a".to_owned(), Type::TEXT, -1, 1, false)
                .with_default_expression("'old'::text".to_owned()),
            text_column("b", 2),
        ]);
        let new_schema = create_replicated_schema(vec![
            text_column("b", 1).with_default_expression("'new'::text".to_owned()),
            text_column("a", 2),
        ]);

        let plan = plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::Identity);

        assert!(matches!(
            plan.ordered_operations(),
            [
                SchemaOperation::AlterColumn {
                    column_schema: first_column_schema,
                    alteration: ColumnAlteration::Rename {
                        new_name: temporary_name,
                    },
                },
                SchemaOperation::AlterColumn {
                    column_schema: second_column_schema,
                    alteration: ColumnAlteration::Rename {
                        new_name: second_new_name,
                    },
                },
                SchemaOperation::AlterColumn {
                    column_schema: final_column_schema,
                    alteration: ColumnAlteration::Rename {
                        new_name: final_new_name,
                    },
                },
                SchemaOperation::AlterColumn {
                    column_schema: nullability_column_schema,
                    alteration: ColumnAlteration::Nullability { .. },
                },
                SchemaOperation::AlterColumn {
                    column_schema: default_column_schema,
                    alteration: ColumnAlteration::Default { .. },
                },
            ] if first_column_schema.name == "a"
                && temporary_name.starts_with(DDL_TEMPORARY_COLUMN_PREFIX)
                && second_column_schema.name == "b"
                && second_new_name == "a"
                && final_column_schema.name == *temporary_name
                && final_new_name == "b"
                && nullability_column_schema.name == "b"
                && default_column_schema.name == "b"
        ));
        assert_operations_converge(&old_schema, &new_schema);
    }

    #[test]
    fn schema_plan_handles_columns_constructed_out_of_ordinal_order() {
        let old_schema = create_replicated_schema(vec![text_column("b", 2), text_column("a", 1)]);
        let new_schema = create_replicated_schema(vec![text_column("c", 3), text_column("b", 2)]);

        let plan = plan_schema_change(&old_schema, &new_schema, ColumnNameMapping::Identity);

        assert_eq!(operation_names(&plan), ["drop:a", "add:c"]);
        assert_operations_converge(&old_schema, &new_schema);
    }
}
