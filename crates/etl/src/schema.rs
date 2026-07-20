//! ETL schema projection and replication schema model.
//!
//! This module owns the schema view that ETL exposes to destinations and event
//! consumers. Base Postgres schema identifiers are shared from `etl-postgres`,
//! while replication masks and projected schemas live here with the ETL domain.

use std::{
    collections::{HashMap, HashSet},
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
    /// - Positions in old but not in new are columns to remove.
    /// - Positions in new but not in old are columns to add.
    pub fn diff(&self, new_schema: &ReplicatedTableSchema) -> SchemaDiff {
        // Build maps: ordinal_position -> ColumnSchema for replicated columns only.
        let old_columns: HashMap<i32, &ColumnSchema> =
            self.column_schemas().map(|col| (col.ordinal_position, col)).collect();

        let new_columns: HashMap<i32, &ColumnSchema> =
            new_schema.column_schemas().map(|col| (col.ordinal_position, col)).collect();

        // Same ordinal position means the same logical column, even if the
        // name or other column metadata changed.
        let mut columns_to_change = Vec::new();
        let mut columns_to_remove = Vec::new();
        for (&ordinal_position, &old_column) in &old_columns {
            match new_columns.get(&ordinal_position) {
                Some(&new_column) => {
                    let mut modifications = Vec::new();

                    if old_column.name != new_column.name {
                        modifications.push(ColumnModification::Rename {
                            old_name: old_column.name.clone(),
                            new_name: new_column.name.clone(),
                        });
                    }

                    if old_column.nullable != new_column.nullable {
                        modifications.push(ColumnModification::Nullability {
                            old_nullable: old_column.nullable,
                            new_nullable: new_column.nullable,
                        });
                    }

                    if old_column.default_expression != new_column.default_expression {
                        modifications.push(ColumnModification::Default {
                            old_expression: old_column.default_expression.clone(),
                            new_expression: new_column.default_expression.clone(),
                        });
                    }

                    if !modifications.is_empty() {
                        columns_to_change.push(ColumnChange {
                            ordinal_position,
                            old_column: old_column.clone(),
                            new_column: new_column.clone(),
                            modifications,
                        });
                    }
                }
                None => columns_to_remove.push(old_column.clone()),
            }
        }
        columns_to_change.sort_by_key(|c| c.ordinal_position);
        columns_to_remove.sort_by_key(|c| c.ordinal_position);

        // Columns to add: positions present only in the new schema.
        let mut columns_to_add: Vec<ColumnSchema> = new_columns
            .iter()
            .filter(|(ordinal_position, _)| !old_columns.contains_key(ordinal_position))
            .map(|(_, &column)| column.clone())
            .collect();
        columns_to_add.sort_by_key(|c| c.ordinal_position);

        SchemaDiff::new(
            columns_to_add,
            columns_to_remove,
            columns_to_change,
            old_columns.values().map(|column| column.name.as_str()),
        )
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

/// Represents differences between two schema versions.
///
/// Used to determine what schema changes need to be applied to a destination
/// when the source schema has evolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDiff {
    /// Columns that need to be added to the destination.
    pub columns_to_add: Vec<ColumnSchema>,
    /// Columns that need to be removed from the destination.
    pub columns_to_remove: Vec<ColumnSchema>,
    /// Existing columns that need to be changed in the destination.
    pub columns_to_change: Vec<ColumnChange>,
    /// Operations in a namespace-safe execution order.
    operations: Vec<SchemaOperation>,
}

impl SchemaDiff {
    /// Builds a diff from already classified column changes.
    ///
    /// Prefer [`ReplicatedTableSchema::diff`] when both endpoint schemas are
    /// available. `existing_column_names` must include every currently
    /// occupied destination name so any generated cycle-breaking name is
    /// collision-free.
    pub fn new<I, S>(
        columns_to_add: Vec<ColumnSchema>,
        columns_to_remove: Vec<ColumnSchema>,
        columns_to_change: Vec<ColumnChange>,
        existing_column_names: I,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let reserved_column_names = existing_column_names
            .into_iter()
            .map(Into::into)
            .chain(columns_to_add.iter().map(|column| column.name.clone()))
            .chain(columns_to_remove.iter().map(|column| column.name.clone()))
            .chain(columns_to_change.iter().flat_map(|change| {
                [change.old_column.name.clone(), change.new_column.name.clone()]
            }))
            .collect();

        Self::with_reserved_column_names(
            columns_to_add,
            columns_to_remove,
            columns_to_change,
            reserved_column_names,
        )
    }

    /// Builds a diff while reserving every endpoint column name.
    fn with_reserved_column_names(
        columns_to_add: Vec<ColumnSchema>,
        columns_to_remove: Vec<ColumnSchema>,
        columns_to_change: Vec<ColumnChange>,
        reserved_column_names: HashSet<String>,
    ) -> Self {
        let operations = plan_schema_operations(
            &columns_to_add,
            &columns_to_remove,
            &columns_to_change,
            reserved_column_names,
        );

        Self { columns_to_add, columns_to_remove, columns_to_change, operations }
    }

    /// Returns `true` if there are no schema changes.
    pub fn is_empty(&self) -> bool {
        self.columns_to_add.is_empty()
            && self.columns_to_remove.is_empty()
            && self.columns_to_change.is_empty()
    }

    /// Returns destination operations in the order they must be applied.
    pub fn operations(&self) -> &[SchemaOperation] {
        &self.operations
    }
}

/// One ordered operation in a destination schema transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaOperation {
    /// Drop a column before its name is reused, or after other operations when
    /// its name does not block the transition.
    DropColumn {
        /// The source column being removed.
        column: ColumnSchema,
    },
    /// Rename an existing logical column.
    RenameColumn {
        /// The ordinal position identifying the logical column.
        ordinal_position: i32,
        /// The currently occupied column name.
        old_name: String,
        /// The name to occupy after the operation.
        new_name: String,
        /// Whether this rename only breaks a namespace cycle.
        temporary: bool,
    },
    /// Add a new logical column.
    AddColumn {
        /// The source column being added.
        column: ColumnSchema,
    },
    /// Apply a non-rename change after the column reaches its final name.
    ModifyColumn {
        /// The complete logical column change.
        change: ColumnChange,
        /// The individual non-rename modification to apply.
        modification: ColumnModification,
    },
}

/// A pending logical rename tracked by ordinal identity.
struct PendingRename {
    /// The ordinal position identifying the logical column.
    ordinal_position: i32,
    /// The name currently occupied by the logical column.
    current_name: String,
    /// The final target name from the new schema.
    target_name: String,
}

/// Plans operations that converge from the old namespace to the new one.
fn plan_schema_operations(
    columns_to_add: &[ColumnSchema],
    columns_to_remove: &[ColumnSchema],
    columns_to_change: &[ColumnChange],
    mut reserved_column_names: HashSet<String>,
) -> Vec<SchemaOperation> {
    let add_names: HashSet<&str> =
        columns_to_add.iter().map(|column| column.name.as_str()).collect();
    let rename_target_names: HashSet<&str> = columns_to_change
        .iter()
        .flat_map(|change| &change.modifications)
        .filter_map(|modification| match modification {
            ColumnModification::Rename { new_name, .. } => Some(new_name.as_str()),
            ColumnModification::Default { .. } | ColumnModification::Nullability { .. } => None,
        })
        .collect();
    let mut occupied_names: HashMap<String, i32> = columns_to_remove
        .iter()
        .map(|column| (column.name.clone(), column.ordinal_position))
        .chain(
            columns_to_change
                .iter()
                .map(|change| (change.old_column.name.clone(), change.ordinal_position)),
        )
        .collect();
    let mut operations = Vec::new();
    let mut deferred_removals = Vec::new();

    // Real drops are preferable to generated renames when they can free a
    // target name. Non-blocking drops are delayed until the namespace has
    // otherwise converged.
    for column in columns_to_remove {
        if add_names.contains(column.name.as_str())
            || rename_target_names.contains(column.name.as_str())
        {
            occupied_names.remove(&column.name);
            operations.push(SchemaOperation::DropColumn { column: column.clone() });
        } else {
            deferred_removals.push(column);
        }
    }

    let mut pending_renames: Vec<_> = columns_to_change
        .iter()
        .filter_map(|change| {
            change.modifications.iter().find_map(|modification| match modification {
                ColumnModification::Rename { old_name, new_name } => Some(PendingRename {
                    ordinal_position: change.ordinal_position,
                    current_name: old_name.clone(),
                    target_name: new_name.clone(),
                }),
                ColumnModification::Default { .. } | ColumnModification::Nullability { .. } => None,
            })
        })
        .collect();
    pending_renames.sort_by_key(|rename| rename.ordinal_position);
    let mut temporary_name_sequence = 0_u64;

    while !pending_renames.is_empty() {
        if let Some(index) = pending_renames
            .iter()
            .position(|rename| !occupied_names.contains_key(&rename.target_name))
        {
            let rename = pending_renames.remove(index);
            occupied_names.remove(&rename.current_name);
            occupied_names.insert(rename.target_name.clone(), rename.ordinal_position);
            operations.push(SchemaOperation::RenameColumn {
                ordinal_position: rename.ordinal_position,
                old_name: rename.current_name,
                new_name: rename.target_name,
                temporary: false,
            });
            continue;
        }

        // Every remaining target is occupied by another pending rename, so
        // the endpoint schemas contain a rename cycle. The source transaction
        // necessarily used an intermediate name (or an equivalent recreate),
        // but endpoint diffing cannot recover it. Generate exactly one free
        // name to break the cycle and then continue with ordinary renames.
        let rename = &mut pending_renames[0];
        let temporary_name = loop {
            let candidate = format!(
                "supabase_etl_schema_tmp_{}_{}",
                rename.ordinal_position, temporary_name_sequence
            );
            temporary_name_sequence += 1;
            if reserved_column_names.insert(candidate.clone()) {
                break candidate;
            }
        };

        occupied_names.remove(&rename.current_name);
        occupied_names.insert(temporary_name.clone(), rename.ordinal_position);
        operations.push(SchemaOperation::RenameColumn {
            ordinal_position: rename.ordinal_position,
            old_name: std::mem::replace(&mut rename.current_name, temporary_name.clone()),
            new_name: temporary_name,
            temporary: true,
        });
    }

    for change in columns_to_change {
        for modification in &change.modifications {
            if matches!(modification, ColumnModification::Rename { .. }) {
                continue;
            }
            operations.push(SchemaOperation::ModifyColumn {
                change: change.clone(),
                modification: modification.clone(),
            });
        }
    }

    for column in columns_to_add {
        operations.push(SchemaOperation::AddColumn { column: column.clone() });
    }

    for column in deferred_removals {
        operations.push(SchemaOperation::DropColumn { column: column.clone() });
    }

    operations
}

/// Represents a change to an existing logical column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnChange {
    /// The column ordinal position used to identify the logical column.
    pub ordinal_position: i32,
    /// The previous column schema.
    pub old_column: ColumnSchema,
    /// The new column schema.
    pub new_column: ColumnSchema,
    /// The concrete modifications detected for this logical column.
    pub modifications: Vec<ColumnModification>,
}

/// Represents a single column modification within a [`ColumnChange`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnModification {
    /// The column was renamed.
    Rename {
        /// The old name of the column.
        old_name: String,
        /// The new name of the column.
        new_name: String,
    },
    /// The column default expression changed.
    Default {
        /// The previous default expression, if one existed.
        old_expression: Option<String>,
        /// The new default expression, if one exists.
        new_expression: Option<String>,
    },
    /// The column nullability changed.
    Nullability {
        /// The previous nullability.
        old_nullable: bool,
        /// The new nullability.
        new_nullable: bool,
    },
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

    fn operation_names(diff: &SchemaDiff) -> Vec<String> {
        diff.operations()
            .iter()
            .map(|operation| match operation {
                SchemaOperation::DropColumn { column } => format!("drop:{}", column.name),
                SchemaOperation::RenameColumn { old_name, new_name, temporary, .. } => {
                    format!("rename:{old_name}->{new_name}:{temporary}")
                }
                SchemaOperation::AddColumn { column } => format!("add:{}", column.name),
                SchemaOperation::ModifyColumn { change, modification } => {
                    format!("modify:{}:{modification:?}", change.new_column.name)
                }
            })
            .collect()
    }

    fn assert_operations_converge(
        old_schema: &ReplicatedTableSchema,
        new_schema: &ReplicatedTableSchema,
    ) {
        let diff = old_schema.diff(new_schema);
        let mut occupied: HashMap<String, i32> = old_schema
            .column_schemas()
            .map(|column| (column.name.clone(), column.ordinal_position))
            .collect();

        for operation in diff.operations() {
            match operation {
                SchemaOperation::DropColumn { column } => {
                    assert_eq!(occupied.remove(&column.name), Some(column.ordinal_position));
                }
                SchemaOperation::RenameColumn { ordinal_position, old_name, new_name, .. } => {
                    assert_eq!(occupied.remove(old_name), Some(*ordinal_position));
                    assert_eq!(occupied.insert(new_name.clone(), *ordinal_position), None);
                }
                SchemaOperation::AddColumn { column } => {
                    assert_eq!(occupied.insert(column.name.clone(), column.ordinal_position), None);
                }
                SchemaOperation::ModifyColumn { .. } => {}
            }
        }

        let expected: HashMap<String, i32> = new_schema
            .column_schemas()
            .map(|column| (column.name.clone(), column.ordinal_position))
            .collect();
        assert_eq!(occupied, expected);
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
        assert!(diff.columns_to_remove.is_empty());
        assert!(diff.columns_to_change.is_empty());
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
        assert_eq!(diff.columns_to_add[0].name, "email");
        assert_eq!(diff.columns_to_add[0].ordinal_position, 3);
        assert!(diff.columns_to_remove.is_empty());
        assert!(diff.columns_to_change.is_empty());
    }

    #[test]
    fn schema_diff_column_removed() {
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
        assert_eq!(diff.columns_to_remove.len(), 1);
        assert_eq!(diff.columns_to_remove[0].name, "age");
        assert_eq!(diff.columns_to_remove[0].ordinal_position, 3);
        assert!(diff.columns_to_change.is_empty());
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
        assert!(diff.columns_to_remove.is_empty());
        assert_eq!(diff.columns_to_change.len(), 1);
        assert_eq!(diff.columns_to_change[0].ordinal_position, 2);
        assert_eq!(
            diff.columns_to_change[0].modifications,
            vec![ColumnModification::Rename {
                old_name: "name".to_owned(),
                new_name: "full_name".to_owned(),
            }]
        );
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
        assert!(diff.columns_to_remove.is_empty());
        assert_eq!(diff.columns_to_change.len(), 1);
        assert_eq!(diff.columns_to_change[0].new_column.name, "status");
        assert_eq!(
            diff.columns_to_change[0].modifications,
            vec![ColumnModification::Default {
                old_expression: None,
                new_expression: Some("'pending'::text".to_owned()),
            }]
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
        assert!(diff.columns_to_remove.is_empty());
        assert_eq!(diff.columns_to_change.len(), 1);
        assert_eq!(diff.columns_to_change[0].new_column.name, "email");
        assert_eq!(
            diff.columns_to_change[0].modifications,
            vec![ColumnModification::Nullability { old_nullable: false, new_nullable: true }]
        );
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
        assert!(diff.columns_to_remove.is_empty());
        assert_eq!(diff.columns_to_change.len(), 1);
        assert_eq!(diff.columns_to_change[0].old_column.name, "status");
        assert_eq!(diff.columns_to_change[0].new_column.name, "state");
        assert_eq!(
            diff.columns_to_change[0].modifications,
            vec![
                ColumnModification::Rename {
                    old_name: "status".to_owned(),
                    new_name: "state".to_owned(),
                },
                ColumnModification::Nullability { old_nullable: false, new_nullable: true },
                ColumnModification::Default {
                    old_expression: Some("'pending'::text".to_owned()),
                    new_expression: Some("'queued'::text".to_owned()),
                },
            ]
        );
    }

    #[test]
    fn schema_diff_mixed_operations() {
        // Old schema: id (pos 1), name (pos 2), age (pos 3)
        // New schema: id (pos 1), full_name (pos 2), email (pos 4)
        // Expected: age removed (pos 3), name -> full_name renamed (pos 2), email added
        // (pos 4)
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

        // Column added: email at position 4.
        assert_eq!(diff.columns_to_add.len(), 1);
        assert_eq!(diff.columns_to_add[0].name, "email");

        // Column removed: age at position 3.
        assert_eq!(diff.columns_to_remove.len(), 1);
        assert_eq!(diff.columns_to_remove[0].name, "age");

        // Column changed: name -> full_name at position 2.
        assert_eq!(diff.columns_to_change.len(), 1);
        assert_eq!(
            diff.columns_to_change[0].modifications,
            vec![ColumnModification::Rename {
                old_name: "name".to_owned(),
                new_name: "full_name".to_owned(),
            }]
        );
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
            diff.columns_to_add.iter().map(|c| c.name.as_str()).collect();
        assert!(added_names.contains("name"));
        assert!(added_names.contains("email"));
        assert!(diff.columns_to_remove.is_empty());
        assert!(diff.columns_to_change.is_empty());
    }

    #[test]
    fn schema_diff_multiple_removals() {
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
        assert_eq!(diff.columns_to_remove.len(), 2);
        let removed_names: HashSet<&str> =
            diff.columns_to_remove.iter().map(|c| c.name.as_str()).collect();
        assert!(removed_names.contains("name"));
        assert!(removed_names.contains("email"));
        assert!(diff.columns_to_change.is_empty());
    }

    #[test]
    fn schema_diff_orders_drop_before_reusing_name_for_add() {
        let old_schema = create_replicated_schema(vec![text_column("value", 1)]);
        let new_schema = create_replicated_schema(vec![text_column("value", 2)]);

        let diff = old_schema.diff(&new_schema);

        assert_eq!(operation_names(&diff), ["drop:value", "add:value"]);
    }

    #[test]
    fn schema_diff_orders_rename_chain_without_temporary_name() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("b", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1), text_column("c", 2)]);

        let diff = old_schema.diff(&new_schema);

        assert_eq!(operation_names(&diff), ["rename:b->c:false", "rename:a->b:false"]);
    }

    #[test]
    fn schema_diff_orders_rename_before_adding_old_name() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1), text_column("a", 2)]);

        let diff = old_schema.diff(&new_schema);

        assert_eq!(operation_names(&diff), ["rename:a->b:false", "add:a"]);
    }

    #[test]
    fn schema_diff_orders_blocking_drop_before_rename() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("b", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1)]);

        let diff = old_schema.diff(&new_schema);

        assert_eq!(operation_names(&diff), ["drop:b", "rename:a->b:false"]);
    }

    #[test]
    fn schema_diff_defers_non_blocking_drop() {
        let old_schema =
            create_replicated_schema(vec![text_column("a", 1), text_column("unused", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1)]);

        let diff = old_schema.diff(&new_schema);

        assert_eq!(operation_names(&diff), ["rename:a->b:false", "drop:unused"]);
    }

    #[test]
    fn schema_diff_uses_one_temporary_name_for_rename_cycle() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("b", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1), text_column("a", 2)]);

        let diff = old_schema.diff(&new_schema);

        assert_eq!(
            operation_names(&diff),
            [
                "rename:a->supabase_etl_schema_tmp_1_0:true",
                "rename:b->a:false",
                "rename:supabase_etl_schema_tmp_1_0->b:false",
            ]
        );
    }

    #[test]
    fn schema_diff_temporary_name_avoids_endpoint_columns() {
        let old_schema = create_replicated_schema(vec![
            text_column("a", 1),
            text_column("b", 2),
            text_column("supabase_etl_schema_tmp_1_0", 3),
        ]);
        let new_schema = create_replicated_schema(vec![
            text_column("b", 1),
            text_column("a", 2),
            text_column("supabase_etl_schema_tmp_1_0", 3),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert_eq!(
            operation_names(&diff),
            [
                "rename:a->supabase_etl_schema_tmp_1_1:true",
                "rename:b->a:false",
                "rename:supabase_etl_schema_tmp_1_1->b:false",
            ]
        );
    }

    #[test]
    fn schema_diff_operations_converge_for_every_four_column_rename_permutation() {
        let old_schema = create_replicated_schema(vec![
            text_column("a", 1),
            text_column("b", 2),
            text_column("c", 3),
            text_column("d", 4),
        ]);
        let mut names = vec!["a".to_owned(), "b".to_owned(), "c".to_owned(), "d".to_owned()];
        let mut name_permutations = Vec::new();
        permutations(&mut names, 0, &mut name_permutations);

        for names in name_permutations {
            let new_schema = create_replicated_schema(
                names
                    .into_iter()
                    .enumerate()
                    .map(|(index, name)| text_column(&name, i32::try_from(index + 1).unwrap()))
                    .collect(),
            );

            assert_operations_converge(&old_schema, &new_schema);
        }
    }

    #[test]
    fn schema_diff_modifies_column_after_final_rename() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1)]);
        let new_schema = create_replicated_schema(vec![
            text_column("b", 1).with_default_expression("'new'::text".to_owned()),
        ]);

        let diff = old_schema.diff(&new_schema);

        assert!(matches!(
            diff.operations(),
            [
                SchemaOperation::RenameColumn { new_name, temporary: false, .. },
                SchemaOperation::ModifyColumn { change, .. },
            ] if new_name == "b" && change.new_column.name == "b"
        ));
    }
}
