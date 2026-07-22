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
const SCHEMA_TEMPORARY_COLUMN_PREFIX: &str = "supabase_etl_schema_tmp_";

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

/// Defines when a destination treats two column names as the same identifier.
///
/// This equivalence is used only to validate destination namespaces and plan
/// physical schema operations. Logical columns remain identified by their
/// PostgreSQL ordinal positions, and emitted operations retain exact source
/// names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnNameEquivalence {
    /// Column names that differ only by case are distinct.
    CaseSensitive,
    /// Column names are compared after Unicode lowercase conversion.
    UnicodeCaseInsensitive,
    /// Column names that differ only by ASCII case occupy the same namespace
    /// entry.
    AsciiCaseInsensitive,
}

impl ColumnNameEquivalence {
    /// Returns a key representing the destination identifier equivalence class.
    ///
    /// The returned value is only a comparison key. It is never used as the
    /// physical column name emitted to a destination.
    fn equivalence_key(self, name: &str) -> String {
        match self {
            Self::CaseSensitive => name.to_owned(),
            Self::UnicodeCaseInsensitive => name.to_lowercase(),
            Self::AsciiCaseInsensitive => name.to_ascii_lowercase(),
        }
    }
}

impl fmt::Display for ColumnNameEquivalence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CaseSensitive => write!(f, "case-sensitive"),
            Self::UnicodeCaseInsensitive => write!(f, "Unicode case-insensitive"),
            Self::AsciiCaseInsensitive => write!(f, "ASCII case-insensitive"),
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
        let mut columns_to_modify = Vec::new();
        let mut columns_to_drop = Vec::new();

        while let (Some(&old_column), Some(&new_column)) =
            (old_columns.get(old_index), new_columns.get(new_index))
        {
            match old_column.ordinal_position.cmp(&new_column.ordinal_position) {
                Ordering::Less => {
                    columns_to_drop.push(old_column.clone());
                    old_index += 1;
                }
                Ordering::Greater => {
                    columns_to_add.push(new_column.clone());
                    new_index += 1;
                }
                Ordering::Equal => {
                    // Equal `attnum` means the same logical column. A rename
                    // and its metadata changes therefore stay grouped even
                    // when the endpoint name changed.
                    let mut modification_types = Vec::new();

                    if old_column.name != new_column.name {
                        modification_types.push(ColumnModificationType::Rename);
                    }

                    if old_column.nullable != new_column.nullable {
                        modification_types.push(ColumnModificationType::Nullability);
                    }

                    if old_column.default_expression != new_column.default_expression {
                        modification_types.push(ColumnModificationType::Default);
                    }

                    if !modification_types.is_empty() {
                        columns_to_modify.push(ColumnModification {
                            old_column_schema: old_column.clone(),
                            new_column_schema: new_column.clone(),
                            modification_types,
                        });
                    }

                    old_index += 1;
                    new_index += 1;
                }
            }
        }

        columns_to_drop.extend(old_columns[old_index..].iter().map(|column| (**column).clone()));
        columns_to_add.extend(new_columns[new_index..].iter().map(|column| (**column).clone()));

        SchemaDiff::new(columns_to_add, columns_to_drop, columns_to_modify)
    }

    /// Computes the exact endpoint diff and plans it for a destination.
    ///
    /// The diff is independent of destination identifier behavior. Planning
    /// then validates both endpoint namespaces and orders physical operations
    /// using `column_name_equivalence`.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaPlanError`] when the destination cannot represent an
    /// endpoint namespace or the derived diff cannot be planned safely.
    pub fn plan_schema_change(
        &self,
        new_schema: &ReplicatedTableSchema,
        column_name_equivalence: ColumnNameEquivalence,
    ) -> Result<SchemaPlan, SchemaPlanError> {
        self.diff(new_schema)
            .plan(self.column_schemas().map(|column| column.name.as_str()), column_name_equivalence)
    }

    /// Validates that replicated column names are unique for a destination.
    ///
    /// Exact PostgreSQL names remain unchanged. This method only checks that
    /// no two names belong to the same destination equivalence class.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaPlanError::DestinationColumnNameCollision`] when two
    /// replicated columns would occupy the same destination namespace entry.
    pub fn validate_destination_column_names(
        &self,
        column_name_equivalence: ColumnNameEquivalence,
    ) -> Result<(), SchemaPlanError> {
        let column_names: Vec<_> =
            self.column_schemas().map(|column| column.name.clone()).collect();
        collect_unique_column_names(
            &column_names,
            column_name_equivalence,
            SchemaEndpoint::Target,
        )?;
        Ok(())
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

/// Identifies the column field changed by a [`ColumnModification`].
///
/// Variant order is the canonical per-column order: establish the final name,
/// then apply nullability and default metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnModificationType {
    /// The column was renamed.
    Rename,
    /// The column nullability changed.
    Nullability,
    /// The column default expression changed.
    Default,
}

/// Represents all endpoint differences for one existing logical column.
///
/// The old and new schemas have the same PostgreSQL ordinal position, which is
/// the logical-column identity used by endpoint diffing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnModification {
    /// The old endpoint column schema.
    pub old_column_schema: ColumnSchema,
    /// The new endpoint column schema.
    pub new_column_schema: ColumnSchema,
    /// The fields that differ between the endpoint schemas.
    pub modification_types: Vec<ColumnModificationType>,
}

/// One directly executable operation in a destination schema transition.
///
/// Modification operations carry the immediate old and new column states.
/// Destinations use `modification_type` to select the field to apply and may
/// inspect the remaining schema metadata for destination-specific validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaOperation {
    /// Drop a column that is absent from the new endpoint schema.
    DropColumn {
        /// The old endpoint column schema.
        column_schema: ColumnSchema,
    },
    /// Add a new logical column.
    AddColumn {
        /// The new endpoint column schema.
        column_schema: ColumnSchema,
    },
    /// Modify one field of an existing logical column.
    ModifyColumn {
        /// The column state immediately before this operation.
        old_column_schema: ColumnSchema,
        /// The column state immediately after this operation.
        new_column_schema: ColumnSchema,
        /// The field this operation changes.
        modification_type: ColumnModificationType,
    },
}

/// Represents differences between two schema versions.
///
/// This type contains only exact facts from the PostgreSQL endpoint snapshots.
/// It does not apply destination identifier semantics or contain generated
/// temporary renames. Use [`SchemaDiff::plan`] to validate destination name
/// compatibility and produce an executable [`SchemaPlan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDiff {
    /// Columns that need to be added to the destination.
    pub columns_to_add: Vec<ColumnSchema>,
    /// Columns that need to be dropped from the destination.
    pub columns_to_drop: Vec<ColumnSchema>,
    /// Existing columns that need to be modified in the destination.
    pub columns_to_modify: Vec<ColumnModification>,
}

impl SchemaDiff {
    /// Builds a diff from already classified column operations.
    ///
    /// Prefer [`ReplicatedTableSchema::diff`] when both endpoint schemas are
    /// available. The column vectors and each modification-type vector must be
    /// in PostgreSQL `attnum` and canonical modification order, respectively.
    pub fn new(
        columns_to_add: Vec<ColumnSchema>,
        columns_to_drop: Vec<ColumnSchema>,
        columns_to_modify: Vec<ColumnModification>,
    ) -> Self {
        Self { columns_to_add, columns_to_drop, columns_to_modify }
    }

    /// Builds a destination-executable plan from this exact endpoint diff.
    ///
    /// `existing_column_names` must contain every column in the current
    /// destination endpoint. Both the current and derived target endpoints are
    /// validated under `column_name_equivalence` before operation ordering
    /// begins. Exact names are retained in every emitted operation.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaPlanError`] if either endpoint contains destination-
    /// equivalent column names or if the diff does not describe a valid
    /// transition from the supplied current names.
    pub fn plan<I, S>(
        self,
        existing_column_names: I,
        column_name_equivalence: ColumnNameEquivalence,
    ) -> Result<SchemaPlan, SchemaPlanError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let existing_column_names: Vec<String> =
            existing_column_names.into_iter().map(Into::into).collect();
        let current_names_by_equivalence = collect_unique_column_names(
            &existing_column_names,
            column_name_equivalence,
            SchemaEndpoint::Current,
        )?;
        validate_current_column_references(
            &current_names_by_equivalence,
            &self.columns_to_drop,
            &self.columns_to_modify,
            column_name_equivalence,
        )?;
        let mut target_names_by_equivalence = current_names_by_equivalence.clone();

        // Remove every name absent from the target before inserting rename and
        // addition targets. Endpoint changes are simultaneous facts here; the
        // physical order needed to realize them is computed only afterward.
        for column in &self.columns_to_drop {
            remove_current_column_name(
                &mut target_names_by_equivalence,
                &column.name,
                column_name_equivalence,
            )?;
        }
        for modification in &self.columns_to_modify {
            if modification.modification_types.contains(&ColumnModificationType::Rename) {
                remove_current_column_name(
                    &mut target_names_by_equivalence,
                    &modification.old_column_schema.name,
                    column_name_equivalence,
                )?;
            }
        }
        for modification in &self.columns_to_modify {
            if modification.modification_types.contains(&ColumnModificationType::Rename) {
                insert_target_column_name(
                    &mut target_names_by_equivalence,
                    &modification.new_column_schema.name,
                    column_name_equivalence,
                )?;
            }
        }
        for column in &self.columns_to_add {
            insert_target_column_name(
                &mut target_names_by_equivalence,
                &column.name,
                column_name_equivalence,
            )?;
        }

        let occupied_column_name_equivalence_keys =
            current_names_by_equivalence.keys().cloned().collect();
        let reserved_column_name_equivalence_keys = current_names_by_equivalence
            .keys()
            .chain(target_names_by_equivalence.keys())
            .cloned()
            .collect();

        let ordered_operations = plan_schema_operations(
            &self.columns_to_add,
            &self.columns_to_drop,
            &self.columns_to_modify,
            occupied_column_name_equivalence_keys,
            reserved_column_name_equivalence_keys,
            column_name_equivalence,
        )?;

        Ok(SchemaPlan { diff: self, ordered_operations })
    }

    /// Returns `true` if there are no schema changes.
    pub fn is_empty(&self) -> bool {
        self.columns_to_add.is_empty()
            && self.columns_to_drop.is_empty()
            && self.columns_to_modify.is_empty()
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
        /// The destination name equivalence that produced the collision.
        column_name_equivalence: ColumnNameEquivalence,
        /// The first exact source column name.
        first_column_name: String,
        /// The second exact source column name.
        second_column_name: String,
    },
    /// A classified removal or rename source is absent from the current names.
    CurrentColumnNotFound {
        /// The exact source column name referenced by the diff.
        column_name: String,
    },
    /// One current column was classified as more than one endpoint change.
    CurrentColumnClassifiedMultipleTimes {
        /// The exact current column name classified more than once.
        column_name: String,
    },
    /// A blocked rename target is not owned by another pending rename.
    BlockedRenameTarget {
        /// The exact current column name.
        current_column_name: String,
        /// The exact requested target name.
        target_column_name: String,
    },
}

impl fmt::Display for SchemaPlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DestinationColumnNameCollision {
                endpoint,
                column_name_equivalence,
                first_column_name,
                second_column_name,
            } => write!(
                f,
                "Destination column names '{first_column_name}' and '{second_column_name}' \
                 collide in the {endpoint} schema under {column_name_equivalence} equivalence"
            ),
            Self::CurrentColumnNotFound { column_name } => {
                write!(f, "Schema diff references missing current column '{column_name}'")
            }
            Self::CurrentColumnClassifiedMultipleTimes { column_name } => {
                write!(f, "Schema diff classifies current column '{column_name}' more than once")
            }
            Self::BlockedRenameTarget { current_column_name, target_column_name } => write!(
                f,
                "Rename from '{current_column_name}' to '{target_column_name}' is blocked by a \
                 column outside the pending rename set"
            ),
        }
    }
}

impl std::error::Error for SchemaPlanError {}

/// A destination-validated schema diff with physical operations in safe order.
///
/// The exact endpoint facts remain available through [`SchemaPlan::diff`]. The
/// generated operation sequence may additionally contain temporary renames
/// used to break cycles under the destination's name equivalence rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaPlan {
    /// The exact source endpoint difference.
    diff: SchemaDiff,
    /// Operations in destination-safe execution order.
    ordered_operations: Vec<SchemaOperation>,
}

impl SchemaPlan {
    /// Returns the exact endpoint diff represented by this plan.
    pub fn diff(&self) -> &SchemaDiff {
        &self.diff
    }

    /// Returns `true` if the endpoint schemas are identical.
    pub fn is_empty(&self) -> bool {
        self.diff.is_empty()
    }

    /// Returns the only destination-executable schema operations.
    ///
    /// Drops precede renames, followed by additions and nullability/default
    /// modifications. Destinations must preserve this exact order and must not
    /// independently apply the classified `columns_to_*` fields.
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
            .columns_to_modify
            .iter()
            .filter(|modification| {
                modification.modification_types.contains(&ColumnModificationType::Rename)
            })
            .count();
        let planned_rename_count = self
            .ordered_operations
            .iter()
            .filter(|operation| {
                matches!(
                    operation,
                    SchemaOperation::ModifyColumn {
                        modification_type: ColumnModificationType::Rename,
                        ..
                    }
                )
            })
            .count();

        planned_rename_count > endpoint_rename_count
    }
}

/// Collects exact names by destination equivalence key and rejects collisions.
fn collect_unique_column_names(
    column_names: &[String],
    column_name_equivalence: ColumnNameEquivalence,
    endpoint: SchemaEndpoint,
) -> Result<HashMap<String, String>, SchemaPlanError> {
    let mut names_by_equivalence = HashMap::with_capacity(column_names.len());

    for column_name in column_names {
        let equivalence_key = column_name_equivalence.equivalence_key(column_name);
        if let Some(first_column_name) =
            names_by_equivalence.insert(equivalence_key, column_name.clone())
        {
            return Err(SchemaPlanError::DestinationColumnNameCollision {
                endpoint,
                column_name_equivalence,
                first_column_name,
                second_column_name: column_name.clone(),
            });
        }
    }

    Ok(names_by_equivalence)
}

/// Validates that every removal or modification owns one current column.
fn validate_current_column_references(
    current_names_by_equivalence: &HashMap<String, String>,
    columns_to_drop: &[ColumnSchema],
    columns_to_modify: &[ColumnModification],
    column_name_equivalence: ColumnNameEquivalence,
) -> Result<(), SchemaPlanError> {
    let mut referenced_name_equivalence_keys = HashSet::new();
    let referenced_column_names = columns_to_drop.iter().map(|column| column.name.as_str()).chain(
        columns_to_modify.iter().map(|modification| modification.old_column_schema.name.as_str()),
    );

    for column_name in referenced_column_names {
        let equivalence_key = column_name_equivalence.equivalence_key(column_name);
        if current_names_by_equivalence.get(&equivalence_key).map(String::as_str)
            != Some(column_name)
        {
            return Err(SchemaPlanError::CurrentColumnNotFound {
                column_name: column_name.to_owned(),
            });
        }
        if !referenced_name_equivalence_keys.insert(equivalence_key) {
            return Err(SchemaPlanError::CurrentColumnClassifiedMultipleTimes {
                column_name: column_name.to_owned(),
            });
        }
    }

    Ok(())
}

/// Removes a current exact name while deriving the target endpoint namespace.
fn remove_current_column_name(
    names_by_equivalence: &mut HashMap<String, String>,
    column_name: &str,
    column_name_equivalence: ColumnNameEquivalence,
) -> Result<(), SchemaPlanError> {
    let equivalence_key = column_name_equivalence.equivalence_key(column_name);
    let Some(existing_column_name) = names_by_equivalence.remove(&equivalence_key) else {
        return Err(SchemaPlanError::CurrentColumnNotFound { column_name: column_name.to_owned() });
    };

    if existing_column_name != column_name {
        return Err(SchemaPlanError::CurrentColumnNotFound { column_name: column_name.to_owned() });
    }

    Ok(())
}

/// Inserts an exact target name and rejects destination-equivalent names.
fn insert_target_column_name(
    names_by_equivalence: &mut HashMap<String, String>,
    column_name: &str,
    column_name_equivalence: ColumnNameEquivalence,
) -> Result<(), SchemaPlanError> {
    let equivalence_key = column_name_equivalence.equivalence_key(column_name);
    if let Some(first_column_name) =
        names_by_equivalence.insert(equivalence_key, column_name.to_owned())
    {
        return Err(SchemaPlanError::DestinationColumnNameCollision {
            endpoint: SchemaEndpoint::Target,
            column_name_equivalence,
            first_column_name,
            second_column_name: column_name.to_owned(),
        });
    }

    Ok(())
}

/// A pending logical rename tracked by ordinal identity.
#[derive(Debug)]
struct PendingRename {
    /// The current state of the logical column.
    current_column_schema: ColumnSchema,
    /// The final target name from the new endpoint.
    target_name: String,
}

/// Plans the minimum column-name-safe operations for the endpoint diff.
///
/// The endpoints may summarize several DDL statements because stored DDL
/// snapshots become destination-visible only when a later
/// [`crate::event::RelationEvent`] is received. The planner must therefore
/// order the complete endpoint difference without assuming it came from one
/// PostgreSQL statement or attempting to reconstruct the source statements.
///
/// Every add, drop, modification, and acyclic rename produces exactly one
/// operation. A rename cycle produces one additional temporary rename, which
/// is the minimum needed when no target name is initially free. Ready renames
/// and cycle roots are selected by ordinal position, making the output stable
/// regardless of hash-map iteration order. For `r` renames, scheduling takes
/// `O(r log r)` time and `O(r)` additional space.
///
/// A rename is ready when its target is not currently occupied. Applying it
/// frees its source name, which can make the rename targeting that source
/// ready. For example, `a -> b, b -> c` is scheduled as `b -> c, a -> b`. If
/// no rename is ready, every remaining target is owned by another pending
/// rename and the component is a cycle. Moving one cycle member to a reserved
/// temporary name frees the first target and lets the same ready-name process
/// unwind the rest of the cycle. The cycle root remains pending as
/// `temporary -> final`, so completing the plan always consumes the generated
/// name without dropping or recreating the logical column.
///
/// Exact names are retained in emitted operations. Destination equivalence
/// keys are used only for occupancy, dependency, and temporary-name checks.
fn plan_schema_operations(
    columns_to_add: &[ColumnSchema],
    columns_to_drop: &[ColumnSchema],
    columns_to_modify: &[ColumnModification],
    mut occupied_name_equivalence_keys: HashSet<String>,
    mut reserved_column_name_equivalence_keys: HashSet<String>,
    column_name_equivalence: ColumnNameEquivalence,
) -> Result<Vec<SchemaOperation>, SchemaPlanError> {
    let mut operations = Vec::new();

    // Phase 1: drop every column absent from the new endpoint. This frees all
    // names that renames or additions may reuse and gives every destination the
    // same simple structural ordering.
    for column in columns_to_drop {
        occupied_name_equivalence_keys
            .remove(&column_name_equivalence.equivalence_key(&column.name));
        operations.push(SchemaOperation::DropColumn { column_schema: column.clone() });
    }

    // Phase 2: schedule renames from their free targets toward their sources.
    // Pending work is keyed by ordinal identity. The ordered map makes
    // cycle-root selection deterministic.
    let mut pending_renames = BTreeMap::new();

    // `waiting` answers which pending rename wants a name as its final target.
    // Endpoint name keys are unique by the caller contract, so each lookup has
    // at most one answer.
    let mut waiting_ordinal_by_target_name_equivalence_key = HashMap::new();
    for modification in columns_to_modify {
        if !modification.modification_types.contains(&ColumnModificationType::Rename) {
            continue;
        }

        let ordinal_position = modification.old_column_schema.ordinal_position;
        let new_name = &modification.new_column_schema.name;

        waiting_ordinal_by_target_name_equivalence_key
            .insert(column_name_equivalence.equivalence_key(new_name), ordinal_position);
        pending_renames.insert(
            ordinal_position,
            PendingRename {
                current_column_schema: modification.old_column_schema.clone(),
                target_name: new_name.clone(),
            },
        );
    }

    // Renames targeting an already-free name form the initial ready frontier.
    // The ordered set chooses the smallest ordinal when independent chains can
    // both advance, keeping the plan stable without constraining correctness.
    let mut ready_renames: BTreeSet<i32> = pending_renames
        .iter()
        .filter_map(|(&ordinal_position, rename)| {
            (!occupied_name_equivalence_keys
                .contains(&column_name_equivalence.equivalence_key(&rename.target_name)))
            .then_some(ordinal_position)
        })
        .collect();
    let mut temporary_name_sequence = 0_u64;

    while !pending_renames.is_empty() {
        if let Some(ordinal_position) = ready_renames.pop_first() {
            debug_assert!(pending_renames.contains_key(&ordinal_position));
            let Some(rename) = pending_renames.remove(&ordinal_position) else {
                continue;
            };

            // Moving this column consumes its target and frees its current
            // name. If another rename targets the freed name, that rename is
            // now ready; no scan of all pending renames is required.
            let current_name_equivalence_key =
                column_name_equivalence.equivalence_key(&rename.current_column_schema.name);
            occupied_name_equivalence_keys.remove(&current_name_equivalence_key);
            occupied_name_equivalence_keys
                .insert(column_name_equivalence.equivalence_key(&rename.target_name));
            if let Some(waiting_ordinal) =
                waiting_ordinal_by_target_name_equivalence_key.get(&current_name_equivalence_key)
                && pending_renames.contains_key(waiting_ordinal)
            {
                ready_renames.insert(*waiting_ordinal);
            }
            let mut new_column_schema = rename.current_column_schema.clone();
            new_column_schema.name = rename.target_name;
            operations.push(SchemaOperation::ModifyColumn {
                old_column_schema: rename.current_column_schema,
                new_column_schema,
                modification_type: ColumnModificationType::Rename,
            });

            continue;
        }

        // Endpoint validation guarantees that an unchanged column cannot own
        // a pending rename's target: that would make the target endpoint
        // non-unique. Therefore, every remaining target must be owned by
        // another pending rename and the component is a genuine cycle.
        let Some((&ordinal_position, rename)) = pending_renames.first_key_value() else {
            break;
        };
        if !pending_renames.values().any(|pending_rename| {
            column_name_equivalence.equivalence_key(&pending_rename.current_column_schema.name)
                == column_name_equivalence.equivalence_key(&rename.target_name)
        }) {
            return Err(SchemaPlanError::BlockedRenameTarget {
                current_column_name: rename.current_column_schema.name.clone(),
                target_column_name: rename.target_name.clone(),
            });
        }
        let temporary_name = loop {
            let candidate = format!(
                "{SCHEMA_TEMPORARY_COLUMN_PREFIX}{ordinal_position}_{temporary_name_sequence}"
            );
            temporary_name_sequence += 1;
            if reserved_column_name_equivalence_keys
                .insert(column_name_equivalence.equivalence_key(&candidate))
            {
                break candidate;
            }
        };

        debug_assert!(pending_renames.contains_key(&ordinal_position));
        let Some(rename) = pending_renames.get_mut(&ordinal_position) else {
            break;
        };

        // Only the current physical name changes here. The logical rename
        // remains pending with the same final target, now represented as
        // `temporary -> target`. Freeing its old name wakes the preceding
        // cycle member, after which the ordinary ready path unwinds the cycle.
        let old_column_schema = rename.current_column_schema.clone();
        rename.current_column_schema.name = temporary_name.clone();
        let old_name_equivalence_key =
            column_name_equivalence.equivalence_key(&old_column_schema.name);
        occupied_name_equivalence_keys.remove(&old_name_equivalence_key);
        occupied_name_equivalence_keys
            .insert(column_name_equivalence.equivalence_key(&temporary_name));
        if let Some(waiting_ordinal) =
            waiting_ordinal_by_target_name_equivalence_key.get(&old_name_equivalence_key)
        {
            ready_renames.insert(*waiting_ordinal);
        }
        operations.push(SchemaOperation::ModifyColumn {
            old_column_schema,
            new_column_schema: rename.current_column_schema.clone(),
            modification_type: ColumnModificationType::Rename,
        });
    }

    // Phase 3: additions are now safe because drops and renames have released
    // every reused name.
    for column in columns_to_add {
        operations.push(SchemaOperation::AddColumn { column_schema: column.clone() });
    }

    // Phase 4: every structural operation is complete, so metadata changes can
    // address every existing logical column by its endpoint name. An added
    // column carries its own endpoint metadata in `AddColumn` and does not
    // produce a separate modification operation.
    for modification in columns_to_modify {
        let mut current_column_schema = modification.old_column_schema.clone();

        // Renames were applied in the structural phase, so subsequent metadata
        // operations must address the column by its final endpoint name.
        if modification.modification_types.contains(&ColumnModificationType::Rename) {
            current_column_schema.name.clone_from(&modification.new_column_schema.name);
        }

        for modification_type in &modification.modification_types {
            let old_column_schema = current_column_schema.clone();
            match modification_type {
                ColumnModificationType::Rename => continue,
                ColumnModificationType::Nullability => {
                    current_column_schema.nullable = modification.new_column_schema.nullable;
                }
                ColumnModificationType::Default => {
                    current_column_schema
                        .default_expression
                        .clone_from(&modification.new_column_schema.default_expression);
                }
            }
            operations.push(SchemaOperation::ModifyColumn {
                old_column_schema,
                new_column_schema: current_column_schema.clone(),
                modification_type: *modification_type,
            });
        }
    }

    Ok(operations)
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
        column_name_equivalence: ColumnNameEquivalence,
    ) -> SchemaPlan {
        old_schema
            .plan_schema_change(new_schema, column_name_equivalence)
            .expect("test schema transition should be plannable")
    }

    fn operation_names(plan: &SchemaPlan) -> Vec<String> {
        plan.ordered_operations()
            .iter()
            .map(|operation| match operation {
                SchemaOperation::DropColumn { column_schema } => {
                    format!("drop:{}", column_schema.name)
                }
                SchemaOperation::AddColumn { column_schema } => {
                    format!("add:{}", column_schema.name)
                }
                SchemaOperation::ModifyColumn {
                    old_column_schema,
                    new_column_schema,
                    modification_type: ColumnModificationType::Rename,
                } => {
                    format!("rename:{}->{}", old_column_schema.name, new_column_schema.name)
                }
                SchemaOperation::ModifyColumn {
                    new_column_schema,
                    modification_type: ColumnModificationType::Nullability,
                    ..
                } => {
                    format!("modify-nullability:{}", new_column_schema.name)
                }
                SchemaOperation::ModifyColumn {
                    new_column_schema,
                    modification_type: ColumnModificationType::Default,
                    ..
                } => {
                    format!("modify-default:{}", new_column_schema.name)
                }
            })
            .collect()
    }

    fn assert_operations_converge(
        old_schema: &ReplicatedTableSchema,
        new_schema: &ReplicatedTableSchema,
    ) {
        assert_operations_converge_with_equivalence(
            old_schema,
            new_schema,
            ColumnNameEquivalence::CaseSensitive,
        );
    }

    fn assert_operations_converge_with_equivalence(
        old_schema: &ReplicatedTableSchema,
        new_schema: &ReplicatedTableSchema,
        column_name_equivalence: ColumnNameEquivalence,
    ) {
        let plan = plan_schema_change(old_schema, new_schema, column_name_equivalence);
        let mut columns_by_ordinal: BTreeMap<i32, ColumnSchema> = old_schema
            .column_schemas()
            .map(|column| (column.ordinal_position, column.clone()))
            .collect();
        let mut occupied_names: HashMap<String, i32> = old_schema
            .column_schemas()
            .map(|column| {
                (column_name_equivalence.equivalence_key(&column.name), column.ordinal_position)
            })
            .collect();

        for operation in plan.ordered_operations() {
            match operation {
                SchemaOperation::DropColumn { column_schema } => {
                    assert_eq!(
                        columns_by_ordinal.remove(&column_schema.ordinal_position),
                        Some(column_schema.clone())
                    );
                    assert_eq!(
                        occupied_names
                            .remove(&column_name_equivalence.equivalence_key(&column_schema.name)),
                        Some(column_schema.ordinal_position)
                    );
                }
                SchemaOperation::AddColumn { column_schema } => {
                    assert_eq!(
                        columns_by_ordinal
                            .insert(column_schema.ordinal_position, column_schema.clone()),
                        None
                    );
                    assert_eq!(
                        occupied_names.insert(
                            column_name_equivalence.equivalence_key(&column_schema.name),
                            column_schema.ordinal_position,
                        ),
                        None
                    );
                }
                SchemaOperation::ModifyColumn { old_column_schema, new_column_schema, .. } => {
                    assert_eq!(
                        columns_by_ordinal.get(&old_column_schema.ordinal_position),
                        Some(old_column_schema)
                    );
                    if old_column_schema.name != new_column_schema.name {
                        assert_eq!(
                            occupied_names.remove(
                                &column_name_equivalence.equivalence_key(&old_column_schema.name)
                            ),
                            Some(old_column_schema.ordinal_position)
                        );
                        assert_eq!(
                            occupied_names.insert(
                                column_name_equivalence.equivalence_key(&new_column_schema.name),
                                new_column_schema.ordinal_position,
                            ),
                            None
                        );
                    }
                    columns_by_ordinal
                        .insert(new_column_schema.ordinal_position, new_column_schema.clone());
                }
            }
        }

        let expected: BTreeMap<i32, ColumnSchema> = new_schema
            .column_schemas()
            .map(|column| (column.ordinal_position, column.clone()))
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
        assert!(diff.columns_to_modify.is_empty());
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
        assert!(diff.columns_to_drop.is_empty());
        assert!(diff.columns_to_modify.is_empty());
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
        assert_eq!(diff.columns_to_drop[0].name, "age");
        assert_eq!(diff.columns_to_drop[0].ordinal_position, 3);
        assert!(diff.columns_to_modify.is_empty());
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
        assert_eq!(diff.columns_to_modify.len(), 1);
        assert_eq!(diff.columns_to_modify[0].old_column_schema.ordinal_position, 2);
        assert_eq!(diff.columns_to_modify[0].old_column_schema.name, "name");
        assert_eq!(diff.columns_to_modify[0].new_column_schema.name, "full_name");
        assert_eq!(
            diff.columns_to_modify[0].modification_types,
            vec![ColumnModificationType::Rename]
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
        assert!(diff.columns_to_drop.is_empty());
        assert_eq!(diff.columns_to_modify.len(), 1);
        assert_eq!(diff.columns_to_modify[0].new_column_schema.name, "status");
        assert_eq!(
            diff.columns_to_modify[0].modification_types,
            vec![ColumnModificationType::Default]
        );
        assert_eq!(diff.columns_to_modify[0].old_column_schema.default_expression, None);
        assert_eq!(
            diff.columns_to_modify[0].new_column_schema.default_expression.as_deref(),
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
        assert_eq!(diff.columns_to_modify.len(), 1);
        assert_eq!(diff.columns_to_modify[0].new_column_schema.name, "email");
        assert_eq!(
            diff.columns_to_modify[0].modification_types,
            vec![ColumnModificationType::Nullability]
        );
        assert!(!diff.columns_to_modify[0].old_column_schema.nullable);
        assert!(diff.columns_to_modify[0].new_column_schema.nullable);
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
        assert_eq!(diff.columns_to_modify.len(), 1);
        assert_eq!(diff.columns_to_modify[0].old_column_schema.name, "status");
        assert_eq!(diff.columns_to_modify[0].new_column_schema.name, "state");
        assert_eq!(
            diff.columns_to_modify[0].modification_types,
            vec![
                ColumnModificationType::Rename,
                ColumnModificationType::Nullability,
                ColumnModificationType::Default,
            ]
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
        assert_eq!(diff.columns_to_add[0].name, "email");

        assert_eq!(diff.columns_to_drop.len(), 1);
        assert_eq!(diff.columns_to_drop[0].name, "age");

        assert_eq!(diff.columns_to_modify.len(), 1);
        assert_eq!(diff.columns_to_modify[0].old_column_schema.name, "name");
        assert_eq!(diff.columns_to_modify[0].new_column_schema.name, "full_name");
        assert_eq!(
            diff.columns_to_modify[0].modification_types,
            vec![ColumnModificationType::Rename]
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
        assert!(diff.columns_to_drop.is_empty());
        assert!(diff.columns_to_modify.is_empty());
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
            diff.columns_to_drop.iter().map(|c| c.name.as_str()).collect();
        assert!(dropped_names.contains("name"));
        assert!(dropped_names.contains("email"));
        assert!(diff.columns_to_modify.is_empty());
    }

    #[test]
    fn schema_plan_orders_drop_before_reusing_name_for_add() {
        let old_schema = create_replicated_schema(vec![text_column("value", 1)]);
        let new_schema = create_replicated_schema(vec![text_column("value", 2)]);

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

        assert_eq!(operation_names(&plan), ["drop:value", "add:value"]);
    }

    #[test]
    fn schema_plan_orders_rename_chain_without_temporary_name() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("b", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1), text_column("c", 2)]);

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

        assert_eq!(operation_names(&plan), ["rename:b->c", "rename:a->b"]);
    }

    #[test]
    fn schema_plan_orders_rename_before_adding_old_name() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1), text_column("a", 2)]);

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

        assert_eq!(operation_names(&plan), ["rename:a->b", "add:a"]);
    }

    #[test]
    fn schema_plan_orders_drop_before_rename_target_reuse() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("b", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1)]);

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

        assert_eq!(operation_names(&plan), ["drop:b", "rename:a->b"]);
    }

    #[test]
    fn schema_plan_orders_unrelated_drop_before_rename() {
        let old_schema =
            create_replicated_schema(vec![text_column("a", 1), text_column("unused", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1)]);

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

        assert_eq!(operation_names(&plan), ["drop:unused", "rename:a->b"]);
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

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

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

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

        assert!(plan.has_rename_cycles());
        assert_eq!(
            operation_names(&plan),
            [
                "rename:a->supabase_etl_schema_tmp_1_0",
                "rename:b->a",
                "rename:supabase_etl_schema_tmp_1_0->b",
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
            "rename:a->supabase_etl_schema_tmp_1_0",
            "rename:b->a",
            "rename:supabase_etl_schema_tmp_1_0->b",
            "rename:c->supabase_etl_schema_tmp_3_1",
            "rename:d->c",
            "rename:supabase_etl_schema_tmp_3_1->d",
        ];

        for _ in 0..10 {
            let plan =
                plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

            assert!(plan.has_rename_cycles());
            assert_eq!(operation_names(&plan), expected);
            assert_operations_converge(&old_schema, &new_schema);
        }
    }

    #[test]
    fn schema_plan_temporary_name_avoids_endpoint_columns() {
        let old_schema = create_replicated_schema(vec![
            text_column("a", 1),
            text_column("b", 2),
            text_column("SUPABASE_ETL_SCHEMA_TMP_1_0", 3),
        ]);
        let new_schema = create_replicated_schema(vec![
            text_column("b", 1),
            text_column("a", 2),
            text_column("SUPABASE_ETL_SCHEMA_TMP_1_0", 3),
        ]);

        let plan = plan_schema_change(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::AsciiCaseInsensitive,
        );

        assert_eq!(
            operation_names(&plan),
            [
                "rename:a->supabase_etl_schema_tmp_1_1",
                "rename:b->a",
                "rename:supabase_etl_schema_tmp_1_1->b",
            ]
        );
    }

    #[test]
    fn schema_plan_respects_destination_column_name_equivalence() {
        let old_schema = create_replicated_schema(vec![text_column("A", 1), text_column("b", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("B", 1), text_column("A", 2)]);

        let case_sensitive_plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);
        assert_eq!(operation_names(&case_sensitive_plan), ["rename:A->B", "rename:b->A"]);
        assert_operations_converge_with_equivalence(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::CaseSensitive,
        );

        let case_insensitive_plan = plan_schema_change(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::AsciiCaseInsensitive,
        );
        assert_eq!(
            operation_names(&case_insensitive_plan),
            [
                "rename:A->supabase_etl_schema_tmp_1_0",
                "rename:b->A",
                "rename:supabase_etl_schema_tmp_1_0->B",
            ]
        );
        assert_operations_converge_with_equivalence(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::AsciiCaseInsensitive,
        );

        let old_schema = create_replicated_schema(vec![text_column("Ä", 1), text_column("b", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("B", 1), text_column("ä", 2)]);
        let unicode_case_insensitive_plan = plan_schema_change(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::UnicodeCaseInsensitive,
        );
        assert_eq!(
            operation_names(&unicode_case_insensitive_plan),
            [
                "rename:Ä->supabase_etl_schema_tmp_1_0",
                "rename:b->ä",
                "rename:supabase_etl_schema_tmp_1_0->B",
            ]
        );
        assert_operations_converge_with_equivalence(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::UnicodeCaseInsensitive,
        );
    }

    #[test]
    fn schema_plan_rejects_destination_equivalent_target_columns() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("c", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("a", 1), text_column("A", 2)]);

        // Diffing records the exact PostgreSQL rename independently of the
        // destination namespace in which it will eventually be planned.
        let diff = old_schema.diff(&new_schema);
        assert_eq!(diff.columns_to_modify.len(), 1);
        assert_eq!(diff.columns_to_modify[0].old_column_schema.name, "c");
        assert_eq!(diff.columns_to_modify[0].new_column_schema.name, "A");

        for equivalence in [
            ColumnNameEquivalence::AsciiCaseInsensitive,
            ColumnNameEquivalence::UnicodeCaseInsensitive,
        ] {
            let error = diff
                .clone()
                .plan(old_schema.column_schemas().map(|column| column.name.as_str()), equivalence)
                .expect_err("case-insensitive destination should reject colliding target names");

            assert_eq!(
                error,
                SchemaPlanError::DestinationColumnNameCollision {
                    endpoint: SchemaEndpoint::Target,
                    column_name_equivalence: equivalence,
                    first_column_name: "a".to_owned(),
                    second_column_name: "A".to_owned(),
                }
            );
        }

        let case_sensitive_plan = diff
            .plan(
                old_schema.column_schemas().map(|column| column.name.as_str()),
                ColumnNameEquivalence::CaseSensitive,
            )
            .expect("case-sensitive destination should represent both target names");
        assert_eq!(operation_names(&case_sensitive_plan), ["rename:c->A"]);
    }

    #[test]
    fn schema_plan_rejects_destination_equivalent_current_columns() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("A", 2)]);
        let new_schema = old_schema.clone();

        old_schema
            .validate_destination_column_names(ColumnNameEquivalence::CaseSensitive)
            .expect("case-sensitive destination should represent both exact names");

        let error = old_schema
            .plan_schema_change(&new_schema, ColumnNameEquivalence::AsciiCaseInsensitive)
            .expect_err("case-insensitive destination should reject colliding current names");

        assert_eq!(
            error,
            SchemaPlanError::DestinationColumnNameCollision {
                endpoint: SchemaEndpoint::Current,
                column_name_equivalence: ColumnNameEquivalence::AsciiCaseInsensitive,
                first_column_name: "a".to_owned(),
                second_column_name: "A".to_owned(),
            }
        );
    }

    #[test]
    fn schema_plan_rejects_destination_equivalent_added_column() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1)]);
        let new_schema = create_replicated_schema(vec![text_column("a", 1), text_column("A", 2)]);

        let error = old_schema
            .plan_schema_change(&new_schema, ColumnNameEquivalence::AsciiCaseInsensitive)
            .expect_err("case-insensitive destination should reject colliding added name");

        assert_eq!(
            error,
            SchemaPlanError::DestinationColumnNameCollision {
                endpoint: SchemaEndpoint::Target,
                column_name_equivalence: ColumnNameEquivalence::AsciiCaseInsensitive,
                first_column_name: "a".to_owned(),
                second_column_name: "A".to_owned(),
            }
        );
    }

    #[test]
    fn schema_plan_distinguishes_ascii_and_unicode_case_equivalence() {
        let old_schema = create_replicated_schema(vec![text_column("x", 1)]);
        let new_schema = create_replicated_schema(vec![
            text_column("x", 1),
            text_column("Ä", 2),
            text_column("ä", 3),
        ]);

        old_schema
            .plan_schema_change(&new_schema, ColumnNameEquivalence::AsciiCaseInsensitive)
            .expect("ASCII case equivalence should keep non-ASCII names distinct");
        let error = old_schema
            .plan_schema_change(&new_schema, ColumnNameEquivalence::UnicodeCaseInsensitive)
            .expect_err("Unicode case equivalence should reject the colliding names");

        assert_eq!(
            error,
            SchemaPlanError::DestinationColumnNameCollision {
                endpoint: SchemaEndpoint::Target,
                column_name_equivalence: ColumnNameEquivalence::UnicodeCaseInsensitive,
                first_column_name: "Ä".to_owned(),
                second_column_name: "ä".to_owned(),
            }
        );
    }

    #[test]
    fn schema_plan_rejects_diff_referencing_missing_current_column() {
        let diff = SchemaDiff::new(
            Vec::new(),
            Vec::new(),
            vec![ColumnModification {
                old_column_schema: text_column("missing", 1),
                new_column_schema: text_column("renamed", 1),
                modification_types: vec![ColumnModificationType::Rename],
            }],
        );

        let error = diff
            .plan(["existing"], ColumnNameEquivalence::CaseSensitive)
            .expect_err("invalid diff should fail instead of entering the rename planner");

        assert_eq!(
            error,
            SchemaPlanError::CurrentColumnNotFound { column_name: "missing".to_owned() }
        );
    }

    #[test]
    fn schema_plan_rejects_metadata_change_for_missing_current_column() {
        let diff = SchemaDiff::new(
            Vec::new(),
            Vec::new(),
            vec![ColumnModification {
                old_column_schema: text_column("missing", 1),
                new_column_schema: ColumnSchema::new(
                    "missing".to_owned(),
                    Type::TEXT,
                    -1,
                    1,
                    false,
                ),
                modification_types: vec![ColumnModificationType::Nullability],
            }],
        );

        let error = diff
            .plan(["existing"], ColumnNameEquivalence::CaseSensitive)
            .expect_err("all modification kinds should validate their current column");

        assert_eq!(
            error,
            SchemaPlanError::CurrentColumnNotFound { column_name: "missing".to_owned() }
        );
    }

    #[test]
    fn schema_plan_rejects_current_column_classified_multiple_times() {
        let column = text_column("existing", 1);
        let diff = SchemaDiff::new(
            Vec::new(),
            vec![column.clone()],
            vec![ColumnModification {
                old_column_schema: column.clone(),
                new_column_schema: ColumnSchema::new(
                    column.name.clone(),
                    Type::TEXT,
                    -1,
                    column.ordinal_position,
                    false,
                ),
                modification_types: vec![ColumnModificationType::Nullability],
            }],
        );

        let error = diff
            .plan(["existing"], ColumnNameEquivalence::CaseSensitive)
            .expect_err("one current column cannot be both dropped and modified");

        assert_eq!(
            error,
            SchemaPlanError::CurrentColumnClassifiedMultipleTimes {
                column_name: "existing".to_owned(),
            }
        );
    }

    #[test]
    fn schema_plan_handles_case_insensitive_self_cycles_chains_and_drops() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1)]);
        let new_schema = create_replicated_schema(vec![text_column("A", 1)]);
        let plan = plan_schema_change(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::AsciiCaseInsensitive,
        );
        assert_eq!(
            operation_names(&plan),
            ["rename:a->supabase_etl_schema_tmp_1_0", "rename:supabase_etl_schema_tmp_1_0->A",]
        );
        assert_operations_converge_with_equivalence(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::AsciiCaseInsensitive,
        );

        let old_schema = create_replicated_schema(vec![text_column("a", 1), text_column("B", 2)]);
        let new_schema = create_replicated_schema(vec![text_column("b", 1), text_column("C", 2)]);
        let plan = plan_schema_change(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::AsciiCaseInsensitive,
        );
        assert_eq!(operation_names(&plan), ["rename:B->C", "rename:a->b"]);
        assert_operations_converge_with_equivalence(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::AsciiCaseInsensitive,
        );

        let new_schema = create_replicated_schema(vec![text_column("b", 1)]);
        let plan = plan_schema_change(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::AsciiCaseInsensitive,
        );
        assert_eq!(operation_names(&plan), ["drop:B", "rename:a->b"]);
        assert_operations_converge_with_equivalence(
            &old_schema,
            &new_schema,
            ColumnNameEquivalence::AsciiCaseInsensitive,
        );
    }

    #[test]
    fn schema_plan_operations_converge_for_every_four_column_rename_permutation() {
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
    fn schema_plan_exhaustively_validates_case_insensitive_endpoint_transitions() {
        let old_schema = create_replicated_schema(vec![
            text_column("a", 1),
            text_column("b", 2),
            text_column("Ä", 3),
        ]);

        for column_name_equivalence in [
            ColumnNameEquivalence::AsciiCaseInsensitive,
            ColumnNameEquivalence::UnicodeCaseInsensitive,
        ] {
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
                    let unique_equivalence_key_count = names
                        .iter()
                        .map(|name| column_name_equivalence.equivalence_key(name))
                        .collect::<HashSet<_>>()
                        .len();
                    let result =
                        old_schema.plan_schema_change(&new_schema, column_name_equivalence);

                    if unique_equivalence_key_count != names.len() {
                        assert!(matches!(
                            result,
                            Err(SchemaPlanError::DestinationColumnNameCollision {
                                endpoint: SchemaEndpoint::Target,
                                column_name_equivalence: error_equivalence,
                                ..
                            }) if error_equivalence == column_name_equivalence
                        ));
                    } else {
                        let plan = result.expect("injective endpoints should be plannable");
                        assert_eq!(
                            plan,
                            plan_schema_change(&old_schema, &new_schema, column_name_equivalence,)
                        );
                        assert_operations_converge_with_equivalence(
                            &old_schema,
                            &new_schema,
                            column_name_equivalence,
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
                let plan = plan_schema_change(
                    &old_schema,
                    &new_schema,
                    ColumnNameEquivalence::CaseSensitive,
                );

                assert_eq!(
                    plan,
                    plan_schema_change(
                        &old_schema,
                        &new_schema,
                        ColumnNameEquivalence::CaseSensitive
                    )
                );
                assert_operations_converge(&old_schema, &new_schema);
            }
        }
    }

    #[test]
    fn schema_plan_modifies_column_after_final_rename() {
        let old_schema = create_replicated_schema(vec![text_column("a", 1)]);
        let new_schema = create_replicated_schema(vec![
            text_column("b", 1).with_default_expression("'new'::text".to_owned()),
        ]);

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

        assert!(matches!(
            plan.ordered_operations(),
            [
                SchemaOperation::ModifyColumn {
                    new_column_schema: renamed_column_schema,
                    modification_type: ColumnModificationType::Rename,
                    ..
                },
                SchemaOperation::ModifyColumn {
                    new_column_schema: default_column_schema,
                    modification_type: ColumnModificationType::Default,
                    ..
                },
            ] if renamed_column_schema.name == "b" && default_column_schema.name == "b"
        ));
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

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

        assert!(matches!(
            plan.ordered_operations(),
            [
                SchemaOperation::DropColumn { column_schema: blocking_drop },
                SchemaOperation::DropColumn { column_schema: unrelated_drop },
                SchemaOperation::ModifyColumn {
                    old_column_schema: rename_old_column_schema,
                    new_column_schema: rename_new_column_schema,
                    modification_type: ColumnModificationType::Rename,
                },
                SchemaOperation::AddColumn { column_schema: addition },
                SchemaOperation::ModifyColumn {
                    new_column_schema: nullability_column_schema,
                    modification_type: ColumnModificationType::Nullability,
                    ..
                },
                SchemaOperation::ModifyColumn {
                    new_column_schema: default_column_schema,
                    modification_type: ColumnModificationType::Default,
                    ..
                },
            ] if blocking_drop.name == "b"
                && unrelated_drop.name == "unused"
                && rename_old_column_schema.name == "a"
                && rename_new_column_schema.name == "b"
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

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

        assert!(matches!(
            plan.ordered_operations(),
            [
                SchemaOperation::ModifyColumn {
                    old_column_schema: first_old_column_schema,
                    new_column_schema: temporary_column_schema,
                    modification_type: ColumnModificationType::Rename,
                },
                SchemaOperation::ModifyColumn {
                    old_column_schema: second_old_column_schema,
                    new_column_schema: second_new_column_schema,
                    modification_type: ColumnModificationType::Rename,
                },
                SchemaOperation::ModifyColumn {
                    old_column_schema: final_old_column_schema,
                    new_column_schema: final_new_column_schema,
                    modification_type: ColumnModificationType::Rename,
                },
                SchemaOperation::ModifyColumn {
                    new_column_schema: nullability_column_schema,
                    modification_type: ColumnModificationType::Nullability,
                    ..
                },
                SchemaOperation::ModifyColumn {
                    new_column_schema: default_column_schema,
                    modification_type: ColumnModificationType::Default,
                    ..
                },
            ] if first_old_column_schema.name == "a"
                && temporary_column_schema.name.starts_with(SCHEMA_TEMPORARY_COLUMN_PREFIX)
                && second_old_column_schema.name == "b"
                && second_new_column_schema.name == "a"
                && final_old_column_schema.name == temporary_column_schema.name
                && final_new_column_schema.name == "b"
                && nullability_column_schema.name == "b"
                && default_column_schema.name == "b"
        ));
        assert_operations_converge(&old_schema, &new_schema);
    }

    #[test]
    fn schema_plan_handles_columns_constructed_out_of_ordinal_order() {
        let old_schema = create_replicated_schema(vec![text_column("b", 2), text_column("a", 1)]);
        let new_schema = create_replicated_schema(vec![text_column("c", 3), text_column("b", 2)]);

        let plan =
            plan_schema_change(&old_schema, &new_schema, ColumnNameEquivalence::CaseSensitive);

        assert_eq!(operation_names(&plan), ["drop:a", "add:c"]);
        assert_operations_converge(&old_schema, &new_schema);
    }
}
