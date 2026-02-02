//! Primary key representation for the secondary index.

use std::hash::{Hash, Hasher};

use etl::types::Cell;

/// Represents a primary key consisting of one or more column values.
///
/// The primary key is used as the lookup key in the secondary index.
/// It supports composite keys by storing multiple Cell values.
#[derive(Debug, Clone, PartialEq)]
pub struct PrimaryKey {
    /// The actual values of the primary key columns.
    values: Vec<Cell>,
}

// Manual Eq implementation since Cell doesn't derive Eq
// This is safe because Cell's PartialEq is reflexive for all practical cases
impl Eq for PrimaryKey {}

impl PrimaryKey {
    /// Creates a new PrimaryKey from a slice of Cell values.
    pub fn new(values: Vec<Cell>) -> Self {
        Self { values }
    }

    /// Creates a new PrimaryKey by extracting values from a row at the given indices.
    pub fn from_row(row: &[Cell], pk_column_indices: &[usize]) -> Self {
        let values: Vec<Cell> = pk_column_indices
            .iter()
            .map(|&idx| row[idx].clone())
            .collect();
        Self { values }
    }

    /// Returns the values of the primary key.
    pub fn values(&self) -> &[Cell] {
        &self.values
    }
}

impl Hash for PrimaryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for value in &self.values {
            cell_hash(value, state);
        }
    }
}

/// Hashes a Cell value in a deterministic way.
fn cell_hash<H: Hasher>(cell: &Cell, state: &mut H) {
    // Hash discriminant for type safety
    std::mem::discriminant(cell).hash(state);

    match cell {
        Cell::Null => {}
        Cell::Bool(v) => v.hash(state),
        Cell::String(v) => v.hash(state),
        Cell::I16(v) => v.hash(state),
        Cell::I32(v) => v.hash(state),
        Cell::U32(v) => v.hash(state),
        Cell::I64(v) => v.hash(state),
        Cell::F32(v) => v.to_bits().hash(state),
        Cell::F64(v) => v.to_bits().hash(state),
        Cell::Uuid(v) => v.hash(state),
        Cell::Bytes(v) => v.hash(state),
        Cell::Date(v) => v.hash(state),
        Cell::Time(v) => v.hash(state),
        Cell::Timestamp(v) => v.hash(state),
        Cell::TimestampTz(v) => v.hash(state),
        // For complex types, use a string representation for hashing
        Cell::Numeric(v) => format!("{v:?}").hash(state),
        Cell::Json(v) => v.to_string().hash(state),
        Cell::Array(v) => format!("{v:?}").hash(state),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn hash_pk(pk: &PrimaryKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        pk.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn test_primary_key_hash_consistency() {
        let pk1 = PrimaryKey::new(vec![Cell::I64(1), Cell::String("test".to_string())]);
        let pk2 = PrimaryKey::new(vec![Cell::I64(1), Cell::String("test".to_string())]);

        assert_eq!(pk1, pk2);
        assert_eq!(hash_pk(&pk1), hash_pk(&pk2));
    }

    #[test]
    fn test_primary_key_different_values() {
        let pk1 = PrimaryKey::new(vec![Cell::I64(1)]);
        let pk2 = PrimaryKey::new(vec![Cell::I64(2)]);

        assert_ne!(pk1, pk2);
        assert_ne!(hash_pk(&pk1), hash_pk(&pk2));
    }

    #[test]
    fn test_primary_key_from_row() {
        let row = vec![
            Cell::I64(42),
            Cell::String("name".to_string()),
            Cell::Bool(true),
        ];

        // Single column PK
        let pk1 = PrimaryKey::from_row(&row, &[0]);
        assert_eq!(pk1.values(), &[Cell::I64(42)]);

        // Composite PK
        let pk2 = PrimaryKey::from_row(&row, &[0, 1]);
        assert_eq!(
            pk2.values(),
            &[Cell::I64(42), Cell::String("name".to_string())]
        );
    }

    #[test]
    fn test_primary_key_null_handling() {
        let pk1 = PrimaryKey::new(vec![Cell::Null]);
        let pk2 = PrimaryKey::new(vec![Cell::Null]);
        let pk3 = PrimaryKey::new(vec![Cell::I64(0)]);

        assert_eq!(pk1, pk2);
        assert_eq!(hash_pk(&pk1), hash_pk(&pk2));
        assert_ne!(pk1, pk3);
    }

    #[test]
    fn test_primary_key_float_handling() {
        // Floats are hashed by their bit representation
        let pk1 = PrimaryKey::new(vec![Cell::F64(1.5)]);
        let pk2 = PrimaryKey::new(vec![Cell::F64(1.5)]);

        assert_eq!(pk1, pk2);
        assert_eq!(hash_pk(&pk1), hash_pk(&pk2));
    }
}
