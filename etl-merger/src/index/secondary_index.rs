//! In-memory secondary index implementation.

use std::collections::HashMap;

use super::{PrimaryKey, RowLocation};

/// Statistics about index operations.
#[derive(Debug, Default, Clone)]
pub struct IndexStats {
    /// Number of insert operations.
    pub insert_count: u64,
    /// Number of update operations.
    pub update_count: u64,
    /// Number of delete operations.
    pub delete_count: u64,
    /// Number of lookup operations.
    pub lookup_count: u64,
    /// Number of failed lookups (key not found).
    pub lookup_miss_count: u64,
}

/// In-memory secondary index mapping primary keys to row locations.
///
/// This index enables O(1) lookups for UPDATE and DELETE operations,
/// avoiding full table scans. The index is designed to be:
/// - Memory-efficient for large tables
/// - Serializable for future S3 persistence
/// - Thread-safe for concurrent access (when wrapped in appropriate lock)
#[derive(Debug)]
pub struct SecondaryIndex {
    /// The actual index data.
    index: HashMap<PrimaryKey, RowLocation>,
    /// Primary key column indices in the schema.
    pk_column_indices: Vec<usize>,
    /// Statistics for monitoring.
    stats: IndexStats,
}

impl SecondaryIndex {
    /// Creates a new empty secondary index.
    pub fn new(pk_column_indices: Vec<usize>) -> Self {
        Self {
            index: HashMap::new(),
            pk_column_indices,
            stats: IndexStats::default(),
        }
    }

    /// Creates an index with pre-allocated capacity.
    pub fn with_capacity(pk_column_indices: Vec<usize>, capacity: usize) -> Self {
        Self {
            index: HashMap::with_capacity(capacity),
            pk_column_indices,
            stats: IndexStats::default(),
        }
    }

    /// Returns the primary key column indices.
    pub fn pk_column_indices(&self) -> &[usize] {
        &self.pk_column_indices
    }

    /// Inserts a new entry into the index.
    ///
    /// Returns the previous location if the key already existed.
    pub fn insert(&mut self, key: PrimaryKey, location: RowLocation) -> Option<RowLocation> {
        self.stats.insert_count += 1;
        self.index.insert(key, location)
    }

    /// Looks up a row location by primary key.
    pub fn get(&mut self, key: &PrimaryKey) -> Option<&RowLocation> {
        self.stats.lookup_count += 1;
        let result = self.index.get(key);
        if result.is_none() {
            self.stats.lookup_miss_count += 1;
        }
        result
    }

    /// Looks up a row location by primary key (immutable version).
    pub fn get_immut(&self, key: &PrimaryKey) -> Option<&RowLocation> {
        self.index.get(key)
    }

    /// Updates an existing entry's location.
    ///
    /// Returns the old location for creating deletion vectors.
    pub fn update(&mut self, key: PrimaryKey, new_location: RowLocation) -> Option<RowLocation> {
        self.stats.update_count += 1;
        self.index.insert(key, new_location)
    }

    /// Removes an entry from the index.
    ///
    /// Returns the location for creating deletion vectors.
    pub fn remove(&mut self, key: &PrimaryKey) -> Option<RowLocation> {
        self.stats.delete_count += 1;
        self.index.remove(key)
    }

    /// Returns the number of entries in the index.
    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Returns true if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    /// Returns index statistics.
    pub fn stats(&self) -> &IndexStats {
        &self.stats
    }

    /// Clears all entries from the index.
    pub fn clear(&mut self) {
        self.index.clear();
    }

    /// Returns an iterator over all entries in the index.
    pub fn iter(&self) -> impl Iterator<Item = (&PrimaryKey, &RowLocation)> {
        self.index.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::Cell;

    fn make_pk(id: i64) -> PrimaryKey {
        PrimaryKey::new(vec![Cell::I64(id)])
    }

    fn make_location(file: &str, row: u64, seq: &str) -> RowLocation {
        RowLocation::new(file.to_string(), row, seq.to_string())
    }

    #[test]
    fn test_secondary_index_insert_and_get() {
        let mut index = SecondaryIndex::new(vec![0]);

        let pk = make_pk(1);
        let location = make_location("file1.parquet", 0, "seq1");

        assert!(index.insert(pk.clone(), location.clone()).is_none());
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(&pk), Some(&location));
    }

    #[test]
    fn test_secondary_index_update() {
        let mut index = SecondaryIndex::new(vec![0]);

        let pk = make_pk(1);
        let location1 = make_location("file1.parquet", 0, "seq1");
        let location2 = make_location("file2.parquet", 5, "seq2");

        index.insert(pk.clone(), location1.clone());
        let old = index.update(pk.clone(), location2.clone());

        assert_eq!(old, Some(location1));
        assert_eq!(index.get(&pk), Some(&location2));
    }

    #[test]
    fn test_secondary_index_remove() {
        let mut index = SecondaryIndex::new(vec![0]);

        let pk = make_pk(1);
        let location = make_location("file1.parquet", 0, "seq1");

        index.insert(pk.clone(), location.clone());
        let removed = index.remove(&pk);

        assert_eq!(removed, Some(location));
        assert_eq!(index.len(), 0);
        assert!(index.get(&pk).is_none());
    }

    #[test]
    fn test_secondary_index_stats() {
        let mut index = SecondaryIndex::new(vec![0]);

        let pk = make_pk(1);
        let location = make_location("file1.parquet", 0, "seq1");

        index.insert(pk.clone(), location);
        index.get(&pk);
        index.get(&make_pk(999)); // Miss

        let stats = index.stats();
        assert_eq!(stats.insert_count, 1);
        assert_eq!(stats.lookup_count, 2);
        assert_eq!(stats.lookup_miss_count, 1);
    }

    #[test]
    fn test_secondary_index_with_capacity() {
        let index = SecondaryIndex::with_capacity(vec![0, 1], 1000);
        assert!(index.is_empty());
        assert_eq!(index.pk_column_indices(), &[0, 1]);
    }
}
