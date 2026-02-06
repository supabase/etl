/// Table mappings for MySQL replication.
///
/// This module provides utilities for managing table mappings between source
/// and destination tables in the ETL pipeline.

use crate::types::{TableId, TableName};
use std::collections::HashMap;

/// A mapping between source table IDs and their names.
pub struct TableMappings {
    mappings: HashMap<TableId, TableName>,
}

impl TableMappings {
    pub fn new() -> Self {
        Self {
            mappings: HashMap::new(),
        }
    }

    /// Adds a mapping between a table ID and table name.
    pub fn add_mapping(&mut self, table_id: TableId, table_name: TableName) {
        self.mappings.insert(table_id, table_name);
    }

    /// Retrieves the table name for a given table ID.
    pub fn get_table_name(&self, table_id: &TableId) -> Option<&TableName> {
        self.mappings.get(table_id)
    }

    /// Returns all table mappings.
    pub fn all_mappings(&self) -> &HashMap<TableId, TableName> {
        &self.mappings
    }

    /// Returns the number of mappings.
    pub fn len(&self) -> usize {
        self.mappings.len()
    }

    /// Returns whether the mappings are empty.
    pub fn is_empty(&self) -> bool {
        self.mappings.is_empty()
    }
}

impl Default for TableMappings {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_mappings() {
        let mut mappings = TableMappings::new();
        assert!(mappings.is_empty());

        let table_id = TableId::new(123);
        let table_name = TableName::new("test_db".to_string(), "users".to_string());

        mappings.add_mapping(table_id, table_name.clone());
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings.get_table_name(&table_id), Some(&table_name));
    }
}
