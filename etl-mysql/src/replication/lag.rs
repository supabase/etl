/// MySQL binlog lag monitoring.
///
/// This module provides utilities for monitoring replication lag using MySQL's binlog position.
/// Unlike PostgreSQL's LSN, MySQL uses file position based replication tracking.

use sqlx::MySqlPool;
use thiserror::Error;

/// Errors that can occur during lag calculation.
#[derive(Debug, Error)]
pub enum LagError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Failed to parse binlog position: {0}")]
    ParseError(String),
}

/// Represents a MySQL binlog position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BinlogPosition {
    /// The binlog file number.
    pub file_number: u32,
    /// The position within the binlog file.
    pub position: u64,
}

impl BinlogPosition {
    pub fn new(file_number: u32, position: u64) -> Self {
        Self {
            file_number,
            position,
        }
    }

    /// Calculates the approximate lag between two binlog positions.
    ///
    /// This is an estimation since we can't know the exact size difference
    /// without accessing the actual binlog files.
    pub fn lag_bytes(&self, other: &BinlogPosition) -> i64 {
        if self.file_number == other.file_number {
            other.position as i64 - self.position as i64
        } else {
            let file_diff = (other.file_number as i64 - self.file_number as i64) * 1024 * 1024;
            file_diff + (other.position as i64 - self.position as i64)
        }
    }
}

/// Gets the current binlog position from the MySQL server.
pub async fn get_current_binlog_position(pool: &MySqlPool) -> Result<BinlogPosition, LagError> {
    let row: (String, u64) = sqlx::query_as("SHOW MASTER STATUS")
        .fetch_one(pool)
        .await?;

    let file_name = row.0;
    let position = row.1;

    let file_number = parse_binlog_file_number(&file_name)?;

    Ok(BinlogPosition::new(file_number, position))
}

/// Parses a MySQL binlog file name to extract the file number.
///
/// Binlog files are typically named like "mysql-bin.000123" or "binlog.000456".
fn parse_binlog_file_number(file_name: &str) -> Result<u32, LagError> {
    let parts: Vec<&str> = file_name.split('.').collect();
    if parts.len() < 2 {
        return Err(LagError::ParseError(format!(
            "Invalid binlog file name: {}",
            file_name
        )));
    }

    parts
        .last()
        .and_then(|s| s.parse::<u32>().ok())
        .ok_or_else(|| LagError::ParseError(format!("Failed to parse file number: {}", file_name)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_binlog_file_number() {
        assert_eq!(parse_binlog_file_number("mysql-bin.000123").unwrap(), 123);
        assert_eq!(parse_binlog_file_number("binlog.000456").unwrap(), 456);
        assert_eq!(parse_binlog_file_number("log.001").unwrap(), 1);
    }

    #[test]
    fn test_parse_binlog_file_number_invalid() {
        assert!(parse_binlog_file_number("invalid").is_err());
        assert!(parse_binlog_file_number("").is_err());
        assert!(parse_binlog_file_number("file.abc").is_err());
    }

    #[test]
    fn test_binlog_position_lag() {
        let pos1 = BinlogPosition::new(1, 1000);
        let pos2 = BinlogPosition::new(1, 2000);
        assert_eq!(pos1.lag_bytes(&pos2), 1000);

        let pos3 = BinlogPosition::new(1, 1000);
        let pos4 = BinlogPosition::new(2, 500);
        assert!(pos3.lag_bytes(&pos4) > 0);
    }
}
