use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use etl::types::{Cell, PgNumeric, TableRow};
use etl_destinations::bigquery::encoding::BigQueryTableRow;
use std::str::FromStr;

#[tokio::test]
async fn test_numeric_range_validation_within_bounds() {
    let table_row = TableRow::new(vec![Cell::Numeric(PgNumeric::from_str("123.456789").unwrap())]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Valid numeric should convert successfully");
}

#[tokio::test]
async fn test_numeric_range_validation_large_value() {
    // This is the exact example from the GitHub issue
    let large_numeric_str = "12345678901234567890123456789012345678901234567890";
    let table_row = TableRow::new(vec![Cell::Numeric(PgNumeric::from_str(large_numeric_str).unwrap())]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Large numeric should convert successfully with clamping");
}

#[tokio::test]
async fn test_numeric_range_validation_max_bigquery_value() {
    // Test with BigQuery's maximum NUMERIC value
    let max_numeric_str = "99999999999999999999999999999.999999999";
    let table_row = TableRow::new(vec![Cell::Numeric(PgNumeric::from_str(max_numeric_str).unwrap())]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "BigQuery max numeric should convert successfully");
}

#[tokio::test]
async fn test_numeric_range_validation_min_bigquery_value() {
    // Test with BigQuery's minimum NUMERIC value
    let min_numeric_str = "-99999999999999999999999999999.999999999";
    let table_row = TableRow::new(vec![Cell::Numeric(PgNumeric::from_str(min_numeric_str).unwrap())]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "BigQuery min numeric should convert successfully");
}

#[tokio::test]
async fn test_numeric_special_values_nan() {
    let table_row = TableRow::new(vec![Cell::Numeric(PgNumeric::NaN)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "NaN numeric should convert successfully with clamping");
}

#[tokio::test]
async fn test_numeric_special_values_positive_infinity() {
    let table_row = TableRow::new(vec![Cell::Numeric(PgNumeric::PositiveInfinity)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Positive infinity numeric should convert successfully with clamping");
}

#[tokio::test]
async fn test_numeric_special_values_negative_infinity() {
    let table_row = TableRow::new(vec![Cell::Numeric(PgNumeric::NegativeInfinity)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Negative infinity numeric should convert successfully with clamping");
}

#[tokio::test]
async fn test_date_range_validation_within_bounds() {
    let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let table_row = TableRow::new(vec![Cell::Date(date)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Valid date should convert successfully");
}

#[tokio::test]
async fn test_date_range_validation_before_min() {
    // Test with a date before BigQuery's minimum (year 1)
    let date = NaiveDate::from_ymd_opt(0001, 1, 1).unwrap().pred_opt().unwrap();
    let table_row = TableRow::new(vec![Cell::Date(date)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Date before BigQuery min should convert successfully with clamping");
}

#[tokio::test]
async fn test_date_range_validation_after_max() {
    // Test with a date after BigQuery's maximum (year 9999)
    let date = NaiveDate::from_ymd_opt(10000, 1, 1).unwrap();
    let table_row = TableRow::new(vec![Cell::Date(date)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Date after BigQuery max should convert successfully with clamping");
}

#[tokio::test]
async fn test_timestamp_range_validation_within_bounds() {
    let timestamp = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2024, 6, 15).unwrap(),
        NaiveTime::from_hms_opt(10, 30, 0).unwrap(),
    );
    let table_row = TableRow::new(vec![Cell::TimeStamp(timestamp)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Valid timestamp should convert successfully");
}

#[tokio::test]
async fn test_timestamp_range_validation_ancient_date() {
    // Test with a timestamp that would be valid in PostgreSQL but not in BigQuery
    let ancient_timestamp = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    let table_row = TableRow::new(vec![Cell::TimeStamp(ancient_timestamp)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Ancient timestamp should convert successfully with clamping");
}

#[tokio::test]
async fn test_timestamp_range_validation_future_date() {
    // Test with a future timestamp that would be valid in PostgreSQL but not in BigQuery
    let future_timestamp = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(10000, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    let table_row = TableRow::new(vec![Cell::TimeStamp(future_timestamp)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Future timestamp should convert successfully with clamping");
}

#[tokio::test]
async fn test_timestamptz_range_validation_within_bounds() {
    let timestamptz = DateTime::parse_from_rfc3339("2024-06-15T10:30:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let table_row = TableRow::new(vec![Cell::TimeStampTz(timestamptz)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Valid timestamptz should convert successfully");
}

#[tokio::test]
async fn test_array_numeric_range_validation() {
    // Test array with mix of valid and invalid numeric values
    let valid_numeric = PgNumeric::from_str("123.45").unwrap();
    let large_numeric = PgNumeric::from_str("12345678901234567890123456789012345678901234567890").unwrap();
    let numeric_array = vec![Some(valid_numeric), Some(large_numeric), Some(PgNumeric::NaN), Some(PgNumeric::PositiveInfinity)];
    
    let table_row = TableRow::new(vec![Cell::Array(etl::types::ArrayCell::Numeric(numeric_array))]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Numeric array with mixed values should convert successfully with clamping");
}

#[tokio::test]
async fn test_array_date_range_validation() {
    // Test array with mix of valid and invalid date values
    let valid_date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
    let ancient_date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap();
    let future_date = NaiveDate::from_ymd_opt(10000, 1, 1).unwrap();
    let date_array = vec![Some(valid_date), Some(ancient_date), Some(future_date)];
    
    let table_row = TableRow::new(vec![Cell::Array(etl::types::ArrayCell::Date(date_array))]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Date array with mixed values should convert successfully with clamping");
}

#[tokio::test]
async fn test_mixed_types_range_validation() {
    // Test a table row with multiple columns containing edge case values
    let large_numeric = PgNumeric::from_str("12345678901234567890123456789012345678901234567890").unwrap();
    let ancient_date = NaiveDate::from_ymd_opt(1, 1, 1).unwrap().pred_opt().unwrap();
    let future_timestamp = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(10000, 1, 1).unwrap(),
        NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    );
    
    let table_row = TableRow::new(vec![
        Cell::Numeric(large_numeric),
        Cell::Date(ancient_date),
        Cell::TimeStamp(future_timestamp),
        Cell::Numeric(PgNumeric::NaN),
    ]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Mixed types with edge case values should convert successfully with clamping");
}

#[tokio::test]
async fn test_time_range_validation_edge_cases() {
    // Test with edge time values (should all be within BigQuery's range already)
    let midnight = NaiveTime::from_hms_opt(0, 0, 0).unwrap();
    let max_time = NaiveTime::from_hms_micro_opt(23, 59, 59, 999999).unwrap();
    
    let table_row1 = TableRow::new(vec![Cell::Time(midnight)]);
    let table_row2 = TableRow::new(vec![Cell::Time(max_time)]);
    
    let bigquery_row1 = BigQueryTableRow::try_from(table_row1);
    let bigquery_row2 = BigQueryTableRow::try_from(table_row2);
    
    assert!(bigquery_row1.is_ok(), "Midnight time should convert successfully");
    assert!(bigquery_row2.is_ok(), "Max time should convert successfully");
}

#[tokio::test]
async fn test_string_size_validation_within_limits() {
    let normal_string = "Hello, World! This is a normal string.";
    let table_row = TableRow::new(vec![Cell::String(normal_string.to_string())]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Normal string should convert successfully");
}

#[tokio::test]
async fn test_string_size_validation_exceeds_limit() {
    // Create a string larger than BigQuery's 2MB limit
    let large_string = "A".repeat(3 * 1024 * 1024); // 3MB string
    let table_row = TableRow::new(vec![Cell::String(large_string)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Large string should convert successfully with clamping");
}

#[tokio::test]
async fn test_string_size_validation_utf8_handling() {
    // Create a string with multi-byte UTF-8 characters near the limit
    let mut large_string = "A".repeat(2 * 1024 * 1024 - 10); // Just under 2MB
    large_string.push_str("🌟🌈🦄🚀"); // Add multi-byte UTF-8 characters
    let table_row = TableRow::new(vec![Cell::String(large_string)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "String with UTF-8 characters should convert successfully");
}

#[tokio::test]
async fn test_bytes_size_validation_within_limits() {
    let normal_bytes = b"Hello, World! These are normal bytes.".to_vec();
    let table_row = TableRow::new(vec![Cell::Bytes(normal_bytes)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Normal bytes should convert successfully");
}

#[tokio::test]
async fn test_bytes_size_validation_exceeds_limit() {
    // Create binary data larger than BigQuery's ~1MB limit
    let large_bytes = vec![0x42; 2 * 1024 * 1024]; // 2MB of data
    let table_row = TableRow::new(vec![Cell::Bytes(large_bytes)]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Large bytes should convert successfully with clamping");
}

#[tokio::test]
async fn test_array_string_size_validation() {
    // Test array with mix of normal and oversized strings
    let normal_string = Some("Normal string".to_string());
    let large_string = Some("A".repeat(3 * 1024 * 1024)); // 3MB string
    let string_array = vec![normal_string, large_string, Some("Another normal string".to_string())];
    
    let table_row = TableRow::new(vec![Cell::Array(etl::types::ArrayCell::String(string_array))]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "String array with mixed sizes should convert successfully with clamping");
}

#[tokio::test]
async fn test_array_bytes_size_validation() {
    // Test array with mix of normal and oversized binary data
    let normal_bytes = Some(b"Normal bytes".to_vec());
    let large_bytes = Some(vec![0x42; 2 * 1024 * 1024]); // 2MB of data
    let bytes_array = vec![normal_bytes, large_bytes, Some(b"Another normal bytes".to_vec())];
    
    let table_row = TableRow::new(vec![Cell::Array(etl::types::ArrayCell::Bytes(bytes_array))]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Bytes array with mixed sizes should convert successfully with clamping");
}

#[tokio::test]
async fn test_mixed_types_with_size_limits() {
    // Test a table row with multiple columns containing size edge cases
    let large_string = "A".repeat(3 * 1024 * 1024); // 3MB string
    let large_bytes = vec![0x42; 2 * 1024 * 1024]; // 2MB bytes
    let large_numeric = PgNumeric::from_str("12345678901234567890123456789012345678901234567890").unwrap();
    
    let table_row = TableRow::new(vec![
        Cell::String(large_string),
        Cell::Bytes(large_bytes),
        Cell::Numeric(large_numeric),
        Cell::Date(NaiveDate::from_ymd_opt(10000, 1, 1).unwrap()), // Future date
    ]);
    
    let bigquery_row = BigQueryTableRow::try_from(table_row);
    assert!(bigquery_row.is_ok(), "Mixed types with all size/range edge cases should convert successfully with clamping");
}