use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::etl_error;
use etl::types::ColumnSchema;

/// Validates column schemas for Databend table creation.
///
/// Ensures that all column names are valid Databend identifiers and that
/// the schemas meet Databend's requirements.
pub fn validate_column_schemas(column_schemas: &[ColumnSchema]) -> EtlResult<()> {
    if column_schemas.is_empty() {
        return Err(etl_error!(
            ErrorKind::DestinationValidationFailed,
            "Empty column schemas",
            "Table must have at least one column"
        ));
    }

    for schema in column_schemas {
        validate_column_name(&schema.name)?;
    }

    Ok(())
}

/// Validates that a column name is a valid Databend identifier.
///
/// Databend identifiers:
/// - Can contain letters, digits, and underscores
/// - Must not be empty
/// - Can be quoted with backticks to allow reserved words and special characters
fn validate_column_name(name: &str) -> EtlResult<()> {
    if name.is_empty() {
        return Err(etl_error!(
            ErrorKind::DestinationValidationFailed,
            "Invalid column name",
            "Column name cannot be empty"
        ));
    }

    // Databend allows any name when quoted with backticks, so we don't need
    // strict validation here. The client will quote the names appropriately.
    Ok(())
}

/// Validates a table name for Databend.
///
/// Table names follow similar rules to column names.
pub fn validate_table_name(name: &str) -> EtlResult<()> {
    if name.is_empty() {
        return Err(etl_error!(
            ErrorKind::DestinationValidationFailed,
            "Invalid table name",
            "Table name cannot be empty"
        ));
    }

    // Check for extremely long names (Databend has a limit)
    if name.len() > 255 {
        return Err(etl_error!(
            ErrorKind::DestinationValidationFailed,
            "Invalid table name",
            format!("Table name too long: {} characters (max 255)", name.len())
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl::types::Type;

    #[test]
    fn test_validate_column_name_valid() {
        assert!(validate_column_name("id").is_ok());
        assert!(validate_column_name("user_name").is_ok());
        assert!(validate_column_name("created_at").is_ok());
        assert!(validate_column_name("column123").is_ok());
        assert!(validate_column_name("_private").is_ok());
    }

    #[test]
    fn test_validate_column_name_empty() {
        let result = validate_column_name("");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationValidationFailed);
    }

    #[test]
    fn test_validate_table_name_valid() {
        assert!(validate_table_name("users").is_ok());
        assert!(validate_table_name("user_orders").is_ok());
        assert!(validate_table_name("table_123").is_ok());
    }

    #[test]
    fn test_validate_table_name_empty() {
        let result = validate_table_name("");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationValidationFailed);
    }

    #[test]
    fn test_validate_table_name_too_long() {
        let long_name = "a".repeat(256);
        let result = validate_table_name(&long_name);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationValidationFailed);
    }

    #[test]
    fn test_validate_column_schemas_valid() {
        let schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::Int4,
                optional: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                typ: Type::Text,
                optional: true,
            },
        ];

        assert!(validate_column_schemas(&schemas).is_ok());
    }

    #[test]
    fn test_validate_column_schemas_empty() {
        let schemas = vec![];
        let result = validate_column_schemas(&schemas);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationValidationFailed);
    }

    #[test]
    fn test_validate_column_schemas_with_invalid_name() {
        let schemas = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::Int4,
                optional: false,
            },
            ColumnSchema {
                name: "".to_string(), // Invalid: empty name
                typ: Type::Text,
                optional: true,
            },
        ];

        let result = validate_column_schemas(&schemas);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::DestinationValidationFailed);
    }
}
