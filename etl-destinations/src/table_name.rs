use etl::error::{ErrorKind, EtlResult};
use etl::types::TableName;
use etl::{bail, etl_error};

/// Converts a [`TableName`] into a single underscore-escaped identifier.
///
/// The current underscore-based encoding uses `_` as a separator and `__` as an escape sequence.
/// Leading or trailing underscores would make downstream parsing ambiguous, so those are rejected.
/// For example, `schema = "a"` and `table = "_b"` would encode to `a___b`, which cannot be
/// unambiguously distinguished from other schema/table combinations under this format.
pub fn try_stringify_table_name(table_name: &TableName) -> EtlResult<String> {
    let escaped_schema = stringify_table_name_component(&table_name.schema, "schema name")?;
    let escaped_table = stringify_table_name_component(&table_name.name, "table name")?;

    Ok(format!("{escaped_schema}_{escaped_table}"))
}

/// Escapes underscores in a table name component after validating it can be encoded safely.
fn stringify_table_name_component(value: &str, component_name: &str) -> EtlResult<String> {
    validate_table_name_component_for_underscore_encoding(value, component_name)?;

    Ok(value.replace('_', "__"))
}

/// Validates that a table name component can be encoded with underscore escaping.
pub fn validate_table_name_component_for_underscore_encoding(
    value: &str,
    component_name: &str,
) -> EtlResult<()> {
    if value.starts_with('_') || value.ends_with('_') {
        bail!(
            ErrorKind::ValidationError,
            "destination table name cannot use leading or trailing underscores",
            format!(
                "{component_name} '{value}' cannot start or end with '_' because underscore-based destination table naming would be ambiguous"
            )
        );
    }

    if value.is_empty() {
        return Err(etl_error!(
            ErrorKind::ValidationError,
            "destination table name component cannot be empty",
            format!(
                "{component_name} cannot be empty when building an underscore-escaped destination table name"
            )
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stringifies_valid_table_name() {
        let table_name = TableName::new("a_b".to_string(), "c_d".to_string());
        let escaped = try_stringify_table_name(&table_name).unwrap();

        assert_eq!(escaped, "a__b_c__d");
    }

    #[test]
    fn rejects_leading_underscore() {
        let table_name = TableName::new("_schema".to_string(), "users".to_string());
        let err = try_stringify_table_name(&table_name).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ValidationError);
        assert_eq!(
            err.description(),
            Some("destination table name cannot use leading or trailing underscores")
        );
    }

    #[test]
    fn rejects_trailing_underscore() {
        let table_name = TableName::new("public".to_string(), "users_".to_string());
        let err = try_stringify_table_name(&table_name).unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ValidationError);
        assert_eq!(
            err.description(),
            Some("destination table name cannot use leading or trailing underscores")
        );
    }

    #[test]
    fn prevents_collisions_between_schema_and_table_underscores() {
        let table_name1 = TableName::new("a_b".to_string(), "c".to_string());
        let table_name2 = TableName::new("a".to_string(), "b_c".to_string());

        let id1 = try_stringify_table_name(&table_name1).unwrap();
        let id2 = try_stringify_table_name(&table_name2).unwrap();

        assert_eq!(id1, "a__b_c");
        assert_eq!(id2, "a_b__c");
        assert_ne!(id1, id2);
    }

    #[test]
    fn preserves_multiple_underscores() {
        let table_name = TableName::new("a__b".to_string(), "c__d".to_string());

        assert_eq!(
            try_stringify_table_name(&table_name).unwrap(),
            "a____b_c____d"
        );
    }
}
