use data_encoding::BASE32_NOPAD;
use etl::{
    bail,
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::TableName,
};

/// Prefix for names that cannot use the legacy underscore encoding.
///
/// Legacy names never start with `_`, even after destination case folding.
pub(crate) const ENCODED_TABLE_NAME_PREFIX: &str = "_ETL1_";

/// Validates the source components supported by destination table naming.
fn validate_table_name_component(value: &str, component_name: &str) -> EtlResult<()> {
    const UNSUPPORTED_SQL_IDENTIFIER_CHARS: [char; 2] = ['"', ';'];

    if value.is_empty() {
        return Err(etl_error!(
            ErrorKind::ValidationError,
            "Destination table name component cannot be empty",
            format!("{component_name} cannot be empty when building a destination table name")
        ));
    }

    if let Some(character) =
        value.chars().find(|character| UNSUPPORTED_SQL_IDENTIFIER_CHARS.contains(character))
    {
        bail!(
            ErrorKind::ValidationError,
            "Destination table name contains an unsupported SQL identifier character",
            format!("{component_name} '{value}' contains unsupported character '{character}'")
        );
    }

    Ok(())
}

/// Converts a [`TableName`] into a collision-free destination identifier.
///
/// Preserves the legacy underscore encoding unless either component starts or
/// ends with `_`. Those names use separately encoded, uppercase Base32 UTF-8
/// components. Base32 contains no separator and preserves source case even in
/// destinations that fold identifier case. Generated object suffixes cannot
/// match a base name because they introduce additional separators.
///
/// Two PostgreSQL identifiers of at most 63 bytes produce at most 209 ASCII
/// characters. Destination limits still apply, including generated object
/// suffixes and ClickHouse's database-dependent filename budget.
pub(crate) fn try_stringify_table_name(table_name: &TableName) -> EtlResult<String> {
    validate_table_name_component(&table_name.schema, "schema name")?;
    validate_table_name_component(&table_name.name, "table name")?;

    if [&table_name.schema, &table_name.name]
        .into_iter()
        .any(|component| component.starts_with('_') || component.ends_with('_'))
    {
        let schema = BASE32_NOPAD.encode(table_name.schema.as_bytes());
        let table = BASE32_NOPAD.encode(table_name.name.as_bytes());
        return Ok(format!("{ENCODED_TABLE_NAME_PREFIX}{schema}_{table}"));
    }

    let escaped_schema = table_name.schema.replace('_', "__");
    let escaped_table = table_name.name.replace('_', "__");

    Ok(format!("{escaped_schema}_{escaped_table}"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use data_encoding::BASE32_NOPAD;
    use etl::{error::ErrorKind, schema::TableName};
    use proptest::prelude::*;

    use crate::table_name::{ENCODED_TABLE_NAME_PREFIX, try_stringify_table_name};

    /// Locks legacy and boundary-underscore mappings, including ambiguous
    /// pairs.
    #[test]
    fn stringifies_table_names() {
        for (schema, table, expected) in [
            ("public", "users", "public_users"),
            ("a_b", "c_d", "a__b_c__d"),
            ("a__b", "c__d", "a____b_c____d"),
            ("Mixed", "Case", "Mixed_Case"),
            ("a_b", "c", "a__b_c"),
            ("a", "b_c", "a_b__c"),
            ("a", "_b", "_ETL1_ME_L5RA"),
            ("a_", "b", "_ETL1_MFPQ_MI"),
            ("a", "_B", "_ETL1_ME_L5BA"),
            ("_", "_", "_ETL1_L4_L4"),
            ("__", "__", "_ETL1_L5PQ_L5PQ"),
        ] {
            let table_name = TableName::new(schema.to_owned(), table.to_owned());
            assert_eq!(try_stringify_table_name(&table_name).unwrap(), expected);
        }
    }

    /// Keeps new base names disjoint from existing names and generated objects.
    #[test]
    fn encoded_names_do_not_collide_with_legacy_names_or_generated_objects() {
        let components = ["a", "A", "1", "_", "__", "a_", "_a", "a__", "__a", "_a_", "a_b"];
        let mut encoded = HashSet::new();
        let mut legacy = HashSet::new();
        for schema in components {
            for table in components {
                let source = TableName::new(schema.to_owned(), table.to_owned());
                let name = try_stringify_table_name(&source).unwrap().to_uppercase();
                if [schema, table].iter().any(|part| part.starts_with('_') || part.ends_with('_')) {
                    assert!(encoded.insert(name));
                } else {
                    // Existing destination case folding is intentionally
                    // unchanged.
                    legacy.insert(name);
                }
            }
        }
        assert!(encoded.is_disjoint(&legacy));
        for name in encoded.iter().chain(&legacy) {
            for suffix in ["_0", "_18446744073709551615", "__CURRENT", "-STREAMING", "_CHANGELOG"] {
                assert!(!encoded.contains(&format!("{name}{suffix}")));
            }
        }
    }

    /// Measures identifier lengths in source bytes, including generated
    /// suffixes.
    #[test]
    fn encoded_name_lengths_account_for_utf8_bytes_and_generated_suffixes() {
        for component in ["_".repeat(63), format!("_{}", "é".repeat(31))] {
            assert_eq!(component.len(), 63);
            let source = TableName::new(component.clone(), component);
            let name = try_stringify_table_name(&source).unwrap();
            assert!(name.is_ascii());
            assert_eq!(name.len(), 209);
            assert_eq!(format!("{name}-STREAMING").len(), 219);
            assert_eq!(format!("{name}__current").len(), 218);
            assert_eq!(format!("{name}_{}", u64::MAX).len(), 230);
        }
    }

    proptest! {
        /// Recovers the original UTF-8 components after destination case folding.
        #[test]
        fn encoded_components_preserve_utf8_and_case(
            schema in "[_a-zA-Z0-9é]{1,30}",
            table in "[_a-zA-Z0-9é]{1,30}",
        ) {
            let schema = format!("_{schema}");
            let source = TableName::new(schema.clone(), table.clone());
            let name = try_stringify_table_name(&source).unwrap();
            prop_assert_eq!(&name, &name.to_uppercase());
            let (encoded_schema, encoded_table) = name
                .strip_prefix(ENCODED_TABLE_NAME_PREFIX).unwrap().split_once('_').unwrap();
            prop_assert_eq!(BASE32_NOPAD.decode(encoded_schema.as_bytes()).unwrap(), schema.as_bytes());
            prop_assert_eq!(BASE32_NOPAD.decode(encoded_table.as_bytes()).unwrap(), table.as_bytes());
        }
    }

    #[test]
    fn rejects_empty_or_unsupported_components() {
        for (schema, table, description) in [
            ("", "users", "Destination table name component cannot be empty"),
            ("public", "", "Destination table name component cannot be empty"),
            (
                "public",
                "users\"quoted",
                "Destination table name contains an unsupported SQL identifier character",
            ),
            (
                "public",
                "users;drop",
                "Destination table name contains an unsupported SQL identifier character",
            ),
            (
                "_schema",
                "users;drop_",
                "Destination table name contains an unsupported SQL identifier character",
            ),
        ] {
            let table_name = TableName::new(schema.to_owned(), table.to_owned());
            let error = try_stringify_table_name(&table_name).unwrap_err();

            assert_eq!(error.kind(), ErrorKind::ValidationError);
            assert_eq!(error.description(), Some(description));
        }
    }
}
