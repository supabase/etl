use etl::{
    bail,
    error::{ErrorKind, EtlResult},
    etl_error,
    schema::TableName,
};

/// Largest component length representable by two decimal digits.
const MAX_LENGTH_PREFIX_COMPONENT_BYTES: usize = 99;

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

/// Converts a [`TableName`] into a destination identifier.
///
/// Preserves the legacy underscore encoding unless either component starts or
/// ends with `_`. Those names use `_SSTT_<schema>_<table>`, where `SS` and `TT`
/// are two-digit byte lengths. Both components must contain only lowercase
/// ASCII letters, digits, and underscores and fit in 99 bytes. Unsupported
/// components return a validation error; there is no fallback encoding.
///
/// Legacy names never start with `_`. The lengths disambiguate new names and
/// distinguish them from generated suffixes. Restricting new components to
/// lowercase ASCII preserves their identity after destination case folding.
///
/// Two PostgreSQL identifiers of at most 63 bytes produce at most 133 ASCII
/// characters in the length-prefixed form. Destination limits still apply,
/// including generated object and channel names.
pub(crate) fn try_stringify_table_name(table_name: &TableName) -> EtlResult<String> {
    validate_table_name_component(&table_name.schema, "schema name")?;
    validate_table_name_component(&table_name.name, "table name")?;

    if [&table_name.schema, &table_name.name]
        .into_iter()
        .any(|component| component.starts_with('_') || component.ends_with('_'))
    {
        for (value, component_name) in
            [(&table_name.schema, "schema name"), (&table_name.name, "table name")]
        {
            if value.len() > MAX_LENGTH_PREFIX_COMPONENT_BYTES {
                bail!(
                    ErrorKind::ValidationError,
                    "Destination table name component is too long for length-prefix encoding",
                    format!(
                        "{component_name} has {} bytes; names with leading or trailing \
                         underscores require each component to fit in \
                         {MAX_LENGTH_PREFIX_COMPONENT_BYTES} bytes",
                        value.len()
                    )
                );
            }

            if !value.bytes().all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_')) {
                bail!(
                    ErrorKind::ValidationError,
                    "Destination table name requires lowercase ASCII components",
                    format!(
                        "{component_name} must contain only lowercase ASCII letters, digits, and \
                         underscores when either component starts or ends with '_'"
                    )
                );
            }
        }

        return Ok(format!(
            "_{:02}{:02}_{}_{}",
            table_name.schema.len(),
            table_name.name.len(),
            table_name.schema,
            table_name.name
        ));
    }

    let escaped_schema = table_name.schema.replace('_', "__");
    let escaped_table = table_name.name.replace('_', "__");

    Ok(format!("{escaped_schema}_{escaped_table}"))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use etl::{error::ErrorKind, schema::TableName};
    use proptest::prelude::*;

    use crate::table_name::try_stringify_table_name;

    /// Locks legacy and boundary-underscore mappings, including ambiguous
    /// pairs.
    #[test]
    fn stringifies_table_names() {
        for (schema, table, expected) in [
            ("public", "users", "public_users"),
            ("a_b", "c_d", "a__b_c__d"),
            ("a__b", "c__d", "a____b_c____d"),
            ("Mixed", "Case", "Mixed_Case"),
            ("schéma", "用户", "schéma_用户"),
            ("a$b", "c d", "a$b_c d"),
            ("a_b", "c", "a__b_c"),
            ("a", "b_c", "a_b__c"),
            ("_public", "orders", "_0706__public_orders"),
            ("public_", "orders", "_0706_public__orders"),
            ("public", "_orders", "_0607_public__orders"),
            ("public", "orders_", "_0607_public_orders_"),
            ("_public", "_orders", "_0707__public__orders"),
            ("a", "_b", "_0102_a__b"),
            ("a_", "b", "_0201_a__b"),
            ("a_", "_b", "_0202_a___b"),
            ("__a_", "_b__", "_0404___a___b__"),
            ("_", "_", "_0101____"),
            ("__", "__", "_0202______"),
            ("_1", "2_", "_0202__1_2_"),
            ("a", "_b_0", "_0104_a__b_0"),
            ("a", "_b__current", "_0111_a__b__current"),
        ] {
            let table_name = TableName::new(schema.to_owned(), table.to_owned());
            assert_eq!(try_stringify_table_name(&table_name).unwrap(), expected);
        }
    }

    /// Keeps new base names disjoint from existing names and generated objects.
    #[test]
    fn encoded_names_do_not_collide_with_legacy_names_or_generated_objects() {
        let components = [
            "a",
            "1",
            "_",
            "__",
            "a_",
            "_a",
            "a__",
            "__a",
            "_a_",
            "a_b",
            "_a_0",
            "_a__current",
            "0102",
            "_0102_a__b",
        ];
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

    /// Covers decimal-width boundaries and the normal and representable limits
    /// independently for both components.
    #[test]
    fn length_prefixes_cover_component_boundaries() {
        for (schema_length, schema_digits) in
            [(1, "01"), (9, "09"), (10, "10"), (63, "63"), (99, "99")]
        {
            for (table_length, table_digits) in
                [(1, "01"), (9, "09"), (10, "10"), (63, "63"), (99, "99")]
            {
                let schema = "_".repeat(schema_length);
                let table = "a".repeat(table_length);
                let source = TableName::new(schema.clone(), table.clone());
                let name = try_stringify_table_name(&source).unwrap();
                assert_eq!(name, format!("_{schema_digits}{table_digits}_{schema}_{table}"));
                assert_eq!(name.len(), schema_length + table_length + 7);
            }
        }

        let source = TableName::new("_".repeat(63), "_".repeat(63));
        let name = try_stringify_table_name(&source).unwrap();
        assert_eq!(name.len(), 133);
        assert_eq!(format!("{name}-STREAMING").len(), 143);
        assert_eq!(format!("{name}__current").len(), 142);
        assert_eq!(format!("{name}_{}", u64::MAX).len(), 154);
    }

    proptest! {
        /// Recovers both original components after destination case folding.
        #[test]
        fn length_prefixed_components_roundtrip(
            schema in "[a-z0-9_]{0,98}",
            table in "[a-z0-9_]{1,99}",
        ) {
            let schema = format!("_{schema}");
            let source = TableName::new(schema.clone(), table.clone());
            let name = try_stringify_table_name(&source).unwrap().to_uppercase();
            prop_assert!(name.is_ascii());
            prop_assert!(name.starts_with('_'));
            let schema_length = name.get(1..3).unwrap().parse::<usize>().unwrap();
            let table_length = name.get(3..5).unwrap().parse::<usize>().unwrap();
            let components = name.get(5..).unwrap().strip_prefix('_').unwrap();
            let (decoded_schema, rest) = components.split_at(schema_length);
            let decoded_table = rest.strip_prefix('_').unwrap();
            prop_assert_eq!(decoded_table.len(), table_length);
            prop_assert_eq!(decoded_schema.to_ascii_lowercase(), schema);
            prop_assert_eq!(decoded_table.to_ascii_lowercase(), table);
        }
    }

    /// Rejects overflow in either field without changing legacy length
    /// behavior.
    #[test]
    fn rejects_components_that_overflow_length_prefixes() {
        for (schema, table) in [
            ("_".repeat(100), "a".to_owned()),
            ("a".to_owned(), "_".repeat(100)),
            ("a".repeat(100), "_".to_owned()),
            ("_".to_owned(), "a".repeat(100)),
            ("_".repeat(100), "_".repeat(100)),
            ("_".repeat(1000), "a".to_owned()),
        ] {
            let source = TableName::new(schema, table);
            let error = try_stringify_table_name(&source).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::ValidationError);
            assert_eq!(
                error.description(),
                Some("Destination table name component is too long for length-prefix encoding")
            );
        }

        let source = TableName::new("a".repeat(100), "b".repeat(100));
        assert_eq!(
            try_stringify_table_name(&source).unwrap(),
            format!("{}_{}", source.schema, source.name)
        );
    }

    /// Rejects characters that cannot be preserved by the readable namespace,
    /// including when only the other component has a boundary underscore.
    #[test]
    fn rejects_unsupported_length_prefixed_components() {
        for component in
            ["A", "aB", "é", "用户", "e\u{301}", "a b", "a-b", "a.b", "a$b", "a\n", "a\0", "😀"]
        {
            for (schema, table) in [(component, "_"), ("_", component)] {
                let source = TableName::new(schema.to_owned(), table.to_owned());
                let error = try_stringify_table_name(&source).unwrap_err();
                assert_eq!(error.kind(), ErrorKind::ValidationError);
                assert_eq!(
                    error.description(),
                    Some("Destination table name requires lowercase ASCII components")
                );
            }
        }
    }

    #[test]
    fn rejects_empty_or_unsupported_components() {
        for (schema, table, description) in [
            ("", "users", "Destination table name component cannot be empty"),
            ("public", "", "Destination table name component cannot be empty"),
            ("", "_", "Destination table name component cannot be empty"),
            ("_", "", "Destination table name component cannot be empty"),
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
            (
                "_schema\"",
                "users",
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
