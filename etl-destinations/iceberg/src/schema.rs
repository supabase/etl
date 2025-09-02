use std::sync::Arc;

use etl::types::{TableSchema, Type, is_array_type};
use iceberg::spec::{
    ListType, NestedField, PrimitiveType, Schema as IcebergSchema, Type as IcebergType,
};

/// Converts a Postgres array type to equivalent Iceberg type
fn postgres_array_type_to_iceberg_type(typ: &Type, field_id: i32) -> IcebergType {
    match typ {
        &Type::BOOL_ARRAY => create_iceberg_list_type(PrimitiveType::Boolean, field_id),
        &Type::CHAR_ARRAY
        | &Type::BPCHAR_ARRAY
        | &Type::VARCHAR_ARRAY
        | &Type::NAME_ARRAY
        | &Type::TEXT_ARRAY => create_iceberg_list_type(PrimitiveType::String, field_id),
        &Type::INT2_ARRAY | &Type::INT4_ARRAY => {
            create_iceberg_list_type(PrimitiveType::Int, field_id)
        }
        &Type::INT8_ARRAY => create_iceberg_list_type(PrimitiveType::Long, field_id),
        &Type::FLOAT4_ARRAY => create_iceberg_list_type(PrimitiveType::Float, field_id),
        &Type::FLOAT8_ARRAY => create_iceberg_list_type(PrimitiveType::Double, field_id),
        // numeric is mapped to string for now because decimal type in Iceberg needs scale and precision
        // which we don't have in the Type
        &Type::NUMERIC_ARRAY => create_iceberg_list_type(PrimitiveType::String, field_id),
        &Type::DATE_ARRAY => create_iceberg_list_type(PrimitiveType::Date, field_id),
        &Type::TIME_ARRAY => create_iceberg_list_type(PrimitiveType::Time, field_id),
        &Type::TIMESTAMP_ARRAY => create_iceberg_list_type(PrimitiveType::Timestamp, field_id),
        &Type::TIMESTAMPTZ_ARRAY => create_iceberg_list_type(PrimitiveType::Timestamptz, field_id),
        &Type::UUID_ARRAY => create_iceberg_list_type(PrimitiveType::Uuid, field_id),
        &Type::JSON_ARRAY | &Type::JSONB_ARRAY => {
            create_iceberg_list_type(PrimitiveType::String, field_id)
        }
        &Type::OID_ARRAY => create_iceberg_list_type(PrimitiveType::Int, field_id),
        &Type::BYTEA_ARRAY => create_iceberg_list_type(PrimitiveType::Binary, field_id),
        _ => create_iceberg_list_type(PrimitiveType::String, field_id),
    }
}

/// Converts a Postgres scalar type to equivalent Iceberg type
fn postgres_scalar_type_to_iceberg_type(typ: &Type) -> IcebergType {
    match typ {
        &Type::BOOL => IcebergType::Primitive(PrimitiveType::Boolean),
        &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => {
            IcebergType::Primitive(PrimitiveType::String)
        }
        &Type::INT2 | &Type::INT4 => IcebergType::Primitive(PrimitiveType::Int),
        &Type::INT8 => IcebergType::Primitive(PrimitiveType::Long),
        &Type::FLOAT4 => IcebergType::Primitive(PrimitiveType::Float),
        &Type::FLOAT8 => IcebergType::Primitive(PrimitiveType::Double),
        // numeric is mapped to string for now because decimal type in Iceberg needs scale and precision
        // which we don't have in the Type
        &Type::NUMERIC => IcebergType::Primitive(PrimitiveType::String),
        &Type::DATE => IcebergType::Primitive(PrimitiveType::Date),
        &Type::TIME => IcebergType::Primitive(PrimitiveType::Time),
        &Type::TIMESTAMP => IcebergType::Primitive(PrimitiveType::Timestamp),
        &Type::TIMESTAMPTZ => IcebergType::Primitive(PrimitiveType::Timestamptz),
        &Type::UUID => IcebergType::Primitive(PrimitiveType::Uuid),
        &Type::JSON | &Type::JSONB => IcebergType::Primitive(PrimitiveType::String),
        &Type::OID => IcebergType::Primitive(PrimitiveType::Int),
        &Type::BYTEA => IcebergType::Primitive(PrimitiveType::Binary),
        _ => IcebergType::Primitive(PrimitiveType::String),
    }
}

/// Creates an Iceberg list type with the specified element type.
fn create_iceberg_list_type(element_type: PrimitiveType, field_id: i32) -> IcebergType {
    // Create the element field with a standard name
    // Use the provided field_id for the element field to ensure uniqueness
    let element_field = Arc::new(NestedField::list_element(
        field_id,
        IcebergType::Primitive(element_type),
        false,
    ));

    let list_type = ListType { element_field };

    IcebergType::List(list_type)
}

/// Converts a Postgres table schema to an Iceberg schema.
pub fn postgres_to_iceberg_schema(schema: &TableSchema) -> Result<IcebergSchema, iceberg::Error> {
    let mut fields = Vec::new();
    let mut field_id = 1;

    // Convert each column to Iceberg field
    for column in &schema.column_schemas {
        let field_type = if is_array_type(&column.typ) {
            // For array types, we need to assign a unique field ID to the list element
            // We increment field_id and use it for the element field
            field_id += 1;
            postgres_array_type_to_iceberg_type(&column.typ, field_id - 1)
        } else {
            postgres_scalar_type_to_iceberg_type(&column.typ)
        };

        let field = if column.nullable {
            NestedField::optional(field_id, &column.name, field_type)
        } else {
            NestedField::required(field_id, &column.name, field_type)
        };
        fields.push(Arc::new(field));
        field_id += 1;
    }

    let schema = IcebergSchema::builder().with_fields(fields).build()?;

    Ok(schema)
}

#[cfg(test)]
mod tests {
    use iceberg::spec::LIST_FIELD_NAME;

    use super::*;

    /// Converts Postgres types to equivalent Iceberg types
    fn postgres_to_iceberg_type(typ: &Type) -> IcebergType {
        if is_array_type(typ) {
            postgres_array_type_to_iceberg_type(typ, 0) // Use 0 as placeholder for tests
        } else {
            postgres_scalar_type_to_iceberg_type(typ)
        }
    }

    #[test]
    fn test_postgres_to_iceberg_scalar_types() {
        // Boolean types
        assert_eq!(
            postgres_to_iceberg_type(&Type::BOOL),
            IcebergType::Primitive(PrimitiveType::Boolean)
        );

        // String types
        assert_eq!(
            postgres_to_iceberg_type(&Type::CHAR),
            IcebergType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::BPCHAR),
            IcebergType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::VARCHAR),
            IcebergType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::NAME),
            IcebergType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::TEXT),
            IcebergType::Primitive(PrimitiveType::String)
        );

        // Integer types
        assert_eq!(
            postgres_to_iceberg_type(&Type::INT2),
            IcebergType::Primitive(PrimitiveType::Int)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::INT4),
            IcebergType::Primitive(PrimitiveType::Int)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::INT8),
            IcebergType::Primitive(PrimitiveType::Long)
        );

        // Float types
        assert_eq!(
            postgres_to_iceberg_type(&Type::FLOAT4),
            IcebergType::Primitive(PrimitiveType::Float)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::FLOAT8),
            IcebergType::Primitive(PrimitiveType::Double)
        );

        // Numeric type (mapped to string)
        assert_eq!(
            postgres_to_iceberg_type(&Type::NUMERIC),
            IcebergType::Primitive(PrimitiveType::String)
        );

        // Date/Time types
        assert_eq!(
            postgres_to_iceberg_type(&Type::DATE),
            IcebergType::Primitive(PrimitiveType::Date)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::TIME),
            IcebergType::Primitive(PrimitiveType::Time)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::TIMESTAMP),
            IcebergType::Primitive(PrimitiveType::Timestamp)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::TIMESTAMPTZ),
            IcebergType::Primitive(PrimitiveType::Timestamptz)
        );

        // UUID type
        assert_eq!(
            postgres_to_iceberg_type(&Type::UUID),
            IcebergType::Primitive(PrimitiveType::Uuid)
        );

        // JSON types (mapped to string)
        assert_eq!(
            postgres_to_iceberg_type(&Type::JSON),
            IcebergType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            postgres_to_iceberg_type(&Type::JSONB),
            IcebergType::Primitive(PrimitiveType::String)
        );

        // OID type
        assert_eq!(
            postgres_to_iceberg_type(&Type::OID),
            IcebergType::Primitive(PrimitiveType::Int)
        );

        // Binary type
        assert_eq!(
            postgres_to_iceberg_type(&Type::BYTEA),
            IcebergType::Primitive(PrimitiveType::Binary)
        );
    }

    fn assert_list_type(array_type: IcebergType, expected_primitive_type: PrimitiveType) {
        if let IcebergType::List(list_type) = array_type {
            assert_eq!(
                *list_type.element_field.field_type,
                IcebergType::Primitive(expected_primitive_type)
            );
        }
    }

    #[test]
    fn test_postgres_to_iceberg_array_types() {
        // Boolean array
        let bool_array_type = postgres_to_iceberg_type(&Type::BOOL_ARRAY);
        assert!(matches!(bool_array_type, IcebergType::List(_)));
        assert_list_type(bool_array_type, PrimitiveType::Boolean);

        // String arrays
        let text_array_type = postgres_to_iceberg_type(&Type::TEXT_ARRAY);
        assert!(matches!(text_array_type, IcebergType::List(_)));
        assert_list_type(text_array_type, PrimitiveType::String);

        let varchar_array_type = postgres_to_iceberg_type(&Type::VARCHAR_ARRAY);
        assert!(matches!(varchar_array_type, IcebergType::List(_)));
        assert_list_type(varchar_array_type, PrimitiveType::String);

        // Integer arrays
        let int2_array_type = postgres_to_iceberg_type(&Type::INT2_ARRAY);
        assert!(matches!(int2_array_type, IcebergType::List(_)));
        assert_list_type(int2_array_type, PrimitiveType::Int);

        let int4_array_type = postgres_to_iceberg_type(&Type::INT4_ARRAY);
        assert!(matches!(int4_array_type, IcebergType::List(_)));
        assert_list_type(int4_array_type, PrimitiveType::Int);

        let int8_array_type = postgres_to_iceberg_type(&Type::INT8_ARRAY);
        assert!(matches!(int8_array_type, IcebergType::List(_)));
        assert_list_type(int8_array_type, PrimitiveType::Long);

        // Float arrays
        let float4_array_type = postgres_to_iceberg_type(&Type::FLOAT4_ARRAY);
        assert!(matches!(float4_array_type, IcebergType::List(_)));
        assert_list_type(float4_array_type, PrimitiveType::Float);

        let float8_array_type = postgres_to_iceberg_type(&Type::FLOAT8_ARRAY);
        assert!(matches!(float8_array_type, IcebergType::List(_)));
        assert_list_type(float8_array_type, PrimitiveType::Double);

        // Date/Time arrays
        let date_array_type = postgres_to_iceberg_type(&Type::DATE_ARRAY);
        assert!(matches!(date_array_type, IcebergType::List(_)));
        assert_list_type(date_array_type, PrimitiveType::Date);

        let timestamp_array_type = postgres_to_iceberg_type(&Type::TIMESTAMP_ARRAY);
        assert!(matches!(timestamp_array_type, IcebergType::List(_)));
        assert_list_type(timestamp_array_type, PrimitiveType::Timestamp);

        let timestamptz_array_type = postgres_to_iceberg_type(&Type::TIMESTAMPTZ_ARRAY);
        assert!(matches!(timestamptz_array_type, IcebergType::List(_)));
        assert_list_type(timestamptz_array_type, PrimitiveType::Timestamptz);

        // UUID array
        let uuid_array_type = postgres_to_iceberg_type(&Type::UUID_ARRAY);
        assert!(matches!(uuid_array_type, IcebergType::List(_)));
        assert_list_type(uuid_array_type, PrimitiveType::Uuid);

        // JSON arrays (mapped to string)
        let json_array_type = postgres_to_iceberg_type(&Type::JSON_ARRAY);
        assert!(matches!(json_array_type, IcebergType::List(_)));
        assert_list_type(json_array_type, PrimitiveType::String);

        let jsonb_array_type = postgres_to_iceberg_type(&Type::JSONB_ARRAY);
        assert!(matches!(jsonb_array_type, IcebergType::List(_)));
        assert_list_type(jsonb_array_type, PrimitiveType::String);

        // OID array
        let oid_array_type = postgres_to_iceberg_type(&Type::OID_ARRAY);
        assert!(matches!(oid_array_type, IcebergType::List(_)));
        assert_list_type(oid_array_type, PrimitiveType::Int);

        // Binary array
        let bytea_array_type = postgres_to_iceberg_type(&Type::BYTEA_ARRAY);
        assert!(matches!(bytea_array_type, IcebergType::List(_)));
        assert_list_type(bytea_array_type, PrimitiveType::Binary);
    }

    #[test]
    fn test_create_iceberg_list_type() {
        let list_type = create_iceberg_list_type(PrimitiveType::String, 1);

        assert!(matches!(list_type, IcebergType::List(_)));
        if let IcebergType::List(list) = list_type {
            assert_eq!(list.element_field.id, 1);
            assert_eq!(list.element_field.name, LIST_FIELD_NAME);
            assert_eq!(
                *list.element_field.field_type,
                IcebergType::Primitive(PrimitiveType::String)
            );
            assert!(!list.element_field.required);
        }
    }

    #[test]
    fn test_postgres_scalar_type_fallback() {
        // Test fallback for unknown scalar types
        // Using a type that should fall through to the default case
        let result = postgres_scalar_type_to_iceberg_type(&Type::UNKNOWN);
        assert_eq!(result, IcebergType::Primitive(PrimitiveType::String));
    }

    #[test]
    fn test_postgres_array_type_fallback() {
        // Test that non-array types passed to array function still get handled
        // This tests the fallback case in the array function
        let array_type = postgres_array_type_to_iceberg_type(&Type::BOOL, 1); // Not an array type
        assert!(matches!(array_type, IcebergType::List(_)));
        assert_list_type(array_type, PrimitiveType::String);
    }

    #[test]
    fn test_list_type_element_field_properties() {
        let list_type = create_iceberg_list_type(PrimitiveType::Boolean, 2);
        assert!(matches!(list_type, IcebergType::List(_)));

        if let IcebergType::List(list) = list_type {
            // Test that the element field has the correct properties
            assert_eq!(list.element_field.id, 2);
            assert_eq!(list.element_field.name, LIST_FIELD_NAME);
            assert!(!list.element_field.required);
            assert_eq!(
                *list.element_field.field_type,
                IcebergType::Primitive(PrimitiveType::Boolean)
            );
        }
    }
}
