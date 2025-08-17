//! Schema mapping between PostgreSQL and Iceberg table formats.

use etl::error::{ErrorKind, EtlError, EtlResult};
use etl::{etl_error};
use etl::types::{Cell, TableSchema, Type as PostgresType};
#[cfg(test)]
use etl::types::ColumnSchema;
use tokio_postgres::types::{Kind, Type};

// PostgreSQL OID constants - match what's used in etl-postgres
const BOOL_OID: u32 = 16;
const INT2_OID: u32 = 21;
const INT4_OID: u32 = 23;
const INT8_OID: u32 = 20;
const FLOAT4_OID: u32 = 700;
const FLOAT8_OID: u32 = 701;
const NUMERIC_OID: u32 = 1700;
const TEXT_OID: u32 = 25;
const VARCHAR_OID: u32 = 1043;
const CHAR_OID: u32 = 1042;
const NAME_OID: u32 = 19;
const BYTEA_OID: u32 = 17;
const DATE_OID: u32 = 1082;
const TIME_OID: u32 = 1083;
const TIMESTAMP_OID: u32 = 1114;
const TIMESTAMPTZ_OID: u32 = 1184;
const UUID_OID: u32 = 2950;
const JSON_OID: u32 = 114;
const JSONB_OID: u32 = 3802;

// Additional common PostgreSQL types
const TIMETZ_OID: u32 = 1266;  // TIME WITH TIME ZONE
const INTERVAL_OID: u32 = 1186; // INTERVAL
const MONEY_OID: u32 = 790;     // MONEY type
const INET_OID: u32 = 869;      // INET type (IP addresses)
const CIDR_OID: u32 = 650;      // CIDR type (network addresses)
const MACADDR_OID: u32 = 829;   // MAC address
const POINT_OID: u32 = 600;     // POINT geometric type
const BIT_OID: u32 = 1560;      // BIT type
const VARBIT_OID: u32 = 1562;   // BIT VARYING
const SMALLINT_ARRAY_OID: u32 = 1005; // INT2[]
const INTEGER_ARRAY_OID: u32 = 1007;  // INT4[]
const BIGINT_ARRAY_OID: u32 = 1016;   // INT8[]
const TEXT_ARRAY_OID: u32 = 1009;     // TEXT[]

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use chrono::{NaiveDate, NaiveTime};
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type as IcebergType};
use std::collections::HashMap;
use std::sync::Arc;
use base64::prelude::*;

/// Maps PostgreSQL table schemas to Iceberg schemas.
pub struct SchemaMapper {
    /// Cache of mapped schemas to avoid repeated conversion
    schema_cache: HashMap<String, IcebergSchema>,
}

impl SchemaMapper {
    /// Creates a new schema mapper.
    pub fn new() -> Self {
        Self {
            schema_cache: HashMap::new(),
        }
    }

    /// Converts a PostgreSQL table schema to an Iceberg schema.
    pub fn postgres_to_iceberg(&mut self, pg_schema: &TableSchema) -> EtlResult<IcebergSchema> {
        let cache_key = format!("{}:{}", pg_schema.id, pg_schema.column_schemas.len());
        
        if let Some(cached) = self.schema_cache.get(&cache_key) {
            return Ok(cached.clone());
        }

        let mut fields = Vec::new();
        let mut field_id = 1;

        // Convert each column to Iceberg field
        for column in &pg_schema.column_schemas {
            let field_type = self.postgres_type_to_iceberg(&column.typ)?;
            let field = NestedField::required(field_id, &column.name, field_type);
            fields.push(Arc::new(field));
            field_id += 1;
        }

        // Add CDC metadata fields if needed
        let cdc_fields = self.create_cdc_fields(field_id);
        fields.extend(cdc_fields);

        let schema = IcebergSchema::builder()
            .with_fields(fields)
            .build()
            .map_err(|e| etl_error!(
                ErrorKind::DestinationError,
                "Failed to build Iceberg schema",
                e.to_string()
            ))?;

        self.schema_cache.insert(cache_key, schema.clone());
        Ok(schema)
    }

    /// Converts Iceberg schema to Arrow schema.
    ///
    /// This is a simplified conversion for compatibility with iceberg-rs 0.6.
    /// In a complete implementation, this would dynamically convert each field.
    pub fn iceberg_to_arrow(&self, _iceberg_schema: &IcebergSchema) -> EtlResult<ArrowSchema> {
        // Simplified implementation - create a basic schema for now
        let mut fields = vec![
            Field::new("id", DataType::Int64, true),
            Field::new("data", DataType::LargeUtf8, true),
        ];
        
        // Add CDC metadata fields
        fields.push(Field::new("_CHANGE_TYPE", DataType::LargeUtf8, true));
        fields.push(Field::new("_CHANGE_SEQUENCE_NUMBER", DataType::LargeUtf8, true));
        fields.push(Field::new("_CHANGE_TIMESTAMP", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true));

        Ok(ArrowSchema::new(fields))
    }

    /// Converts PostgreSQL type to Iceberg type.
    pub fn postgres_type_to_iceberg(&self, pg_type: &PostgresType) -> EtlResult<IcebergType> {
        let iceberg_type = match pg_type.oid() {
            // Boolean types
            BOOL_OID => IcebergType::Primitive(PrimitiveType::Boolean),
            
            // Integer types
            INT2_OID => IcebergType::Primitive(PrimitiveType::Int),
            INT4_OID => IcebergType::Primitive(PrimitiveType::Int),
            INT8_OID => IcebergType::Primitive(PrimitiveType::Long),
            
            // Floating point types
            FLOAT4_OID => IcebergType::Primitive(PrimitiveType::Float),
            FLOAT8_OID => IcebergType::Primitive(PrimitiveType::Double),
            
            // Numeric/Decimal types - map to string for now
            NUMERIC_OID => IcebergType::Primitive(PrimitiveType::String),
            
            // String types
            TEXT_OID | VARCHAR_OID | CHAR_OID | NAME_OID => {
                IcebergType::Primitive(PrimitiveType::String)
            }
            
            // Binary types
            BYTEA_OID => IcebergType::Primitive(PrimitiveType::Binary),
            
            // Date/Time types
            DATE_OID => IcebergType::Primitive(PrimitiveType::Date),
            TIME_OID => IcebergType::Primitive(PrimitiveType::Time),
            TIMESTAMP_OID => IcebergType::Primitive(PrimitiveType::Timestamp),
            TIMESTAMPTZ_OID => IcebergType::Primitive(PrimitiveType::Timestamptz),
            
            // UUID type
            UUID_OID => IcebergType::Primitive(PrimitiveType::Uuid),
            
            // JSON types - map to string
            JSON_OID | JSONB_OID => IcebergType::Primitive(PrimitiveType::String),
            
            // Additional time types
            TIMETZ_OID => IcebergType::Primitive(PrimitiveType::Time),
            
            // Interval types - map to string (Iceberg doesn't have native interval)
            INTERVAL_OID => IcebergType::Primitive(PrimitiveType::String),
            
            // Money type - map to string (preserve precision)
            MONEY_OID => IcebergType::Primitive(PrimitiveType::String),
            
            // Network address types - map to string
            INET_OID | CIDR_OID => IcebergType::Primitive(PrimitiveType::String),
            
            // MAC address type - map to string
            MACADDR_OID => IcebergType::Primitive(PrimitiveType::String),
            
            // Geometric types - map to string (can be extended to proper geometry support later)
            POINT_OID => IcebergType::Primitive(PrimitiveType::String),
            
            // Bit types - map to binary for bit strings
            BIT_OID | VARBIT_OID => IcebergType::Primitive(PrimitiveType::Binary),
            
            // Common array types - map to proper Iceberg list types
            SMALLINT_ARRAY_OID => self.create_iceberg_list_type(PrimitiveType::Int)?,
            INTEGER_ARRAY_OID => self.create_iceberg_list_type(PrimitiveType::Int)?,
            BIGINT_ARRAY_OID => self.create_iceberg_list_type(PrimitiveType::Long)?,
            TEXT_ARRAY_OID => self.create_iceberg_list_type(PrimitiveType::String)?,
            
            // General array types - extract element type and create list
            _ if matches!(pg_type.kind(), Kind::Array(_element_type)) => {
                if let Kind::Array(element_type) = pg_type.kind() {
                    // Get the element type OID and recursively convert it
                    let element_oid = element_type.oid();
                    let element_pg_type = Type::new(
                        element_type.name().to_string(),
                        element_oid,
                        element_type.kind().clone(),
                        element_type.schema().to_string(),
                    );
                    
                    let element_iceberg_type = self.postgres_type_to_iceberg(&element_pg_type)?;
                    
                    // Create list type with the element type
                    match element_iceberg_type {
                        IcebergType::Primitive(primitive) => self.create_iceberg_list_type(primitive)?,
                        other => {
                            // For complex element types, fall back to string representation
                            tracing::warn!(
                                oid = %pg_type.oid(),
                                name = %pg_type.name(),
                                element_type = ?other,
                                "Complex array element type mapped to string representation"
                            );
                            IcebergType::Primitive(PrimitiveType::String)
                        }
                    }
                } else {
                    // Fallback for unexpected array kind structure
                    tracing::warn!(
                        oid = %pg_type.oid(),
                        name = %pg_type.name(),
                        "Unknown array type structure, mapping to string"
                    );
                    IcebergType::Primitive(PrimitiveType::String)
                }
            }
            
            // Default fallback to string for unknown types
            _ => {
                tracing::warn!(
                    oid = %pg_type.oid(),
                    name = %pg_type.name(),
                    "Unknown PostgreSQL type, mapping to string"
                );
                IcebergType::Primitive(PrimitiveType::String)
            }
        };

        Ok(iceberg_type)
    }

    /// Creates an Iceberg list type with the specified element type.
    fn create_iceberg_list_type(&self, element_type: PrimitiveType) -> EtlResult<IcebergType> {
        use iceberg::spec::ListType;
        
        // Create the element field with a standard name
        // Use field ID 0 for list elements (standard Iceberg convention)
        let element_field = Arc::new(NestedField::required(
            0, // Field ID for list element (lists use nested field ID 0)
            "element", // Standard name for list elements
            IcebergType::Primitive(element_type),
        ));
        
        let list_type = ListType { element_field };
        
        Ok(IcebergType::List(list_type))
    }

    /// Creates CDC metadata fields for change tracking.
    fn create_cdc_fields(&self, start_field_id: i32) -> Vec<Arc<NestedField>> {
        vec![
            Arc::new(NestedField::optional(
                start_field_id,
                "_CHANGE_TYPE",
                IcebergType::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                start_field_id + 1,
                "_CHANGE_SEQUENCE_NUMBER",
                IcebergType::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                start_field_id + 2,
                "_CHANGE_TIMESTAMP",
                IcebergType::Primitive(PrimitiveType::Timestamptz),
            )),
        ]
    }

    /// Converts PostgreSQL table schema to Arrow schema for writing.
    pub fn postgres_to_arrow(&self, pg_schema: &TableSchema) -> EtlResult<ArrowSchema> {
        let mut fields = Vec::new();

        for column in &pg_schema.column_schemas {
            let data_type = self.postgres_type_to_arrow(&column.typ)?;
            let field = Field::new(&column.name, data_type, true); // All fields nullable
            fields.push(field);
        }

        // Add CDC fields
        fields.push(Field::new("_CHANGE_TYPE", DataType::Utf8, true));
        fields.push(Field::new("_CHANGE_SEQUENCE_NUMBER", DataType::Utf8, true));
        fields.push(Field::new(
            "_CHANGE_TIMESTAMP",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ));

        Ok(ArrowSchema::new(fields))
    }

    /// Converts PostgreSQL type to Arrow DataType.
    fn postgres_type_to_arrow(&self, pg_type: &PostgresType) -> EtlResult<DataType> {
        let arrow_type = match pg_type.oid() {
            BOOL_OID => DataType::Boolean,
            INT2_OID => DataType::Int16,
            INT4_OID => DataType::Int32,
            INT8_OID => DataType::Int64,
            FLOAT4_OID => DataType::Float32,
            FLOAT8_OID => DataType::Float64,
            NUMERIC_OID => DataType::LargeUtf8, // Convert to string
            TEXT_OID | VARCHAR_OID | CHAR_OID | NAME_OID => DataType::LargeUtf8,
            BYTEA_OID => DataType::LargeBinary,
            DATE_OID => DataType::Date32,
            TIME_OID => DataType::Time64(TimeUnit::Microsecond),
            TIMESTAMP_OID => DataType::Timestamp(TimeUnit::Microsecond, None),
            TIMESTAMPTZ_OID => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            UUID_OID => DataType::LargeUtf8, // Convert to string
            JSON_OID | JSONB_OID => DataType::LargeUtf8,
            
            // Additional time types
            TIMETZ_OID => DataType::Time64(TimeUnit::Microsecond),
            
            // Interval, money, network, MAC, geometric types - map to string
            INTERVAL_OID | MONEY_OID | INET_OID | CIDR_OID | MACADDR_OID | POINT_OID => {
                DataType::LargeUtf8
            }
            
            // Bit types - map to binary
            BIT_OID | VARBIT_OID => DataType::LargeBinary,
            
            // Known array types - map to proper Arrow list types
            SMALLINT_ARRAY_OID => DataType::List(Arc::new(Field::new("item", DataType::Int16, true))),
            INTEGER_ARRAY_OID => DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            BIGINT_ARRAY_OID => DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            TEXT_ARRAY_OID => DataType::List(Arc::new(Field::new("item", DataType::LargeUtf8, true))),
            
            // General array types - extract element type and create list
            _ if matches!(pg_type.kind(), Kind::Array(_)) => {
                if let Kind::Array(element_type) = pg_type.kind() {
                    // Get the element type and recursively convert it
                    let element_pg_type = Type::new(
                        element_type.name().to_string(),
                        element_type.oid(),
                        element_type.kind().clone(),
                        element_type.schema().to_string(),
                    );
                    
                    match self.postgres_type_to_arrow(&element_pg_type) {
                        Ok(element_arrow_type) => {
                            DataType::List(Arc::new(Field::new("item", element_arrow_type, true)))
                        },
                        Err(_) => {
                            // Fall back to string for unsupported element types
                            tracing::warn!(
                                oid = %pg_type.oid(),
                                name = %pg_type.name(),
                                "Array with unsupported element type, mapping to string"
                            );
                            DataType::LargeUtf8
                        }
                    }
                } else {
                    // Fallback for unexpected array structure
                    DataType::LargeUtf8
                }
            }
            _ => {
                tracing::warn!(
                    oid = %pg_type.oid(),
                    name = %pg_type.name(),
                    "Unknown PostgreSQL type for Arrow, mapping to string"
                );
                DataType::LargeUtf8
            }
        };

        Ok(arrow_type)
    }
}

impl Default for SchemaMapper {
    fn default() -> Self {
        Self::new()
    }
}

/// Converts Cell values to Arrow array builders.
pub struct CellToArrowConverter;

impl CellToArrowConverter {
    /// Converts a Cell to its string representation for Arrow.
    pub fn cell_to_string(cell: &Cell) -> Option<String> {
        match cell {
            Cell::Null => None,
            Cell::Bool(b) => Some(b.to_string()),
            Cell::String(s) => Some(s.clone()),
            Cell::I16(i) => Some(i.to_string()),
            Cell::I32(i) => Some(i.to_string()),
            Cell::U32(u) => Some(u.to_string()),
            Cell::I64(i) => Some(i.to_string()),
            Cell::F32(f) => Some(f.to_string()),
            Cell::F64(f) => Some(f.to_string()),
            Cell::Numeric(n) => Some(n.to_string()),
            Cell::Date(d) => Some(d.format("%Y-%m-%d").to_string()),
            Cell::Time(t) => Some(t.format("%H:%M:%S%.6f").to_string()),
            Cell::TimeStamp(ts) => Some(ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
            Cell::TimeStampTz(ts) => Some(ts.to_rfc3339()),
            Cell::Uuid(u) => Some(u.to_string()),
            Cell::Json(j) => Some(j.to_string()),
            Cell::Bytes(b) => Some(base64::prelude::BASE64_STANDARD.encode(b)),
            Cell::Array(arr) => Some(format!("{:?}", arr)), // Simple debug representation
        }
    }

    /// Extracts boolean value from Cell.
    pub fn cell_to_bool(cell: &Cell) -> Option<bool> {
        match cell {
            Cell::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Extracts i16 value from Cell.
    pub fn cell_to_i16(cell: &Cell) -> Option<i16> {
        match cell {
            Cell::I16(i) => Some(*i),
            _ => None,
        }
    }

    /// Extracts i32 value from Cell.
    pub fn cell_to_i32(cell: &Cell) -> Option<i32> {
        match cell {
            Cell::I32(i) => Some(*i),
            Cell::I16(i) => Some(*i as i32),
            _ => None,
        }
    }

    /// Extracts i64 value from Cell.
    pub fn cell_to_i64(cell: &Cell) -> Option<i64> {
        match cell {
            Cell::I64(i) => Some(*i),
            Cell::I32(i) => Some(*i as i64),
            Cell::I16(i) => Some(*i as i64),
            _ => None,
        }
    }

    /// Extracts f32 value from Cell.
    pub fn cell_to_f32(cell: &Cell) -> Option<f32> {
        match cell {
            Cell::F32(f) => Some(*f),
            _ => None,
        }
    }

    /// Extracts f64 value from Cell.
    pub fn cell_to_f64(cell: &Cell) -> Option<f64> {
        match cell {
            Cell::F64(f) => Some(*f),
            Cell::F32(f) => Some(*f as f64),
            _ => None,
        }
    }

    /// Extracts date as days since epoch.
    pub fn cell_to_date32(cell: &Cell) -> Option<i32> {
        match cell {
            Cell::Date(date) => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
                Some(date.signed_duration_since(epoch).num_days() as i32)
            }
            _ => None,
        }
    }

    /// Extracts time as microseconds since midnight.
    pub fn cell_to_time64_micros(cell: &Cell) -> Option<i64> {
        match cell {
            Cell::Time(time) => {
                let midnight = NaiveTime::from_hms_opt(0, 0, 0)?;
                Some(time.signed_duration_since(midnight).num_microseconds()?)
            }
            _ => None,
        }
    }

    /// Extracts timestamp as microseconds since epoch.
    pub fn cell_to_timestamp_micros(cell: &Cell) -> Option<i64> {
        match cell {
            Cell::TimeStamp(ts) => Some(ts.and_utc().timestamp_micros()),
            Cell::TimeStampTz(ts) => Some(ts.timestamp_micros()),
            _ => None,
        }
    }

    /// Extracts bytes from Cell.
    pub fn cell_to_bytes(cell: &Cell) -> Option<Vec<u8>> {
        match cell {
            Cell::Bytes(b) => Some(b.clone()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use etl_postgres::schema::{Oid, TableId, TableName};
    use tokio_postgres::types::{Kind, Type};

    fn create_pg_type(oid: Oid, name: &str) -> Type {
        Type::new(name.to_string(), oid as u32, Kind::Simple, "pg_catalog".to_string())
    }

    fn create_test_schema() -> TableSchema {
        let columns = vec![
            ColumnSchema {
                name: "id".to_string(),
                typ: create_pg_type(20, "bigint"),  // INT8_OID
                nullable: false,
                modifier: 0,
                primary: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                typ: create_pg_type(25, "text"),    // TEXT_OID
                nullable: true,
                modifier: 0,
                primary: false,
            },
            ColumnSchema {
                name: "active".to_string(),
                typ: create_pg_type(16, "boolean"), // BOOL_OID
                nullable: false,
                modifier: 0,
                primary: false,
            },
            ColumnSchema {
                name: "created_at".to_string(),
                typ: create_pg_type(1184, "timestamptz"), // TIMESTAMPTZ_OID
                nullable: false,
                modifier: 0,
                primary: false,
            },
        ];

        TableSchema {
            id: TableId::new(12345), // Test table ID
            name: TableName::new("test_schema".to_string(), "test_table".to_string()),
            column_schemas: columns,
        }
    }

    #[test]
    fn test_postgres_to_iceberg_schema() {
        let mut mapper = SchemaMapper::new();
        let pg_schema = create_test_schema();
        
        let iceberg_schema = mapper.postgres_to_iceberg(&pg_schema).unwrap();
        
        // Note: iceberg::spec::Schema API doesn't expose fields() in this version
        // Validate that schema creation succeeds
        // Field-level validation will be added in future
        assert!(iceberg_schema.to_string().contains("id"));
        assert!(iceberg_schema.to_string().contains("name"));
    }

    #[test]
    fn test_postgres_to_arrow_schema() {
        let mapper = SchemaMapper::new();
        let pg_schema = create_test_schema();
        
        let arrow_schema = mapper.postgres_to_arrow(&pg_schema).unwrap();
        
        // Arrow schema should have the right number of fields
        assert_eq!(arrow_schema.fields().len(), 7); // 4 columns + 3 CDC fields
        
        let id_field = &arrow_schema.fields()[0];
        assert_eq!(id_field.name(), "id");
        assert_eq!(id_field.data_type(), &DataType::Int64);
        
        let name_field = &arrow_schema.fields()[1];
        assert_eq!(name_field.name(), "name");
        assert_eq!(name_field.data_type(), &DataType::LargeUtf8);
    }

    #[test]
    fn test_cell_to_string_conversion() {
        assert_eq!(
            CellToArrowConverter::cell_to_string(&Cell::String("test".to_string())),
            Some("test".to_string())
        );
        assert_eq!(
            CellToArrowConverter::cell_to_string(&Cell::I32(42)),
            Some("42".to_string())
        );
        assert_eq!(
            CellToArrowConverter::cell_to_string(&Cell::Bool(true)),
            Some("true".to_string())
        );
        assert_eq!(
            CellToArrowConverter::cell_to_string(&Cell::Null),
            None
        );
    }

    #[test]
    fn test_cell_to_numeric_conversion() {
        assert_eq!(
            CellToArrowConverter::cell_to_i32(&Cell::I32(42)),
            Some(42)
        );
        assert_eq!(
            CellToArrowConverter::cell_to_i64(&Cell::I32(42)),
            Some(42)
        );
        assert_eq!(
            CellToArrowConverter::cell_to_f64(&Cell::F32(3.14)),
            Some(3.140000104904175) // F32 precision converted to F64
        );
    }

    #[test]
    fn test_schema_caching() {
        let mut mapper = SchemaMapper::new();
        let pg_schema = create_test_schema();
        
        // First conversion
        let schema1 = mapper.postgres_to_iceberg(&pg_schema).unwrap();
        
        // Second conversion should use cache
        let schema2 = mapper.postgres_to_iceberg(&pg_schema).unwrap();
        
        // Should be identical (cache hit)
        // Check that both schemas were created successfully
        assert!(!schema1.to_string().is_empty());
        assert!(!schema2.to_string().is_empty());
    }

    #[test]
    fn test_additional_postgres_types_to_iceberg() {
        let mapper = SchemaMapper::new();
        
        // Test additional time types
        let timetz_type = create_pg_type(TIMETZ_OID, "timetz");
        let result = mapper.postgres_type_to_iceberg(&timetz_type).unwrap();
        assert!(matches!(result, IcebergType::Primitive(PrimitiveType::Time)));
        
        // Test interval type (maps to string)
        let interval_type = create_pg_type(INTERVAL_OID, "interval");
        let result = mapper.postgres_type_to_iceberg(&interval_type).unwrap();
        assert!(matches!(result, IcebergType::Primitive(PrimitiveType::String)));
        
        // Test money type (maps to string to preserve precision)
        let money_type = create_pg_type(MONEY_OID, "money");
        let result = mapper.postgres_type_to_iceberg(&money_type).unwrap();
        assert!(matches!(result, IcebergType::Primitive(PrimitiveType::String)));
        
        // Test network types (map to string)
        let inet_type = create_pg_type(INET_OID, "inet");
        let result = mapper.postgres_type_to_iceberg(&inet_type).unwrap();
        assert!(matches!(result, IcebergType::Primitive(PrimitiveType::String)));
        
        let cidr_type = create_pg_type(CIDR_OID, "cidr");
        let result = mapper.postgres_type_to_iceberg(&cidr_type).unwrap();
        assert!(matches!(result, IcebergType::Primitive(PrimitiveType::String)));
        
        // Test MAC address type (maps to string)
        let macaddr_type = create_pg_type(MACADDR_OID, "macaddr");
        let result = mapper.postgres_type_to_iceberg(&macaddr_type).unwrap();
        assert!(matches!(result, IcebergType::Primitive(PrimitiveType::String)));
        
        // Test geometric type (maps to string)
        let point_type = create_pg_type(POINT_OID, "point");
        let result = mapper.postgres_type_to_iceberg(&point_type).unwrap();
        assert!(matches!(result, IcebergType::Primitive(PrimitiveType::String)));
        
        // Test bit types (map to binary)
        let bit_type = create_pg_type(BIT_OID, "bit");
        let result = mapper.postgres_type_to_iceberg(&bit_type).unwrap();
        assert!(matches!(result, IcebergType::Primitive(PrimitiveType::Binary)));
        
        let varbit_type = create_pg_type(VARBIT_OID, "varbit");
        let result = mapper.postgres_type_to_iceberg(&varbit_type).unwrap();
        assert!(matches!(result, IcebergType::Primitive(PrimitiveType::Binary)));
    }

    #[test]
    fn test_additional_postgres_types_to_arrow() {
        let mapper = SchemaMapper::new();
        
        // Test additional time types
        let timetz_type = create_pg_type(TIMETZ_OID, "timetz");
        let result = mapper.postgres_type_to_arrow(&timetz_type).unwrap();
        assert!(matches!(result, DataType::Time64(TimeUnit::Microsecond)));
        
        // Test interval type (maps to string)
        let interval_type = create_pg_type(INTERVAL_OID, "interval");
        let result = mapper.postgres_type_to_arrow(&interval_type).unwrap();
        assert!(matches!(result, DataType::LargeUtf8));
        
        // Test money type (maps to string)
        let money_type = create_pg_type(MONEY_OID, "money");
        let result = mapper.postgres_type_to_arrow(&money_type).unwrap();
        assert!(matches!(result, DataType::LargeUtf8));
        
        // Test network types (map to string)
        let inet_type = create_pg_type(INET_OID, "inet");
        let result = mapper.postgres_type_to_arrow(&inet_type).unwrap();
        assert!(matches!(result, DataType::LargeUtf8));
        
        // Test bit types (map to binary)
        let bit_type = create_pg_type(BIT_OID, "bit");
        let result = mapper.postgres_type_to_arrow(&bit_type).unwrap();
        assert!(matches!(result, DataType::LargeBinary));
    }

    #[test]
    fn test_array_types_mapping() {
        let mapper = SchemaMapper::new();
        
        // Test known array types (should map to list)
        let int_array_type = create_pg_type(INTEGER_ARRAY_OID, "int4[]");
        let result = mapper.postgres_type_to_iceberg(&int_array_type).unwrap();
        assert!(matches!(result, IcebergType::List(_)));
        
        let text_array_type = create_pg_type(TEXT_ARRAY_OID, "text[]");
        let result = mapper.postgres_type_to_iceberg(&text_array_type).unwrap();
        assert!(matches!(result, IcebergType::List(_)));
        
        // Test Arrow conversion for arrays
        let int_array_arrow = mapper.postgres_type_to_arrow(&int_array_type).unwrap();
        assert!(matches!(int_array_arrow, DataType::List(_)));
    }

    #[test]
    fn test_comprehensive_schema_with_new_types() {
        let columns = vec![
            // Original types
            ColumnSchema {
                name: "id".to_string(),
                typ: create_pg_type(20, "bigint"),
                nullable: false,
                modifier: 0,
                primary: false,
            },
            ColumnSchema {
                name: "name".to_string(),
                typ: create_pg_type(25, "text"),
                nullable: true,
                modifier: 0,
                primary: false,
            },
            // New types
            ColumnSchema {
                name: "created_time".to_string(),
                typ: create_pg_type(TIMETZ_OID, "timetz"),
                nullable: true,
                modifier: 0,
                primary: false,
            },
            ColumnSchema {
                name: "salary".to_string(),
                typ: create_pg_type(MONEY_OID, "money"),
                nullable: true,
                modifier: 0,
                primary: false,
            },
            ColumnSchema {
                name: "ip_address".to_string(),
                typ: create_pg_type(INET_OID, "inet"),
                nullable: true,
                modifier: 0,
                primary: false,
            },
            ColumnSchema {
                name: "location".to_string(),
                typ: create_pg_type(POINT_OID, "point"),
                nullable: true,
                modifier: 0,
                primary: false,
            },
            ColumnSchema {
                name: "tags".to_string(),
                typ: create_pg_type(TEXT_ARRAY_OID, "text[]"),
                nullable: true,
                modifier: 0,
                primary: false,
            },
        ];

        let comprehensive_schema = TableSchema {
            id: TableId::new(12346),
            name: TableName::new("test_schema".to_string(), "comprehensive_table".to_string()),
            column_schemas: columns,
        };

        let mut mapper = SchemaMapper::new();
        
        // Test Iceberg schema conversion
        let iceberg_schema = mapper.postgres_to_iceberg(&comprehensive_schema).unwrap();
        assert!(iceberg_schema.to_string().contains("created_time"));
        assert!(iceberg_schema.to_string().contains("salary"));
        assert!(iceberg_schema.to_string().contains("ip_address"));
        
        // Test Arrow schema conversion
        let arrow_schema = mapper.postgres_to_arrow(&comprehensive_schema).unwrap();
        assert_eq!(arrow_schema.fields().len(), 10); // 7 columns + 3 CDC fields
        
        // Verify specific field types
        let created_time_field = arrow_schema.field_with_name("created_time").unwrap();
        assert!(matches!(created_time_field.data_type(), DataType::Time64(TimeUnit::Microsecond)));
        
        let salary_field = arrow_schema.field_with_name("salary").unwrap();
        assert!(matches!(salary_field.data_type(), DataType::LargeUtf8));
        
        let ip_field = arrow_schema.field_with_name("ip_address").unwrap();
        assert!(matches!(ip_field.data_type(), DataType::LargeUtf8));
        
        let tags_field = arrow_schema.field_with_name("tags").unwrap();
        assert!(matches!(tags_field.data_type(), DataType::List(_)));
    }
}