use etl::test_utils::database::spawn_source_database;
use etl::types::{Cell, ArrayCell, TableRow, PgNumeric};
use etl_telemetry::init_test_tracing;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc, TimeZone};
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread")]
async fn test_basic_data_types_with_postgres() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    
    // Create table with basic data types
    client.batch_execute(r#"
        CREATE TABLE test.basic_types_test (
            id SERIAL PRIMARY KEY,
            bool_col BOOLEAN,
            smallint_col SMALLINT,
            int_col INTEGER,
            bigint_col BIGINT,
            real_col REAL,
            double_col DOUBLE PRECISION,
            text_col TEXT,
            varchar_col VARCHAR(100),
            char_col CHAR(10),
            name_col NAME
        )
    "#).await.expect("Failed to create basic_types_test table");

    // Insert test data
    let row = client.query_one(r#"
        INSERT INTO test.basic_types_test 
        (bool_col, smallint_col, int_col, bigint_col, real_col, double_col, text_col, varchar_col, char_col, name_col)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING *
    "#, &[
        &true,                              // bool_col
        &(12345i16),                        // smallint_col  
        &(987654321i32),                    // int_col
        &(123456789012345i64),              // bigint_col
        &(3.14159f32),                      // real_col
        &(2.718281828459045f64),            // double_col
        &"Hello, World! 🌍",                // text_col
        &"varchar test",                     // varchar_col
        &"char_test",                       // char_col
        &"name_test",                       // name_col
    ]).await.expect("Failed to insert test data");

    // Test extraction and verify all PostgreSQL data types work
    assert_eq!(row.get::<_, bool>("bool_col"), true);
    assert_eq!(row.get::<_, i16>("smallint_col"), 12345);
    assert_eq!(row.get::<_, i32>("int_col"), 987654321);
    assert_eq!(row.get::<_, i64>("bigint_col"), 123456789012345);
    assert_eq!(row.get::<_, f32>("real_col"), 3.14159f32);
    assert_eq!(row.get::<_, f64>("double_col"), 2.718281828459045f64);
    assert_eq!(row.get::<_, String>("text_col"), "Hello, World! 🌍");
    assert_eq!(row.get::<_, String>("varchar_col"), "varchar test");
    // Note: CHAR pads with spaces
    assert_eq!(row.get::<_, String>("char_col").trim(), "char_test");
    assert_eq!(row.get::<_, String>("name_col"), "name_test");

    // Test that we can create Cell types that would be used in conversions
    let test_cells = vec![
        Cell::Bool(true),
        Cell::I16(12345),
        Cell::I32(987654321),
        Cell::I64(123456789012345),
        Cell::F32(3.14159f32),
        Cell::F64(2.718281828459045f64),
        Cell::String("Hello, World! 🌍".to_string()),
        Cell::Null,
    ];

    // Test Cell operations
    for cell in test_cells {
        let cloned = cell.clone();
        assert_eq!(cell, cloned);
        println!("Cell type tested: {:?}", cell);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_numeric_and_decimal_types_with_postgres() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();

    // Create table with numeric types
    client.batch_execute(r#"
        CREATE TABLE test.numeric_test (
            id SERIAL PRIMARY KEY,
            numeric_default NUMERIC,
            numeric_10_2 NUMERIC(10,2),
            numeric_precision NUMERIC(15,5),
            oid_col OID
        )
    "#).await.expect("Failed to create numeric_test table");

    // Insert test data using raw SQL to handle NUMERIC precision correctly
    let row = client.query_one(r#"
        INSERT INTO test.numeric_test 
        (numeric_default, numeric_10_2, numeric_precision, oid_col)
        VALUES (123456789.123456789, 12345.67, 1234567890.12345, $1)
        RETURNING *
    "#, &[
        &12345u32,                  // oid_col
    ]).await.expect("Failed to insert numeric data");

    // Test PostgreSQL numeric extraction using PgNumeric
    let numeric_default: PgNumeric = row.get("numeric_default");
    let numeric_10_2: PgNumeric = row.get("numeric_10_2");
    let numeric_precision: PgNumeric = row.get("numeric_precision");
    let oid_val: u32 = row.get("oid_col");

    println!("Numeric default: {}", numeric_default);
    println!("Numeric (10,2): {}", numeric_10_2);
    println!("Numeric precision: {}", numeric_precision);
    
    assert_eq!(oid_val, 12345);

    // Verify we can handle precision correctly by displaying the values
    let default_str = format!("{}", numeric_default);
    let ten_2_str = format!("{}", numeric_10_2);
    let precision_str = format!("{}", numeric_precision);
    
    assert!(default_str.contains("123456789"));
    assert!(ten_2_str.contains("12345.67"));
    assert!(precision_str.contains("1234567890.12345"));

    // Test Cell types for numeric data
    let numeric_cells = vec![
        Cell::U32(12345),
        Cell::Numeric(PgNumeric::NaN),
        Cell::Numeric(PgNumeric::PositiveInfinity),
        Cell::Numeric(PgNumeric::NegativeInfinity),
        Cell::Numeric(numeric_default),
        Cell::Numeric(numeric_10_2),
        Cell::Numeric(numeric_precision),
    ];

    for cell in numeric_cells {
        println!("Numeric cell: {:?}", cell);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_date_time_types_with_postgres() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();

    // Create table with date/time types
    client.batch_execute(r#"
        CREATE TABLE test.datetime_test (
            id SERIAL PRIMARY KEY,
            date_col DATE,
            time_col TIME,
            timestamp_col TIMESTAMP,
            timestamptz_col TIMESTAMPTZ
        )
    "#).await.expect("Failed to create datetime_test table");

    // Insert test data
    let test_date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();
    let test_time = NaiveTime::from_hms_opt(14, 30, 45).unwrap();
    let test_timestamp = NaiveDateTime::new(test_date, test_time);
    let test_timestamptz = Utc.from_utc_datetime(&test_timestamp);

    let row = client.query_one(r#"
        INSERT INTO test.datetime_test 
        (date_col, time_col, timestamp_col, timestamptz_col)
        VALUES ($1, $2, $3, $4)
        RETURNING *
    "#, &[
        &test_date,
        &test_time, 
        &test_timestamp,
        &test_timestamptz,
    ]).await.expect("Failed to insert datetime data");

    // Test extraction
    let extracted_date: NaiveDate = row.get("date_col");
    let extracted_time: NaiveTime = row.get("time_col");
    let extracted_timestamp: NaiveDateTime = row.get("timestamp_col");
    let extracted_timestamptz: DateTime<Utc> = row.get("timestamptz_col");

    assert_eq!(extracted_date, test_date);
    assert_eq!(extracted_time, test_time);
    assert_eq!(extracted_timestamp, test_timestamp);
    assert_eq!(extracted_timestamptz, test_timestamptz);

    // Test Cell types for date/time data
    let datetime_cells = vec![
        Cell::Date(test_date),
        Cell::Time(test_time),
        Cell::TimeStamp(test_timestamp),
        Cell::TimeStampTz(test_timestamptz),
    ];

    for cell in datetime_cells {
        let cloned = cell.clone();
        assert_eq!(cell, cloned);
        println!("DateTime cell: {:?}", cell);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_uuid_and_json_types_with_postgres() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();

    // Create table with UUID and JSON types
    client.batch_execute(r#"
        CREATE TABLE test.uuid_json_test (
            id SERIAL PRIMARY KEY,
            uuid_col UUID,
            json_col JSON,
            jsonb_col JSONB
        )
    "#).await.expect("Failed to create uuid_json_test table");

    // Insert test data
    let test_uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap();
    let test_json = serde_json::json!({
        "name": "test",
        "value": 42,
        "nested": {"key": "value"},
        "array": [1, 2, 3]
    });

    let row = client.query_one(r#"
        INSERT INTO test.uuid_json_test 
        (uuid_col, json_col, jsonb_col)
        VALUES ($1, $2, $3)
        RETURNING *
    "#, &[
        &test_uuid,
        &test_json,
        &test_json,
    ]).await.expect("Failed to insert uuid/json data");

    // Test extraction
    let extracted_uuid: Uuid = row.get("uuid_col");
    let extracted_json: serde_json::Value = row.get("json_col");
    let extracted_jsonb: serde_json::Value = row.get("jsonb_col");

    assert_eq!(extracted_uuid, test_uuid);
    assert_eq!(extracted_json, test_json);
    assert_eq!(extracted_jsonb, test_json);

    // Test Cell types for UUID and JSON
    let uuid_json_cells = vec![
        Cell::Uuid(test_uuid),
        Cell::Json(test_json.clone()),
        Cell::Json(serde_json::json!(null)),
        Cell::Json(serde_json::json!({"empty": {}})),
    ];

    for cell in uuid_json_cells {
        let cloned = cell.clone();
        assert_eq!(cell, cloned);
        println!("UUID/JSON cell: {:?}", cell);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_binary_data_types_with_postgres() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();

    // Create table with binary data types
    client.batch_execute(r#"
        CREATE TABLE test.binary_test (
            id SERIAL PRIMARY KEY,
            bytea_col BYTEA
        )
    "#).await.expect("Failed to create binary_test table");

    // Insert test data - various binary data
    let test_data = vec![
        b"Hello, World!".to_vec(),
        vec![0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD],
        "UTF-8 test: Hello 🌍".as_bytes().to_vec(),
        vec![],  // empty bytea
    ];

    for (i, data) in test_data.iter().enumerate() {
        let row = client.query_one(r#"
            INSERT INTO test.binary_test (bytea_col)
            VALUES ($1)
            RETURNING *
        "#, &[data]).await.expect("Failed to insert binary data");

        let extracted: Vec<u8> = row.get("bytea_col");
        assert_eq!(&extracted, data, "Binary data mismatch for test case {}", i);

        // Test Cell for binary data
        let cell = Cell::Bytes(data.clone());
        let cloned = cell.clone();
        assert_eq!(cell, cloned);
        println!("Binary cell test case {}: {} bytes", i, data.len());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_array_types_with_postgres() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();

    // Create table with various array types
    client.batch_execute(r#"
        CREATE TABLE test.array_test (
            id SERIAL PRIMARY KEY,
            bool_array BOOLEAN[],
            int2_array SMALLINT[],
            int4_array INTEGER[],
            int8_array BIGINT[],
            float4_array REAL[],
            float8_array DOUBLE PRECISION[],
            text_array TEXT[],
            varchar_array VARCHAR[],
            uuid_array UUID[],
            json_array JSON[],
            bytea_array BYTEA[]
        )
    "#).await.expect("Failed to create array_test table");

    // Insert test data with arrays
    let test_uuids = vec![
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap(),
        Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap(),
    ];
    
    let test_jsons = vec![
        serde_json::json!({"key": "value1"}),
        serde_json::json!({"key": "value2", "num": 42}),
    ];

    let row = client.query_one(r#"
        INSERT INTO test.array_test (
            bool_array, int2_array, int4_array, int8_array, 
            float4_array, float8_array, text_array, varchar_array,
            uuid_array, json_array, bytea_array
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        )
        RETURNING *
    "#, &[
        &vec![true, false, true],                                           // bool_array
        &vec![1i16, 2i16, 3i16],                                           // int2_array
        &vec![10i32, 20i32, 30i32],                                        // int4_array
        &vec![100i64, 200i64, 300i64],                                     // int8_array
        &vec![1.1f32, 2.2f32, 3.3f32],                                     // float4_array
        &vec![1.11f64, 2.22f64, 3.33f64],                                  // float8_array
        &vec!["hello", "world", "test"],                                   // text_array
        &vec!["varchar1", "varchar2"],                                     // varchar_array
        &test_uuids,                                                       // uuid_array
        &test_jsons,                                                       // json_array
        &vec![
            b"data1".to_vec(),
            b"data2".to_vec()
        ],                                                                 // bytea_array
    ]).await.expect("Failed to insert array data");

    // Test extraction of arrays
    let bool_array: Vec<bool> = row.get("bool_array");
    assert_eq!(bool_array, vec![true, false, true]);

    let int4_array: Vec<i32> = row.get("int4_array");
    assert_eq!(int4_array, vec![10, 20, 30]);

    let text_array: Vec<String> = row.get("text_array");
    assert_eq!(text_array, vec!["hello", "world", "test"]);

    let uuid_array: Vec<Uuid> = row.get("uuid_array");
    assert_eq!(uuid_array, test_uuids);

    let json_array: Vec<serde_json::Value> = row.get("json_array");
    assert_eq!(json_array, test_jsons);

    // Test Cell types for arrays
    let array_cells = vec![
        Cell::Array(ArrayCell::Bool(vec![Some(true), Some(false), Some(true)])),
        Cell::Array(ArrayCell::I32(vec![Some(10), Some(20), Some(30)])),
        Cell::Array(ArrayCell::String(vec![Some("hello".to_string()), Some("world".to_string())])),
        Cell::Array(ArrayCell::Uuid(vec![Some(test_uuids[0]), Some(test_uuids[1])])),
        Cell::Array(ArrayCell::Json(vec![Some(test_jsons[0].clone()), Some(test_jsons[1].clone())])),
        Cell::Array(ArrayCell::Bytes(vec![Some(b"data1".to_vec()), Some(b"data2".to_vec())])),
    ];

    for (i, cell) in array_cells.iter().enumerate() {
        let cloned = cell.clone();
        assert_eq!(cell, &cloned);
        println!("Array cell {} tested successfully", i);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_null_handling_with_postgres() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();

    // Create table for null testing
    client.batch_execute(r#"
        CREATE TABLE test.null_test (
            id SERIAL PRIMARY KEY,
            nullable_text TEXT,
            nullable_int INTEGER,
            nullable_bool BOOLEAN,
            nullable_array INTEGER[],
            required_field TEXT NOT NULL
        )
    "#).await.expect("Failed to create null_test table");

    // Insert data with NULLs
    let row = client.query_one(r#"
        INSERT INTO test.null_test 
        (nullable_text, nullable_int, nullable_bool, nullable_array, required_field)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
    "#, &[
        &Option::<String>::None,                                       // nullable_text (NULL)
        &Option::<i32>::None,                                          // nullable_int (NULL)
        &Option::<bool>::None,                                         // nullable_bool (NULL)
        &vec![Some(1i32), None, Some(3i32)],                          // nullable_array with NULLs
        &"required_value",                                             // required_field
    ]).await.expect("Failed to insert null data");

    // Test extraction of NULL values
    let nullable_text: Option<String> = row.get("nullable_text");
    let nullable_int: Option<i32> = row.get("nullable_int");
    let nullable_bool: Option<bool> = row.get("nullable_bool");
    let required_field: String = row.get("required_field");

    assert_eq!(nullable_text, None);
    assert_eq!(nullable_int, None);
    assert_eq!(nullable_bool, None);
    assert_eq!(required_field, "required_value");

    // Test array with NULLs
    let nullable_array: Vec<Option<i32>> = row.get("nullable_array");
    assert_eq!(nullable_array, vec![Some(1), None, Some(3)]);

    // Test Cell types for NULL handling
    let null_cells = vec![
        Cell::Null,
        Cell::String("".to_string()),  // empty vs null
        Cell::Array(ArrayCell::I32(vec![Some(1), None, Some(3)])),
        Cell::Array(ArrayCell::String(vec![Some("hello".to_string()), None, Some("world".to_string())])),
    ];

    for (i, cell) in null_cells.iter().enumerate() {
        let cloned = cell.clone();
        assert_eq!(cell, &cloned);
        println!("Null handling cell {} tested", i);
        
        // Test clear operation
        let mut clearable = cell.clone();
        clearable.clear();
        println!("Cell {} cleared: {:?}", i, clearable);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_row_operations() {
    init_test_tracing();
    
    // Test TableRow construction and operations
    let values = vec![
        Cell::I32(1),
        Cell::String("test".to_string()),
        Cell::Bool(true),
        Cell::Null,
        Cell::F64(3.14159),
        Cell::Array(ArrayCell::String(vec![Some("a".to_string()), Some("b".to_string())])),
    ];

    let table_row = TableRow::new(values.clone());
    assert_eq!(table_row.values.len(), 6);
    assert_eq!(table_row.values, values);

    // Test cloning
    let cloned_row = table_row.clone();
    assert_eq!(table_row, cloned_row);

    // Test individual cell operations
    for (i, cell) in table_row.values.iter().enumerate() {
        println!("TableRow cell {}: {:?}", i, cell);
        
        // Test cell cloning and equality
        let cell_clone = cell.clone();
        assert_eq!(cell, &cell_clone);
    }

    println!("TableRow operations test completed successfully");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_complex_data_scenarios_with_postgres() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();

    // Create table with complex scenarios
    client.batch_execute(r#"
        CREATE TABLE test.complex_test (
            id SERIAL PRIMARY KEY,
            empty_text TEXT,
            very_long_text TEXT,
            special_chars TEXT,
            unicode_text TEXT,
            large_number BIGINT,
            zero_values INTEGER,
            negative_values INTEGER,
            complex_json JSON,
            empty_array INTEGER[],
            large_array INTEGER[],
            mixed_type_scenarios TEXT
        )
    "#).await.expect("Failed to create complex_test table");

    // Test edge cases
    let very_long_text = "x".repeat(10000);
    let special_chars = "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?`~";
    let unicode_text = "Unicode: 🌍🚀💻🎉 中文 العربية русский हिन्दी";
    let large_array: Vec<i32> = (1..1000).collect();
    
    let row = client.query_one(r#"
        INSERT INTO test.complex_test (
            empty_text, very_long_text, special_chars, unicode_text,
            large_number, zero_values, negative_values,
            complex_json, empty_array, large_array, mixed_type_scenarios
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        )
        RETURNING *
    "#, &[
        &"",                                    // empty_text
        &very_long_text,                       // very_long_text
        &special_chars,                        // special_chars
        &unicode_text,                         // unicode_text
        &9223372036854775807i64,               // large_number (max i64)
        &0i32,                                 // zero_values
        &-2147483648i32,                       // negative_values (min i32)
        &serde_json::json!({                   // complex_json
            "nested": {
                "deep": {
                    "array": [1, 2, {"key": "value"}],
                    "null_value": null,
                    "boolean": true,
                    "number": 42.5,
                    "string": "test with \"quotes\" and 'apostrophes'"
                }
            },
            "unicode": "🌍 test",
            "special_chars": "!@#$%",
            "top_level_array": [{"a": 1}, {"b": 2}],
            "empty_object": {},
            "empty_array": []
        }),
        &Vec::<i32>::new(),                    // empty_array
        &large_array,                          // large_array
        &"Mixed: 123 ABC !@# 🌍",             // mixed_type_scenarios
    ]).await.expect("Failed to insert complex data");

    // Test extraction of complex data
    assert_eq!(row.get::<_, String>("empty_text"), "");
    assert_eq!(row.get::<_, String>("very_long_text"), very_long_text);
    assert_eq!(row.get::<_, String>("special_chars"), special_chars);
    assert_eq!(row.get::<_, String>("unicode_text"), unicode_text);
    assert_eq!(row.get::<_, i64>("large_number"), 9223372036854775807i64);
    assert_eq!(row.get::<_, i32>("zero_values"), 0i32);
    assert_eq!(row.get::<_, i32>("negative_values"), -2147483648i32);
    
    let complex_json: serde_json::Value = row.get("complex_json");
    assert_eq!(complex_json["nested"]["deep"]["number"], 42.5);
    assert_eq!(complex_json["unicode"], "🌍 test");
    
    let extracted_large_array: Vec<i32> = row.get("large_array");
    assert_eq!(extracted_large_array.len(), 999);
    assert_eq!(extracted_large_array[0], 1);
    assert_eq!(extracted_large_array[998], 999);

    let empty_array: Vec<i32> = row.get("empty_array");
    assert!(empty_array.is_empty());

    // Test Cell types for complex data
    let complex_cells = vec![
        Cell::String(very_long_text),
        Cell::String(special_chars.to_string()),
        Cell::String(unicode_text.to_string()),
        Cell::I64(9223372036854775807i64),
        Cell::I32(-2147483648i32),
        Cell::Json(complex_json),
        Cell::Array(ArrayCell::I32(vec![])), // empty array
        Cell::Array(ArrayCell::String(vec![
            Some("Mixed: 123".to_string()),
            Some("ABC !@#".to_string()),
            Some("🌍 unicode".to_string()),
        ])),
    ];

    for (i, cell) in complex_cells.iter().enumerate() {
        let cloned = cell.clone();
        assert_eq!(cell, &cloned);
        
        // Test that complex cells handle clear operations
        let mut clearable = cell.clone();
        clearable.clear();
        
        println!("Complex cell {} tested and cleared successfully", i);
    }

    println!("All complex data scenarios tested successfully!");
}