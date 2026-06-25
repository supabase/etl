use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use etl::{
    data::{ArrayCell, Cell, TableRow},
    error::EtlResult,
    postgres::client::PgReplicationClient,
    test_utils::{
        database::{spawn_source_database, test_table_name},
        pipeline::test_slot_name,
        replication_stream::{parse_copy_row, parse_tuple},
    },
};
use etl_postgres::types::{ColumnSchema, PgNumeric, PgTimeTz};
use etl_telemetry::tracing::init_test_tracing;
use futures::StreamExt;
use postgres_replication::{
    LogicalReplicationStream,
    protocol::{LogicalReplicationMessage, ReplicationMessage},
};
use serde_json::json;
use tokio::pin;

const MATRIX_ROW_ID: i64 = 1;
const MATRIX_UUID: &str = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";

struct UnsupportedParserCase {
    name: &'static str,
    data_type: &'static str,
    expression: &'static str,
}

fn unsupported_parser_cases() -> &'static [UnsupportedParserCase] {
    &[
        UnsupportedParserCase {
            name: "time_24_hour_boundary",
            data_type: "time",
            expression: "'24:00:00'::time",
        },
        UnsupportedParserCase {
            name: "timetz_24_hour_boundary",
            data_type: "timetz",
            expression: "'24:00:00+02'::timetz",
        },
        UnsupportedParserCase {
            name: "time_array_24_hour_boundary",
            data_type: "time[]",
            expression: "array['12:30:00'::time, '24:00:00'::time]::time[]",
        },
        UnsupportedParserCase {
            name: "timetz_array_24_hour_boundary",
            data_type: "timetz[]",
            expression: "array['12:30:00+02'::timetz, '24:00:00+02'::timetz]::timetz[]",
        },
        UnsupportedParserCase {
            name: "date_infinity",
            data_type: "date",
            expression: "'infinity'::date",
        },
        UnsupportedParserCase {
            name: "date_negative_infinity",
            data_type: "date",
            expression: "'-infinity'::date",
        },
        UnsupportedParserCase {
            name: "date_array_infinity",
            data_type: "date[]",
            expression: "array['2026-01-01'::date, 'infinity'::date]::date[]",
        },
        UnsupportedParserCase {
            name: "date_array_negative_infinity",
            data_type: "date[]",
            expression: "array['2026-01-01'::date, '-infinity'::date]::date[]",
        },
        UnsupportedParserCase {
            name: "date_bc",
            data_type: "date",
            expression: "'0044-02-01 BC'::date",
        },
        UnsupportedParserCase {
            name: "date_array_bc",
            data_type: "date[]",
            expression: "array['2026-01-01'::date, '0044-02-01 BC'::date]::date[]",
        },
        UnsupportedParserCase {
            name: "date_beyond_chrono_range",
            data_type: "date",
            expression: "'300000-01-01'::date",
        },
        UnsupportedParserCase {
            name: "date_array_beyond_chrono_range",
            data_type: "date[]",
            expression: "array['2026-01-01'::date, '300000-01-01'::date]::date[]",
        },
        UnsupportedParserCase {
            name: "timestamp_infinity",
            data_type: "timestamp",
            expression: "'infinity'::timestamp",
        },
        UnsupportedParserCase {
            name: "timestamp_negative_infinity",
            data_type: "timestamp",
            expression: "'-infinity'::timestamp",
        },
        UnsupportedParserCase {
            name: "timestamptz_infinity",
            data_type: "timestamptz",
            expression: "'infinity'::timestamptz",
        },
        UnsupportedParserCase {
            name: "timestamptz_negative_infinity",
            data_type: "timestamptz",
            expression: "'-infinity'::timestamptz",
        },
        UnsupportedParserCase {
            name: "timestamp_bc",
            data_type: "timestamp",
            expression: "'0044-02-01 11:12:13 BC'::timestamp",
        },
        UnsupportedParserCase {
            name: "timestamptz_bc",
            data_type: "timestamptz",
            expression: "'0044-02-01 11:12:13+00 BC'::timestamptz",
        },
        UnsupportedParserCase {
            name: "timestamp_beyond_chrono_range",
            data_type: "timestamp",
            expression: "'270000-01-01 00:00:00'::timestamp",
        },
        UnsupportedParserCase {
            name: "timestamptz_beyond_chrono_range",
            data_type: "timestamptz",
            expression: "'270000-01-01 00:00:00+00'::timestamptz",
        },
        UnsupportedParserCase {
            name: "timestamp_array_infinity",
            data_type: "timestamp[]",
            expression: "array['2026-01-01 00:00:00'::timestamp, \
                         'infinity'::timestamp]::timestamp[]",
        },
        UnsupportedParserCase {
            name: "timestamp_array_negative_infinity",
            data_type: "timestamp[]",
            expression: "array['2026-01-01 00:00:00'::timestamp, \
                         '-infinity'::timestamp]::timestamp[]",
        },
        UnsupportedParserCase {
            name: "timestamptz_array_infinity",
            data_type: "timestamptz[]",
            expression: "array['2026-01-01 00:00:00+00'::timestamptz, \
                         'infinity'::timestamptz]::timestamptz[]",
        },
        UnsupportedParserCase {
            name: "timestamptz_array_negative_infinity",
            data_type: "timestamptz[]",
            expression: "array['2026-01-01 00:00:00+00'::timestamptz, \
                         '-infinity'::timestamptz]::timestamptz[]",
        },
        UnsupportedParserCase {
            name: "timestamp_array_bc",
            data_type: "timestamp[]",
            expression: "array['2026-01-01 00:00:00'::timestamp, '0044-02-01 11:12:13 \
                         BC'::timestamp]::timestamp[]",
        },
        UnsupportedParserCase {
            name: "timestamptz_array_bc",
            data_type: "timestamptz[]",
            expression: "array['2026-01-01 00:00:00+00'::timestamptz, '0044-02-01 11:12:13+00 \
                         BC'::timestamptz]::timestamptz[]",
        },
        UnsupportedParserCase {
            name: "timestamp_array_beyond_chrono_range",
            data_type: "timestamp[]",
            expression: "array['2026-01-01 00:00:00'::timestamp, '270000-01-01 \
                         00:00:00'::timestamp]::timestamp[]",
        },
        UnsupportedParserCase {
            name: "timestamptz_array_beyond_chrono_range",
            data_type: "timestamptz[]",
            expression: "array['2026-01-01 00:00:00+00'::timestamptz, '270000-01-01 \
                         00:00:00+00'::timestamptz]::timestamptz[]",
        },
    ]
}

async fn create_type_matrix_table_in(
    database: &etl_postgres::tokio::test_utils::PgDatabase<tokio_postgres::Client>,
    test_name: &str,
) -> (etl_postgres::types::TableName, etl_postgres::types::TableId) {
    let table_name = test_table_name(test_name);
    let table_id = database
        .create_table(
            table_name.clone(),
            false,
            &[
                ("id", "bigint primary key"),
                ("bool_col", "boolean not null"),
                ("char_col", r#""char" not null"#),
                ("bpchar_col", "character(3) not null"),
                ("varchar_col", "varchar(16) not null"),
                ("name_col", "name not null"),
                ("text_col", "text not null"),
                ("money_col", "money not null"),
                ("int2_col", "smallint not null"),
                ("int4_col", "integer not null"),
                ("int8_col", "bigint not null"),
                ("oid_col", "oid not null"),
                ("float4_col", "real not null"),
                ("float8_col", "double precision not null"),
                ("numeric_col", "numeric not null"),
                ("bytea_col", "bytea not null"),
                ("date_col", "date not null"),
                ("time_col", "time without time zone not null"),
                ("timetz_col", "time with time zone not null"),
                ("timestamp_col", "timestamp without time zone not null"),
                ("timestamptz_col", "timestamp with time zone not null"),
                ("uuid_col", "uuid not null"),
                ("json_col", "json not null"),
                ("jsonb_col", "jsonb not null"),
                ("bool_arr", "boolean[] not null"),
                ("char_arr", r#""char"[] not null"#),
                ("bpchar_arr", "character(3)[] not null"),
                ("varchar_arr", "varchar(16)[] not null"),
                ("name_arr", "name[] not null"),
                ("text_arr", "text[] not null"),
                ("money_arr", "money[] not null"),
                ("int2_arr", "smallint[] not null"),
                ("int4_arr", "integer[] not null"),
                ("int8_arr", "bigint[] not null"),
                ("oid_arr", "oid[] not null"),
                ("float4_arr", "real[] not null"),
                ("float8_arr", "double precision[] not null"),
                ("numeric_arr", "numeric[] not null"),
                ("bytea_arr", "bytea[] not null"),
                ("date_arr", "date[] not null"),
                ("time_arr", "time without time zone[] not null"),
                ("timetz_arr", "timetz[] not null"),
                ("timestamp_arr", "timestamp without time zone[] not null"),
                ("timestamptz_arr", "timestamptz[] not null"),
                ("uuid_arr", "uuid[] not null"),
                ("json_arr", "json[] not null"),
                ("jsonb_arr", "jsonb[] not null"),
                ("interval_col", "interval not null"),
                ("interval_arr", "interval[] not null"),
                ("inet_col", "inet not null"),
                ("inet_arr", "inet[] not null"),
                ("cidr_col", "cidr not null"),
                ("cidr_arr", "cidr[] not null"),
                ("macaddr_col", "macaddr not null"),
                ("macaddr_arr", "macaddr[] not null"),
                ("macaddr8_col", "macaddr8 not null"),
                ("macaddr8_arr", "macaddr8[] not null"),
                ("xml_col", "xml not null"),
                ("xml_arr", "xml[] not null"),
                ("int4_range_col", "int4range not null"),
                ("int4_range_arr", "int4range[] not null"),
                ("num_multirange_col", "nummultirange not null"),
                ("num_multirange_arr", "nummultirange[] not null"),
                ("int2_vector_col", "int2vector not null"),
                ("oid_vector_col", "oidvector not null"),
            ],
        )
        .await
        .unwrap();

    (table_name, table_id)
}

async fn create_single_value_table_in(
    database: &etl_postgres::tokio::test_utils::PgDatabase<tokio_postgres::Client>,
    test_name: &str,
    data_type: &str,
) -> (etl_postgres::types::TableName, etl_postgres::types::TableId) {
    let table_name = test_table_name(test_name);
    let value_column_type = format!("{data_type} not null");
    let table_id = database
        .create_table(
            table_name.clone(),
            false,
            &[("id", "bigint primary key"), ("value", &value_column_type)],
        )
        .await
        .unwrap();

    (table_name, table_id)
}

async fn insert_single_value_row(
    database: &etl_postgres::tokio::test_utils::PgDatabase<tokio_postgres::Client>,
    table_name: &etl_postgres::types::TableName,
    expression: &str,
) {
    database
        .run_sql(&format!(
            "insert into {} values (1, {expression})",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
}

async fn insert_type_matrix_row(
    database: &etl_postgres::tokio::test_utils::PgDatabase<tokio_postgres::Client>,
    table_name: &etl_postgres::types::TableName,
) {
    database
        .run_sql(&format!(
            r#"
            insert into {} values (
                {MATRIX_ROW_ID},
                true,
                'x'::"char",
                'ab'::character(3),
                'varchar',
                'pg_name'::name,
                'hello world',
                '12.34'::money,
                -123::smallint,
                456::integer,
                7890123456::bigint,
                42::oid,
                3.5::real,
                -7.25::double precision,
                12345.6789::numeric,
                '\x0102ff'::bytea,
                '2026-01-02'::date,
                '12:30:45.123456'::time,
                '12:30:45.123456+02'::time with time zone,
                '2026-01-02 03:04:05.123456'::timestamp,
                '2026-01-02 03:04:05.123456+00'::timestamp with time zone,
                '{MATRIX_UUID}'::uuid,
                '{{"kind":"json","n":1}}'::json,
                '{{"kind":"jsonb","nested":{{"n":2}}}}'::jsonb,
                array[true, false, null]::boolean[],
                array['a'::"char", null, 'b'::"char"]::"char"[],
                array['ab'::character(3), null, 'cd'::character(3)]::character(3)[],
                array['left'::varchar, null, 'right'::varchar]::varchar[],
                array['alpha'::name, null, 'beta'::name]::name[],
                array['hello'::text, null, 'world'::text]::text[],
                array['12.34'::money, null, '-0.01'::money]::money[],
                array[-123::smallint, null, 321::smallint]::smallint[],
                array[456::integer, null, -654::integer]::integer[],
                array[7890123456::bigint, null, -9876543210::bigint]::bigint[],
                array[42::oid, null, 43::oid]::oid[],
                array[3.5::real, null, -1.25::real]::real[],
                array[-7.25::double precision, null, 8.5::double precision]::double precision[],
                array[12345.6789::numeric, null, -0.5::numeric]::numeric[],
                array['\x00'::bytea, null, '\x0102'::bytea]::bytea[],
                array['2026-01-02'::date, null, '2026-01-03'::date]::date[],
                array['12:30:45.123456'::time, null, '23:59:59'::time]::time[],
                array[
                    '12:30:45.123456+02'::time with time zone,
                    null,
                    '23:59:59-07:30'::time with time zone
                ]::timetz[],
                array[
                    '2026-01-02 03:04:05.123456'::timestamp,
                    null,
                    '2026-01-03 04:05:06'::timestamp
                ]::timestamp[],
                array[
                    '2026-01-02 03:04:05.123456+00'::timestamp with time zone,
                    null,
                    '2026-01-03 04:05:06+00'::timestamp with time zone
                ]::timestamptz[],
                array[
                    '{MATRIX_UUID}'::uuid,
                    null,
                    '00000000-0000-0000-0000-000000000000'::uuid
                ]::uuid[],
                array['{{"a":1}}'::json, null, '{{"b":2}}'::json]::json[],
                array['{{"a":1}}'::jsonb, null, '{{"b":2}}'::jsonb]::jsonb[],
                '1 day 02:03:04'::interval,
                array['1 day'::interval, null, '02:00:00'::interval]::interval[],
                '192.0.2.1'::inet,
                array['192.0.2.1'::inet, null, '2001:db8::1'::inet]::inet[],
                '192.0.2.0/24'::cidr,
                array['192.0.2.0/24'::cidr, null, '2001:db8::/32'::cidr]::cidr[],
                'aa:bb:cc:dd:ee:ff'::macaddr,
                array['aa:bb:cc:dd:ee:ff'::macaddr, null, '00:11:22:33:44:55'::macaddr]::macaddr[],
                '08:00:2b:01:02:03:04:05'::macaddr8,
                array[
                    '08:00:2b:01:02:03:04:05'::macaddr8,
                    null,
                    '02:03:04:05:06:07:08:09'::macaddr8
                ]::macaddr8[],
                '<root a="1"/>'::xml,
                array['<left/>'::xml, null, '<right/>'::xml]::xml[],
                '[1,5)'::int4range,
                array['[1,5)'::int4range, null, '[10,20)'::int4range]::int4range[],
                nummultirange(numrange(1.0, 2.0, '[)')),
                array[
                    nummultirange(numrange(1.0, 2.0, '[)')),
                    null,
                    nummultirange(numrange(3.0, 4.0, '[)'))
                ]::nummultirange[],
                '1 2 3'::int2vector,
                '4 5 6'::oidvector
            )
            "#,
            table_name.as_quoted_identifier(),
        ))
        .await
        .unwrap();
}

async fn collect_copy_rows(
    stream: tokio_postgres::CopyOutStream,
    column_schemas: &[ColumnSchema],
) -> Vec<TableRow> {
    let mut rows = Vec::new();

    pin!(stream);
    while let Some(row) = stream.next().await {
        let row = row.unwrap();
        rows.push(parse_copy_row(&row, column_schemas).unwrap());
    }

    rows
}

async fn collect_single_copy_parse_result(
    stream: tokio_postgres::CopyOutStream,
    column_schemas: &[ColumnSchema],
) -> EtlResult<TableRow> {
    pin!(stream);

    let row = stream
        .next()
        .await
        .expect("copy stream should emit one row")
        .expect("copy stream row should be readable");
    let result = parse_copy_row(&row, column_schemas);
    assert!(stream.next().await.is_none(), "copy stream should emit exactly one row");

    result
}

async fn collect_insert_row(
    stream: LogicalReplicationStream,
    column_schemas: &[ColumnSchema],
) -> TableRow {
    pin!(stream);

    loop {
        let event = stream
            .next()
            .await
            .expect("logical replication stream ended unexpectedly")
            .expect("failed to decode logical replication data");

        let ReplicationMessage::XLogData(event) = event else {
            continue;
        };

        if let LogicalReplicationMessage::Insert(insert) = event.data() {
            return parse_tuple(insert.tuple().tuple_data(), column_schemas).unwrap();
        }
    }
}

async fn collect_insert_parse_result(
    stream: LogicalReplicationStream,
    column_schemas: &[ColumnSchema],
) -> EtlResult<TableRow> {
    pin!(stream);

    loop {
        let event = stream
            .next()
            .await
            .expect("logical replication stream ended unexpectedly")
            .expect("failed to decode logical replication data");

        let ReplicationMessage::XLogData(event) = event else {
            continue;
        };

        if let LogicalReplicationMessage::Insert(insert) = event.data() {
            return parse_tuple(insert.tuple().tuple_data(), column_schemas);
        }
    }
}

fn cell<'a>(row: &'a TableRow, column_schemas: &[ColumnSchema], name: &str) -> &'a Cell {
    let index = column_schemas
        .iter()
        .position(|column_schema| column_schema.name == name)
        .unwrap_or_else(|| panic!("expected column {name} in table schema"));

    &row.values()[index]
}

fn assert_string_cell(row: &TableRow, column_schemas: &[ColumnSchema], name: &str, value: &str) {
    assert_eq!(cell(row, column_schemas, name), &Cell::String(value.to_owned()));
}

fn assert_bpchar_cell(row: &TableRow, column_schemas: &[ColumnSchema]) {
    let Cell::String(value) = cell(row, column_schemas, "bpchar_col") else {
        panic!("bpchar_col should decode as a string");
    };

    assert_eq!(value.trim_end(), "ab");
}

fn assert_money_cell(row: &TableRow, column_schemas: &[ColumnSchema]) {
    let Cell::String(value) = cell(row, column_schemas, "money_col") else {
        panic!("money_col should decode as a string");
    };

    assert!(
        value.contains("12") && value.contains("34"),
        "money output should preserve the Postgres text value, got {value}"
    );
}

fn assert_f32_cell(row: &TableRow, column_schemas: &[ColumnSchema], name: &str, expected: f32) {
    let Cell::F32(value) = cell(row, column_schemas, name) else {
        panic!("{name} should decode as f32");
    };

    assert!((*value - expected).abs() < f32::EPSILON);
}

fn assert_f64_cell(row: &TableRow, column_schemas: &[ColumnSchema], name: &str, expected: f64) {
    let Cell::F64(value) = cell(row, column_schemas, name) else {
        panic!("{name} should decode as f64");
    };

    assert!((*value - expected).abs() < f64::EPSILON);
}

fn assert_string_array_cell(
    row: &TableRow,
    column_schemas: &[ColumnSchema],
    name: &str,
    expected: &[Option<&str>],
) {
    let Cell::Array(ArrayCell::String(values)) = cell(row, column_schemas, name) else {
        panic!("{name} should decode as a string array");
    };

    assert_eq!(
        values,
        &expected.iter().map(|value| value.map(ToOwned::to_owned)).collect::<Vec<_>>()
    );
}

fn assert_string_cell_contains(
    row: &TableRow,
    column_schemas: &[ColumnSchema],
    name: &str,
    expected: &str,
) {
    let Cell::String(value) = cell(row, column_schemas, name) else {
        panic!("{name} should decode as a string");
    };

    assert!(value.contains(expected), "{name} should contain {expected:?}, got {value:?}");
}

fn assert_string_array_cell_contains(
    row: &TableRow,
    column_schemas: &[ColumnSchema],
    name: &str,
    expected: &[Option<&str>],
) {
    let Cell::Array(ArrayCell::String(values)) = cell(row, column_schemas, name) else {
        panic!("{name} should decode as a string array");
    };

    assert_eq!(values.len(), expected.len());
    for (value, expected) in values.iter().zip(expected) {
        match (value, expected) {
            (Some(value), Some(expected)) => {
                assert!(
                    value.contains(expected),
                    "{name} should contain {expected:?}, got {value:?}"
                );
            }
            (None, None) => {}
            _ => panic!("{name} should have matching null positions"),
        }
    }
}

fn assert_bpchar_array_cell(row: &TableRow, column_schemas: &[ColumnSchema]) {
    let Cell::Array(ArrayCell::String(values)) = cell(row, column_schemas, "bpchar_arr") else {
        panic!("bpchar_arr should decode as a string array");
    };

    assert_eq!(values.len(), 3);
    assert_eq!(values[0].as_deref().map(str::trim_end), Some("ab"));
    assert_eq!(values[1], None);
    assert_eq!(values[2].as_deref().map(str::trim_end), Some("cd"));
}

fn assert_money_array_cell(row: &TableRow, column_schemas: &[ColumnSchema]) {
    let Cell::Array(ArrayCell::String(values)) = cell(row, column_schemas, "money_arr") else {
        panic!("money_arr should decode as a string array");
    };

    assert_eq!(values.len(), 3);
    assert!(values[0].as_deref().is_some_and(|value| value.contains("12")));
    assert_eq!(values[1], None);
    assert!(values[2].as_deref().is_some_and(|value| value.contains("0.01")));
}

fn assert_type_matrix_row(row: &TableRow, column_schemas: &[ColumnSchema]) {
    assert_eq!(cell(row, column_schemas, "id"), &Cell::I64(MATRIX_ROW_ID));
    assert_eq!(cell(row, column_schemas, "bool_col"), &Cell::Bool(true));
    assert_string_cell(row, column_schemas, "char_col", "x");
    assert_bpchar_cell(row, column_schemas);
    assert_string_cell(row, column_schemas, "varchar_col", "varchar");
    assert_string_cell(row, column_schemas, "name_col", "pg_name");
    assert_string_cell(row, column_schemas, "text_col", "hello world");
    assert_money_cell(row, column_schemas);
    assert_eq!(cell(row, column_schemas, "int2_col"), &Cell::I16(-123));
    assert_eq!(cell(row, column_schemas, "int4_col"), &Cell::I32(456));
    assert_eq!(cell(row, column_schemas, "int8_col"), &Cell::I64(7_890_123_456));
    assert_eq!(cell(row, column_schemas, "oid_col"), &Cell::U32(42));
    assert_f32_cell(row, column_schemas, "float4_col", 3.5);
    assert_f64_cell(row, column_schemas, "float8_col", -7.25);
    assert_eq!(
        cell(row, column_schemas, "numeric_col"),
        &Cell::Numeric("12345.6789".parse::<PgNumeric>().unwrap())
    );
    assert_eq!(cell(row, column_schemas, "bytea_col"), &Cell::Bytes(vec![0x01, 0x02, 0xff]));
    assert_eq!(
        cell(row, column_schemas, "date_col"),
        &Cell::Date(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap())
    );
    assert_eq!(
        cell(row, column_schemas, "time_col"),
        &Cell::Time(NaiveTime::from_hms_micro_opt(12, 30, 45, 123_456).unwrap())
    );
    assert_eq!(
        cell(row, column_schemas, "timetz_col"),
        &Cell::TimeTz("12:30:45.123456+02".parse::<PgTimeTz>().unwrap())
    );
    assert_eq!(
        cell(row, column_schemas, "timestamp_col"),
        &Cell::Timestamp(
            NaiveDateTime::parse_from_str("2026-01-02 03:04:05.123456", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap()
        )
    );
    assert_eq!(
        cell(row, column_schemas, "timestamptz_col"),
        &Cell::TimestampTz(
            Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5)
                .unwrap()
                .with_nanosecond(123_456_000)
                .unwrap()
        )
    );
    assert_eq!(cell(row, column_schemas, "uuid_col"), &Cell::Uuid(MATRIX_UUID.parse().unwrap()));
    assert_eq!(cell(row, column_schemas, "json_col"), &Cell::Json(json!({"kind": "json", "n": 1})));
    assert_eq!(
        cell(row, column_schemas, "jsonb_col"),
        &Cell::Json(json!({"kind": "jsonb", "nested": {"n": 2}}))
    );

    assert_eq!(
        cell(row, column_schemas, "bool_arr"),
        &Cell::Array(ArrayCell::Bool(vec![Some(true), Some(false), None]))
    );
    assert_string_array_cell(row, column_schemas, "char_arr", &[Some("a"), None, Some("b")]);
    assert_bpchar_array_cell(row, column_schemas);
    assert_string_array_cell(
        row,
        column_schemas,
        "varchar_arr",
        &[Some("left"), None, Some("right")],
    );
    assert_string_array_cell(row, column_schemas, "name_arr", &[Some("alpha"), None, Some("beta")]);
    assert_string_array_cell(
        row,
        column_schemas,
        "text_arr",
        &[Some("hello"), None, Some("world")],
    );
    assert_money_array_cell(row, column_schemas);
    assert_eq!(
        cell(row, column_schemas, "int2_arr"),
        &Cell::Array(ArrayCell::I16(vec![Some(-123), None, Some(321)]))
    );
    assert_eq!(
        cell(row, column_schemas, "int4_arr"),
        &Cell::Array(ArrayCell::I32(vec![Some(456), None, Some(-654)]))
    );
    assert_eq!(
        cell(row, column_schemas, "int8_arr"),
        &Cell::Array(ArrayCell::I64(vec![Some(7_890_123_456), None, Some(-9_876_543_210)]))
    );
    assert_eq!(
        cell(row, column_schemas, "oid_arr"),
        &Cell::Array(ArrayCell::U32(vec![Some(42), None, Some(43)]))
    );
    assert_eq!(
        cell(row, column_schemas, "float4_arr"),
        &Cell::Array(ArrayCell::F32(vec![Some(3.5), None, Some(-1.25)]))
    );
    assert_eq!(
        cell(row, column_schemas, "float8_arr"),
        &Cell::Array(ArrayCell::F64(vec![Some(-7.25), None, Some(8.5)]))
    );
    assert_eq!(
        cell(row, column_schemas, "numeric_arr"),
        &Cell::Array(ArrayCell::Numeric(vec![
            Some("12345.6789".parse::<PgNumeric>().unwrap()),
            None,
            Some("-0.5".parse::<PgNumeric>().unwrap()),
        ]))
    );
    assert_eq!(
        cell(row, column_schemas, "bytea_arr"),
        &Cell::Array(ArrayCell::Bytes(vec![Some(vec![0x00]), None, Some(vec![0x01, 0x02])]))
    );
    assert_eq!(
        cell(row, column_schemas, "date_arr"),
        &Cell::Array(ArrayCell::Date(vec![
            Some(NaiveDate::from_ymd_opt(2026, 1, 2).unwrap()),
            None,
            Some(NaiveDate::from_ymd_opt(2026, 1, 3).unwrap()),
        ]))
    );
    assert_eq!(
        cell(row, column_schemas, "time_arr"),
        &Cell::Array(ArrayCell::Time(vec![
            Some(NaiveTime::from_hms_micro_opt(12, 30, 45, 123_456).unwrap()),
            None,
            Some(NaiveTime::from_hms_opt(23, 59, 59).unwrap()),
        ]))
    );
    assert_eq!(
        cell(row, column_schemas, "timetz_arr"),
        &Cell::Array(ArrayCell::TimeTz(vec![
            Some("12:30:45.123456+02".parse::<PgTimeTz>().unwrap()),
            None,
            Some("23:59:59-07:30".parse::<PgTimeTz>().unwrap()),
        ]))
    );
    assert_eq!(
        cell(row, column_schemas, "timestamp_arr"),
        &Cell::Array(ArrayCell::Timestamp(vec![
            Some(
                NaiveDateTime::parse_from_str(
                    "2026-01-02 03:04:05.123456",
                    "%Y-%m-%d %H:%M:%S%.f",
                )
                .unwrap()
            ),
            None,
            Some(
                NaiveDateTime::parse_from_str("2026-01-03 04:05:06", "%Y-%m-%d %H:%M:%S").unwrap()
            ),
        ]))
    );
    assert_eq!(
        cell(row, column_schemas, "timestamptz_arr"),
        &Cell::Array(ArrayCell::TimestampTz(vec![
            Some(
                Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5)
                    .unwrap()
                    .with_nanosecond(123_456_000)
                    .unwrap()
            ),
            None,
            Some(Utc.with_ymd_and_hms(2026, 1, 3, 4, 5, 6).unwrap()),
        ]))
    );
    assert_eq!(
        cell(row, column_schemas, "uuid_arr"),
        &Cell::Array(ArrayCell::Uuid(vec![
            Some(MATRIX_UUID.parse().unwrap()),
            None,
            Some("00000000-0000-0000-0000-000000000000".parse().unwrap()),
        ]))
    );
    assert_eq!(
        cell(row, column_schemas, "json_arr"),
        &Cell::Array(ArrayCell::Json(vec![Some(json!({"a": 1})), None, Some(json!({"b": 2}))]))
    );
    assert_eq!(
        cell(row, column_schemas, "jsonb_arr"),
        &Cell::Array(ArrayCell::Json(vec![Some(json!({"a": 1})), None, Some(json!({"b": 2}))]))
    );

    assert_string_cell(row, column_schemas, "interval_col", "1 day 02:03:04");
    assert_string_array_cell(
        row,
        column_schemas,
        "interval_arr",
        &[Some("1 day"), None, Some("02:00:00")],
    );
    assert_string_cell(row, column_schemas, "inet_col", "192.0.2.1");
    assert_string_array_cell(
        row,
        column_schemas,
        "inet_arr",
        &[Some("192.0.2.1"), None, Some("2001:db8::1")],
    );
    assert_string_cell(row, column_schemas, "cidr_col", "192.0.2.0/24");
    assert_string_array_cell(
        row,
        column_schemas,
        "cidr_arr",
        &[Some("192.0.2.0/24"), None, Some("2001:db8::/32")],
    );
    assert_string_cell(row, column_schemas, "macaddr_col", "aa:bb:cc:dd:ee:ff");
    assert_string_array_cell(
        row,
        column_schemas,
        "macaddr_arr",
        &[Some("aa:bb:cc:dd:ee:ff"), None, Some("00:11:22:33:44:55")],
    );
    assert_string_cell(row, column_schemas, "macaddr8_col", "08:00:2b:01:02:03:04:05");
    assert_string_array_cell(
        row,
        column_schemas,
        "macaddr8_arr",
        &[Some("08:00:2b:01:02:03:04:05"), None, Some("02:03:04:05:06:07:08:09")],
    );
    assert_string_cell_contains(row, column_schemas, "xml_col", "root");
    assert_string_array_cell_contains(
        row,
        column_schemas,
        "xml_arr",
        &[Some("left"), None, Some("right")],
    );
    assert_string_cell(row, column_schemas, "int4_range_col", "[1,5)");
    assert_string_array_cell(
        row,
        column_schemas,
        "int4_range_arr",
        &[Some("[1,5)"), None, Some("[10,20)")],
    );
    assert_string_cell_contains(row, column_schemas, "num_multirange_col", "1.0");
    assert_string_array_cell_contains(
        row,
        column_schemas,
        "num_multirange_arr",
        &[Some("1.0"), None, Some("3.0")],
    );
    assert_string_cell(row, column_schemas, "int2_vector_col", "1 2 3");
    assert_string_cell(row, column_schemas, "oid_vector_col", "4 5 6");
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_stream_converts_postgres_type_matrix() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let (table_name, table_id) = create_type_matrix_table_in(&database, "copy_type_matrix").await;
    insert_type_matrix_row(&database, &table_name).await;

    let mut client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let (transaction, _) =
        client.create_slot_with_transaction(&test_slot_name("copy_type_matrix")).await.unwrap();
    let table_schema = transaction.get_table_schema(table_id).await.unwrap();
    let stream = transaction
        .get_table_copy_stream(table_id, &table_schema.column_schemas, None)
        .await
        .unwrap();
    let rows = collect_copy_rows(stream, &table_schema.column_schemas).await;
    transaction.commit().await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_type_matrix_row(&rows[0], &table_schema.column_schemas);
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_stream_converts_postgres_type_matrix() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let (table_name, table_id) = create_type_matrix_table_in(&database, "cdc_type_matrix").await;
    let publication_name = "cdc_type_matrix_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let mut client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name("cdc_type_matrix");
    let (transaction, slot) = client.create_slot_with_transaction(&slot_name).await.unwrap();
    let table_schema = transaction.get_table_schema(table_id).await.unwrap();
    transaction.commit().await.unwrap();

    let stream = client
        .start_logical_replication(publication_name, &slot_name, slot.consistent_point)
        .await
        .unwrap();
    insert_type_matrix_row(&database, &table_name).await;

    let row = collect_insert_row(stream, &table_schema.column_schemas).await;
    assert_type_matrix_row(&row, &table_schema.column_schemas);
}

#[tokio::test(flavor = "multi_thread")]
async fn table_copy_stream_rejects_known_unsupported_postgres_values() {
    init_test_tracing();
    let database = spawn_source_database().await;
    let mut tables = Vec::new();

    for (index, case) in unsupported_parser_cases().iter().enumerate() {
        let (table_name, table_id) = create_single_value_table_in(
            &database,
            &format!("copy_unsupported_{index}"),
            case.data_type,
        )
        .await;
        insert_single_value_row(&database, &table_name, case.expression).await;
        tables.push((case, table_id));
    }

    let mut client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let (transaction, _) =
        client.create_slot_with_transaction(&test_slot_name("copy_unsupported")).await.unwrap();

    for (case, table_id) in tables {
        let table_schema = transaction.get_table_schema(table_id).await.unwrap();
        let stream = transaction
            .get_table_copy_stream(table_id, &table_schema.column_schemas, None)
            .await
            .unwrap();
        let result = collect_single_copy_parse_result(stream, &table_schema.column_schemas).await;

        assert!(
            result.is_err(),
            "COPY parsing unexpectedly succeeded for {}; move this case to the supported type \
             matrix",
            case.name
        );
    }

    transaction.commit().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_stream_rejects_known_unsupported_postgres_values() {
    init_test_tracing();
    let database = spawn_source_database().await;

    for (index, case) in unsupported_parser_cases().iter().enumerate() {
        let (table_name, table_id) = create_single_value_table_in(
            &database,
            &format!("cdc_unsupported_{index}"),
            case.data_type,
        )
        .await;
        let publication_name = format!("cdc_unsupported_pub_{index}");
        database
            .create_publication(&publication_name, std::slice::from_ref(&table_name))
            .await
            .unwrap();

        let mut client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
        let slot_name = test_slot_name(&format!("cdc_unsupported_{index}"));
        let (transaction, slot) = client.create_slot_with_transaction(&slot_name).await.unwrap();
        let table_schema = transaction.get_table_schema(table_id).await.unwrap();
        transaction.commit().await.unwrap();

        let stream = client
            .start_logical_replication(&publication_name, &slot_name, slot.consistent_point)
            .await
            .unwrap();
        insert_single_value_row(&database, &table_name, case.expression).await;
        let result = collect_insert_parse_result(stream, &table_schema.column_schemas).await;

        assert!(
            result.is_err(),
            "logical replication parsing unexpectedly succeeded for {}; move this case to the \
             supported type matrix",
            case.name
        );
    }
}
