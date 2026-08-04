use chrono::{NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc};
use etl::{
    data::{ArrayCell, Cell, PgNumeric, PgTimeTz, TableRow},
    error::EtlResult,
    postgres::client::PgReplicationClient,
    schema::{ColumnSchema, SnapshotId, TableId, TableName},
    test_utils::{
        database::{spawn_source_database, test_table_name},
        pipeline::test_slot_name,
        replication_stream::{parse_copy_row, parse_tuple},
        test_schema::create_partitioned_table,
    },
};
use etl_postgres::{
    below_version,
    tokio::test_utils::{PgDatabase, connect_to_pg_database},
    version::POSTGRES_15,
};
use etl_telemetry::tracing::init_test_tracing;
use futures::StreamExt;
use pg_escape::quote_identifier;
use postgres_replication::{
    LogicalReplicationStream,
    protocol::{LogicalReplicationMessage, ReplicationMessage},
};
use serde_json::{Value as JsonValue, json};
use tokio::{
    pin,
    time::{Duration, timeout},
};
use tokio_postgres::{Client, types::PgLsn};

const MATRIX_ROW_ID: i64 = 1;
const MATRIX_UUID: &str = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";

/// A decoded marker whose DDL variant retains its logical-message LSN.
#[derive(Debug, PartialEq, Eq)]
enum StreamMarker {
    Begin(PgLsn),
    DdlMessage(PgLsn, u32, Option<String>, Vec<String>),
    Relation(u32, Vec<String>),
    Insert(u32),
    Commit(PgLsn),
}

/// The expected protocol shape, excluding dynamic logical-message LSN values.
#[derive(Debug, PartialEq, Eq)]
enum ExpectedStreamMarker {
    Begin,
    DdlMessage(u32, Option<String>, Vec<String>),
    Relation(u32, Vec<String>),
    Insert(u32),
    Commit,
}

impl StreamMarker {
    /// Returns the stable protocol shape without the dynamic message LSN.
    fn expected(&self) -> ExpectedStreamMarker {
        match self {
            Self::Begin(_) => ExpectedStreamMarker::Begin,
            Self::DdlMessage(_, table_id, publication_name, columns) => {
                ExpectedStreamMarker::DdlMessage(
                    *table_id,
                    publication_name.clone(),
                    columns.clone(),
                )
            }
            Self::Relation(table_id, columns) => {
                ExpectedStreamMarker::Relation(*table_id, columns.clone())
            }
            Self::Insert(table_id) => ExpectedStreamMarker::Insert(*table_id),
            Self::Commit(_) => ExpectedStreamMarker::Commit,
        }
    }
}

/// Collects protocol markers while preserving their decoded order.
async fn collect_stream_markers(
    stream: LogicalReplicationStream,
    expected_count: usize,
) -> Vec<StreamMarker> {
    timeout(Duration::from_secs(10), async {
        let mut markers = Vec::with_capacity(expected_count);

        pin!(stream);
        while markers.len() < expected_count {
            let event = stream
                .next()
                .await
                .expect("Logical replication stream ended unexpectedly")
                .expect("Failed to decode logical replication data");

            let ReplicationMessage::XLogData(event) = event else {
                continue;
            };

            match event.data() {
                LogicalReplicationMessage::Begin(begin) => {
                    markers.push(StreamMarker::Begin(PgLsn::from(begin.final_lsn())));
                }
                LogicalReplicationMessage::Commit(commit) => {
                    markers.push(StreamMarker::Commit(PgLsn::from(commit.commit_lsn())));
                }
                LogicalReplicationMessage::Message(message) => {
                    let prefix = message.prefix().expect("Message prefix should decode");
                    if prefix != "supabase_etl_ddl" {
                        continue;
                    }

                    let content = message.content().expect("Message content should decode");
                    let json: JsonValue =
                        serde_json::from_str(content).expect("DDL message should be valid JSON");
                    let table_id = json["oid"]
                        .as_u64()
                        .and_then(|oid| u32::try_from(oid).ok())
                        .expect("DDL message should contain a PostgreSQL table OID");
                    let publication_name = json["publication_name"].as_str().map(ToOwned::to_owned);
                    let column_names = json["columns"]
                        .as_array()
                        .expect("DDL message columns should be an array")
                        .iter()
                        .map(|column| {
                            column["attname"]
                                .as_str()
                                .expect("DDL message column should have attname")
                                .to_owned()
                        })
                        .collect();

                    markers.push(StreamMarker::DdlMessage(
                        PgLsn::from(message.message_lsn()),
                        table_id,
                        publication_name,
                        column_names,
                    ));
                }
                LogicalReplicationMessage::Relation(relation) => {
                    let table_id = relation.rel_id();
                    let column_names = relation
                        .columns()
                        .iter()
                        .map(|column| {
                            column.name().expect("Relation column name should decode").to_owned()
                        })
                        .collect();

                    markers.push(StreamMarker::Relation(table_id, column_names));
                }
                LogicalReplicationMessage::Insert(insert) => {
                    markers.push(StreamMarker::Insert(insert.rel_id()));
                }
                _ => {}
            }
        }

        markers
    })
    .await
    .expect("Timed out while collecting logical replication markers")
}

/// Asserts an initial decoding session and a no-feedback replay from the same
/// LSN.
async fn assert_stream_markers_and_replay(
    initial_client: PgReplicationClient,
    initial_stream: LogicalReplicationStream,
    database: &PgDatabase<Client>,
    publication_name: &str,
    slot_name: &str,
    start_lsn: PgLsn,
    expected: &[ExpectedStreamMarker],
) {
    let initial_markers = collect_stream_markers(initial_stream, expected.len()).await;
    let initial_shapes = initial_markers.iter().map(StreamMarker::expected).collect::<Vec<_>>();
    assert_eq!(initial_shapes, expected);

    let mut transaction_commit_lsn = None;
    for marker in &initial_markers {
        match marker {
            StreamMarker::Begin(commit_lsn) => {
                assert!(transaction_commit_lsn.replace(*commit_lsn).is_none());
            }
            StreamMarker::DdlMessage(message_lsn, ..) => {
                let commit_lsn =
                    transaction_commit_lsn.expect("DDL message must be inside a transaction");
                assert_ne!(*message_lsn, PgLsn::from(0_u64));
                assert!(*message_lsn <= commit_lsn);
            }
            StreamMarker::Commit(commit_lsn) => {
                assert_eq!(transaction_commit_lsn.take(), Some(*commit_lsn));
            }
            _ => {}
        }
    }
    assert!(transaction_commit_lsn.is_none());

    let ddl_lsns = initial_markers
        .iter()
        .filter_map(|marker| match marker {
            StreamMarker::DdlMessage(message_lsn, ..) => Some(*message_lsn),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(ddl_lsns.windows(2).all(|window| window[0] < window[1]));

    drop(initial_client);
    database.wait_for_slot_inactive(slot_name).await;

    let replay_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let replay_stream = replay_client
        .start_logical_replication(publication_name, slot_name, start_lsn)
        .await
        .unwrap();
    let replay_markers = collect_stream_markers(replay_stream, expected.len()).await;
    assert_eq!(replay_markers, initial_markers);
}

/// Starts a logical replication stream whose slot can be replayed from its
/// consistent point without sending feedback.
async fn start_replayable_stream(
    database: &PgDatabase<Client>,
    publication_name: &str,
    slot_suffix: &str,
) -> (PgReplicationClient, LogicalReplicationStream, String, PgLsn) {
    let client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let slot_name = test_slot_name(slot_suffix);
    let start_lsn = client.create_slot(&slot_name).await.unwrap().consistent_point;
    let stream =
        client.start_logical_replication(publication_name, &slot_name, start_lsn).await.unwrap();

    (client, stream, slot_name, start_lsn)
}

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
    database: &PgDatabase<Client>,
    test_name: &str,
) -> (TableName, TableId) {
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
                ("text_null_col", "text"),
                ("text_null_marker_literal_col", "text not null"),
                ("text_embedded_null_marker_col", "text not null"),
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
    database: &PgDatabase<Client>,
    test_name: &str,
    data_type: &str,
) -> (TableName, TableId) {
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
    database: &PgDatabase<Client>,
    table_name: &TableName,
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

async fn insert_type_matrix_row(database: &PgDatabase<Client>, table_name: &TableName) {
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
                null::text,
                $$\N$$::text,
                $$value\Ntail$$::text,
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
    assert!(stream.next().await.is_none());

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

    assert!(value.contains("12") && value.contains("34"));
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

    assert!(value.contains(expected));
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
                assert!(value.contains(expected));
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
    assert_eq!(cell(row, column_schemas, "text_null_col"), &Cell::Null);
    assert_string_cell(row, column_schemas, "text_null_marker_literal_col", "\\N");
    assert_string_cell(row, column_schemas, "text_embedded_null_marker_col", "value\\Ntail");
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

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_consecutive_ddl_only_transactions_without_relations() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("consecutive_ddl_only_transactions");
    let quoted_table_name = table_name.as_quoted_identifier();
    let table_id = database
        .create_table(table_name.clone(), true, &[("a", "integer not null")])
        .await
        .unwrap();

    let publication_name = "consecutive_ddl_only_transactions_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let (client, initial_stream, slot_name, start_lsn) = start_replayable_stream(
        &database,
        publication_name,
        "consecutive_ddl_only_transactions_slot",
    )
    .await;

    // Each ALTER runs in its own transaction. With no DML, pgoutput carries
    // the self-describing DDL messages but has no reason to emit Relation.
    database
        .run_sql(&format!("alter table {quoted_table_name} add column b integer"))
        .await
        .unwrap();
    database.run_sql(&format!("alter table {quoted_table_name} add column c text")).await.unwrap();

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned(), "c".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_orders_concurrent_ddl_transactions_by_commit_lsn() {
    init_test_tracing();
    let mut database = spawn_source_database().await;

    let first_table = test_table_name("concurrent_ddl_first");
    let first_table_id = database
        .create_table(first_table.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();
    let second_table = test_table_name("concurrent_ddl_second");
    let second_table_id = database
        .create_table(second_table.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();

    let publication_name = "concurrent_ddl_commit_order_pub";
    database
        .create_publication(publication_name, &[first_table.clone(), second_table.clone()])
        .await
        .unwrap();

    let (client, stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "concurrent_ddl_commit_order_slot")
            .await;

    // The event trigger's pg_publication_tables lookup reads every published
    // table, which serializes otherwise independent concurrent ALTER TABLE
    // commands. Suppress the trigger and emit its transactional schema records
    // explicitly so this test isolates logical-decoding commit order.
    let (mut second_client, _) = connect_to_pg_database(&database.config).await;
    let first_transaction = database.begin_transaction().await;
    first_transaction.run_sql("set local supabase_etl.skip_ddl_log = true").await.unwrap();
    first_transaction
        .run_sql(&format!(
            "alter table {} add column first_change text",
            first_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    first_transaction
        .run_sql(&format!(
            "select pg_catalog.pg_logical_emit_message(
                true,
                'supabase_etl_ddl',
                pg_catalog.jsonb_build_object(
                    'oid', {},
                    'columns', pg_catalog.jsonb_build_array(
                        pg_catalog.jsonb_build_object('attname', 'id'),
                        pg_catalog.jsonb_build_object('attname', 'value'),
                        pg_catalog.jsonb_build_object('attname', 'first_change')
                    )
                )::pg_catalog.text
            )",
            first_table_id.into_inner()
        ))
        .await
        .unwrap();
    first_transaction
        .run_sql(&format!(
            "alter table {} add column second_change text",
            first_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    first_transaction
        .run_sql(&format!(
            "select pg_catalog.pg_logical_emit_message(
                true,
                'supabase_etl_ddl',
                pg_catalog.jsonb_build_object(
                    'oid', {},
                    'columns', pg_catalog.jsonb_build_array(
                        pg_catalog.jsonb_build_object('attname', 'id'),
                        pg_catalog.jsonb_build_object('attname', 'value'),
                        pg_catalog.jsonb_build_object('attname', 'first_change'),
                        pg_catalog.jsonb_build_object('attname', 'second_change')
                    )
                )::pg_catalog.text
            )",
            first_table_id.into_inner()
        ))
        .await
        .unwrap();

    let second_transaction = second_client.transaction().await.unwrap();
    second_transaction
        .batch_execute(&format!(
            "set local supabase_etl.skip_ddl_log = true;
            alter table {} add column committed_first text;
            select pg_catalog.pg_logical_emit_message(
                true,
                'supabase_etl_ddl',
                pg_catalog.jsonb_build_object(
                    'oid', {},
                    'columns', pg_catalog.jsonb_build_array(
                        pg_catalog.jsonb_build_object('attname', 'id'),
                        pg_catalog.jsonb_build_object('attname', 'value'),
                        pg_catalog.jsonb_build_object('attname', 'committed_first')
                    )
                )::pg_catalog.text
            );",
            second_table.as_quoted_identifier(),
            second_table_id.into_inner()
        ))
        .await
        .unwrap();
    second_transaction.commit().await.unwrap();
    first_transaction.commit_transaction().await;

    let markers = collect_stream_markers(stream, 7).await;
    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            second_table_id.into_inner(),
            None,
            vec!["id".to_owned(), "value".to_owned(), "committed_first".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            first_table_id.into_inner(),
            None,
            vec!["id".to_owned(), "value".to_owned(), "first_change".to_owned()],
        ),
        ExpectedStreamMarker::DdlMessage(
            first_table_id.into_inner(),
            None,
            vec![
                "id".to_owned(),
                "value".to_owned(),
                "first_change".to_owned(),
                "second_change".to_owned(),
            ],
        ),
        ExpectedStreamMarker::Commit,
    ];
    assert_eq!(markers.iter().map(StreamMarker::expected).collect::<Vec<_>>(), expected);

    let [
        StreamMarker::Begin(second_commit_lsn),
        StreamMarker::DdlMessage(second_message_lsn, ..),
        StreamMarker::Commit(second_commit_lsn_again),
        StreamMarker::Begin(first_commit_lsn),
        StreamMarker::DdlMessage(first_message_lsn, ..),
        StreamMarker::DdlMessage(first_second_message_lsn, ..),
        StreamMarker::Commit(first_commit_lsn_again),
    ] = markers.as_slice()
    else {
        panic!("expected concurrent DDL transaction markers");
    };

    assert_eq!(second_commit_lsn, second_commit_lsn_again);
    assert_eq!(first_commit_lsn, first_commit_lsn_again);
    assert!(first_message_lsn < first_second_message_lsn);
    assert!(first_second_message_lsn < second_message_lsn);
    assert!(second_commit_lsn < first_commit_lsn);

    let delivered_message_lsns =
        [*second_message_lsn, *first_message_lsn, *first_second_message_lsn];
    assert!(!delivered_message_lsns.windows(2).all(|window| window[0] < window[1]));

    let delivered_snapshot_ids = [
        SnapshotId::new(*second_commit_lsn, *second_message_lsn),
        SnapshotId::new(*first_commit_lsn, *first_message_lsn),
        SnapshotId::new(*first_commit_lsn, *first_second_message_lsn),
    ];
    assert!(delivered_snapshot_ids.windows(2).all(|window| window[0] < window[1]));

    drop(client);
    database.wait_for_slot_inactive(&slot_name).await;

    let replay_client = PgReplicationClient::connect(database.config.clone()).await.unwrap();
    let replay_stream = replay_client
        .start_logical_replication(publication_name, &slot_name, start_lsn)
        .await
        .unwrap();
    let replay_markers = collect_stream_markers(replay_stream, expected.len()).await;
    assert_eq!(replay_markers, markers);
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_schema_changes_before_first_dml() {
    init_test_tracing();
    let database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    let table_name = test_table_name("schema_changes_before_first_dml");
    let quoted_table_name = table_name.as_quoted_identifier();
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("a", "integer not null"), ("b", "integer not null"), ("c", "integer not null")],
        )
        .await
        .unwrap();

    let publication_name = "schema_changes_before_first_dml_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let (client, initial_stream, slot_name, start_lsn) = start_replayable_stream(
        &database,
        publication_name,
        "schema_changes_before_first_dml_slot",
    )
    .await;

    // Both schema changes happen before any DML. Each emits a stable schema
    // snapshot, but neither synthesizes a Relation message.
    database
        .run_sql(&format!(
            "alter table {quoted_table_name} add column d integer not null default 0"
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "alter publication {} set table {quoted_table_name} (id, a, c, d)",
            quote_identifier(publication_name)
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!("insert into {quoted_table_name} (a, b, c, d) values (1, 2, 3, 4)"))
        .await
        .unwrap();

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned(), "c".to_owned(), "d".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned(), "c".to_owned(), "d".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "a".to_owned(), "c".to_owned(), "d".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_multi_table_publication_column_filter_change_order() {
    init_test_tracing();
    let database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    let first_table = test_table_name("publication_filter_first");
    let first_table_id = database
        .create_table(
            first_table.clone(),
            true,
            &[("a", "integer not null"), ("b", "integer not null"), ("c", "integer not null")],
        )
        .await
        .unwrap();
    let second_table = test_table_name("publication_filter_second");
    let second_table_id = database
        .create_table(
            second_table.clone(),
            true,
            &[("a", "integer not null"), ("b", "integer not null"), ("c", "integer not null")],
        )
        .await
        .unwrap();

    let publication_name = "publication_filter_multiple_tables_pub";
    database
        .run_sql(&format!(
            "create publication {} for table {} (id, a, b, c), {} (id, a, b, c)",
            quote_identifier(publication_name),
            first_table.as_quoted_identifier(),
            second_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let (client, initial_stream, slot_name, start_lsn) = start_replayable_stream(
        &database,
        publication_name,
        "publication_filter_multiple_tables_slot",
    )
    .await;

    database
        .run_sql(&format!(
            "alter publication {} set table {} (id, a, b), {} (id, a, b)",
            quote_identifier(publication_name),
            first_table.as_quoted_identifier(),
            second_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {} (a, b, c) values (1, 2, 3)",
            first_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {} (a, b, c) values (4, 5, 6)",
            second_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            first_table_id.into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned(), "c".to_owned()],
        ),
        ExpectedStreamMarker::DdlMessage(
            second_table_id.into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned(), "c".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            first_table_id.into_inner(),
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned()],
        ),
        ExpectedStreamMarker::Insert(first_table_id.into_inner()),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            second_table_id.into_inner(),
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned()],
        ),
        ExpectedStreamMarker::Insert(second_table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_filters_unpublished_transactions_by_server_version() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let published_table = test_table_name("filtered_transaction_published");
    let published_table_id = database
        .create_table(published_table.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();
    let unpublished_table = test_table_name("filtered_transaction_unpublished");
    database
        .create_table(unpublished_table.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();

    let publication_name = "filtered_transaction_pub";
    database
        .create_publication(publication_name, std::slice::from_ref(&published_table))
        .await
        .unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "filtered_transaction_slot").await;

    database.insert_values(unpublished_table.clone(), &["value"], &[&1]).await.unwrap();
    database.insert_values(published_table.clone(), &["value"], &[&2]).await.unwrap();
    database.insert_values(unpublished_table, &["value"], &[&3]).await.unwrap();
    database.insert_values(published_table, &["value"], &[&4]).await.unwrap();

    let mut expected = Vec::new();
    // PostgreSQL 14 emits BEGIN/COMMIT when pgoutput filters every change in a
    // transaction. PostgreSQL 15+ suppresses the empty transaction.
    if below_version!(database.server_version(), POSTGRES_15) {
        expected.extend([ExpectedStreamMarker::Begin, ExpectedStreamMarker::Commit]);
    }
    expected.extend([
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            published_table_id.into_inner(),
            vec!["id".to_owned(), "value".to_owned()],
        ),
        ExpectedStreamMarker::Insert(published_table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ]);
    if below_version!(database.server_version(), POSTGRES_15) {
        expected.extend([ExpectedStreamMarker::Begin, ExpectedStreamMarker::Commit]);
    }
    expected.extend([
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Insert(published_table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ]);

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        expected.as_slice(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_recursive_alter_partitioned_table_emits_no_leaf_ddl_messages() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let root_table = test_table_name("recursive_alter_partitioned_table");
    let (_root_table_id, leaf_table_ids) = create_partitioned_table(
        &database,
        root_table.clone(),
        &[("first", "from (0) to (100)"), ("second", "from (100) to (200)")],
    )
    .await
    .unwrap();

    let publication_name = "recursive_alter_partitioned_table_pub";
    database
        .create_publication_with_config(publication_name, std::slice::from_ref(&root_table), false)
        .await
        .unwrap();

    let (client, initial_stream, slot_name, start_lsn) = start_replayable_stream(
        &database,
        publication_name,
        "recursive_alter_partitioned_table_slot",
    )
    .await;

    database
        .run_sql(&format!(
            "alter table {} add column category text not null default 'default_category'",
            root_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key, category) values ('first', 50, 'category'), \
             ('second', 150, 'category')",
            root_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // PostgreSQL reports only the explicitly altered partitioned parent to the
    // event trigger. It does not enumerate the regular-table leaves changed by
    // the recursive `alter table`, so the next leaf DML emits its updated
    // relation without any preceding DDL message. PostgreSQL 14 still emits an
    // empty transaction for the filtered ALTER, while PostgreSQL 15+ suppresses
    // it.
    let mut expected = Vec::new();
    if below_version!(database.server_version(), POSTGRES_15) {
        expected.extend([ExpectedStreamMarker::Begin, ExpectedStreamMarker::Commit]);
    }
    expected.extend([
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            leaf_table_ids[0].into_inner(),
            vec![
                "id".to_owned(),
                "data".to_owned(),
                "partition_key".to_owned(),
                "category".to_owned(),
            ],
        ),
        ExpectedStreamMarker::Insert(leaf_table_ids[0].into_inner()),
        ExpectedStreamMarker::Relation(
            leaf_table_ids[1].into_inner(),
            vec![
                "id".to_owned(),
                "data".to_owned(),
                "partition_key".to_owned(),
                "category".to_owned(),
            ],
        ),
        ExpectedStreamMarker::Insert(leaf_table_ids[1].into_inner()),
        ExpectedStreamMarker::Commit,
    ]);

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        expected.as_slice(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_publication_add_partition_root_emits_snapshot_when_using_root() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let initial_table = test_table_name("publication_add_root_identity_initial");
    database
        .create_table(initial_table.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();
    let root_table = test_table_name("publication_add_root_identity_root");
    let (root_table_id, leaf_table_ids) = create_partitioned_table(
        &database,
        root_table.clone(),
        &[("first", "from (0) to (100)"), ("second", "from (100) to (200)")],
    )
    .await
    .unwrap();

    let publication_name = "publication_add_root_identity_pub";
    database
        .create_publication_with_config(
            publication_name,
            std::slice::from_ref(&initial_table),
            true,
        )
        .await
        .unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "publication_add_root_identity_slot")
            .await;

    database
        .run_sql(&format!(
            "alter publication {} add table {}",
            quote_identifier(publication_name),
            root_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('first', 50), ('second', 150)",
            root_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    // pgoutput describes each physical leaf before publishing its tuple under
    // the root relation identity.
    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            root_table_id.into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "data".to_owned(), "partition_key".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            root_table_id.into_inner(),
            vec!["id".to_owned(), "data".to_owned(), "partition_key".to_owned()],
        ),
        ExpectedStreamMarker::Relation(
            leaf_table_ids[0].into_inner(),
            vec!["id".to_owned(), "data".to_owned(), "partition_key".to_owned()],
        ),
        ExpectedStreamMarker::Insert(root_table_id.into_inner()),
        ExpectedStreamMarker::Relation(
            root_table_id.into_inner(),
            vec!["id".to_owned(), "data".to_owned(), "partition_key".to_owned()],
        ),
        ExpectedStreamMarker::Relation(
            leaf_table_ids[1].into_inner(),
            vec!["id".to_owned(), "data".to_owned(), "partition_key".to_owned()],
        ),
        ExpectedStreamMarker::Insert(root_table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_publication_add_partition_root_emits_no_snapshot_when_using_leaves() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let initial_table = test_table_name("publication_add_partition_initial");
    database
        .create_table(initial_table.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();
    let root_table = test_table_name("publication_add_partition_root");
    let (_root_table_id, leaf_table_ids) = create_partitioned_table(
        &database,
        root_table.clone(),
        &[("first", "from (0) to (100)"), ("second", "from (100) to (200)")],
    )
    .await
    .unwrap();

    let publication_name = "publication_add_partition_root_pub";
    database
        .create_publication_with_config(
            publication_name,
            std::slice::from_ref(&initial_table),
            false,
        )
        .await
        .unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "publication_add_partition_root_slot")
            .await;

    database
        .run_sql(&format!(
            "alter publication {} add table {}",
            quote_identifier(publication_name),
            root_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('first', 50), ('second', 150)",
            root_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let mut expected = Vec::new();
    // PostgreSQL reports the explicitly added partitioned root as the
    // publication-relation event object, but pgoutput publishes its leaves when
    // publish_via_partition_root is false. The trigger therefore emits no leaf
    // schema snapshots. PostgreSQL 14 still exposes the empty transaction.
    if below_version!(database.server_version(), POSTGRES_15) {
        expected.extend([ExpectedStreamMarker::Begin, ExpectedStreamMarker::Commit]);
    }
    expected.extend([
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            leaf_table_ids[0].into_inner(),
            vec!["id".to_owned(), "data".to_owned(), "partition_key".to_owned()],
        ),
        ExpectedStreamMarker::Insert(leaf_table_ids[0].into_inner()),
        ExpectedStreamMarker::Relation(
            leaf_table_ids[1].into_inner(),
            vec!["id".to_owned(), "data".to_owned(), "partition_key".to_owned()],
        ),
        ExpectedStreamMarker::Insert(leaf_table_ids[1].into_inner()),
        ExpectedStreamMarker::Commit,
    ]);

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        expected.as_slice(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_publication_add_partition_leaf_emits_leaf_snapshot() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let initial_table = test_table_name("publication_add_leaf_initial");
    database
        .create_table(initial_table.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();
    let root_table = test_table_name("publication_add_leaf_root");
    let (_root_table_id, leaf_table_ids) = create_partitioned_table(
        &database,
        root_table.clone(),
        &[("first", "from (0) to (100)"), ("second", "from (100) to (200)")],
    )
    .await
    .unwrap();
    let leaf_table = TableName::new(root_table.schema, format!("{}_first", root_table.name));

    let publication_name = "publication_add_leaf_pub";
    // An explicitly named leaf stays the effective relation even when the
    // publication otherwise uses partition-root identity.
    database
        .create_publication_with_config(
            publication_name,
            std::slice::from_ref(&initial_table),
            true,
        )
        .await
        .unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "publication_add_leaf_slot").await;

    database
        .run_sql(&format!(
            "alter publication {} add table {}",
            quote_identifier(publication_name),
            leaf_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database.insert_values(leaf_table, &["data", "partition_key"], &[&"first", &50]).await.unwrap();

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            leaf_table_ids[0].into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "data".to_owned(), "partition_key".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            leaf_table_ids[0].into_inner(),
            vec!["id".to_owned(), "data".to_owned(), "partition_key".to_owned()],
        ),
        ExpectedStreamMarker::Insert(leaf_table_ids[0].into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_publication_partition_root_option_changes_ddl_and_dml_identity() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let root_table = test_table_name("publication_partition_identity_root");
    let (root_table_id, leaf_table_ids) = create_partitioned_table(
        &database,
        root_table.clone(),
        &[("first", "from (0) to (100)"), ("second", "from (100) to (200)")],
    )
    .await
    .unwrap();

    let publication_name = "publication_partition_identity_pub";
    database
        .create_publication_with_config(publication_name, std::slice::from_ref(&root_table), false)
        .await
        .unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "publication_partition_identity_slot")
            .await;

    let quoted_publication_name = quote_identifier(publication_name);
    database
        .run_sql(&format!(
            "alter publication {quoted_publication_name} set (publish_via_partition_root = true)"
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('root first', 50), ('root second', 150)",
            root_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "alter publication {quoted_publication_name} set (publish_via_partition_root = false)"
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "insert into {} (data, partition_key) values ('leaf first', 50), ('leaf second', 150)",
            root_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let columns = vec!["id".to_owned(), "data".to_owned(), "partition_key".to_owned()];
    // Publication-wide option changes expand the post-command effective table
    // set: first the root, then the two leaves in relation-OID order.
    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            root_table_id.into_inner(),
            Some(publication_name.to_owned()),
            columns.clone(),
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(root_table_id.into_inner(), columns.clone()),
        ExpectedStreamMarker::Relation(leaf_table_ids[0].into_inner(), columns.clone()),
        ExpectedStreamMarker::Insert(root_table_id.into_inner()),
        ExpectedStreamMarker::Relation(root_table_id.into_inner(), columns.clone()),
        ExpectedStreamMarker::Relation(leaf_table_ids[1].into_inner(), columns.clone()),
        ExpectedStreamMarker::Insert(root_table_id.into_inner()),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            leaf_table_ids[0].into_inner(),
            Some(publication_name.to_owned()),
            columns.clone(),
        ),
        ExpectedStreamMarker::DdlMessage(
            leaf_table_ids[1].into_inner(),
            Some(publication_name.to_owned()),
            columns.clone(),
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(leaf_table_ids[0].into_inner(), columns.clone()),
        ExpectedStreamMarker::Insert(leaf_table_ids[0].into_inner()),
        ExpectedStreamMarker::Relation(leaf_table_ids[1].into_inner(), columns),
        ExpectedStreamMarker::Insert(leaf_table_ids[1].into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_publication_add_table_schemas_in_oid_order() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let initial_table = test_table_name("publication_add_multiple_initial");
    database
        .create_table(initial_table.clone(), true, &[("initial_value", "integer not null")])
        .await
        .unwrap();
    let first_added_table = test_table_name("publication_add_multiple_first");
    let first_added_table_id = database
        .create_table(first_added_table.clone(), true, &[("first_value", "integer not null")])
        .await
        .unwrap();
    let second_added_table = test_table_name("publication_add_multiple_second");
    let second_added_table_id = database
        .create_table(second_added_table.clone(), true, &[("second_value", "text not null")])
        .await
        .unwrap();
    assert!(first_added_table_id < second_added_table_id);

    let publication_name = "publication_add_multiple_pub";
    database
        .create_publication(publication_name, std::slice::from_ref(&initial_table))
        .await
        .unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "publication_add_multiple_slot").await;

    database
        .run_sql(&format!(
            "alter publication {} add table {}, {}",
            quote_identifier(publication_name),
            first_added_table.as_quoted_identifier(),
            second_added_table.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            first_added_table_id.into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "first_value".to_owned()],
        ),
        ExpectedStreamMarker::DdlMessage(
            second_added_table_id.into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "second_value".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_publication_add_and_option_snapshots_but_not_drop() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let first_table = test_table_name("publication_options_first");
    let first_table_id = database
        .create_table(first_table.clone(), true, &[("first_value", "integer not null")])
        .await
        .unwrap();
    let second_table = test_table_name("publication_options_second");
    let second_table_id = database
        .create_table(second_table.clone(), true, &[("second_value", "integer not null")])
        .await
        .unwrap();
    let added_table = test_table_name("publication_options_added");
    let added_table_id = database
        .create_table(added_table.clone(), true, &[("added_value", "integer not null")])
        .await
        .unwrap();

    let publication_name = "publication_membership_and_options_pub";
    database
        .create_publication(publication_name, &[first_table.clone(), second_table.clone()])
        .await
        .unwrap();

    let (client, initial_stream, slot_name, start_lsn) = start_replayable_stream(
        &database,
        publication_name,
        "publication_membership_and_options_slot",
    )
    .await;

    let quoted_publication_name = quote_identifier(publication_name);
    database
        .run_sql(&format!(
            "alter publication {quoted_publication_name} add table {}",
            added_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!(
            "alter publication {quoted_publication_name} drop table {}",
            added_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database
        .run_sql(&format!("alter publication {quoted_publication_name} set (publish = 'insert')"))
        .await
        .unwrap();
    database.insert_values(first_table.clone(), &["first_value"], &[&1]).await.unwrap();

    let mut expected = vec![
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            added_table_id.into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "added_value".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
    ];
    // PostgreSQL 14's pgoutput emits BEGIN/COMMIT for this empty
    // ALTER PUBLICATION ... DROP TABLE transaction. PostgreSQL 15+ postpones
    // BEGIN until the first publishable change and suppresses both markers when
    // no such change exists:
    // https://www.postgresql.org/docs/15/release-15.html
    // https://github.com/postgres/postgres/commit/d5a9d86d8ffcadc52ff3729cd00fbd83bc38643c
    if below_version!(database.server_version(), POSTGRES_15) {
        expected.extend([ExpectedStreamMarker::Begin, ExpectedStreamMarker::Commit]);
    }
    expected.extend([
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            first_table_id.into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "first_value".to_owned()],
        ),
        ExpectedStreamMarker::DdlMessage(
            second_table_id.into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "second_value".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            first_table_id.into_inner(),
            vec!["id".to_owned(), "first_value".to_owned()],
        ),
        ExpectedStreamMarker::Insert(first_table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ]);

    // The absence of an added-table marker between the ADD and SET-option
    // transactions proves that DROP TABLE emits no schema snapshot.
    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        expected.as_slice(),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_consecutive_ddl_before_dml_within_transaction() {
    init_test_tracing();
    let mut database = spawn_source_database().await;

    let table_name = test_table_name("consecutive_ddl_before_dml");
    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();

    let publication_name = "consecutive_ddl_before_dml_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "consecutive_ddl_before_dml_slot")
            .await;

    let transaction = database.begin_transaction().await;
    transaction
        .run_sql(&format!(
            "alter table {} add column first_change text",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction
        .run_sql(&format!(
            "alter table {} add column second_change text",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction.insert_values(table_name, &["value"], &[&1]).await.unwrap();
    transaction.commit_transaction().await;

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec!["id".to_owned(), "value".to_owned(), "first_change".to_owned()],
        ),
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec![
                "id".to_owned(),
                "value".to_owned(),
                "first_change".to_owned(),
                "second_change".to_owned(),
            ],
        ),
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec![
                "id".to_owned(),
                "value".to_owned(),
                "first_change".to_owned(),
                "second_change".to_owned(),
            ],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_consecutive_ddl_after_dml_within_transaction() {
    init_test_tracing();
    let mut database = spawn_source_database().await;

    let table_name = test_table_name("consecutive_ddl_after_dml");
    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();

    let publication_name = "consecutive_ddl_after_dml_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "consecutive_ddl_after_dml_slot")
            .await;

    let transaction = database.begin_transaction().await;
    transaction.insert_values(table_name.clone(), &["value"], &[&1]).await.unwrap();
    transaction
        .run_sql(&format!(
            "alter table {} add column first_change text",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction
        .run_sql(&format!(
            "alter table {} add column second_change text",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction.commit_transaction().await;

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "value".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec!["id".to_owned(), "value".to_owned(), "first_change".to_owned()],
        ),
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec![
                "id".to_owned(),
                "value".to_owned(),
                "first_change".to_owned(),
                "second_change".to_owned(),
            ],
        ),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_cross_table_ddl_dml_interleavings_within_transaction() {
    init_test_tracing();
    let mut database = spawn_source_database().await;

    let first_table = test_table_name("cross_table_interleaving_first");
    let first_table_id = database
        .create_table(first_table.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();
    let second_table = test_table_name("cross_table_interleaving_second");
    let second_table_id = database
        .create_table(second_table.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();

    let publication_name = "cross_table_interleaving_pub";
    database
        .create_publication(publication_name, &[first_table.clone(), second_table.clone()])
        .await
        .unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "cross_table_interleaving_slot").await;

    let transaction = database.begin_transaction().await;
    transaction
        .run_sql(&format!(
            "alter table {} add column schema_version text",
            first_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction.insert_values(second_table.clone(), &["value"], &[&1]).await.unwrap();
    transaction
        .run_sql(&format!(
            "alter table {} add column schema_version text",
            second_table.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction.insert_values(first_table, &["value"], &[&2]).await.unwrap();
    transaction.insert_values(second_table, &["value"], &[&3]).await.unwrap();
    transaction.commit_transaction().await;

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            first_table_id.into_inner(),
            None,
            vec!["id".to_owned(), "value".to_owned(), "schema_version".to_owned()],
        ),
        ExpectedStreamMarker::Relation(
            second_table_id.into_inner(),
            vec!["id".to_owned(), "value".to_owned()],
        ),
        ExpectedStreamMarker::Insert(second_table_id.into_inner()),
        ExpectedStreamMarker::DdlMessage(
            second_table_id.into_inner(),
            None,
            vec!["id".to_owned(), "value".to_owned(), "schema_version".to_owned()],
        ),
        ExpectedStreamMarker::Relation(
            first_table_id.into_inner(),
            vec!["id".to_owned(), "value".to_owned(), "schema_version".to_owned()],
        ),
        ExpectedStreamMarker::Insert(first_table_id.into_inner()),
        ExpectedStreamMarker::Relation(
            second_table_id.into_inner(),
            vec!["id".to_owned(), "value".to_owned(), "schema_version".to_owned()],
        ),
        ExpectedStreamMarker::Insert(second_table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_table_ddl_between_dml_transactions() {
    init_test_tracing();
    let database = spawn_source_database().await;

    let table_name = test_table_name("table_ddl_between_dml_transactions");
    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();

    let publication_name = "table_ddl_between_dml_transactions_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let (client, initial_stream, slot_name, start_lsn) = start_replayable_stream(
        &database,
        publication_name,
        "table_ddl_between_dml_transactions_slot",
    )
    .await;

    database.insert_values(table_name.clone(), &["value"], &[&1]).await.unwrap();
    database
        .run_sql(&format!(
            "alter table {} add column schema_version text",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    database.insert_values(table_name, &["value"], &[&2]).await.unwrap();

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "value".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec!["id".to_owned(), "value".to_owned(), "schema_version".to_owned()],
        ),
        ExpectedStreamMarker::Commit,
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "value".to_owned(), "schema_version".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_discards_rolled_back_ddl_between_dml() {
    init_test_tracing();
    let mut database = spawn_source_database().await;

    let table_name = test_table_name("rolled_back_ddl_between_dml");
    let table_id = database
        .create_table(table_name.clone(), true, &[("value", "integer not null")])
        .await
        .unwrap();

    let publication_name = "rolled_back_ddl_between_dml_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "rolled_back_ddl_between_dml_slot")
            .await;

    let transaction = database.begin_transaction().await;
    transaction.insert_values(table_name.clone(), &["value"], &[&1]).await.unwrap();
    transaction.run_sql("savepoint discarded_schema_change").await.unwrap();
    transaction
        .run_sql(&format!(
            "alter table {} add column discarded_change text",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction.run_sql("rollback to savepoint discarded_schema_change").await.unwrap();
    transaction
        .run_sql(&format!(
            "alter table {} add column retained_change text",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction.insert_values(table_name, &["value"], &[&2]).await.unwrap();
    transaction.commit_transaction().await;

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "value".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec!["id".to_owned(), "value".to_owned(), "retained_change".to_owned()],
        ),
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "value".to_owned(), "retained_change".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_interleaved_table_schema_changes_within_transaction() {
    init_test_tracing();
    let mut database = spawn_source_database().await;

    let table_name = test_table_name("ddl_message_ordering");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("name", "text not null"), ("age", "integer not null default 0")],
        )
        .await
        .unwrap();

    let publication_name = "ddl_message_ordering_pub";
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let (client, initial_stream, slot_name, start_lsn) =
        start_replayable_stream(&database, publication_name, "ddl_message_ordering_slot").await;

    let transaction = database.begin_transaction().await;
    transaction
        .run_sql(&format!(
            "alter table {} add column status text not null default 'pending'",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();
    transaction
        .insert_values(table_name.clone(), &["name", "age", "status"], &[&"alice", &30, &"active"])
        .await
        .unwrap();
    transaction
        .run_sql(&format!("alter table {} drop column age", table_name.as_quoted_identifier()))
        .await
        .unwrap();
    transaction
        .insert_values(table_name.clone(), &["name", "status"], &[&"bob", &"pending"])
        .await
        .unwrap();
    transaction.commit_transaction().await;

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec!["id".to_owned(), "name".to_owned(), "age".to_owned(), "status".to_owned()],
        ),
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "name".to_owned(), "age".to_owned(), "status".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec!["id".to_owned(), "name".to_owned(), "status".to_owned()],
        ),
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "name".to_owned(), "status".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn logical_replication_replays_dml_before_table_and_publication_ddl_within_transaction() {
    init_test_tracing();
    let mut database = spawn_source_database().await;

    if below_version!(database.server_version(), POSTGRES_15) {
        eprintln!("Skipping test: PostgreSQL 15+ required for column filters");
        return;
    }

    let table_name = test_table_name("interleaved_table_publication_ddl");
    let table_id = database
        .create_table(
            table_name.clone(),
            true,
            &[("a", "integer not null"), ("b", "integer not null"), ("c", "integer not null")],
        )
        .await
        .unwrap();
    let quoted_table_name = table_name.as_quoted_identifier();

    let publication_name = "interleaved_table_publication_ddl_pub";
    let quoted_publication_name = quote_identifier(publication_name);
    database.create_publication(publication_name, std::slice::from_ref(&table_name)).await.unwrap();

    let (client, initial_stream, slot_name, start_lsn) = start_replayable_stream(
        &database,
        publication_name,
        "interleaved_table_publication_ddl_slot",
    )
    .await;

    let transaction = database.begin_transaction().await;
    transaction.insert_values(table_name.clone(), &["a", "b", "c"], &[&1, &2, &3]).await.unwrap();
    transaction
        .run_sql(&format!(
            "alter table {quoted_table_name} add column d integer not null default 0"
        ))
        .await
        .unwrap();
    transaction
        .insert_values(table_name.clone(), &["a", "b", "c", "d"], &[&4, &5, &6, &7])
        .await
        .unwrap();
    transaction
        .run_sql(&format!(
            "alter publication {quoted_publication_name} set table {quoted_table_name} (id, a, c, \
             d)"
        ))
        .await
        .unwrap();
    transaction
        .insert_values(table_name.clone(), &["a", "b", "c", "d"], &[&8, &9, &10, &11])
        .await
        .unwrap();
    transaction.commit_transaction().await;

    let expected = [
        ExpectedStreamMarker::Begin,
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned(), "c".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            None,
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned(), "c".to_owned(), "d".to_owned()],
        ),
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned(), "c".to_owned(), "d".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::DdlMessage(
            table_id.into_inner(),
            Some(publication_name.to_owned()),
            vec!["id".to_owned(), "a".to_owned(), "b".to_owned(), "c".to_owned(), "d".to_owned()],
        ),
        ExpectedStreamMarker::Relation(
            table_id.into_inner(),
            vec!["id".to_owned(), "a".to_owned(), "c".to_owned(), "d".to_owned()],
        ),
        ExpectedStreamMarker::Insert(table_id.into_inner()),
        ExpectedStreamMarker::Commit,
    ];

    assert_stream_markers_and_replay(
        client,
        initial_stream,
        &database,
        publication_name,
        &slot_name,
        start_lsn,
        &expected,
    )
    .await;
}
