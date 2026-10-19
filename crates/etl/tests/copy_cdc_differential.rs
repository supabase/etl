//! Differential property between the two ways a row reaches the codec.
//!
//! Table copy decodes `copy ... to stdout` text rows, while streaming decodes
//! pgoutput insert tuples. Both carry each value's text output, but COPY adds
//! its own escaping on top, so a value needing escapes could decode
//! differently on one path and silently drift between the backfill and CDC.
//! The property inserts generated rows, reads them back both ways, and
//! requires both decoded rows to be identical.

use bytes::Bytes;
use etl::{
    data::{Cell, TableRow},
    schema::ColumnSchema,
    test_utils::{
        database::spawn_source_database,
        pipeline::test_slot_name,
        property::{block_on, run_expensive_property},
        replication_stream::{parse_copy_row, parse_tuple},
    },
};
use etl_config::shared::PgConnectionOptions;
use etl_postgres::tokio::test_utils::connect_to_pg_database;
use etl_telemetry::tracing::init_test_tracing;
use futures::StreamExt;
use pg_escape::quote_identifier;
use postgres_replication::protocol::LogicalReplicationMessage;
use proptest::{option, prelude::*};
use tokio_postgres::{
    Client, Statement,
    types::{ToSql, Type},
};

use crate::support::pg_literals::{
    bool_literal, box_literal, bytea_literal, date_literal, float4_literal, float8_literal,
    inet_literal, int2_literal, int4_literal, int8_literal, interval_literal, json_literal,
    money_literal, numeric_literal, text_literal, time_literal, timestamp_literal,
    timestamptz_literal, timetz_text_literal, uuid_literal, varchar_literal,
};

/// Publication the slot decodes.
const PUBLICATION: &str = "copy_cdc_pub";

/// One column of the differential table.
struct Column {
    /// Column name.
    name: &'static str,
    /// Postgres type name of the column, or of its elements for arrays.
    sql_type: &'static str,
    /// Column type the decoders receive.
    typ: Type,
    /// Whether the column is a one-dimensional array of `sql_type`.
    array: bool,
    /// Input literal strategy for one value or array element.
    literal: fn() -> BoxedStrategy<String>,
}

/// Builds a scalar [`Column`].
fn scalar(
    name: &'static str,
    sql_type: &'static str,
    typ: Type,
    literal: fn() -> BoxedStrategy<String>,
) -> Column {
    Column { name, sql_type, typ, array: false, literal }
}

/// Builds an array [`Column`].
fn array(
    name: &'static str,
    sql_type: &'static str,
    typ: Type,
    literal: fn() -> BoxedStrategy<String>,
) -> Column {
    Column { name, sql_type, typ, array: true, literal }
}

/// Columns covering the specialized codec types, the text fallback, and
/// arrays of both, including `box[]` with its `;` element delimiter.
fn columns() -> Vec<Column> {
    vec![
        scalar("t", "text", Type::TEXT, || text_literal().boxed()),
        scalar("vc", "varchar", Type::VARCHAR, || varchar_literal().boxed()),
        scalar("by", "bytea", Type::BYTEA, || bytea_literal().boxed()),
        scalar("b", "bool", Type::BOOL, || bool_literal().boxed()),
        scalar("i2", "int2", Type::INT2, || int2_literal().boxed()),
        scalar("i4", "int4", Type::INT4, || int4_literal().boxed()),
        scalar("i8", "int8", Type::INT8, || int8_literal().boxed()),
        scalar("f4", "float4", Type::FLOAT4, || float4_literal().boxed()),
        scalar("f8", "float8", Type::FLOAT8, || float8_literal().boxed()),
        scalar("num", "numeric", Type::NUMERIC, || numeric_literal().boxed()),
        scalar("d", "date", Type::DATE, || date_literal().boxed()),
        scalar("tm", "time", Type::TIME, || time_literal().boxed()),
        scalar("ttz", "timetz", Type::TIMETZ, || timetz_text_literal().boxed()),
        scalar("ts", "timestamp", Type::TIMESTAMP, || timestamp_literal().boxed()),
        scalar("tstz", "timestamptz", Type::TIMESTAMPTZ, || timestamptz_literal().boxed()),
        scalar("u", "uuid", Type::UUID, || uuid_literal().boxed()),
        scalar("j", "json", Type::JSON, || json_literal(true).boxed()),
        scalar("jb", "jsonb", Type::JSONB, || json_literal(false).boxed()),
        scalar("iv", "interval", Type::INTERVAL, || interval_literal().boxed()),
        scalar("ip", "inet", Type::INET, || inet_literal().boxed()),
        scalar("m", "money", Type::MONEY, || money_literal().boxed()),
        array("t_a", "text", Type::TEXT_ARRAY, || text_literal().boxed()),
        array("vc_a", "varchar", Type::VARCHAR_ARRAY, || varchar_literal().boxed()),
        array("by_a", "bytea", Type::BYTEA_ARRAY, || bytea_literal().boxed()),
        array("b_a", "bool", Type::BOOL_ARRAY, || bool_literal().boxed()),
        array("i4_a", "int4", Type::INT4_ARRAY, || int4_literal().boxed()),
        array("i8_a", "int8", Type::INT8_ARRAY, || int8_literal().boxed()),
        array("f8_a", "float8", Type::FLOAT8_ARRAY, || float8_literal().boxed()),
        array("num_a", "numeric", Type::NUMERIC_ARRAY, || numeric_literal().boxed()),
        array("d_a", "date", Type::DATE_ARRAY, || date_literal().boxed()),
        array("ttz_a", "timetz", Type::TIMETZ_ARRAY, || timetz_text_literal().boxed()),
        array("tstz_a", "timestamptz", Type::TIMESTAMPTZ_ARRAY, || timestamptz_literal().boxed()),
        array("u_a", "uuid", Type::UUID_ARRAY, || uuid_literal().boxed()),
        array("j_a", "json", Type::JSON_ARRAY, || json_literal(true).boxed()),
        array("jb_a", "jsonb", Type::JSONB_ARRAY, || json_literal(false).boxed()),
        array("iv_a", "interval", Type::INTERVAL_ARRAY, || interval_literal().boxed()),
        array("ip_a", "inet", Type::INET_ARRAY, || inet_literal().boxed()),
        array("box_a", "box", Type::BOX_ARRAY, || box_literal().boxed()),
    ]
}

/// One generated column value: a scalar literal or an array of element
/// literals, either of which may be NULL.
#[derive(Clone, Debug)]
enum ColumnValue {
    /// A scalar column value.
    Scalar(Option<String>),
    /// An array column value.
    Array(Option<Vec<Option<String>>>),
}

impl ColumnValue {
    /// Returns the value as an insert parameter.
    fn as_param(&self) -> &(dyn ToSql + Sync) {
        match self {
            Self::Scalar(value) => value,
            Self::Array(values) => values,
        }
    }
}

/// Strategy for one row: a value per column, in column order.
fn row_strategy(columns: &[Column]) -> Vec<BoxedStrategy<ColumnValue>> {
    columns
        .iter()
        .map(|column| {
            let literal = (column.literal)();
            if column.array {
                option::of(proptest::collection::vec(option::of(literal), 0..=4))
                    .prop_map(ColumnValue::Array)
                    .boxed()
            } else {
                option::of(literal).prop_map(ColumnValue::Scalar).boxed()
            }
        })
        .collect()
}

/// Applies the replicator's pinned session options to `client`.
///
/// The replicator pins output-affecting GUCs as startup options on its
/// connections. pgoutput runs output functions with the decoding session's
/// settings, so pinning them here makes both the COPY text and the
/// `pg_logical_slot_get_binary_changes` tuples match what production reads.
async fn apply_replicator_session_options(client: &Client) {
    let options = PgConnectionOptions::builder("copy_cdc_differential").build();
    for (name, value) in options.to_key_value_pairs() {
        client.execute("select set_config($1, $2, false)", &[&name, &value]).await.unwrap();
    }
}

/// Source-side fixture: the table, the publication, the slot, and the
/// statements one case runs.
struct Fixture {
    /// Inserts one row; parameter 1 is the row ordinal.
    insert: Statement,
    /// Drains the slot as pgoutput protocol version 1 messages.
    changes: Statement,
    /// Temporary logical slot that decodes the inserts.
    slot_name: String,
    /// Decoder schemas in table column order, including the ordinal.
    column_schemas: Vec<ColumnSchema>,
}

impl Fixture {
    /// Creates `test.copy_cdc` with the given columns, publishes its inserts,
    /// and creates a temporary pgoutput slot on `client`'s session.
    async fn create(client: &Client, columns: &[Column]) -> Self {
        let definitions = columns
            .iter()
            .map(|column| {
                let brackets = if column.array { "[]" } else { "" };
                format!("{} {}{brackets}", column.name, column.sql_type)
            })
            .collect::<Vec<_>>()
            .join(", ");
        client
            .execute(&format!("create table test.copy_cdc (ord int4, {definitions})"), &[])
            .await
            .unwrap();
        // Publishing inserts only lets each case truncate without emitting
        // changes or needing a replica identity.
        client
            .execute(
                &format!(
                    "create publication {PUBLICATION} for table test.copy_cdc with (publish = \
                     'insert')"
                ),
                &[],
            )
            .await
            .unwrap();

        let slot_name = test_slot_name("copy_cdc");
        client
            .execute(
                "select pg_create_logical_replication_slot($1, 'pgoutput', true)",
                &[&slot_name],
            )
            .await
            .unwrap();

        let casts = columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let brackets = if column.array { "[]" } else { "" };
                format!("${}::text{brackets}::{}{brackets}", index + 2, column.sql_type)
            })
            .collect::<Vec<_>>()
            .join(", ");
        let insert = client
            .prepare(&format!("insert into test.copy_cdc values ($1, {casts})"))
            .await
            .unwrap();
        let changes = client
            .prepare(&format!(
                "select data from pg_logical_slot_get_binary_changes($1, null, null, \
                 'proto_version', '1', 'publication_names', '{PUBLICATION}')"
            ))
            .await
            .unwrap();

        let mut column_schemas = vec![ColumnSchema::new("ord".to_owned(), Type::INT4, -1, 1, true)];
        for (index, column) in columns.iter().enumerate() {
            let ordinal_position = i32::try_from(index + 2).unwrap();
            column_schemas.push(ColumnSchema::new(
                column.name.to_owned(),
                column.typ.clone(),
                -1,
                ordinal_position,
                true,
            ));
        }

        Self { insert, changes, slot_name, column_schemas }
    }

    /// Replaces the table contents with `rows` in one transaction and returns
    /// the raw COPY rows and the raw pgoutput messages, both in insert order.
    async fn write_and_read(
        &self,
        client: &Client,
        rows: &[Vec<ColumnValue>],
    ) -> Result<(Vec<Vec<u8>>, Vec<Vec<u8>>), tokio_postgres::Error> {
        client.batch_execute("truncate test.copy_cdc").await?;
        client.batch_execute("begin").await?;
        for (ord, row) in rows.iter().enumerate() {
            let ord = i32::try_from(ord).unwrap();
            let mut params: Vec<&(dyn ToSql + Sync)> = vec![&ord];
            params.extend(row.iter().map(ColumnValue::as_param));
            client.execute(&self.insert, &params).await?;
        }
        client.batch_execute("commit").await?;

        // Postgres sends one COPY data message per row.
        let stream =
            client.copy_out("copy (select * from test.copy_cdc order by ord) to stdout").await?;
        futures::pin_mut!(stream);
        let mut copy_rows = Vec::new();
        while let Some(chunk) = stream.next().await {
            copy_rows.push(chunk?.to_vec());
        }

        let messages = client
            .query(&self.changes, &[&self.slot_name])
            .await?
            .into_iter()
            .map(|row| row.get::<_, Vec<u8>>(0))
            .collect();

        Ok((copy_rows, messages))
    }
}

/// Renders decoded rows for comparison.
///
/// `Debug` output distinguishes every value the codec can produce, including
/// signed zeros and infinities, while treating NaN as equal to NaN, which
/// `Cell`'s `PartialEq` does not.
fn comparable(rows: &[TableRow]) -> Vec<String> {
    rows.iter().map(|row| format!("{:?}", row.values())).collect()
}

/// Writes `rows` through `fixture` and decodes them back through both the
/// COPY path and the pgoutput path, returning `(copy_rows, cdc_rows)`.
fn decode_both_ways(
    client: &Client,
    fixture: &Fixture,
    rows: &[Vec<ColumnValue>],
) -> Result<(Vec<TableRow>, Vec<TableRow>), TestCaseError> {
    let (copy_rows, messages) = block_on(fixture.write_and_read(client, rows))
        .map_err(|err| TestCaseError::fail(format!("postgres round trip failed: {err}")))?;

    let copy_decoded = copy_rows
        .iter()
        .map(|row| {
            parse_copy_row(row, &fixture.column_schemas)
                .map_err(|err| TestCaseError::fail(format!("COPY row {row:?} rejected: {err}")))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut cdc_decoded = Vec::new();
    for message in messages {
        let message = LogicalReplicationMessage::parse(&Bytes::from(message))
            .map_err(|err| TestCaseError::fail(format!("pgoutput parse failed: {err}")))?;
        if let LogicalReplicationMessage::Insert(insert) = message {
            let tuple = insert.tuple().tuple_data();
            cdc_decoded.push(parse_tuple(tuple, &fixture.column_schemas).map_err(|err| {
                TestCaseError::fail(format!("pgoutput tuple {tuple:?} rejected: {err}"))
            })?);
        }
    }

    Ok((copy_decoded, cdc_decoded))
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_and_pgoutput_decode_identical_rows() {
    init_test_tracing();

    let database = spawn_source_database().await;
    let client = database.client.as_ref().unwrap();
    apply_replicator_session_options(client).await;

    let columns = columns();
    let fixture = Fixture::create(client, &columns).await;

    // Several rows per case amortize the slot round trip.
    let strategy = proptest::collection::vec(row_strategy(&columns), 1..=6);
    run_expensive_property("copy vs pgoutput decode", &strategy, |rows| {
        let (copy_decoded, cdc_decoded) = decode_both_ways(client, &fixture, rows)?;

        prop_assert_eq!(copy_decoded.len(), rows.len());
        prop_assert_eq!(comparable(&copy_decoded), comparable(&cdc_decoded));
        Ok(())
    });
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "known defect: bytea_output is not pinned, so a database default of escape halts bytea \
            decoding"]
async fn bytea_decodes_under_database_default_bytea_output_escape() {
    init_test_tracing();

    let database = spawn_source_database().await;
    database
        .client
        .as_ref()
        .unwrap()
        .execute(
            &format!(
                "alter database {} set bytea_output = 'escape'",
                quote_identifier(&database.config.name)
            ),
            &[],
        )
        .await
        .unwrap();

    // A new session picks up the database default, which the pinned startup
    // options leave in place.
    let (client, _) = connect_to_pg_database(&database.config).await;
    apply_replicator_session_options(&client).await;

    let columns = vec![scalar("by", "bytea", Type::BYTEA, || bytea_literal().boxed())];
    let fixture = Fixture::create(&client, &columns).await;

    let rows = vec![vec![ColumnValue::Scalar(Some("\\x00ff5c".to_owned()))]];
    let (copy_decoded, cdc_decoded) = decode_both_ways(&client, &fixture, &rows).unwrap();

    let expected = vec![Cell::I32(0), Cell::Bytes(vec![0x00, 0xff, 0x5c])];
    assert_eq!(copy_decoded[0].values(), expected.as_slice());
    assert_eq!(cdc_decoded[0].values(), expected.as_slice());
}
