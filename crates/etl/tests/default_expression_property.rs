//! Differential properties for portable column default expressions.
//!
//! Destinations receive a column default as the [`DefaultExpression`] that
//! `parse_default_expression` extracts from `pg_get_expr` output. Postgres
//! evaluates the same default once during `alter table ... add column` and
//! stores it for every existing row, so a literal that denotes a different
//! value silently diverges the destination's pre-existing rows from the
//! source. Postgres itself is the oracle for every property here.
//!
//! The DDL path captures the default inside the user's session through the
//! `supabase_etl_ddl` logical message, while the bootstrap path reads it in an
//! ETL-configured session. Both call `etl.describe_table_schema`, so session
//! settings that change `pg_get_expr` output change what destinations see.
//!
//! Known defects are pinned by ignored tests: the parser strips casts that
//! change the literal's value, destinations never see the column's type
//! modifier, and the DDL message renders defaults with the user's
//! `DateStyle`, `IntervalStyle`, `extra_float_digits` and `TimeZone`. The
//! default-run properties exclude exactly those inputs.

use etl::{
    config::PgConnectionOptions,
    schema::{DefaultExpression, parse_default_expression, unquote_postgres_string_literal},
    test_utils::{
        database::spawn_source_database,
        pipeline::test_slot_name,
        property::{any_f64, block_on, run_property},
    },
};
use etl_postgres::tokio::test_utils::{PgDatabase, connect_to_pg_database};
use etl_telemetry::tracing::init_test_tracing;
use proptest::{prelude::*, sample::select};
use tokio_postgres::{Client, types::Type};

/// Table every case recreates, so cases never see each other's columns.
const CASE_TABLE: &str = "test.default_case";

/// Publication covering every table, so the DDL trigger emits messages.
const PUBLICATION: &str = "default_expression_pub";

/// Logical message prefix the DDL event trigger emits.
const DDL_MESSAGE_PREFIX: &str = "supabase_etl_ddl";

/// One column type and the default literal written in the DDL.
#[derive(Debug, Clone)]
struct DefaultCase {
    /// Column type including any type modifier, as written in the DDL.
    column_type: String,
    /// Default expression source text, as written in the DDL.
    literal: String,
}

/// Session settings that change how Postgres renders values as text.
#[derive(Debug, Clone)]
struct SessionSettings {
    /// `DateStyle` setting.
    datestyle: String,
    /// `TimeZone` setting.
    timezone: String,
    /// `IntervalStyle` setting.
    intervalstyle: String,
    /// `extra_float_digits` setting.
    extra_float_digits: i32,
}

impl SessionSettings {
    /// Returns the settings every ETL connection applies.
    fn etl() -> Self {
        let options = PgConnectionOptions::builder("default_expression_property").build();

        Self {
            datestyle: options.datestyle,
            timezone: options.timezone,
            intervalstyle: options.intervalstyle,
            extra_float_digits: options.extra_float_digits,
        }
    }

    /// Returns the SQL that applies these settings to a session.
    fn set_sql(&self) -> String {
        format!(
            "set datestyle = '{}'; set timezone = '{}'; set intervalstyle = '{}'; set \
             extra_float_digits = {};",
            self.datestyle, self.timezone, self.intervalstyle, self.extra_float_digits
        )
    }
}

/// The column `c` as `etl.describe_table_schema` reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DescribedColumn {
    /// Postgres type of the column.
    typ: Type,
    /// Type name without the modifier, as destinations map it.
    typname: String,
    /// `format_type` output, including the type modifier.
    formatted_type: String,
    /// `pg_get_expr` output for the column default.
    default_expression: Option<String>,
}

/// Connections for one property: the user's session that issues DDL and an
/// ETL-configured session that reads the bootstrap view.
struct Harness {
    /// Session issuing DDL under arbitrary settings.
    user: Client,
    /// Session configured exactly like ETL's connections.
    etl: Client,
    /// Logical slot that captures the DDL trigger's messages.
    slot: String,
    /// Keeps the database alive; declared last so it drops after the clients.
    _database: PgDatabase<Client>,
}

impl Harness {
    /// Creates a migrated database, a publication over the test schema, and a
    /// slot that captures DDL messages.
    async fn new() -> Self {
        let database = spawn_source_database().await;
        let (user, _) = connect_to_pg_database(&database.config).await;
        let (etl, _) = connect_to_pg_database(&database.config).await;
        etl.batch_execute(&SessionSettings::etl().set_sql()).await.unwrap();

        // `for tables in schema` needs Postgres 15; each test owns a fresh
        // database, so `for all tables` covers the same recreated case table.
        user.batch_execute(&format!("create publication {PUBLICATION} for all tables"))
            .await
            .unwrap();
        // Slot names are cluster-wide, and a temporary slot would vanish on the
        // first rejected DDL because Postgres drops temporary slots on error.
        // Dropping the test database drops this permanent slot.
        let slot = test_slot_name("default_expression");
        user.execute("select pg_create_logical_replication_slot($1, 'pgoutput')", &[&slot])
            .await
            .unwrap();

        Self { user, etl, slot, _database: database }
    }

    /// Recreates the one-row case table and returns whether `alter table ...
    /// add column c <type> default <literal>` succeeded under `settings`.
    async fn add_defaulted_column(&self, case: &DefaultCase, settings: &SessionSettings) -> bool {
        self.etl
            .batch_execute(&format!(
                "drop table if exists {CASE_TABLE}; create table {CASE_TABLE} (id int primary \
                 key); insert into {CASE_TABLE} values (1);"
            ))
            .await
            .unwrap();
        self.user.batch_execute(&settings.set_sql()).await.unwrap();
        let added = self
            .user
            .batch_execute(&format!(
                "alter table {CASE_TABLE} add column c {} default {}",
                case.column_type, case.literal
            ))
            .await
            .is_ok();
        self.user.batch_execute(&SessionSettings::etl().set_sql()).await.unwrap();

        added
    }

    /// Returns column `c` as the bootstrap path sees it in the ETL session.
    async fn bootstrap_column(&self) -> DescribedColumn {
        let row = self
            .etl
            .query_one(
                &format!(
                    "select s.atttypid, s.typname, s.formatted_type, s.default_expression from \
                     etl.describe_table_schema('{CASE_TABLE}'::regclass) s where s.attname = 'c'"
                ),
                &[],
            )
            .await
            .unwrap();

        DescribedColumn {
            typ: Type::from_oid(row.get(0)).unwrap(),
            typname: row.get(1),
            formatted_type: row.get(2),
            default_expression: row.get(3),
        }
    }

    /// Returns column `c` from the DDL messages emitted since the last call.
    ///
    /// Reading through the slot runs the real event trigger in the user's
    /// session, so the captured text is exactly what replication delivers.
    async fn ddl_message_column(&self) -> DescribedColumn {
        let rows = self
            .user
            .query(
                "select data from pg_logical_slot_get_binary_changes($1, null, null, \
                 'proto_version', '1', 'publication_names', $2, 'messages', 'true')",
                &[&self.slot, &PUBLICATION],
            )
            .await
            .unwrap();
        let mut contents: Vec<serde_json::Value> = rows
            .iter()
            .filter_map(|row| logical_message_content(row.get(0)))
            .map(|content| serde_json::from_slice(content).unwrap())
            .collect();
        assert_eq!(contents.len(), 1, "expected one DDL message per case: {contents:?}");

        let message = contents.remove(0);
        let column = message["columns"]
            .as_array()
            .unwrap()
            .iter()
            .find(|column| column["attname"] == "c")
            .unwrap();

        DescribedColumn {
            typ: Type::from_oid(u32::try_from(column["atttypid"].as_i64().unwrap()).unwrap())
                .unwrap(),
            typname: column["typname"].as_str().unwrap().to_owned(),
            formatted_type: column["formatted_type"].as_str().unwrap().to_owned(),
            default_expression: column["default_expression"].as_str().map(ToOwned::to_owned),
        }
    }

    /// Returns the value Postgres backfilled into the pre-existing row.
    ///
    /// `format('%s', ...)` renders through the type's output function, as
    /// replication does; a `::text` cast would trim `character(n)` padding.
    async fn backfilled_value(&self) -> Option<String> {
        self.etl
            .query_one(&format!("select format('%s', c) from {CASE_TABLE}"), &[])
            .await
            .unwrap()
            .get(0)
    }

    /// Returns what a destination would read from the parsed default: the
    /// variant name and the literal's value as `target_type` in the ETL
    /// session, or the Postgres error when the literal is not such a value.
    ///
    /// A numeric literal means the SQL numeric constant, which Postgres then
    /// assigns to the column type; every other variant carries the type's own
    /// text input.
    async fn denoted_value(
        &self,
        column: &DescribedColumn,
        target_type: &str,
    ) -> Option<(&'static str, Result<String, String>)> {
        let parsed = parse_default_expression(column.default_expression.as_deref()?, &column.typ)?;
        let (variant, literal) = plain_literal(&parsed);
        let source_type = if variant == "numeric" { "numeric" } else { "text" };
        let value = self
            .etl
            .query_one(
                &format!("select format('%s', ($1::text::{source_type})::{target_type})"),
                &[&literal],
            )
            .await
            .map(|row| row.get(0))
            .map_err(|err| {
                err.as_db_error().map_or_else(|| err.to_string(), |db| db.message().to_owned())
            });

        Some((variant, value))
    }
}

/// Extracts the content of a pgoutput logical decoding message, if `data` is a
/// `supabase_etl_ddl` message.
///
/// Protocol version 1 lays the message out as `'M'`, a flags byte, the 8-byte
/// LSN, the NUL-terminated prefix, a 4-byte length, and the content.
fn logical_message_content(data: &[u8]) -> Option<&[u8]> {
    let rest = data.strip_prefix(b"M")?.get(9..)?;
    let prefix_end = rest.iter().position(|byte| *byte == 0)?;
    if &rest[..prefix_end] != DDL_MESSAGE_PREFIX.as_bytes() {
        return None;
    }

    let rest = &rest[prefix_end + 1..];
    let length = usize::try_from(u32::from_be_bytes(rest.get(..4)?.try_into().ok()?)).ok()?;

    rest.get(4..4 + length)
}

/// Returns the variant name and plain value text carried by a parsed default.
///
/// Quoted literals are unquoted, so the result is the value a destination
/// would write, independent of its quoting dialect.
fn plain_literal(expression: &DefaultExpression) -> (&'static str, String) {
    let (variant, text) = match expression {
        DefaultExpression::StringLiteral(text) => ("string", text),
        DefaultExpression::NumericLiteral(text) => ("numeric", text),
        DefaultExpression::BooleanLiteral(text) => ("boolean", text),
        DefaultExpression::DateLiteral(text) => ("date", text),
        DefaultExpression::TimeLiteral(text) => ("time", text),
        DefaultExpression::TimeTzLiteral(text) => ("timetz", text),
        DefaultExpression::TimestampLiteral(text) => ("timestamp", text),
        DefaultExpression::TimestampTzLiteral(text) => ("timestamptz", text),
        DefaultExpression::IntervalLiteral(text) => ("interval", text),
        DefaultExpression::JsonLiteral(text) => ("json", text),
    };

    (variant, unquote_postgres_string_literal(text).unwrap_or_else(|| text.clone()))
}

/// Quotes `value` as a standard-conforming SQL string literal.
fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Optional `(p)` precision suffix for temporal types.
fn precision_suffix() -> impl Strategy<Value = String> {
    prop::option::of(0u8..=6).prop_map(|p| p.map(|p| format!("({p})")).unwrap_or_default())
}

/// Integer columns with integral, fractional, and quoted literals.
fn integer_case() -> impl Strategy<Value = DefaultCase> {
    (
        select(vec!["smallint", "integer", "bigint"]),
        prop_oneof![-40_000i64..40_000, any::<i64>()],
        prop::option::of("[0-9]{1,3}"),
        any::<bool>(),
    )
        .prop_map(|(column_type, whole, fraction, quoted)| {
            let number = match fraction {
                Some(fraction) => format!("{whole}.{fraction}"),
                None => whole.to_string(),
            };
            let literal = if quoted { quote(&number) } else { number };

            DefaultCase { column_type: column_type.to_owned(), literal }
        })
}

/// Numeric columns, constrained or not, with literals of arbitrary scale.
fn numeric_case() -> impl Strategy<Value = DefaultCase> {
    let column_type = prop_oneof![
        Just("numeric".to_owned()),
        (1i32..=12)
            .prop_flat_map(|p| (Just(p), -3i32..=p + 3))
            .prop_map(|(p, s)| format!("numeric({p},{s})")),
    ];

    (column_type, any::<bool>(), "[0-9]{1,10}", prop::option::of("[0-9]{1,8}"), any::<bool>())
        .prop_map(|(column_type, negative, whole, fraction, quoted)| {
            let sign = if negative { "-" } else { "" };
            let number = match fraction {
                Some(fraction) => format!("{sign}{whole}.{fraction}"),
                None => format!("{sign}{whole}"),
            };
            let literal = if quoted { quote(&number) } else { number };

            DefaultCase { column_type, literal }
        })
}

/// Float columns with numeric-constant and typed float literals.
///
/// With `value_changing_casts`, a `float4` literal may default a `double
/// precision` column, where the cast changes the value.
fn float_case(value_changing_casts: bool) -> impl Strategy<Value = DefaultCase> {
    (
        select(vec!["real", "double precision"]),
        prop_oneof![any_f64(), -1e6f64..1e6],
        select(vec!["", "float4", "float8"]),
    )
        .prop_map(move |(column_type, value, cast)| {
            let cast = if !value_changing_casts && column_type != "real" && cast == "float4" {
                "float8"
            } else {
                cast
            };
            // Numeric constants become `numeric` values; quoted literals with a
            // float cast become float constants rendered by the float output
            // function, which is the one that honors `extra_float_digits`.
            let literal = match cast {
                "" if value.is_finite() => format!("{value:?}"),
                "" => format!("{}::float8", quote(&format!("{value:?}"))),
                cast => format!("{}::{cast}", quote(&format!("{value:?}"))),
            };

            DefaultCase { column_type: column_type.to_owned(), literal }
        })
}

/// Character columns with literals longer than the declared length.
///
/// With `value_changing_casts`, the literal may carry a length-limited cast,
/// which truncates it whatever the column type, or a `bpchar` cast, whose
/// hidden conversion to `text` or `varchar` trims trailing blanks.
fn text_case(value_changing_casts: bool) -> impl Strategy<Value = DefaultCase> {
    let column_type = prop_oneof![
        Just("text".to_owned()),
        Just("character varying".to_owned()),
        (1u8..=6).prop_map(|n| format!("character varying({n})")),
        (1u8..=6).prop_map(|n| format!("character({n})")),
    ];
    let value = prop::collection::vec(select(vec!['a', 'b', ' ', '\'', '\\', 'é', 'ß']), 0..9)
        .prop_map(|chars| chars.into_iter().collect::<String>());
    let unchanging_cast = select(vec!["", "::text", "::character varying"]).prop_map(str::to_owned);
    let cast = if value_changing_casts {
        prop_oneof![
            unchanging_cast,
            Just("::bpchar".to_owned()),
            (1u8..=6).prop_map(|n| format!("::character varying({n})")),
            (1u8..=6).prop_map(|n| format!("::character({n})")),
        ]
        .boxed()
    } else {
        unchanging_cast.boxed()
    };

    (column_type, value, cast).prop_map(|(column_type, value, cast)| DefaultCase {
        column_type,
        literal: format!("{}{cast}", quote(&value)),
    })
}

/// Boolean columns with every accepted spelling.
fn bool_case() -> impl Strategy<Value = DefaultCase> {
    select(vec!["true", "false", "'t'", "'f'", "'yes'", "'no'", "'on'", "'off'", "'1'", "'0'"])
        .prop_map(|literal| DefaultCase {
            column_type: "boolean".to_owned(),
            literal: literal.to_owned(),
        })
}

/// ISO date text, optionally BC.
fn date_text() -> impl Strategy<Value = String> {
    (1u32..=9999, 1u32..=12, 1u32..=28, any::<bool>()).prop_map(|(year, month, day, bc)| {
        format!("{year:04}-{month:02}-{day:02}{}", if bc { " BC" } else { "" })
    })
}

/// Time-of-day text with up to nine fractional digits.
fn time_text() -> impl Strategy<Value = String> {
    (0u32..24, 0u32..60, 0u32..60, prop::option::of("[0-9]{1,9}")).prop_map(
        |(hour, minute, second, fraction)| match fraction {
            Some(fraction) => format!("{hour:02}:{minute:02}:{second:02}.{fraction}"),
            None => format!("{hour:02}:{minute:02}:{second:02}"),
        },
    )
}

/// UTC offset text in the forms Postgres accepts.
fn offset_text() -> impl Strategy<Value = String> {
    (any::<bool>(), 0u32..=14, select(vec![0u32, 30, 45])).prop_map(|(negative, hours, minutes)| {
        format!("{}{hours:02}:{minutes:02}", if negative { "-" } else { "+" })
    })
}

/// Date, time, and timestamp columns with extra-precision and offset literals.
fn temporal_case() -> impl Strategy<Value = DefaultCase> {
    prop_oneof![
        (date_text(), select(vec!["", "::date"])).prop_map(|(date, cast)| DefaultCase {
            column_type: "date".to_owned(),
            literal: format!("{}{cast}", quote(&date)),
        }),
        (precision_suffix(), time_text()).prop_map(|(p, time)| DefaultCase {
            column_type: format!("time{p} without time zone"),
            literal: quote(&time),
        }),
        (precision_suffix(), time_text(), offset_text()).prop_map(|(p, time, offset)| {
            DefaultCase {
                column_type: format!("time{p} with time zone"),
                literal: quote(&format!("{time}{offset}")),
            }
        }),
        (
            select(vec!["without", "with"]),
            precision_suffix(),
            date_text(),
            time_text(),
            prop::option::of(offset_text()),
            select(vec!["", "::timestamp", "::timestamptz"]),
        )
            .prop_map(|(zone, p, date, time, offset, cast)| {
                // Offsets go before an era suffix, which Postgres requires.
                let (date, era) =
                    date.strip_suffix(" BC").map_or((date.as_str(), ""), |d| (d, " BC"));
                let value = format!("{date} {time}{}{era}", offset.unwrap_or_default());

                DefaultCase {
                    column_type: format!("timestamp{p} {zone} time zone"),
                    literal: format!("{}{cast}", quote(&value)),
                }
            }),
    ]
}

/// Interval columns, including field-restricted ones, with mixed-sign
/// literals.
fn interval_case() -> impl Strategy<Value = DefaultCase> {
    let column_type = prop_oneof![
        Just("interval".to_owned()),
        precision_suffix().prop_map(|p| format!("interval{p}")),
        precision_suffix().prop_map(|p| format!("interval second{p}")),
        precision_suffix().prop_map(|p| format!("interval minute to second{p}")),
        Just("interval day to hour".to_owned()),
        Just("interval hour to minute".to_owned()),
        Just("interval year to month".to_owned()),
    ];
    let field = |unit: &'static str| {
        prop::option::of(-30i32..30)
            .prop_map(move |n| n.map(|n| format!("{n} {unit}")).unwrap_or_default())
    };
    let value = (
        field("years"),
        field("mons"),
        field("days"),
        field("hours"),
        field("minutes"),
        prop::option::of((-90i32..90, prop::option::of("[0-9]{1,8}"))),
    )
        .prop_map(|(years, months, days, hours, minutes, seconds)| {
            let seconds = seconds
                .map(|(whole, fraction)| match fraction {
                    Some(fraction) => format!("{whole}.{fraction} seconds"),
                    None => format!("{whole} seconds"),
                })
                .unwrap_or_default();
            let text = [years, months, days, hours, minutes, seconds]
                .into_iter()
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>()
                .join(" ");

            if text.is_empty() { "0".to_owned() } else { text }
        });

    (column_type, value)
        .prop_map(|(column_type, value)| DefaultCase { column_type, literal: quote(&value) })
}

/// JSON columns with documents whose `json` and `jsonb` text differ.
fn json_case() -> impl Strategy<Value = DefaultCase> {
    (
        select(vec!["json", "jsonb"]),
        select(vec![
            r#"{"b": 1, "a": 2, "a": 3}"#,
            r#"[1, 2.50, "x", null]"#,
            r#""it's""#,
            "1.0e2",
            r#"{"nested": {"k": [true, false]}}"#,
            r#"  {"spaced" :  "v"}  "#,
        ]),
    )
        .prop_map(|(column_type, document)| DefaultCase {
            column_type: column_type.to_owned(),
            literal: quote(document),
        })
}

/// Any supported column type with a literal that may change value on store.
///
/// `value_changing_casts` enables explicit casts that change the literal's
/// value before the column type applies; see
/// `parsed_default_keeps_value_changing_cast`.
fn default_case(value_changing_casts: bool) -> impl Strategy<Value = DefaultCase> {
    prop_oneof![
        integer_case(),
        numeric_case(),
        float_case(value_changing_casts),
        text_case(value_changing_casts),
        bool_case(),
        temporal_case(),
        interval_case(),
        json_case(),
    ]
}

/// Arbitrary user session settings that change value rendering.
fn session_settings() -> impl Strategy<Value = SessionSettings> {
    (
        select(vec!["ISO", "SQL", "Postgres", "German"]),
        select(vec!["MDY", "DMY", "YMD"]),
        select(vec![
            "UTC",
            "America/St_Johns",
            "Asia/Kathmandu",
            "Pacific/Chatham",
            "America/Los_Angeles",
        ]),
        select(vec!["postgres", "postgres_verbose", "sql_standard", "iso_8601"]),
        select(vec![-15, 0, 1, 2, 3]),
    )
        .prop_map(|(style, order, timezone, intervalstyle, extra_float_digits)| {
            SessionSettings {
                datestyle: format!("{style}, {order}"),
                timezone: timezone.to_owned(),
                intervalstyle: intervalstyle.to_owned(),
                extra_float_digits,
            }
        })
}

/// Checks that a parsed default denotes the value Postgres backfilled.
///
/// With `ignore_type_modifier`, the literal is read as the unmodified type,
/// which is how every destination column receives it.
fn check_literal_denotes_backfill(
    harness: &Harness,
    case: &DefaultCase,
    ignore_type_modifier: bool,
) -> Result<(), TestCaseError> {
    block_on(async {
        if !harness.add_defaulted_column(case, &SessionSettings::etl()).await {
            return Ok(());
        }
        let column = harness.bootstrap_column().await;
        let target_type =
            if ignore_type_modifier { &column.typname } else { &column.formatted_type };
        let Some((variant, denoted)) = harness.denoted_value(&column, target_type).await else {
            return Ok(());
        };
        let backfilled = harness.backfilled_value().await.ok_or_else(|| "null".to_owned());

        prop_assert_eq!(
            denoted,
            backfilled,
            "{} column default {:?} parsed as {} literal",
            column.formatted_type,
            column.default_expression,
            variant
        );
        Ok(())
    })
}

/// Returns whether `settings` change how Postgres renders the default of
/// `column` in a way that is a known defect, pinned by its own ignored test.
fn is_known_session_defect(column: &DescribedColumn, settings: &SessionSettings) -> bool {
    let typ = &column.typ;
    // See `ddl_message_date_default_is_independent_of_datestyle`. Every
    // non-ISO style fails: day-first orders swap fields, and even `SQL, MDY`
    // renders zone abbreviations such as `LMT` that Postgres cannot read back.
    let datestyle_defect = matches!(*typ, Type::DATE | Type::TIMESTAMP | Type::TIMESTAMPTZ)
        && !settings.datestyle.starts_with("ISO");
    // See `ddl_message_interval_default_is_independent_of_intervalstyle`.
    let intervalstyle_defect = *typ == Type::INTERVAL && settings.intervalstyle == "sql_standard";
    // See `ddl_message_float_default_is_independent_of_extra_float_digits`.
    // Positive values select shortest-exact output, so only the rest round.
    let float_digits_defect =
        matches!(*typ, Type::FLOAT4 | Type::FLOAT8) && settings.extra_float_digits < 1;
    // See `ddl_message_timestamp_default_from_timestamptz_is_independent_of_timezone`.
    // The hidden `timestamptz` to `timestamp` cast depends on `TimeZone`.
    let timezone_defect = *typ == Type::TIMESTAMP
        && settings.timezone != "UTC"
        && column.default_expression.as_deref().is_some_and(|d| d.ends_with("with time zone"));

    datestyle_defect || intervalstyle_defect || float_digits_defect || timezone_defect
}

/// Checks that the DDL message default denotes the same value as the bootstrap
/// default when the DDL ran under `settings`.
///
/// With `skip_known_defects`, cases [`is_known_session_defect`] flags pass
/// without a check.
fn check_ddl_message_matches_bootstrap(
    harness: &Harness,
    case: &DefaultCase,
    settings: &SessionSettings,
    skip_known_defects: bool,
) -> Result<(), TestCaseError> {
    block_on(async {
        if !harness.add_defaulted_column(case, settings).await {
            return Ok(());
        }
        // Always drain the slot, so the next case sees only its own message.
        let message = harness.ddl_message_column().await;
        let bootstrap = harness.bootstrap_column().await;
        prop_assert_eq!(&message.typ, &bootstrap.typ);
        if skip_known_defects && is_known_session_defect(&bootstrap, settings) {
            return Ok(());
        }

        let message_value = harness.denoted_value(&message, &message.formatted_type).await;
        let bootstrap_value = harness.denoted_value(&bootstrap, &bootstrap.formatted_type).await;
        prop_assert_eq!(
            message_value,
            bootstrap_value,
            "{} default: ddl message {:?}, bootstrap {:?}",
            bootstrap.formatted_type,
            message.default_expression,
            bootstrap.default_expression
        );
        Ok(())
    })
}

/// Runs [`check_literal_denotes_backfill`] on fixed cases and panics listing
/// every counterexample.
async fn assert_literals_denote_backfill(cases: &[(&str, &str)], ignore_type_modifier: bool) {
    let harness = Harness::new().await;
    let failures: Vec<_> = cases
        .iter()
        .filter_map(|(column_type, literal)| {
            let case = DefaultCase {
                column_type: (*column_type).to_owned(),
                literal: (*literal).to_owned(),
            };
            check_literal_denotes_backfill(&harness, &case, ignore_type_modifier).err()
        })
        .collect();

    assert!(failures.is_empty(), "{failures:#?}");
}

/// Runs [`check_ddl_message_matches_bootstrap`] on fixed cases, each issued
/// under the ETL settings with one setting replaced by `apply`, and panics
/// listing every counterexample.
async fn assert_ddl_messages_match_bootstrap(
    cases: &[(&str, &str)],
    apply: impl Fn(&mut SessionSettings),
) {
    let harness = Harness::new().await;
    let mut settings = SessionSettings::etl();
    apply(&mut settings);
    let failures: Vec<_> = cases
        .iter()
        .filter_map(|(column_type, literal)| {
            let case = DefaultCase {
                column_type: (*column_type).to_owned(),
                literal: (*literal).to_owned(),
            };
            check_ddl_message_matches_bootstrap(&harness, &case, &settings, false).err()
        })
        .collect();

    assert!(failures.is_empty(), "{failures:#?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn parsed_default_literal_denotes_backfilled_value() {
    init_test_tracing();
    let harness = Harness::new().await;

    // Value-changing casts are excluded; they are a known defect pinned by
    // `parsed_default_literal_denotes_backfilled_value_with_value_changing_casts`
    // and `parsed_default_keeps_value_changing_cast`.
    run_property("parsed default denotes backfilled value", &default_case(false), |case| {
        check_literal_denotes_backfill(&harness, case, false)
    });
}

#[ignore = "known defect: the parser strips explicit casts that change the literal's value"]
#[tokio::test(flavor = "multi_thread")]
async fn parsed_default_literal_denotes_backfilled_value_with_value_changing_casts() {
    init_test_tracing();
    let harness = Harness::new().await;

    run_property(
        "parsed default denotes backfill with value-changing casts",
        &default_case(true),
        |case| check_literal_denotes_backfill(&harness, case, false),
    );
}

#[ignore = "known defect: the parser strips explicit casts that change the literal's value"]
#[tokio::test(flavor = "multi_thread")]
async fn parsed_default_keeps_value_changing_cast() {
    init_test_tracing();

    // `pg_get_expr` renders these as `'abcdef'::character varying(3)`,
    // `'ab  '::bpchar` and `'0.1'::real`, hiding the conversion to the column
    // type; Postgres backfills `abc`, `ab` and `0.10000000149011612`.
    assert_literals_denote_backfill(
        &[
            ("text", "'abcdef'::varchar(3)"),
            ("text", "'ab  '::bpchar"),
            ("double precision", "'0.1'::float4"),
        ],
        false,
    )
    .await;
}

#[ignore = "known defect: parsed defaults drop the column type modifier that Postgres applied"]
#[tokio::test(flavor = "multi_thread")]
async fn parsed_default_literal_denotes_backfilled_value_without_type_modifier() {
    init_test_tracing();
    let harness = Harness::new().await;

    run_property(
        "parsed default denotes backfill without modifier",
        &default_case(false),
        |case| check_literal_denotes_backfill(&harness, case, true),
    );
}

#[ignore = "known defect: parsed defaults drop the column type modifier that Postgres applied"]
#[tokio::test(flavor = "multi_thread")]
async fn parsed_default_applies_column_type_modifier() {
    init_test_tracing();

    // Destinations map these columns without the modifier, so they store the
    // literal as written while Postgres backfills `1.24`, `ab   `, `abc` and
    // `12:00:01`.
    assert_literals_denote_backfill(
        &[
            ("numeric(10,2)", "1.239"),
            ("character(5)", "'ab'"),
            ("varchar(3)", "'abcdef'::varchar(3)"),
            ("time(0)", "'12:00:00.7'"),
        ],
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ddl_message_default_matches_bootstrap_under_any_session_settings() {
    init_test_tracing();
    let harness = Harness::new().await;
    let strategy = (default_case(false), session_settings());

    // Known defects are skipped by `is_known_session_defect` and checked by
    // `ddl_message_default_matches_bootstrap_including_known_defects`.
    run_property("ddl message default matches bootstrap", &strategy, |(case, settings)| {
        check_ddl_message_matches_bootstrap(&harness, case, settings, true)
    });
}

#[ignore = "known defect: ddl message defaults are rendered with the user's session settings"]
#[tokio::test(flavor = "multi_thread")]
async fn ddl_message_default_matches_bootstrap_including_known_defects() {
    init_test_tracing();
    let harness = Harness::new().await;
    let strategy = (default_case(false), session_settings());

    run_property("ddl message default matches bootstrap", &strategy, |(case, settings)| {
        check_ddl_message_matches_bootstrap(&harness, case, settings, false)
    });
}

#[ignore = "known defect: ddl message date defaults follow the user's DateStyle"]
#[tokio::test(flavor = "multi_thread")]
async fn ddl_message_date_default_is_independent_of_datestyle() {
    init_test_tracing();

    // The message carries `'02.01.2026'::date`, which ETL reads as February 1.
    assert_ddl_messages_match_bootstrap(&[("date", "'2026-01-02'")], |settings| {
        settings.datestyle = "German, MDY".to_owned();
    })
    .await;
}

#[ignore = "known defect: ddl message interval defaults follow the user's IntervalStyle"]
#[tokio::test(flavor = "multi_thread")]
async fn ddl_message_interval_default_is_independent_of_intervalstyle() {
    init_test_tracing();

    // The message carries `'-1 2:00:00'::interval`, which ETL reads as
    // `-1 days +02:00:00` instead of `-1 days -02:00:00`.
    assert_ddl_messages_match_bootstrap(&[("interval", "'-1 days -2 hours'")], |settings| {
        settings.intervalstyle = "sql_standard".to_owned();
    })
    .await;
}

#[ignore = "known defect: ddl message float defaults follow the user's extra_float_digits"]
#[tokio::test(flavor = "multi_thread")]
async fn ddl_message_float_default_is_independent_of_extra_float_digits() {
    init_test_tracing();

    // The message carries `'0.3'::double precision`, losing the last digits.
    assert_ddl_messages_match_bootstrap(
        &[("double precision", "'0.30000000000000004'::float8")],
        |settings| settings.extra_float_digits = 0,
    )
    .await;
}

#[ignore = "known defect: a timestamptz default on a timestamp column depends on TimeZone"]
#[tokio::test(flavor = "multi_thread")]
async fn ddl_message_timestamp_default_from_timestamptz_is_independent_of_timezone() {
    init_test_tracing();

    // The message carries `'2026-01-02 08:49:05+05:45'::timestamp with time
    // zone`, which ETL reads as 08:49:05, while the bootstrap carries the
    // `+00` rendering, read as 03:04:05.
    assert_ddl_messages_match_bootstrap(
        &[("timestamp", "'2026-01-02 03:04:05+00'::timestamptz")],
        |settings| settings.timezone = "Asia/Kathmandu".to_owned(),
    )
    .await;
}
