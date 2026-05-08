use tokio_postgres::types::Type;

macro_rules! define_type_mappings {
    (
        $(
            $pg_type:ident => $string_name:literal
        ),* $(,)?
    ) => {
        /// Converts a Postgres type name string to a [`Type`].
        #[must_use]
        pub fn string_to_postgres_type(type_str: &str) -> Type {
            match type_str {
                $(
                    $string_name => Type::$pg_type,
                )*
                _ => Type::TEXT,
            }
        }

        /// Converts a Postgres [`Type`] to its string representation.
        #[must_use]
        pub fn postgres_type_to_string(pg_type: &Type) -> String {
            match *pg_type {
                $(
                    Type::$pg_type => $string_name.to_string(),
                )*
                _ => format!("UNKNOWN({})", pg_type.name()),
            }
        }
    };
}

define_type_mappings! {
    BOOL => "BOOL",
    CHAR => "CHAR",
    INT2 => "INT2",
    INT4 => "INT4",
    INT8 => "INT8",
    FLOAT4 => "FLOAT4",
    FLOAT8 => "FLOAT8",
    TEXT => "TEXT",
    VARCHAR => "VARCHAR",
    TIMESTAMP => "TIMESTAMP",
    TIMESTAMPTZ => "TIMESTAMPTZ",
    DATE => "DATE",
    TIME => "TIME",
    TIMETZ => "TIMETZ",
    BYTEA => "BYTEA",
    UUID => "UUID",
    JSON => "JSON",
    JSONB => "JSONB",
    NAME => "NAME",
    BPCHAR => "BPCHAR",
    NUMERIC => "NUMERIC",
    MONEY => "MONEY",
    INTERVAL => "INTERVAL",
    INET => "INET",
    CIDR => "CIDR",
    MACADDR => "MACADDR",
    MACADDR8 => "MACADDR8",
    BIT => "BIT",
    VARBIT => "VARBIT",
    POINT => "POINT",
    LSEG => "LSEG",
    PATH => "PATH",
    BOX => "BOX",
    POLYGON => "POLYGON",
    LINE => "LINE",
    CIRCLE => "CIRCLE",
    OID => "OID",
    XML => "XML",
    PG_LSN => "PG_LSN",
    TS_VECTOR => "TSVECTOR",
    TSQUERY => "TSQUERY",
    BOOL_ARRAY => "BOOL_ARRAY",
    INT2_ARRAY => "INT2_ARRAY",
    INT4_ARRAY => "INT4_ARRAY",
    INT8_ARRAY => "INT8_ARRAY",
    FLOAT4_ARRAY => "FLOAT4_ARRAY",
    FLOAT8_ARRAY => "FLOAT8_ARRAY",
    TEXT_ARRAY => "TEXT_ARRAY",
    VARCHAR_ARRAY => "VARCHAR_ARRAY",
    TIMESTAMP_ARRAY => "TIMESTAMP_ARRAY",
    TIMESTAMPTZ_ARRAY => "TIMESTAMPTZ_ARRAY",
    DATE_ARRAY => "DATE_ARRAY",
    UUID_ARRAY => "UUID_ARRAY",
    JSON_ARRAY => "JSON_ARRAY",
    JSONB_ARRAY => "JSONB_ARRAY",
    NUMERIC_ARRAY => "NUMERIC_ARRAY",
    INT4_RANGE => "INT4_RANGE",
    INT8_RANGE => "INT8_RANGE",
    NUM_RANGE => "NUM_RANGE",
    TS_RANGE => "TS_RANGE",
    TSTZ_RANGE => "TSTZ_RANGE",
    DATE_RANGE => "DATE_RANGE"
}
