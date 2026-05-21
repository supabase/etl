# Data Type Compatibility

ETL materializes PostgreSQL source types into destination column types before it
writes rows. Two destination strategies control that process:

1. Which physical destination type ETL uses when it creates or updates a
   destination schema.
2. Whether ETL accepts, rejects, preserves, or normalizes values that do not fit
   the destination type exactly.

The default is `type_strategy = "native_or_string"` and
`value_strategy = "reject"`. That default prefers native destination types, uses
string fallbacks for PostgreSQL-only types, and rejects values that would need
normalization.

## Strategies

| Strategy | Values | Behavior |
|----------|--------|----------|
| `type_strategy` | `native_only`, `native_or_string`, `string_if_risky` | Chooses destination column types. |
| `value_strategy` | `reject`, `normalize`, `preserve` | Chooses how incoming values are handled when they do not fit the selected destination type exactly. |

The `string_if_risky` type strategy uses `STRING` when the source type has no
native destination representation, or when the native destination representation
may reject, round, clamp, canonicalize, or otherwise lose source semantics. It
uses the native destination type only when that type is supported and safe.

The strategies are independent and are applied in order. ETL first applies the
`type_strategy` to select the physical destination type for the column. It then
applies the `value_strategy` against that selected destination type. For example,
with `type_strategy = "native_or_string"`, a PostgreSQL `uuid` column
materializes as BigQuery `STRING`; after that, any value strategy writes UUID
values as strings because `STRING` is the selected type.

The strategies are destination-agnostic configuration values, but each
destination defines its own native type surface and value rules. The table below
documents common BigQuery strategy pairs:

| Type strategy | Value strategy | Shorthand description |
|---------------|----------------|-----------------------|
| `native_only` | `reject` | Native-only schema and exact values. |
| `native_or_string` | `reject` | Native types with string fallbacks, exact values. |
| `string_if_risky` | `preserve` | Strings for risky types to preserve source representations. |
| `native_or_string` | `normalize` | Native types with string fallbacks, normalized values. |

## BigQuery

BigQuery maps PostgreSQL `timestamp without time zone` to BigQuery `DATETIME`
and PostgreSQL `timestamp with time zone` to BigQuery `TIMESTAMP`. This preserves
the source distinction between a local date-time and an absolute instant.

For array columns with `type_strategy = "native_only"` or
`type_strategy = "native_or_string"`, ETL uses BigQuery repeated fields when
the array can be represented. BigQuery repeated fields cannot preserve a `NULL`
array value or `NULL` array elements, so those values are rejected in
repeated-field strategies. With `type_strategy = "string_if_risky"`, every
PostgreSQL array column is stored as a scalar `STRING` using PostgreSQL-style
array text, which preserves `NULL` arrays and `NULL` elements but is no longer
queryable as a BigQuery array.

When the selected BigQuery type is `STRING`, the value strategy does not attempt
destination-native validation or normalization for the original PostgreSQL type.
It writes `NULL` as `NULL` and non-`NULL` source values as strings. When the
selected BigQuery type is a repeated field, all value strategies must still
reject `NULL` arrays and `NULL` elements because BigQuery repeated fields cannot
represent them.

### Type Strategy Behavior

This table describes only destination type selection. Value validation and
normalization happen afterward against the selected BigQuery type.

| PostgreSQL source type | `native_only` | `native_or_string` | `string_if_risky` |
|------------------------|---------------|-------------------------|--------------------|
| `boolean` | `BOOL` | `BOOL` | `BOOL` |
| `char`, `bpchar`, `varchar`, `name`, `text` | `STRING` | `STRING` | `STRING` |
| `smallint`, `integer` | `INT64` | `INT64` | `INT64` |
| `bigint`, `oid` | `INT64` | `INT64` | `INT64` |
| `real`, `double precision` | `FLOAT64` | `FLOAT64` | `STRING` |
| `numeric` | `BIGNUMERIC` | `BIGNUMERIC` | `STRING` |
| `date` | `DATE` | `DATE` | `STRING` |
| `time` | `TIME` | `TIME` | `STRING` |
| `timestamp` | `DATETIME` | `DATETIME` | `STRING` |
| `timestamptz` | `TIMESTAMP` | `TIMESTAMP` | `STRING` |
| `json`, `jsonb` | `JSON` | `JSON` | `STRING` |
| `bytea` | `BYTES` | `BYTES` | `BYTES` |
| `uuid` | Rejected | `STRING` | `STRING` |
| `money` | Rejected | `STRING` | `STRING` |
| Other non-native scalar types, such as `interval`, `timetz`, `regclass`, `inet`, `cidr`, `macaddr`, `bit`, geometric types, text search types, `pg_lsn`, and range types | Rejected | `STRING` | `STRING` |
| Arrays of native scalar types, such as `boolean[]`, text arrays, integer arrays, float arrays, `numeric[]`, temporal arrays, `json[]`, `jsonb[]`, and `bytea[]` | `ARRAY<...>` | `ARRAY<...>` | `STRING` |
| Arrays of non-native scalar types, such as `uuid[]`, `money[]`, `interval[]`, `timetz[]`, network arrays, bit arrays, and range arrays | Rejected | `ARRAY<STRING>` | `STRING` |

### Value Strategy Behavior

This table applies after type selection. If the type strategy selected
`STRING`, ETL first converts the source cell to the string representation that
will be written to BigQuery, and the value strategy does not perform
destination-native validation for the original PostgreSQL type.

| Selected BigQuery type or value domain | `reject` | `normalize` | `preserve` |
|----------------------------------------|----------|-------------|------------|
| `STRING` | Writes `NULL` as `NULL` and non-`NULL` source values as strings. | Writes `NULL` as `NULL` and non-`NULL` source values as strings. | Writes `NULL` as `NULL` and non-`NULL` source values as strings. |
| `ARRAY<STRING>` | Writes non-`NULL` elements as strings and rejects `NULL` arrays or elements. | Writes non-`NULL` elements as strings and rejects `NULL` arrays or elements. | Writes non-`NULL` elements as strings and rejects `NULL` arrays or elements. |
| Exact native primitives: `BOOL`, `INT64`, text-backed `STRING`, and `BYTES` | Writes the value unchanged. | Writes the value unchanged. | Writes the value unchanged. |
| `FLOAT64` | Rejects negative zero. | Converts negative zero to positive zero. | Writes the value unchanged. |
| `BIGNUMERIC` | Rejects `NaN`, infinities, values outside BigQuery's finite `BIGNUMERIC` range, and values with more than 38 fractional digits. | Rounds to 38 fractional digits using half-away-from-zero, clamps out-of-range values and infinities to BigQuery's finite `BIGNUMERIC` bounds, and rejects `NaN`. | Writes the value unchanged. BigQuery may reject values outside its supported domain. |
| `DATE` | Requires finite dates in `0001-01-01..=9999-12-31`. | Clamps infinities and out-of-range dates to BigQuery's date bounds. | Writes the value unchanged. BigQuery may reject unsupported values. |
| `TIME` | Rejects PostgreSQL `24:00:00`. | Maps PostgreSQL `24:00:00` to `23:59:59.999999`. | Writes the value unchanged. BigQuery may reject unsupported values. |
| `DATETIME` | Requires finite timestamps in BigQuery's supported range. | Clamps infinities and out-of-range timestamps to BigQuery's timestamp bounds. | Writes the value unchanged. BigQuery may reject unsupported values. |
| `TIMESTAMP` | Requires finite UTC instants in BigQuery's supported range. | Clamps infinities and out-of-range instants to BigQuery's timestamp bounds. | Writes the value unchanged. BigQuery may reject unsupported values. |
| `JSON` | Rejects invalid JSON, duplicate object keys, nesting deeper than 500 levels, integers outside BigQuery's exact signed or unsigned 64-bit JSON integer domain, and non-integer numbers outside finite `FLOAT64`. | Keeps the first value for duplicate object keys and rounds numbers outside BigQuery's exact JSON number domain when they can still fit as finite `FLOAT64`; invalid JSON, nesting deeper than 500 levels, and numbers that cannot be rounded into BigQuery's JSON number domain are still rejected. | Writes the raw JSON text unchanged. BigQuery may reject invalid JSON or values outside its supported JSON domain. |
| `ARRAY<...>` except `ARRAY<STRING>` | Rejects `NULL` arrays and `NULL` elements, then applies the selected element type's `reject` behavior. | Rejects `NULL` arrays and `NULL` elements, then applies the selected element type's `normalize` behavior. | Rejects `NULL` arrays and `NULL` elements, then writes elements unchanged in the selected repeated type. BigQuery may reject unsupported element values. |

`native_or_string` plus `reject` is the BigQuery default because it avoids
silent value changes while still allowing common PostgreSQL-only types, such as
`uuid`, to replicate as `STRING`.
