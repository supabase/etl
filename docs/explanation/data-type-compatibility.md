# Data Type Compatibility

ETL materializes PostgreSQL source types into destination column types before it
writes rows. A destination's `type_compatibility` mode controls both parts of
that process:

1. Which physical destination type ETL uses when it creates or updates a
   destination schema.
2. Whether ETL accepts, rejects, preserves, or normalizes values that do not fit
   the destination type exactly.

The default mode is `compatible`. In serialized configuration and API payloads,
the mode values are `strict`, `compatible`, `preserve`, and `coerce`.

## Compatibility Modes

| Mode | Schema behavior | Value behavior | Use when |
|------|-----------------|----------------|----------|
| `strict` | Requires a native destination type. If a PostgreSQL type has no native destination representation, the schema is rejected. | Requires exact destination values. Values that the destination would round, clamp, canonicalize, or otherwise change are rejected. | You want the destination schema and values to fail fast unless they are exact. |
| `compatible` | Uses native destination types when ETL can preserve values exactly, and falls back to `STRING` for PostgreSQL-only scalar types or `ARRAY<STRING>` for PostgreSQL-only array types. | Rejects values that would need coercion in native destination columns. Fallback string columns keep the source representation. | You want the safest default for analytics: queryable native types where they are reliable, with string fallbacks instead of pipeline failure for unsupported types. |
| `preserve` | Uses `STRING` for types whose native destination representation could change the source value. | Keeps the source representation instead of asking the destination to interpret risky values. | You care more about preserving the source representation than about native destination query types. |
| `coerce` | Prefers native destination types, with `STRING` fallbacks for PostgreSQL-only scalar types or `ARRAY<STRING>` for PostgreSQL-only array types. | Allows documented value changes such as rounding, clamping, or canonicalization when that keeps data in a native destination type. | You want more queryable destination columns and accept destination-shaped values. |

The modes are destination-agnostic configuration values, but each destination
defines its own native type surface and value rules. The table below documents
the current BigQuery behavior.

## BigQuery

BigQuery maps PostgreSQL `timestamp without time zone` to BigQuery `DATETIME`
and PostgreSQL `timestamp with time zone` to BigQuery `TIMESTAMP`. This preserves
the source distinction between a local date-time and an absolute instant.

For array columns in `strict`, `compatible`, and `coerce`, ETL uses BigQuery
repeated fields when the array can be represented. BigQuery repeated fields
cannot preserve a `NULL` array value or `NULL` array elements, so those values
are rejected in repeated-field modes. In `preserve`, every PostgreSQL array
column is stored as a scalar `STRING` using PostgreSQL-style array text, which
preserves `NULL` arrays and `NULL` elements but is no longer queryable as a
BigQuery array.

| PostgreSQL source type | `strict` | `compatible` default | `preserve` | `coerce` | Value notes |
|------------------------|----------|----------------------|------------|----------|-------------|
| `boolean` | `BOOL` | `BOOL` | `BOOL` | `BOOL` | Exact in all modes. |
| `char`, `bpchar`, `varchar`, `name`, `text` | `STRING` | `STRING` | `STRING` | `STRING` | Exact in all modes. |
| `smallint`, `integer` | `INT64` | `INT64` | `INT64` | `INT64` | Exact in all modes. |
| `bigint`, `oid` | `INT64` | `INT64` | `INT64` | `INT64` | Exact in all modes. |
| `real`, `double precision` | `FLOAT64` | `FLOAT64` | `STRING` | `FLOAT64` | `strict` and `compatible` reject negative zero. `preserve` stores the source text. `coerce` normalizes negative zero to positive zero. |
| `numeric` | `BIGNUMERIC` | `BIGNUMERIC` | `STRING` | `BIGNUMERIC` | `strict` and `compatible` reject `NaN`, infinities, values outside the BigQuery `BIGNUMERIC` range, and values with more than 38 fractional digits. `preserve` stores the source text. `coerce` rounds to 38 fractional digits using half-away-from-zero, clamps out-of-range values and infinities to the finite `BIGNUMERIC` bounds, and still rejects `NaN`. |
| `date` | `DATE` | `DATE` | `STRING` | `DATE` | `strict` and `compatible` require finite dates in `0001-01-01..=9999-12-31`. `preserve` stores the source text. `coerce` clamps infinities and out-of-range dates to BigQuery's date bounds. |
| `time` | `TIME` | `TIME` | `STRING` | `TIME` | `strict` and `compatible` reject PostgreSQL `24:00:00`. `preserve` stores the source text. `coerce` maps `24:00:00` to `23:59:59.999999`. |
| `timestamp` | `DATETIME` | `DATETIME` | `STRING` | `DATETIME` | `strict` and `compatible` require finite timestamps in BigQuery's supported range. `preserve` stores the source text. `coerce` clamps infinities and out-of-range values to BigQuery's timestamp bounds. |
| `timestamptz` | `TIMESTAMP` | `TIMESTAMP` | `STRING` | `TIMESTAMP` | `strict` and `compatible` require finite UTC instants in BigQuery's supported range. `preserve` stores the source text. `coerce` clamps infinities and out-of-range values to BigQuery's timestamp bounds. |
| `json`, `jsonb` | `JSON` | `JSON` | `STRING` | `JSON` | `strict` and `compatible` reject invalid JSON, duplicate object keys, nesting deeper than 500 levels, integers outside BigQuery's exact signed or unsigned 64-bit JSON integer domain, and non-integer numbers outside finite `FLOAT64`. `preserve` stores the raw JSON text. `coerce` keeps the first value for duplicate object keys and rounds numbers outside BigQuery's exact JSON number domain when they can still fit as finite `FLOAT64`; invalid JSON, nesting deeper than 500 levels, and numbers that cannot be rounded into BigQuery's JSON number domain are still rejected. |
| `bytea` | `BYTES` | `BYTES` | `BYTES` | `BYTES` | Exact in all modes. |
| `uuid` | Rejected | `STRING` | `STRING` | `STRING` | Stored as the UUID text representation outside `strict`. |
| `money` | Rejected | `STRING` | `STRING` | `STRING` | Stored as source text outside `strict`. |
| Other non-native scalar types, such as `interval`, `timetz`, `regclass`, `inet`, `cidr`, `macaddr`, `bit`, geometric types, text search types, `pg_lsn`, and range types | Rejected | `STRING` | `STRING` | `STRING` | Stored as source text outside `strict`. |
| Arrays of native scalar types, such as `boolean[]`, text arrays, integer arrays, float arrays, `numeric[]`, temporal arrays, `json[]`, `jsonb[]`, and `bytea[]` | `ARRAY<...>` | `ARRAY<...>` | `STRING` | `ARRAY<...>` | Native array modes apply the same element type and value rules as the scalar type, and reject `NULL` arrays or `NULL` elements. `preserve` stores PostgreSQL-style array text. |
| Arrays of non-native scalar types, such as `uuid[]`, `money[]`, `interval[]`, `timetz[]`, network arrays, bit arrays, and range arrays | Rejected | `ARRAY<STRING>` | `STRING` | `ARRAY<STRING>` | `compatible` and `coerce` preserve element boundaries, order, and array queryability, but reject `NULL` arrays or `NULL` elements. `preserve` stores PostgreSQL-style array text. |

`compatible` is the BigQuery default because it avoids silent value changes while
still allowing common PostgreSQL-only types, such as `uuid`, to replicate as
`STRING`.
