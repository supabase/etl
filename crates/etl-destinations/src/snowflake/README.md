# Snowflake

Snowflake destination for [Supabase ETL](https://supabase.github.io/etl/).
Managed-service status: Public alpha. Local and self-hosted use of the
open-source destination is unrestricted. See the
[Destinations reference](https://supabase.github.io/etl/reference/destinations/)
and the [`etl-examples` Snowflake section](../../../etl-examples/README.md#snowflake)
for the runnable example.

## Partial updates and materialization

ETL stores inserts, updates, and deletes as CDC records. Complete rows and
partial updates use the same destination and table format. With a supported
primary key and `REPLICA IDENTITY DEFAULT`, Postgres can omit unchanged large
TOAST values from updates. `REPLICA IDENTITY FULL` supplies complete update rows.

Each record includes `_cdc_operation`, `_cdc_sequence_number`, and
`_cdc_missing_columns ARRAY`. An omitted value is stored as a NULL placeholder
with its column name in the array. An explicitly supplied SQL NULL has no entry
and replaces the previous value. Complete records have an empty array. Deletes
end a row's lifetime; their values are not carried forward into a later insert.

### Querying current state

`etl_destinations::snowflake::materialization_query` generates a `SELECT` query
as a Rust `String`. Pass unquoted database, namespace, and physical table names
and the matching ETL schema. The caller executes the SQL in Snowflake; ETL does
not create, refresh, or manage downstream views or tables.

The query handles complete, partial, and mixed histories. It scans retained
history for insert/delete boundaries, then resolves values from each key's
latest complete record and later partial updates. It preserves explicit NULLs
and flags unanchored rows or unresolved columns. These diagnostics cannot detect
arbitrary publication gaps or removed events.

For guaranteed complete insert/update histories, a latest-event-per-key query
avoids per-column value resolution. DEFAULT can reduce repeated large payloads;
FULL supplies complete rows suitable for latest-event selection. Changing to
FULL does not fill in earlier partial records. Evaluate ingestion and query
costs together for the workload.

Partial updates require a supported nondeferrable primary key with every
component replicated. Materialization also requires stable keys, a fixed
replicated schema, uninterrupted publication coverage, and all accepted history
from a coherent initial snapshot. History pruning is unsupported. Incompatible
key, schema, or publication changes and lost history require a full resync.
See [Partial updates and materialization](https://supabase.github.io/etl/reference/destinations/#partial-updates-and-materialization)
for supported key types, query diagnostics, and resync requirements.

### Existing destination tables

For ETL-owned tables using the supported previous CDC format, ETL adds the
nullable `_cdc_missing_columns` column before opening the write channel.
Existing insert/update records have NULL metadata, interpreted as complete
records within their replicated schema at event time. The upgrade preserves
stored history and replication offsets and requires no backfill.

The configured Snowflake role must allow the ALTER. Unrelated schema mismatches
remain errors. ETL updates the table format; customer-owned queries must support
partial records before consuming them.

## Request Packing and Large Rows

Snowpipe Streaming rejects any request body over 4 MiB (4,194,304 bytes,
measured; the docs say "4 MB") with HTTP 413. The encoder in
`streaming/batch/` fills requests up to that limit and accepts large rows
whose level-3 frame, using the bounded window described below, fits it.

- A request body is a sequence of zstd frames. Rows up to 256 KiB serialized
  share one stream frame; each larger row is compressed into its own frame
  first, so its size is exact before it is admitted. Large rows do not share
  compression history with adjacent rows, which can increase request sizes for
  near-duplicate values; the 32 MiB window applies within each row.
- Every row follows one rule, in arrival order: it joins the open request when
  the body provably stays under the limit, otherwise the open request completes
  and the row starts the next one. Small rows are admitted under a derived
  headroom (`BatchLimits::stream_headroom`). For an all-small-row workload,
  completed requests are at least 90% full except for the final partial request;
  large rows pack by their exact compressed size.
- Each row frame uses and declares a fixed 32 MiB zstd window instead of level
  3's default 2 MiB. This makes longer-distance matches reachable without
  measuring the row first; zstd need not find every match. The window bounds
  compression history, not row size: rows above 32 MiB can still be accepted
  when their complete level-3 frame fits the 4 MiB request cap. zstd allocates
  about 33.5 MiB of compressor workspace per active row frame at this setting
  (measured with zstd 1.5.7), in addition to the 4 MiB output cap, the 64 KiB
  write buffer, and the source row itself. Resident memory depends on the input
  and the allocator; concurrent large-row compressors multiply this cost.
- A large row is compressed once at level 3 into a buffer capped at 4 MiB.
  A frame that exceeds the limit is rejected with `Error::RowTooLarge`,
  carrying the table, operation, column count and sizes, never values.
  It maps to `UnsupportedValueInDestination`.
- Completed requests are sent as soon as they complete; nothing is retained
  across rows beyond one open request body and a 256 KiB scratch buffer.

The counter `etl_snowflake_row_frames_total{outcome}` reports rows that took
the frame path as `fit` or `rejected`.

## Running Integration Tests

Run the Snowflake destination test suite with:

```bash
cargo x test-snowflake
```

This requires local Postgres to already be running. Run `cargo x init` first if the local development stack is not up. The command first runs the non-credentialed Snowflake destination preset, then runs the credentialed integration tiers when `TESTS_SNOWFLAKE_CONNECTION` is set. Use `--credentials skip` to run only the non-credentialed tier, or `--credentials required` to fail when credentials are missing.

The `CI` workflow runs credential-free Snowflake tests in the shared test suite
and checks the Snowflake-only feature configuration in `Features (Snowflake
only)`. Both contribute to the `CI passed` check. Routine CI does not
receive Snowflake credentials.

The separate `Snowflake tests` workflow runs credentialed destination integration
tests daily at 04:17 UTC on the default branch, or manually from the Actions tab.
It requires the `TESTS_SNOWFLAKE_CONNECTION` repository secret. GitHub may delay
scheduled runs. Failures appear in the Actions tab under `Snowflake tests`.

To run a specific destination test directly:

```bash
source .env
cargo test -p etl-destinations --no-default-features --features snowflake,test-utils,tls-rustls-ring -- --ignored authenticate_against_snowflake
```

### Connection String

Snowflake tests, examples, and benchmarks use one JSON connection string. Tests and examples read `TESTS_SNOWFLAKE_CONNECTION`; benchmarks read `BENCH_SNOWFLAKE_CONNECTION`. Put `private_key` last so the non-sensitive target is easy to inspect:

```json
{
  "account": "myorg-myaccount",
  "user": "etl_test_user",
  "database": "ETL_DEV",
  "schema": "PUBLIC",
  "role": "etl_test_role",
  "private_key_passphrase": null,
  "private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
}
```

Required fields are `account`, `user`, `database`, `schema`, and `private_key`. `role` and
`private_key_passphrase` are optional. The Snowflake user or role must have a default warehouse
configured because tests issue SQL queries.

### Local Configuration

Put the JSON in `.env` as a single exported value:

```bash
export TESTS_SNOWFLAKE_CONNECTION='{"account":"myorg-myaccount","user":"etl_test_user","database":"ETL_DEV","schema":"PUBLIC","role":"etl_test_role","private_key_passphrase":null,"private_key":"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"}'
export BENCH_SNOWFLAKE_CONNECTION='{"account":"myorg-myaccount","user":"etl_test_user","database":"ETL_BENCH","schema":"PUBLIC","role":"etl_test_role","private_key_passphrase":null,"private_key":"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"}'
```

The examples read `TESTS_SNOWFLAKE_CONNECTION` from the environment; do not pass the connection JSON
on the command line.

### CI Configuration

GitHub Actions uses the same one-var contract:

- `TESTS_SNOWFLAKE_CONNECTION` for `.github/workflows/snowflake-tests.yml`.
- `BENCH_SNOWFLAKE_CONNECTION` for manual Snowflake benchmark workflow runs.

Both repository secrets use the same JSON shape shown above. The workflows pass the JSON only
through the environment; Snowflake secrets are not expanded into command-line arguments.

### Key-Pair Authentication Setup

Snowflake tests use key-pair authentication (not password). To generate a key:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out rsa_key.p8
```

Then register the public key with your Snowflake user:

```sql
ALTER USER ETL_USER SET RSA_PUBLIC_KEY='<paste public key without header/footer>';
```

The user or selected role needs a default warehouse and privileges on the target database/schema:

```sql
-- Shared role used by Snowflake tests, examples, and benchmarks.
CREATE ROLE IF NOT EXISTS etl_test_role;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE etl_test_role;

-- Test/example target used by TESTS_SNOWFLAKE_CONNECTION.
GRANT USAGE ON DATABASE ETL_DEV TO ROLE etl_test_role;
GRANT USAGE ON SCHEMA ETL_DEV.PUBLIC TO ROLE etl_test_role;
GRANT CREATE TABLE ON SCHEMA ETL_DEV.PUBLIC TO ROLE etl_test_role;
GRANT CREATE STAGE ON SCHEMA ETL_DEV.PUBLIC TO ROLE etl_test_role;
GRANT CREATE PIPE ON SCHEMA ETL_DEV.PUBLIC TO ROLE etl_test_role;

-- Benchmark target used by BENCH_SNOWFLAKE_CONNECTION.
GRANT USAGE ON DATABASE ETL_BENCH TO ROLE etl_test_role;
GRANT USAGE ON SCHEMA ETL_BENCH.PUBLIC TO ROLE etl_test_role;
GRANT CREATE TABLE ON SCHEMA ETL_BENCH.PUBLIC TO ROLE etl_test_role;
GRANT CREATE STAGE ON SCHEMA ETL_BENCH.PUBLIC TO ROLE etl_test_role;
GRANT CREATE PIPE ON SCHEMA ETL_BENCH.PUBLIC TO ROLE etl_test_role;

GRANT ROLE etl_test_role TO USER ETL_USER;
```
