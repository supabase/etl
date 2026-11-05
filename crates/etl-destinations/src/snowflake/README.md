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

Each record includes `_cdc_operation`, `_cdc_sequence_number`, and `_cdc_missing_columns ARRAY`. An omitted value is stored as a NULL placeholder with its PostgreSQL attribute number (`pg_attribute.attnum`) in the array. These IDs survive column renames and are not reused after a drop within the same source table. They are not row indexes or Snowflake column positions. An explicitly supplied SQL NULL has no entry and replaces the previous value. Complete records have an empty array. Deletes end a row's lifetime; their values are not carried forward into a later insert.

### Querying current state

ETL creates an ordinary `<physical_table>_CURRENT` view for tables meeting the
materialization requirements below. Query it directly in Snowflake:

```sql
select * from PUBLIC_ORDERS_CURRENT;
```

The view returns source columns plus `_cdc_anchored`, `_cdc_unresolved_columns`,
and `_cdc_last_sequence`. It executes the materialization query when read;
it does not store results or schedule background refreshes. The raw CDC table
remains available. Queries require a Snowflake warehouse.

ETL publishes each source column's `column_id` and `primary_key_position` as JSON
in the view column's comment. Read them through `INFORMATION_SCHEMA.COLUMNS` or
`DESCRIBE VIEW`; PostgreSQL access and ETL binaries are unnecessary. The view
SQL is also available through `GET_DDL`. Snowflake column positions are not
PostgreSQL attribute numbers. For Rust callers,
`etl_destinations::snowflake::materialization_query` remains a SQL-generation
helper; the function itself does not execute SQL or manage objects.

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
component replicated. Materialization also requires stable key values and
representation, uninterrupted publication coverage, and all accepted history
from a coherent initial snapshot. History pruning is unsupported. Incompatible
key, schema, or publication changes and lost history require a full resync.
Column renames and drops preserve partial-update semantics. ETL updates the
companion view using the schema snapshot applied to Snowflake; customer-owned
queries must be regenerated after DDL.
`_cdc_unresolved_columns` reports current column names, while stored omission
metadata contains IDs. A dropped/re-added
name identifies a new column. Added columns follow the same default-value support
as complete rows: unsupported defaults and type changes can require a resync.
Stable IDs do not extend that support. Supported DDL emits ordered schema
messages, so ETL updates the destination even when no row changes follow.
See [Partial updates and materialization](https://supabase.github.io/etl/reference/destinations/#partial-updates-and-materialization)
for supported key types, query diagnostics, and resync requirements.

### View permissions and lifecycle

The ETL role needs `CREATE VIEW` on the destination schema and must retain
ownership of its raw tables and companion views. Missing privileges fail setup
or view reconciliation. ETL does not grant privileges itself. Grant consumers
`SELECT` on the companion view and the usual database/schema/warehouse usage.

ETL reserves `<physical_table>_CURRENT` and `<physical_table>_CURRENT_PENDING`.
For physical names longer than 239 characters, it uses
`_ETL_<uppercase SHA-256 of the physical name>_CURRENT` and its `_PENDING` name.
It checks object type and its ownership comment before changing either name.
For tables requiring a view, foreign objects are errors; raw-only tables leave
them untouched. Two owned views at the public and pending names are ambiguous
and cause an error. Do not modify
these objects or their comments. Create a separate customer-owned view for
custom projections or access policies. ETL preserves explicit grants using
`COPY GRANTS`; it does not preserve user-added policies, tags, or change tracking.
Streams on an ETL view are unsupported because replacement makes them stale.

Before drops or renames, ETL temporarily renames the view to the reserved
`_CURRENT_PENDING` name. The public name is unavailable until the raw schema is
applied and the definition is replaced. Add-only changes replace the definition
without this outage. Startup validates applied raw tables and preserves existing
public views. Creating or restoring a view requires authoritative relation or
copy metadata, because stored destination schemas do not retain replica identity.
A parked view on an idle table stays unavailable until that metadata arrives;
replayed schema messages can restore it without a row change. Tables awaiting a
fresh copy and errored tables are skipped. Interrupted raw DDL still requires the
existing recovery/resync procedure.

Same-name resync retains view grants while replacing CDC history. Resync after
a source rename removes the old view and creates one beside the new raw table;
consumer grants and references must be set up for the new name. The view can
expose a partial snapshot while initial copying is in progress. Query only the
public name; the pending name is an internal recovery object.

Tables without a supported primary-key identity, or with source names beginning
with `_cdc_`, retain complete-row ingestion without a companion view and emit a
warning. A table that loses eligibility after having a view fails reconciliation
and leaves that view unavailable. Recreating a pipeline with fresh ETL state
requires an unused destination namespace or deliberate removal of the old raw
tables and both reserved view names.

### Existing destination tables

For ETL-owned tables using the supported previous CDC format, ETL adds the
nullable `_cdc_missing_columns` column before opening the write channel.
Existing insert/update records have NULL metadata, interpreted as complete
records within their replicated schema at event time. The upgrade preserves
stored history and replication offsets and requires no backfill.

The configured Snowflake role must allow the ALTER. Unrelated schema mismatches
remain errors. ETL updates the table format; customer-owned queries must support
partial records before consuming them. Existing deployments also need the
`CREATE VIEW` grant before upgrading; ETL creates views for applied tables when
authoritative relation metadata arrives.

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
GRANT CREATE VIEW ON SCHEMA ETL_DEV.PUBLIC TO ROLE etl_test_role;
GRANT CREATE STAGE ON SCHEMA ETL_DEV.PUBLIC TO ROLE etl_test_role;
GRANT CREATE PIPE ON SCHEMA ETL_DEV.PUBLIC TO ROLE etl_test_role;

-- Benchmark target used by BENCH_SNOWFLAKE_CONNECTION. Each benchmark run
-- creates and drops its own `<schema>_<pipeline_id>` schema in ETL_BENCH.
GRANT USAGE ON DATABASE ETL_BENCH TO ROLE etl_test_role;
GRANT CREATE SCHEMA ON DATABASE ETL_BENCH TO ROLE etl_test_role;
GRANT USAGE ON SCHEMA ETL_BENCH.PUBLIC TO ROLE etl_test_role;
GRANT CREATE TABLE ON SCHEMA ETL_BENCH.PUBLIC TO ROLE etl_test_role;
GRANT CREATE VIEW ON SCHEMA ETL_BENCH.PUBLIC TO ROLE etl_test_role;
GRANT CREATE STAGE ON SCHEMA ETL_BENCH.PUBLIC TO ROLE etl_test_role;
GRANT CREATE PIPE ON SCHEMA ETL_BENCH.PUBLIC TO ROLE etl_test_role;

GRANT ROLE etl_test_role TO USER ETL_USER;
```
