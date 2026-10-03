# Snowflake

Snowflake destination for [Supabase ETL](https://supabase.github.io/etl/).
Status: In progress. See the
[Destinations reference](https://supabase.github.io/etl/reference/destinations/)
and the [`etl-examples` Snowflake section](../../../etl-examples/README.md#snowflake)
for the runnable example.

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
