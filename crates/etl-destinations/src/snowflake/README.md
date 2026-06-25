# Snowflake Destination

## Running Integration Tests

The easiest way to run the full Snowflake test suite (API tests, validator integration, and
destination integration) is a single xtask command:

```bash
cargo x test-snowflake
```

This requires local Postgres to already be running. Run `cargo x init` first if the local development stack is not up. The command first runs the non-credentialed Snowflake destination preset, then runs the credentialed integration tiers when `TESTS_SNOWFLAKE_CONNECTION` is set. Use `--credentials skip` to run only the non-credentialed tier, or `--credentials required` to fail when credentials are missing.

To run a specific destination test directly:

```bash
source .env
cargo test -p etl-destinations --no-default-features --features snowflake,test-utils -- --ignored authenticate_against_snowflake
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

- `TESTS_SNOWFLAKE_CONNECTION` for `.github/workflows/snowflake-ci.yml`.
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
