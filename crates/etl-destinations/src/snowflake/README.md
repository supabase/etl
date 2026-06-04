# Snowflake Destination

## Running Integration Tests

The easiest way to run the full Snowflake test suite (API tests, validator integration, and
destination integration) is a single xtask command:

```bash
cargo x test-snowflake
```

This requires local Postgres to already be running. Run `cargo x init` first if the local
development stack is not up. The command runs API-level Snowflake tests that do not require
credentials, then runs the credentialed integration tiers when `TESTS_SNOWFLAKE_ACCOUNT`,
`TESTS_SNOWFLAKE_USER`, and `TESTS_SNOWFLAKE_PRIVATE_KEY_PATH` are set. See `.env.example` for
setup instructions. Use `--no-credentials` to run only the non-credentialed tier.

To run a specific destination test directly:

```bash
source .env
cargo test -p etl-destinations --features snowflake,test-utils -- --ignored authenticate_against_snowflake
```

### Environment Variables

| Variable                           | Required | Default   | Description                                |
| ---------------------------------- | -------- | --------- | ------------------------------------------ |
| `TESTS_SNOWFLAKE_ACCOUNT`          | yes      |           | Account identifier, e.g. `myorg-myaccount` |
| `TESTS_SNOWFLAKE_USER`             | yes      |           | Login user name                            |
| `TESTS_SNOWFLAKE_PRIVATE_KEY_PATH` | yes      |           | Path to PEM-encoded private key            |
| `TESTS_SNOWFLAKE_DATABASE`         | no       | `ETL_DEV` | Target database                            |
| `TESTS_SNOWFLAKE_SCHEMA`           | no       | `PUBLIC`  | Target schema                              |
| `TESTS_SNOWFLAKE_ROLE`             | no       |           | Role to assume after connecting            |

### Key-Pair Authentication Setup

Snowflake tests use key-pair authentication (not password). To generate a key:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out rsa_key.p8
```

Then register the public key with your Snowflake user:

```sql
ALTER USER ETL_USER SET RSA_PUBLIC_KEY='<paste public key without header/footer>';
```
