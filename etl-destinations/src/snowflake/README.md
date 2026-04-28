# Snowflake Destination

## Running Integration Tests

Snowflake integration tests are marked `#[ignore]` because they require a real Snowflake account.

To run them:

1. Copy the example env file and fill in your Snowflake credentials (see variables below):

```bash
cp .env.example .env
# edit .env with your values
```

2. Source the file and run tests:

```bash
source .env
cargo test -p etl-destinations --features snowflake,test-utils -- --ignored
```

To run a specific test:

```bash
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
| `TESTS_SNOWFLAKE_WAREHOUSE`        | no       |           | Virtual warehouse for queries              |
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
