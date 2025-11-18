# Apply Postgres State Store Migrations

**Prepare the Postgres-backed state store before running pipelines**

`PostgresStore` (and the matching schema store) keep replication metadata inside your own Postgres database. The tables live in the `etl` schema and must be created before a pipeline starts, otherwise you will see errors such as `relation "etl.table_mappings" does not exist`.

Follow these steps whenever you configure a Postgres-backed store.

## 1. Pick the database and user

- Choose the Postgres database that should store ETL metadata (often separate from the source database).
- Ensure the user credentials configured in `PgConnectionConfig` have privileges to create schemas, tables, and indexes in that database.

## 2. Apply the migrations

All SQL migrations for the Postgres store reside in `etl-replicator/migrations/`. Apply them in order (they are timestamp-prefixed) using your preferred tooling. With `psql`:

```bash
cd /path/to/etl
psql "postgres://user:password@host:port/database" -f etl-replicator/migrations/20250827000000_base.sql
```

If additional migration files appear in that directory, run them sequentially (for example with `ls etl-replicator/migrations/*.sql | sort | xargs -I{} psql <conn> -f {}`) before restarting your pipeline.

## 3. Verify the schema

After applying the migrations:

- Confirm the `etl` schema exists.
- Check that tables like `replication_state`, `table_mappings`, and `schema_definitions` are present.

You can now safely configure `PostgresStore`/`PostgresSchemaStore` in your pipeline. Future migrations can be applied on top.
