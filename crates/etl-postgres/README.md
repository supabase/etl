# `etl-postgres`

Postgres primitives shared by [Supabase ETL](https://supabase.github.io/etl/)
workspace crates. This is not the primary end-user pipeline API; most pipeline
users should import from `etl` and destination modules from `etl-destinations`.

The public modules here cover reusable boundaries such as schema identifiers,
Postgres value wrappers, replication slot naming, source database metadata, and
Postgres-backed store records. Runtime internals that are only needed by the
core pipeline stay inside the `etl` crate.
