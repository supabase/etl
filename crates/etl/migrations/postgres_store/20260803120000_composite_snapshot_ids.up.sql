-- Store schema snapshot identifiers as decimal `commit_lsn:message_lsn` text.
--
-- Before this migration, each stored identifier contains only logical-message
-- LSN `M`. The commit LSN that made that schema visible was never persisted, so
-- it cannot be reconstructed from these rows. Migrate each value to `M:M`: the
-- old message LSN becomes both the synthetic commit-order coordinate and the
-- message-order coordinate.
--
-- A WAL checkpoint `P` is represented in the new ordering as
-- `(P, u64::MAX)`. Therefore `M:M <= (P, u64::MAX)` exactly when `M <= P`,
-- preserving every old checkpoint comparison and the total order of migrated
-- rows. Applying the same conversion to `table_schemas` and
-- `destination_tables_metadata` also preserves equality between a stored
-- schema and either endpoint of destination schema-change metadata.
--
-- Future schema-change identifiers use their actual
-- `(commit_lsn, message_lsn)`. Their commit LSNs occur after the historical WAL
-- positions already stored as `M:M`, so old and new identifiers share one
-- monotonic order. `M:M` preserves all ordering information the old format
-- contained; it does not invent the unavailable historical commit LSN.
--
-- PostgreSQL returns `pg_lsn - pg_lsn` as exact `numeric`. Converting that
-- integer directly to text preserves the complete unsigned 64-bit LSN range
-- without using floating-point or signed 64-bit intermediates.

alter table etl.table_schemas
    alter column snapshot_id drop default;

alter table etl.table_schemas
    alter column snapshot_id type pg_catalog.text
    using (
        (snapshot_id - '0/0'::pg_catalog.pg_lsn)::pg_catalog.text || ':' ||
        (snapshot_id - '0/0'::pg_catalog.pg_lsn)::pg_catalog.text
    ),
    alter column snapshot_id set default '0:0';

alter table etl.destination_tables_metadata
    alter column snapshot_id type pg_catalog.text
    using (
        (snapshot_id - '0/0'::pg_catalog.pg_lsn)::pg_catalog.text || ':' ||
        (snapshot_id - '0/0'::pg_catalog.pg_lsn)::pg_catalog.text
    ),
    alter column previous_snapshot_id type pg_catalog.text
    using (
        (previous_snapshot_id - '0/0'::pg_catalog.pg_lsn)::pg_catalog.text || ':' ||
        (previous_snapshot_id - '0/0'::pg_catalog.pg_lsn)::pg_catalog.text
    );
