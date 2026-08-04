-- Restore the former message-LSN-only `pg_lsn` columns.
--
-- A successful up migration leaves every non-NULL value in
-- `commit_lsn:message_lsn` form. The old schema has room only for the message
-- LSN, so downgrade from the second component and discard the commit component.
-- This exactly reverses migrated `M:M` rows, but native composite identifiers
-- written after the upgrade lose their commit coordinate.
--
-- This conversion intentionally has no fallback for scalar `pg_lsn` text: a
-- value without a composite message component fails instead of masking a
-- skipped or incomplete up migration.

alter table etl.table_schemas
    alter column snapshot_id drop default;

alter table etl.table_schemas
    alter column snapshot_id type pg_catalog.pg_lsn
    using (
        '0/0'::pg_catalog.pg_lsn +
        pg_catalog.split_part(snapshot_id, ':', 2)::pg_catalog.numeric
    ),
    alter column snapshot_id set default '0/0'::pg_catalog.pg_lsn;

alter table etl.destination_tables_metadata
    alter column snapshot_id type pg_catalog.pg_lsn
    using (
        '0/0'::pg_catalog.pg_lsn +
        pg_catalog.split_part(snapshot_id, ':', 2)::pg_catalog.numeric
    ),
    alter column previous_snapshot_id type pg_catalog.pg_lsn
    using (
        '0/0'::pg_catalog.pg_lsn +
        pg_catalog.split_part(previous_snapshot_id, ':', 2)::pg_catalog.numeric
    );
