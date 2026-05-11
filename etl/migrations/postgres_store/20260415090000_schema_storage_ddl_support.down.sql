create table if not exists etl.table_mappings (
    id bigint generated always as identity primary key,
    pipeline_id bigint not null,
    source_table_id oid not null,
    destination_table_id text not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (pipeline_id, source_table_id)
);

insert into etl.table_mappings (
    pipeline_id,
    source_table_id,
    destination_table_id,
    created_at,
    updated_at
)
select
    pipeline_id,
    table_id,
    destination_table_id,
    created_at,
    updated_at
from etl.destination_tables_metadata
on conflict (pipeline_id, source_table_id) do update
set
    destination_table_id = excluded.destination_table_id,
    updated_at = excluded.updated_at;

with ranked_schemas as (
    select
        ts.id,
        row_number() over (
            partition by ts.pipeline_id, ts.table_id
            order by
                case
                    when exists (
                        select 1
                        from etl.destination_tables_metadata dtm
                        where dtm.pipeline_id = ts.pipeline_id
                          and dtm.table_id = ts.table_id
                          and dtm.snapshot_id = ts.snapshot_id
                    ) then 0
                    else 1
                end,
                ts.snapshot_id desc,
                ts.id desc
        ) as row_number
    from etl.table_schemas ts
)
delete from etl.table_schemas ts
using ranked_schemas rs
where ts.id = rs.id
  and rs.row_number > 1;

drop table etl.destination_tables_metadata;
drop type etl.destination_table_schema_status;

drop index if exists etl.idx_table_schemas_pipeline_table_snapshot_id;

alter table etl.table_schemas
    drop constraint if exists table_schemas_pipeline_id_table_id_snapshot_id_key;

alter table etl.table_schemas
    drop column if exists snapshot_id;

alter table etl.table_schemas
    add constraint table_schemas_pipeline_id_table_id_key
    unique (pipeline_id, table_id);

alter table etl.table_columns
    add column if not exists primary_key boolean;

update etl.table_columns
set primary_key = primary_key_ordinal_position is not null;

alter table etl.table_columns
    alter column primary_key set not null;

alter table etl.table_columns
    drop column primary_key_ordinal_position;

update etl.table_columns
set ordinal_position = ordinal_position + 1000000;

update etl.table_columns
set ordinal_position = ordinal_position - 1000001;

alter table etl.table_columns
    rename column ordinal_position to column_order;
