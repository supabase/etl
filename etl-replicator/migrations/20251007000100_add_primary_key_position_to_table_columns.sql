-- Adds primary key and ordinal position metadata to stored table column schemas.
alter table etl.table_columns
    add column if not exists primary_key_position integer;

alter table etl.table_columns
    rename column column_order to ordinal_position;

with primary_positions as (
    select
        table_schema_id,
        column_name,
        row_number() over (
            partition by table_schema_id
            order by ordinal_position
        ) as position
    from etl.table_columns
    where primary_key
)
update etl.table_columns tc
set primary_key_position = primary_positions.position
from primary_positions
where tc.table_schema_id = primary_positions.table_schema_id
  and tc.column_name = primary_positions.column_name;

with ordered_columns as (
    select
        id,
        row_number() over (
            partition by table_schema_id
            order by ordinal_position
        ) as normalized_position
    from etl.table_columns
)
update etl.table_columns tc
set ordinal_position = ordered_columns.normalized_position
from ordered_columns
where tc.id = ordered_columns.id;
