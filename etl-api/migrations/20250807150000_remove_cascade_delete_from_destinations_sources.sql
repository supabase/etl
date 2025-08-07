-- Remove CASCADE DELETE for destinations and sources to prevent automatic pipeline deletion since
-- we want full control over deleting resources (except when a tenant id deleted)

alter table app.pipelines
drop constraint pipelines_sink_id_fkey,
add constraint pipelines_sink_id_fkey
    foreign key (destination_id)
    references app.destinations (id);

alter table app.pipelines
drop constraint pipelines_source_id_fkey,
add constraint pipelines_source_id_fkey
    foreign key (source_id)
    references app.sources (id);