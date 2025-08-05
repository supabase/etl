alter table app.pipelines
drop constraint pipelines_source_id_fkey,
add constraint pipelines_source_id_fkey
    foreign key (source_id)
    references app.sources (id)
    on delete cascade;

alter table app.pipelines
drop constraint pipelines_sink_id_fkey,
add constraint pipelines_sink_id_fkey
    foreign key (sink_id)
    references app.sinks (id)
    on delete cascade;