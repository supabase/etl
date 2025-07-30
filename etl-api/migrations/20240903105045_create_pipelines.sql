create table
    app.pipelines (
        id bigint generated always as identity primary key,
        tenant_id text references app.tenants (id) not null,
        source_id bigint references app.sources (id) not null,
        sink_id bigint references app.sinks (id) not null,
        replicator_id bigint references app.replicators (id) not null,
        publication_name text not null,
        config jsonb not null
    );