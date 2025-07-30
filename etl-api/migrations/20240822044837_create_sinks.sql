create table
    app.sinks (
        id bigint generated always as identity primary key,
        tenant_id text references app.tenants (id) not null,
        name text not null,
        config jsonb not null
    );