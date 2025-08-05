create type app.replicator_status as enum ('stopped', 'starting', 'started', 'stopping');

create table
    app.replicators (
        id bigint generated always as identity primary key,
        tenant_id text references app.tenants (id) not null,
        image_id bigint references app.images (id) not null
    );