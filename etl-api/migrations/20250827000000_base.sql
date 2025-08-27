-- Base schema for etl-api

-- Create application schema
create schema if not exists app;

-- Tenants
create table app.tenants (
    id text primary key,
    name text not null
);

-- Images
create table app.images (
    id bigint generated always as identity primary key,
    name text not null,
    is_default boolean not null
);

-- Ensure at most one default image exists
create unique index images_one_default_idx
    on app.images (is_default)
    where is_default = true;

-- Destinations
create table app.destinations (
    id bigint generated always as identity primary key,
    tenant_id text not null references app.tenants (id) on delete cascade,
    name text not null,
    config jsonb not null
);

-- Sources
create table app.sources (
    id bigint generated always as identity primary key,
    tenant_id text not null references app.tenants (id) on delete cascade,
    name text not null,
    config jsonb not null
);

-- Replicators
create table app.replicators (
    id bigint generated always as identity primary key,
    tenant_id text not null references app.tenants (id) on delete cascade,
    image_id bigint not null references app.images (id)
);

-- Pipelines
create table app.pipelines (
    id bigint generated always as identity primary key,
    tenant_id text not null references app.tenants (id) on delete cascade,
    source_id bigint not null references app.sources (id),
    destination_id bigint not null references app.destinations (id),
    replicator_id bigint not null references app.replicators (id),
    config jsonb not null,
    unique (tenant_id, source_id, destination_id)
);
