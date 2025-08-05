alter table app.sources
drop constraint sources_tenant_id_fkey,
add constraint sources_tenant_id_fkey
    foreign key (tenant_id)
    references app.tenants (id)
    on delete cascade;

alter table app.sinks
drop constraint sinks_tenant_id_fkey,
add constraint sinks_tenant_id_fkey
    foreign key (tenant_id)
    references app.tenants (id)
    on delete cascade;

alter table app.replicators
drop constraint replicators_tenant_id_fkey,
add constraint replicators_tenant_id_fkey
    foreign key (tenant_id)
    references app.tenants (id)
    on delete cascade;

alter table app.pipelines
drop constraint pipelines_tenant_id_fkey,
add constraint pipelines_tenant_id_fkey
    foreign key (tenant_id)
    references app.tenants (id)
    on delete cascade;