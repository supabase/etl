alter table app.tenants
    add constraint tenants_id_kubernetes_safe_check
    check (
        char_length(id) between 1 and 25
        and id ~ '^[a-z0-9]([a-z0-9-]{0,23}[a-z0-9])?$'
    )
    not valid;
