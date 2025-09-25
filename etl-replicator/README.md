# `etl` - Replicator

Long-lived process that performs Postgres logical replication using the `etl` crate.

## Error Notifications

Set the `notifications.email` section in the replicator configuration to trigger
an HTTP request to `<env_url>/system/email/send` whenever the process exits with
an error:

```yaml
notifications:
  email:
    base_url: "https://example.supabase.co"
    addresses:
      - "oncall@example.com"
    template_alias: "replicator_failure"
    custom_properties:
    service: "replicator"
    timeout_seconds: 5
```