# CI metrics

`supabase-ca.crt` is Supabase's public root CA (`Supabase Root 2021 CA`),
downloaded from the Supabase dashboard (Project Settings → Database → SSL
Configuration). It is the same file for every Supabase project and contains
no credentials.

The `ci-metrics` workflow passes it to `cargo x ci-metrics sync` through
`CI_METRICS_SSL_ROOT_CERT`, so the collector verifies the database server's
certificate and host name (`sslmode=verify-full`). The collector refuses
non-local connections that do not verify the server.

- SHA-256 fingerprint:
  `80:70:25:AD:50:D4:ED:21:9D:2C:9C:7D:29:9C:00:4F:82:4E:B0:0C:F7:F6:5A:FE:F6:07:D0:7B:72:E6:CA:FA`
- Expires: 2031-04-26. Replace it from the dashboard before then.
