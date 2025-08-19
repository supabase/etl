<br />
<p align="center">
  <a href="https://supabase.com">
    <picture>
      <img alt="ETL by Supabase" width="100%" src="docs/assets/etl-logo-extended.png">
    </picture>
  </a>

  <h1 align="center">ETL</h1>

  <p align="center">
    Build real-time Postgres replication applications in Rust
    <br />
    <a href="https://supabase.github.io/etl"><strong>Documentation</strong></a>
    ·
    <a href="https://github.com/supabase/etl/tree/main/etl-examples"><strong>Examples</strong></a>
    ·
    <a href="https://github.com/supabase/etl/issues"><strong>Issues</strong></a>
  </p>
</p>

ETL is a Rust framework by [Supabase](https://supabase.com) for building high‑performance, real‑time data replication apps on Postgres. It sits on top of Postgres [logical replication](https://www.postgresql.org/docs/current/protocol-logical-replication.html) and gives you a clean, Rust‑native API for streaming changes to your own destinations.

## Highlights

- 🚀 Real‑time replication: stream changes as they happen
- ⚡ High performance: batching and parallel workers
- 🛡️ Fault tolerant: retries and recovery built in
- 🔧 Extensible: implement custom stores and destinations
- 🧭 Typed, ergonomic Rust API

## Get Started

Install via Git while we prepare for a crates.io release:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
```

Quick example using the in‑memory destination:

```rust
use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
    destination::memory::MemoryDestination,
    pipeline::Pipeline,
    store::both::memory::MemoryStore,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pg = PgConnectionConfig {
        host: "localhost".into(),
        port: 5432,
        name: "mydb".into(),
        username: "postgres".into(),
        password: Some("password".into()),
        tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
    };

    let store = MemoryStore::new();
    let destination = MemoryDestination::new();

    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".into(),
        pg_connection: pg,
        batch: BatchConfig { max_size: 1000, max_fill_ms: 5000 },
        table_error_retry_delay_ms: 10_000,
        max_table_sync_workers: 4,
    };

    let mut pipeline = Pipeline::new(config, store, destination);
    pipeline.start().await?;
    // pipeline.wait().await?; // Optional: block until completion

    Ok(())
}
```

For tutorials and deeper guidance, see the [Documentation](https://supabase.github.io/etl) or jump into the [examples](etl-examples/README.md).

## Destinations

ETL is designed to be extensible. You can implement your own destinations to send data to any destination you like, however it comes with a few built in destinations:

- BigQuery

Out-of-the-box destinations are available in the `etl-destinations` crate:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
etl-destinations = { git = "https://github.com/supabase/etl", features = ["bigquery"] }
```

## License

Apache‑2.0. See `LICENSE` for details.

---

<p align="center">
  Made with ❤️ by the <a href="https://supabase.com">Supabase</a> team
</p>
