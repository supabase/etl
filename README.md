<br />
<p align="center">
  <a href="https://supabase.io">
        <picture>
      <img alt="Supabase Logo" width="100%" src="res/etl-logo-extended.png">
    </picture>
  </a>

  <h1 align="center">ETL</h1>

  <p align="center">
    Build real-time Postgres replication applications in Rust
    <br />
    <a href="https://supabase.github.io/etl"><strong>📖 Documentation</strong></a>
    ·
    <a href="https://github.com/supabase/etl/tree/main/etl-examples"><strong>💡 Examples</strong></a>
    ·
    <a href="https://github.com/supabase/etl/issues"><strong>🐛 Issues</strong></a>
  </p>
</p>

**ETL** is a Rust framework by [Supabase](https://supabase.com) that enables you to build high-performance, real-time data replication applications for PostgreSQL. Stream changes as they happen, route to multiple destinations, and build robust data pipelines with minimal complexity.

Built on PostgreSQL's [logical replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html), ETL handles the complexities so you can focus on your data.

## ✨ Key Features

- 🚀 **Real-time streaming** - Changes flow instantly from PostgreSQL
- 🔄 **Multiple destinations** - BigQuery, custom APIs, and more  
- 🛡️ **Built-in resilience** - Automatic retries and recovery
- ⚡ **High performance** - Efficient batching and parallel processing
- 🔧 **Extensible** - Plugin architecture for any destination

## 🚦 Quick Start

```rust
use etl::{
    config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig},
    destination::memory::MemoryDestination,
    pipeline::Pipeline,
    store::both::memory::MemoryStore,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure PostgreSQL connection
    let pg_config = PgConnectionConfig {
        host: "localhost".to_string(),
        port: 5432,
        name: "mydb".to_string(),
        username: "postgres".to_string(),
        password: Some("password".to_string().into()),
        tls: TlsConfig { enabled: false, trusted_root_certs: String::new() },
    };

    // Create memory-based store and destination for testing
    let store = MemoryStore::new();
    let destination = MemoryDestination::new();

    // Configure the pipeline
    let config = PipelineConfig {
        id: 1,
        publication_name: "my_publication".to_string(),
        pg_connection: pg_config,
        batch: BatchConfig { max_size: 1000, max_fill_ms: 5000 },
        table_error_retry_delay_ms: 10000,
        max_table_sync_workers: 4,
    };

    // Create and start the pipeline
    let mut pipeline = Pipeline::new(1, config, store, destination);
    pipeline.start().await?;

    // Pipeline will run until stopped
    pipeline.wait().await?;

    Ok(())
}
```

**Want to try it?** → [**Build your first pipeline in 15 minutes**](https://supabase.github.io/etl/tutorials/first-pipeline/) 📚

## 📚 Learn More

Our comprehensive documentation covers everything you need:

- **🎓 [Tutorials](https://supabase.github.io/etl/tutorials/)** - Step-by-step learning experiences
- **🔧 [How-To Guides](https://supabase.github.io/etl/how-to/)** - Practical solutions for common tasks  
- **📖 [Reference](https://supabase.github.io/etl/reference/)** - Complete API documentation
- **💡 [Explanations](https://supabase.github.io/etl/explanation/)** - Architecture and design decisions

## 📦 Installation  

Add to your `Cargo.toml`:

```toml
[dependencies]
etl = { git = "https://github.com/supabase/etl" }
```

> **Note**: ETL will be available on crates.io soon!

## 🏗️ Development

```bash
# Run tests
cargo test --all-features

# Build Docker images
docker build -f ./etl-replicator/Dockerfile .
docker build -f ./etl-api/Dockerfile .
```

## 📄 License

Apache-2.0 License - see [`LICENSE`](LICENSE) for details.

---

<p align="center">
  Made with ❤️ by the <a href="https://supabase.com">Supabase</a> team
</p></p>
