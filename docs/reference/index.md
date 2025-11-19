# Reference

**Complete API documentation for ETL**

API documentation is available through Rust's built-in documentation system. Generate and browse the complete API reference locally:

```bash
cargo doc --workspace --all-features --no-deps --open
```

This opens comprehensive rustdoc documentation covering:
- All public APIs, traits, and structs
- Configuration types and options
- Store and destination trait definitions
- Code examples and method signatures

## Key Traits

The core extension points in ETL:

- **`Destination`** - Implement to send data to custom destinations
- **`StateStore`** - Manage replication state and table mappings
- **`SchemaStore`** - Handle table schema information
- **`CleanupStore`** - Atomic cleanup operations for removed tables

## Configuration Types

Main configuration structures:

- **`PipelineConfig`** - Complete pipeline configuration
- **`PgConnectionConfig`** - Postgres connection settings
- **`BatchConfig`** - Batching and performance tuning
- **`TlsConfig`** - TLS/SSL configuration

## See Also

- [How-to guides](../how-to/index.md) - Task-oriented instructions
- [Tutorials](../tutorials/index.md) - Learning-oriented lessons
- [Explanations](../explanation/index.md) - Understanding-oriented discussions
- [GitHub Repository](https://github.com/supabase/etl) - Source code and issues