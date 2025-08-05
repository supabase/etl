# `etl` - Destinations

This crate provides destination implementations for the ETL system, allowing data to be replicated from PostgreSQL to various target systems.

## Supported Destinations

- **BigQuery**: Google Cloud BigQuery data warehouse

## Features

| Feature    | Description                         |
|------------|-------------------------------------|
| `bigquery` | Enables BigQuery destination support |

## Usage

Each destination is enabled through feature flags. Include the desired destinations in your `Cargo.toml`:

```toml
[dependencies]
etl-destinations = { git = "https://github.com/supabase/etl", features = ["bigquery"] }
```

## BigQuery Destination

The BigQuery destination provides:
- Automatic schema creation and evolution
- Efficient batch loading of data
- Support for all PostgreSQL data types
- Row-level encryption capabilities
- Error handling and retry logic