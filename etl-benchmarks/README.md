# `etl` - Benchmarks

This crate contains performance benchmarks for the ETL system to measure and track replication performance across different scenarios and configurations.

## Available Benchmarks

- **table_copies**: Measures performance of initial table copying operations

## Running Benchmarks

To run all benchmarks:

```bash
cargo bench -p etl-benchmarks
```

To run a specific benchmark:

```bash
cargo bench -p etl-benchmarks --bench table_copies
```

## Prerequisites

Before running benchmarks, ensure you have:
- A PostgreSQL database set up (see [Database Setup Guide](../docs/guides/database-setup.md))
- Appropriate destination configurations (e.g., BigQuery credentials)

## Benchmark Results

Benchmark results help track performance improvements and regressions across different:
- Table sizes and schemas
- Network conditions
- Destination systems
- Configuration parameters