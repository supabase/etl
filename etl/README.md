# `etl` - Core

> **Note:** Version 2 is currently under development under `/v2`, which includes a complete rework of the pipeline
> architecture for improved performance and scalability.

This is the main crate of the ETL (Extract, Transform, Load) system, providing a unified interface for data replication and transformation.

## Features

| Feature                  | Description                                |
| ------------------------ | ------------------------------------------ |
| `bigquery`               | Enables BigQuery integration               |
| `unknown_types_to_bytes` | Converts unknown PostgreSQL types to bytes |
