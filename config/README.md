# `etl` - Config

This crate provides configuration management and shared configuration types for the ETL system. It includes:

- Environment-based configuration loading
- Shared configuration structures for pipelines, connections, and destinations
- Secure handling of sensitive configuration values
- Support for YAML configuration files

## Features

| Feature  | Description                                   |
|----------|-----------------------------------------------|
| `utoipa` | Enables OpenAPI documentation for config types |

## Configuration Types

- **Pipeline Configuration**: Settings for replication pipelines
- **Connection Configuration**: Database connection parameters
- **Destination Configuration**: Target system settings
- **Batch Configuration**: Batching and retry policies
- **Security Configuration**: Encryption and authentication settings