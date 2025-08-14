---
type: reference
title: API Reference
---

# Reference

**Technical documentation for ETL configuration and usage**

## API Documentation

Complete API documentation is available through Rust's built-in documentation system. We publish comprehensive rustdoc documentation that covers all public APIs, traits, and configuration structures.

**View the API docs:** [Rust API Documentation](https://supabase.github.io/etl/docs/) *(coming soon)*

The rustdoc includes:

- All public APIs with detailed descriptions
- Code examples for major components
- Trait implementations and bounds
- Configuration structures and their fields
- Error types and their variants

## Feature Flags

ETL supports the following Cargo features:

| Feature | Description | Default |
|---------|-------------|---------|
| `unknown-types-to-bytes` | Convert unknown PostgreSQL types to byte arrays | ✓ |
| `test-utils` | Include testing utilities and helpers | - |
| `failpoints` | Enable failure injection for testing | - |

## Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `ETL_LOG_LEVEL` | Logging verbosity (error, warn, info, debug, trace) | `info` |
| `ETL_METRICS_ENABLED` | Enable metrics collection | `false` |

## Error Codes

### Pipeline Errors

| Code | Description | Action |
|------|-------------|---------|
| `P001` | Connection to PostgreSQL failed | Check connection configuration |
| `P002` | Publication not found | Verify publication exists |
| `P003` | Replication slot creation failed | Check PostgreSQL permissions |

### Destination Errors

| Code | Description | Action |
|------|-------------|---------|
| `D001` | Batch write failed | Check destination system health |
| `D002` | Authentication failed | Verify credentials |
| `D003` | Data serialization error | Check data format compatibility |

## Compatibility

### Supported Versions

- **Rust:** 1.75 or later
- **PostgreSQL:** 12, 13, 14, 15, 16
- **Tokio:** 1.0 or later

### Platform Support

- **Linux:** Full support (x86_64, aarch64)
- **macOS:** Full support (Intel, Apple Silicon)  
- **Windows:** Experimental support

## Performance Characteristics

### Memory Usage
- **Base overhead:** ~10MB per pipeline
- **Per-table overhead:** ~1MB 
- **Batch memory:** Configurable via `BatchConfig`

### Throughput
- **Typical range:** 10,000-100,000 operations/second
- **Factors:** Network latency, batch size, destination performance
- **Bottlenecks:** Usually destination write speed

## Navigation

**By component type:**
- [Pipeline APIs](pipeline/) - Core orchestration
- [Destination APIs](destinations/) - Data output interfaces  
- [Store APIs](stores/) - State management
- [Configuration](config/) - All configuration structures

**By use case:**
- [Testing](testing/) - Test utilities and mocks
- [Monitoring](monitoring/) - Metrics and observability
- [Extensions](extensions/) - Building custom components

## See Also

- [How-to guides](../how-to/) - Task-oriented instructions
- [Tutorials](../tutorials/) - Learning-oriented lessons  
- [Explanations](../explanation/) - Understanding-oriented discussions