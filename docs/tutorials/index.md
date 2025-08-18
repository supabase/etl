
# Tutorials

**Learn ETL through guided, hands-on experiences**

Tutorials provide step-by-step learning paths that take you from zero knowledge to working applications. Each tutorial is designed to be completed successfully by following the exact steps provided.

## Getting Started

### [Build Your First ETL Pipeline](first-pipeline.md)
**15 minutes** • **Beginner** 

Create a complete ETL pipeline that replicates data from PostgreSQL to a memory destination. You'll learn the core concepts of publications, replication slots, and pipeline configuration.

*What you'll build:* A working pipeline that streams changes from a sample PostgreSQL table to an in-memory destination.


## Advanced Topics

### [Build Custom Stores and Destinations](custom-implementations.md)
**45 minutes** • **Advanced**

Implement production-ready custom stores and destinations. Learn ETL's design patterns, build persistent SQLite storage, and create HTTP-based destinations with retry logic.

*What you'll build:* Custom in-memory store for state/schema storage and HTTP destination.

## Before You Start

**Prerequisites for all tutorials:**

- Rust installed (1.75 or later)
- PostgreSQL server (local or remote)
- Basic familiarity with Rust and SQL

**What you'll need:**

- A terminal/command line
- Your favorite text editor
- About 30-60 minutes total time

## Next Steps

After completing the tutorials:

- **Solve specific problems** → [How-To Guides](../how-to/index.md)
- **Understand the architecture** → [ETL Architecture](../explanation/architecture.md)

## Need Help?

If you get stuck:

1. Double-check the prerequisites
2. Ensure your PostgreSQL setup matches the requirements
3. Check the [PostgreSQL configuration guide](../how-to/configure-postgres.md)
4. [Open an issue](https://github.com/supabase/etl/issues) with your specific problem