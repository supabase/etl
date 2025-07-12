---
hide:
  - navigation
---

# ETL

Welcome to the ETL project, a Rust-based collection of tooling designed to build efficient and reliable Postgres replication applications. This documentation page provides an overview of the ETL project, the benefits of using ETL, the advantages of implementing it in Rust, and an introduction to Postgres logical replication. It also outlines the resources available in this documentation to help you get started.

## What is ETL

ETL is a collection of Rust crates which can be used to build replication data pipelines on top of [Postgres's logical replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html). It provides a high-level API to work with Postgres logical replication, allowing developers to focus on building their applications without worrying about the low-level details of the replication protocol. The ETL crate abstracts away the complexities of managing replication slots, publications, and subscriptions, enabling you to create robust data pipelines that can continually copy data from Postgres to various destinations like BigQuery and other OLAP databases.

## What is Postgres Logical Replication?

Postgres logical replication is a method for replicating data between PostgreSQL databases at the logical (table or row) level, rather than the physical (block-level) level. It allows selective replication of specific tables or data subsets, making it ideal for scenarios like data warehousing, real-time analytics, or cross-database synchronization.

Logical replication uses a publish/subscribe model, where a source database (publisher) sends changes to a replication slot, and a destination system (subscriber) applies those changes to its own tables. This approach supports selective data replication and is compatible with different PostgreSQL versions or even external systems.

### How Does Postgres Logical Replication Work?

Postgres logical replication operates through the following steps:

**Publication Creation**: A publication is created in the source database, specifying which tables or data to replicate. For example:

```sql
create publication my_publication for table orders, customers;
```

**Replication Slot**: A logical replication slot is created on the source database to track changes (inserts, updates, deletes) for the published tables. The slot ensures that changes are preserved until they are consumed by a subscriber.

**Subscription Setup**: The destination system (subscriber) creates a subscription that connects to the publication, specifying the source database and replication slot. For example:

```sql
create subscription my_subscription
connection 'host=localhost port=5432 dbname=postgres user=postgres password=password'
publication my_publication;
```

**Change Data Capture (CDC)**: The source database streams changes (via the Write-Ahead Log, or WAL) to the replication slot. The subscriber receives these changes and applies them to its tables, maintaining data consistency.

This process enables real-time data synchronization with minimal overhead, making it suitable for ETL workflows where data needs to be transformed and loaded into destinations like data warehouses or analytical databases.

## Why Use ETL

ETL provides a set of building blocks to construct data pipelines which can continually copy data from Postgres to other systems. It abstracts away the low-level details of the logical replication protocol and provides a high-level API to work with. This allows developers to focus on building their applications without worrying about the intricacies of the replication protocol.

### Why is ETL Written in Rust?

The ETL crate is written in Rust to leverage the language's unique strengths, making it an ideal choice for building robust data pipelines:

- **Performance**: Rust's zero-cost abstractions and low-level control enable high-performance data processing, critical for handling large-scale ETL workloads.
- **Safety**: Rust's strong type system and memory safety guarantees minimize bugs and ensure reliable data handling, reducing the risk of data corruption or crashes.
- **Concurrency**: Rust’s ownership model and async capabilities allow efficient parallel processing, ideal for managing complex, high-throughput ETL pipelines.
- **Ecosystem Integration**: Rust’s growing ecosystem and compatibility with modern cloud and database technologies make it a natural fit for Postgres-focused infrastructure.

By using Rust, the ETL crate provides a fast, safe, and scalable solution for building Postgres replication applications.

## What Does the Documentation Cover?

This documentation is designed to help you effectively use the ETL crate to build Postgres replication applications. It includes the following resources:

- [**Tutorials**](tutorials/index.md): Step-by-step guides to get started with the ETL crate, including setting up a basic data pipeline, configuring Postgres logical replication, and connecting to destinations like BigQuery or other OLAP databases. Check the [examples folder](https://github.com/supabase/etl/tree/main/etl/examples) for practical code samples.
- [**Guides**](guides/index.md): In-depth explanations of key concepts, such as building custom data pipelines, handling change data capture (CDC), and optimizing performance for specific use cases.
- [**Reference**](reference/index.md): Detailed documentation of the crate’s API, including modules like `etl::pipeline`, `etl::sources::postgres`, and available destinations (e.g., `BigQueryDestination`). Each destination is feature-gated, so you can enable only what you need.
- [**Design**](design/index.md): Overview of the crate’s architecture, including its modular pipeline structure, source-destination flow, and extensibility for custom integrations.

The ETL crate is distributed under the [Apache-2.0 License](https://www.apache.org/licenses/LICENSE-2.0). See the [LICENSE](https://github.com/supabase/etl/blob/main/LICENSE) file for more information.

Ready to start building your Postgres replication pipeline? Dive into the [Getting started guide](tutorials/getting-started.md) to set up your first pipeline.
