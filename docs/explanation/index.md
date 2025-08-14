---
type: explanation
title: Understanding ETL
---

# Explanations

**Deep dives into ETL concepts, architecture, and design decisions**

Explanations help you build mental models of how ETL works and why it's designed the way it is. These topics provide background knowledge, compare alternatives, and explore the reasoning behind key architectural choices.

## Core Concepts

### [ETL Architecture Overview](architecture/)
**The big picture of how ETL components work together**

Understand the relationship between pipelines, destinations, stores, and the PostgreSQL replication protocol. Learn how data flows through the system and where extension points exist.

*Topics covered:* Component architecture, data flow, extension patterns, scalability considerations.

### [Why Postgres Logical Replication?](replication/)
**The foundation technology and its trade-offs**

Explore how PostgreSQL's logical replication works, why ETL builds on this foundation, and how it compares to other change data capture approaches.

*Topics covered:* WAL-based replication, publications and subscriptions, alternatives like triggers or polling, performance characteristics.

### [Design Decisions and Trade-offs](design/)
**Key choices that shape ETL's behavior**

Learn about the major design decisions in ETL, the problems they solve, and the trade-offs they represent. Understanding these choices helps you use ETL effectively.

*Topics covered:* Rust as implementation language, async architecture, batching strategy, error handling philosophy.

## System Characteristics

### [Performance and Scalability](performance/)
**How ETL behaves under different loads and configurations**

Understand ETL's performance characteristics, bottlenecks, and scaling patterns. Learn how different configuration choices affect throughput and resource usage.

*Topics covered:* Throughput patterns, memory usage, network considerations, scaling strategies.

### [Crate Structure and Organization](crate-structure/)
**How ETL's modular design supports different use cases**

Explore how ETL is organized into multiple crates, what each crate provides, and how they work together. Understand the reasoning behind this modular architecture.

*Topics covered:* Core vs. optional crates, dependency management, feature flags, extensibility.

## Integration Patterns

### [Working with Destinations](destinations-explained/)
**Understanding the destination abstraction and ecosystem**

Learn how destinations work conceptually, why they're designed as they are, and how to choose between different destination options.

*Topics covered:* Destination trait design, batching strategy, error handling patterns, building ecosystems.

### [State Management Philosophy](state-management/)  
**How ETL tracks replication state and schema changes**

Understand ETL's approach to managing replication state, handling schema evolution, and ensuring consistency across restarts.

*Topics covered:* State storage options, schema change handling, consistency guarantees, recovery behavior.

## Broader Context

### [ETL vs. Other Replication Tools](comparisons/)
**How ETL fits in the data replication landscape**

Compare ETL to other PostgreSQL replication tools, general-purpose ETL systems, and cloud-managed solutions. Understand when to choose each approach.

*Topics covered:* Tool comparisons, use case fit, ecosystem integration, operational trade-offs.

### [Future Directions](roadmap/)
**Where ETL is heading and how to influence its evolution**

Learn about planned features, architectural improvements, and community priorities. Understand how to contribute to ETL's development.

*Topics covered:* Planned features, architectural evolution, community involvement, contribution guidelines.

## Reading Guide

**New to data replication?** Start with [Postgres Logical Replication](replication/) to understand the foundation technology.

**Coming from other tools?** Jump to [ETL vs. Other Tools](comparisons/) to see how ETL fits in the landscape.

**Planning a production deployment?** Read [Architecture](architecture/) and [Performance](performance/) to understand system behavior.

**Building extensions?** Focus on [Crate Structure](crate-structure/) and [Destinations](destinations-explained/) for extension patterns.

## Next Steps

After building conceptual understanding:
- **Start building** → [Tutorials](../tutorials/)
- **Solve specific problems** → [How-To Guides](../how-to/)
- **Look up technical details** → [Reference](../reference/)

## Contributing to Explanations

Found gaps in these explanations? See something that could be clearer? 
[Open an issue](https://github.com/supabase/etl/issues) or contribute improvements to help other users build better mental models of ETL.