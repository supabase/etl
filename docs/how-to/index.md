---
type: how-to
title: How-To Guides
---

# How-To Guides

**Practical solutions for common ETL tasks**

How-to guides provide step-by-step instructions for accomplishing specific goals when working with ETL. Each guide assumes you're already familiar with ETL basics and focuses on the task at hand.

## Database Configuration

### [Configure PostgreSQL for Replication](configure-postgres/)
Set up PostgreSQL with the correct permissions, settings, and publications for ETL pipelines.

**When to use:** Setting up a new PostgreSQL source for replication.

## Destinations and Output

### [Build Custom Destinations](custom-destinations/)  
Create your own destination implementations for specific data warehouses or storage systems.

**When to use:** ETL doesn't support your target system out of the box.

### [Handle Schema Changes](schema-changes/)
Manage table schema changes without breaking your replication pipeline.

**When to use:** Your source database schema evolves over time.

## Operations and Monitoring

### [Debug Pipeline Issues](debugging/)
Diagnose and resolve common pipeline problems like connection failures, data inconsistencies, and performance bottlenecks.

**When to use:** Your pipeline isn't working as expected.

### [Optimize Performance](performance/)
Tune your ETL pipeline for maximum throughput and minimal resource usage.

**When to use:** Your pipeline is working but needs to handle more data or run faster.

### [Test ETL Pipelines](testing/)
Build comprehensive test suites for your ETL applications using mocks and test utilities.

**When to use:** Ensuring reliability before deploying to production.

## Before You Start

**Prerequisites:**
- Complete the [first pipeline tutorial](../tutorials/first-pipeline/)
- Have a working ETL development environment
- Understanding of your specific use case requirements

## Guide Structure

Each how-to guide follows this pattern:

1. **Goal statement** - What you'll accomplish
2. **Prerequisites** - Required setup and knowledge  
3. **Decision points** - Key choices that affect the approach
4. **Step-by-step procedure** - Actions to take
5. **Validation** - How to verify success
6. **Troubleshooting** - Common issues and solutions

## Next Steps

After solving your immediate problem:
- **Learn more concepts** → [Explanations](../explanation/)
- **Look up technical details** → [Reference](../reference/)
- **Build foundational knowledge** → [Tutorials](../tutorials/)

## Need Help?

If these guides don't cover your specific situation:
1. Check if it's addressed in [Debugging](debugging/)
2. Search existing [GitHub issues](https://github.com/supabase/etl/issues)
3. [Open a new issue](https://github.com/supabase/etl/issues/new) with details about your use case