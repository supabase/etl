Applications can use data sources and destinations from `etl` to build a data pipeline to continually copy data from the source to the destination. For example, a data pipeline to copy data from Postgres to BigQuery takes about 100 lines of Rust.

There are three components in a data pipeline:

1. A data source
2. A data destination
3. A pipline

The data source is an object from where data will be copied. The data destination is an object to which data will be copied. The pipeline is an object which drives the data copy operations from the source to the destination.

```
 +----------+                       +-----------------+
 |          |                       |                 |
 |  Source  |---- Data Pipeline --->|   Destination   |
 |          |                       |                 |
 +----------+                       +-----------------+
```

So roughly you write code like this:

```rust
let postgres_source = PostgresSource::new(...);
let bigquery_destination = BigQueryDestination::new(..);
let pipeline = DataPipeline(postgres_source, bigquery_destination);
pipeline.start();
```

Of course, the real code is more than these four lines, but this is the basic idea. For a complete example look at the [bigquery example](https://github.com/supabase/etl/blob/main/etl/examples/bigquery.rs).

### Data Sources

A data source is the source for data which the pipeline will copy to the data destination. Currently, the repository has only one data source: [`PostgresSource`](https://github.com/supabase/etl/blob/main/etl/src/pipeline/sources/postgres.rs). `PostgresSource` is the primary data source; data in any other source or destination would have originated from it.

### Data Destinations

A data destination is where the data from a data source is copied. There are two kinds of data destinations. Those which retain the essential nature of data coming out of a `PostgresSource` and those which don't. The former kinds of data destinations can act as a data source in future. The latter kind can't act as a data source and are data's final resting place.

For instance, [`BigQueryDestination`](https://github.com/supabase/etl/blob/main/etl/src/pipeline/destinations/bigquery.rs) ensures that the change data capture (CDC) stream coming in from a source is materialized into tables in a BigQuery database. Once this lossy data transformation is done, it can not be used as a CDC stream again.

Contrast this with a potential future destination `S3Destination` or `KafkaDestination` which just copies the CDC stream as is. The data deposited in the destination can later be used as if it was coming from Postgres directly.

### Data Pipeline

A data pipeline encapsulates the business logic to copy the data from the source to the destination. It also orchestrates resumption of the CDC stream from the exact location it was last stopped at. The data destination participates in this by persisting the resumption state and returning it to the pipeline when it restarts.

If a data destination is not transactional (e.g. `S3Destination`), it is not always possible to keep the CDC stream and the resumption state consistent with each other. This can result in these non-transactional destinations having duplicate portions of the CDC stream. Data pipeline helps in deduplicating these duplicate CDC events when the data is being copied over to a transactional store like BigQuery.

Finally, the data pipeline reports back the log sequence number (LSN) upto which the CDC stream has been copied in the destination to the `PostgresSource`. This allows the Postgres database to reclaim disk space by removing WAL segment files which are no longer required by the data destination.

```
 +----------+                       +-----------------+
 |          |                       |                 |
 |  Source  |<---- LSN Numbers -----|   Destination   |
 |          |                       |                 |
 +----------+                       +-----------------+
```

### Kinds of Data Copies

CDC stream is not the only kind of data a data pipeline performs. There's also full table copy, aka backfill. These two kinds can be performed either together or separately. For example, a one-off data copy can use the backfill. But if you want to regularly copy data out of Postgres and into your OLAP database, backfill and CDC stream both should be used. Backfill to get the intial copies of the data and CDC stream to keep those copies up to date and changes in Postgres happen to the copied tables.
