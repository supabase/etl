mod support;

#[cfg(all(feature = "bigquery", feature = "test-utils"))]
mod bigquery_pipeline;
#[cfg(all(feature = "clickhouse", feature = "test-utils"))]
mod clickhouse_pipeline;
#[cfg(feature = "ducklake")]
mod ducklake_destination;
#[cfg(feature = "ducklake")]
mod ducklake_pipeline;
#[cfg(all(feature = "iceberg", feature = "test-utils"))]
mod iceberg_client;
#[cfg(all(feature = "iceberg", feature = "test-utils"))]
mod iceberg_destination;
