use etl::{destination::Destination, store::both::postgres::PostgresStore};
use etl_config::shared::{DestinationConfig, IcebergConfig};
use etl_destinations::{
    bigquery::BigQueryDestination, ducklake::DuckLakeDestination, iceberg::IcebergDestination,
};

use crate::error_reporting::ErrorReportingStateStore;

type ReportingPostgresStore = ErrorReportingStateStore<PostgresStore>;

/// Returns the configured destination implementation name.
pub(crate) fn destination_name(destination_config: &DestinationConfig) -> &'static str {
    match destination_config {
        DestinationConfig::BigQuery { .. } => BigQueryDestination::<ReportingPostgresStore>::name(),
        DestinationConfig::Iceberg {
            config: IcebergConfig::Supabase { .. } | IcebergConfig::Rest { .. },
        } => IcebergDestination::<ReportingPostgresStore>::name(),
        DestinationConfig::Ducklake { .. } => DuckLakeDestination::<ReportingPostgresStore>::name(),
    }
}
