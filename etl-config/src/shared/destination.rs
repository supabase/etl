use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

use crate::SerializableSecretString;

/// Configuration for supported ETL data destinations.
///
/// Specifies the destination type and its associated configuration parameters.
/// Each variant corresponds to a different supported destination system.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DestinationConfig {
    /// In-memory destination for ephemeral or test data.
    Memory,
    /// Google BigQuery destination configuration.
    ///
    /// Use this variant to configure a BigQuery destination, including
    /// project and dataset identifiers, service account credentials, and
    /// optional staleness settings.
    BigQuery {
        /// Google Cloud project identifier.
        #[cfg_attr(feature = "utoipa", schema(example = "my-gcp-project"))]
        project_id: String,
        /// BigQuery dataset identifier.
        #[cfg_attr(feature = "utoipa", schema(example = "my_dataset"))]
        dataset_id: String,
        /// Service account key for authenticating with BigQuery.
        #[cfg_attr(
            feature = "utoipa",
            schema(example = "{\"type\": \"service_account\", \"project_id\": \"my-project\"}")
        )]
        service_account_key: SerializableSecretString,
        /// Maximum staleness in minutes for BigQuery CDC reads.
        ///
        /// If not set, the default staleness behavior is used. See
        /// <https://cloud.google.com/bigquery/docs/change-data-capture#create-max-staleness>.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "utoipa", schema(example = 15))]
        max_staleness_mins: Option<u16>,
        /// Maximum number of concurrent streams for BigQuery append operations.
        ///
        /// Defines the upper limit of concurrent streams used for a **single** append
        /// request to BigQuery.
        ///
        /// This does not limit the total number of streams across the entire system.
        /// The actual number of streams in use at any given time depends on:
        /// - the number of tables being replicated,
        /// - the volume of events processed by the ETL,
        /// - and the configured batch size.
        #[serde(skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "utoipa", schema(example = 8))]
        max_concurrent_streams: Option<usize>,
    },
}

impl Default for DestinationConfig {
    fn default() -> Self {
        Self::Memory
    }
}
