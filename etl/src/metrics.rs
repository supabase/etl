use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub const ETL_TABLES_TOTAL: &str = "etl_tables_total";
pub const ETL_BATCH_ITEMS_SEND_DURATION_SECONDS: &str = "etl_batch_items_send_duration_seconds";
pub const ETL_TRANSACTION_DURATION_SECONDS: &str = "etl_transaction_duration_seconds";
pub const ETL_TRANSACTIONS_TOTAL: &str = "etl_transactions_total";
pub const ETL_TRANSACTION_SIZE: &str = "etl_transaction_size";
pub const ETL_TABLE_COPY_DURATION_SECONDS: &str = "etl_table_copy_duration_seconds";
pub const ETL_BYTES_PROCESSED_TOTAL: &str = "etl_bytes_processed_total";
pub const ETL_EVENTS_PROCESSED_TOTAL: &str = "etl_events_processed_total";
pub const ETL_STATUS_UPDATES_TOTAL: &str = "etl_status_updates_total";
pub const ETL_STATUS_UPDATES_SKIPPED_TOTAL: &str = "etl_status_updates_skipped_total";

pub const ETL_HEARTBEAT_EMISSIONS_TOTAL: &str = "etl_heartbeat_emissions_total";
pub const ETL_HEARTBEAT_FAILURES_TOTAL: &str = "etl_heartbeat_failures_total";
pub const ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL: &str = "etl_heartbeat_connection_attempts_total";
pub const ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP: &str = "etl_heartbeat_last_emission_timestamp";
pub const ETL_HEARTBEAT_CONSECUTIVE_FAILURES: &str = "etl_heartbeat_consecutive_failures";

pub const PHASE_LABEL: &str = "phase";
pub const WORKER_TYPE_LABEL: &str = "worker_type";
pub const ACTION_LABEL: &str = "action";
pub const DESTINATION_LABEL: &str = "destination";
pub const PIPELINE_ID_LABEL: &str = "pipeline_id";
pub const EVENT_TYPE_LABEL: &str = "event_type";
pub const FORCED_LABEL: &str = "forced";
pub const STATUS_UPDATE_TYPE_LABEL: &str = "status_update_type";

pub(crate) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_gauge!(ETL_TABLES_TOTAL, Unit::Count, "Total number of tables being copied");
        describe_histogram!(ETL_BATCH_ITEMS_SEND_DURATION_SECONDS, Unit::Seconds, "Time taken in seconds to send a batch of items to the destination");
        describe_histogram!(ETL_TRANSACTION_DURATION_SECONDS, Unit::Seconds, "Duration in seconds between BEGIN and COMMIT for a transaction");
        describe_counter!(ETL_TRANSACTIONS_TOTAL, Unit::Count, "Total number of transactions seen");
        describe_histogram!(ETL_TRANSACTION_SIZE, Unit::Count, "Number of events per transaction");
        describe_histogram!(ETL_TABLE_COPY_DURATION_SECONDS, Unit::Seconds, "Duration in seconds to complete initial table copy");
        describe_counter!(ETL_EVENTS_PROCESSED_TOTAL, Unit::Count, "Total number of events successfully processed");
        describe_counter!(ETL_BYTES_PROCESSED_TOTAL, Unit::Bytes, "Total bytes processed by the pipeline");
        describe_counter!(ETL_STATUS_UPDATES_TOTAL, Unit::Count, "Total number of status updates sent to Postgres");
        describe_counter!(ETL_STATUS_UPDATES_SKIPPED_TOTAL, Unit::Count, "Total number of status updates skipped due to throttling");
        describe_counter!(ETL_HEARTBEAT_EMISSIONS_TOTAL, Unit::Count, "Total number of successful heartbeat emissions");
        describe_counter!(ETL_HEARTBEAT_FAILURES_TOTAL, Unit::Count, "Total number of failed heartbeat attempts");
        describe_counter!(ETL_HEARTBEAT_CONNECTION_ATTEMPTS_TOTAL, Unit::Count, "Total number of connection attempts to primary for heartbeats");
        describe_gauge!(ETL_HEARTBEAT_LAST_EMISSION_TIMESTAMP, Unit::Seconds, "Unix timestamp of last successful heartbeat");
        describe_gauge!(ETL_HEARTBEAT_CONSECUTIVE_FAILURES, Unit::Count, "Number of consecutive heartbeat failures");
    });
}
