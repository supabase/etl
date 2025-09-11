use std::sync::Once;

use metrics::{Unit, describe_counter, describe_gauge, describe_histogram};

static REGISTER_METRICS: Once = Once::new();

pub const ETL_TABLES_TOTAL: &str = "etl_tables_total";
pub const ETL_ITEMS_COPIED_TOTAL: &str = "etl_items_copied_total";
pub const ETL_BATCH_SIZE: &str = "etl_batch_size";
pub const ETL_BATCH_SEND_DURATION_MILLISECONDS: &str = "etl_batch_send_duration_milliseconds";
pub const ETL_TABLE_SYNC_WORKERS_ACTIVE: &str = "etl_table_sync_workers_active";
pub const ETL_PUBLICATION_TABLES_TOTAL: &str = "etl_publication_tables_total";
pub const ETL_TRANSACTION_DURATION_MILLISECONDS: &str = "etl_transaction_duration_milliseconds";
pub const ETL_TRANSACTION_SIZE_EVENTS: &str = "etl_transaction_size_events";
pub const ETL_COPIED_ROW_SIZE_BYTES: &str = "etl_copied_row_size_bytes";
pub const ETL_STREAMED_EVENT_SIZE_BYTES: &str = "etl_streamed_event_size_bytes";

/// Label key for table id.
pub const TABLE_ID_LABEL: &str = "table_id";
/// Label key for replication phase (used by table state metrics).
pub const PHASE_LABEL: &str = "phase";
/// Label key for the ETL worker type ("table_sync" or "apply").
pub const WORKER_TYPE_LABEL: &str = "worker_type";
/// Label key used to tag metrics by destination implementation (e.g., "big_query").
pub const DESTINATION_LABEL: &str = "destination";
/// Label key for pipeline id.
pub const PIPELINE_ID_LABEL: &str = "pipeline_id";
/// Label key for publication name.
pub const PUBLICATION_LABEL: &str = "publication";
/// Label key for logical event type.
pub const EVENT_TYPE_LABEL: &str = "event_type";

/// Register metrics emitted by etl. This should be called before starting a pipeline.
/// It is safe to call this method multiple times. It is guaranteed to register the
/// metrics only once.
pub(crate) fn register_metrics() {
    REGISTER_METRICS.call_once(|| {
        describe_gauge!(
            ETL_TABLES_TOTAL,
            Unit::Count,
            "Total number of tables being copied"
        );

        describe_counter!(
            ETL_ITEMS_COPIED_TOTAL,
            Unit::Count,
            "Total number of rows or events copied to destination in table sync or apply phase"
        );

        describe_gauge!(
            ETL_BATCH_SIZE,
            Unit::Count,
            "Batch size of events sent to the destination"
        );

        describe_histogram!(
            ETL_BATCH_SEND_DURATION_MILLISECONDS,
            Unit::Milliseconds,
            "Time taken in milliseconds to send a batch of events to the destination"
        );

        describe_gauge!(
            ETL_TABLE_SYNC_WORKERS_ACTIVE,
            Unit::Count,
            "Number of active table sync workers in the pool"
        );

        describe_gauge!(
            ETL_PUBLICATION_TABLES_TOTAL,
            Unit::Count,
            "Number of tables found in the publication during initialization"
        );

        describe_histogram!(
            ETL_TRANSACTION_DURATION_MILLISECONDS,
            Unit::Milliseconds,
            "Duration in milliseconds between BEGIN and COMMIT for a transaction"
        );

        describe_histogram!(
            ETL_TRANSACTION_SIZE_EVENTS,
            Unit::Count,
            "Number of logical replication events contained in a transaction"
        );

        describe_histogram!(
            ETL_COPIED_ROW_SIZE_BYTES,
            Unit::Bytes,
            "Approximate size in bytes of a row copied during table sync"
        );

        describe_histogram!(
            ETL_STREAMED_EVENT_SIZE_BYTES,
            Unit::Bytes,
            "Approximate size in bytes of a streamed logical replication event"
        );
    });
}
