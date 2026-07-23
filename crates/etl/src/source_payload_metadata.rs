//! Source-side PostgreSQL row payload metadata and accounting.
//!
//! Initial-copy metadata measures the body of each backend `CopyData` message:
//! the text-format row including delimiters, escaping, and its terminating
//! newline. Streaming metadata measures the text or binary value data inside
//! pgoutput tuples. PostgreSQL message framing, tuple framing, TCP/TLS
//! overhead, ETL metadata, and destination encoding are excluded.
//!
//! Streaming operation presence is tracked separately from byte count so a
//! zero-byte row event remains an observation instead of being treated as
//! absent.

use metrics::{counter, histogram};

use crate::observability::{
    ETL_BYTES_PROCESSED_TOTAL, ETL_BYTES_RECEIVED_TOTAL, ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL,
};

/// Metadata for PostgreSQL COPY row-body bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableCopyPayloadMetadata {
    /// Initial-copy row bytes.
    copy_bytes: u64,
}

impl TableCopyPayloadMetadata {
    /// Creates metadata for one PostgreSQL COPY `CopyData` row body.
    pub(crate) fn new(copy_bytes: u64) -> Self {
        Self { copy_bytes }
    }

    /// Records these source bytes as received from PostgreSQL.
    pub(crate) fn record_received(self) {
        record_received_bytes([("copy", self.copy_bytes)]);
    }

    /// Records the row-size distribution for an individual COPY row.
    ///
    /// This method must only be called before merging, when this metadata
    /// represents one individual COPY row.
    pub(crate) fn record_row_size(self) {
        record_row_size_metrics([("copy", self.copy_bytes)]);
    }

    /// Records these source bytes after a successful destination write result.
    pub(crate) fn record_processed(self, destination_type: &'static str) {
        record_processed_metrics(
            [("copy", self.copy_bytes)],
            destination_type,
            "table_copy",
            self.copy_bytes,
        );
    }

    /// Merges compatible source payload metadata into this metadata.
    pub(crate) fn merge(&mut self, other: Self) {
        self.copy_bytes = self.copy_bytes.saturating_add(other.copy_bytes);
    }
}

/// Metadata for PostgreSQL logical-replication row-event tuple-value bytes.
///
/// `None` means that the operation is absent. `Some(0)` means that the
/// operation is present but its emitted tuple values contain zero bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamingPayloadMetadata {
    /// Insert new-tuple bytes, if an insert is represented.
    insert_bytes: Option<u64>,
    /// Update new- and old-identity-tuple bytes, if an update is represented.
    update_bytes: Option<u64>,
    /// Delete old-identity-tuple bytes, if a delete is represented.
    delete_bytes: Option<u64>,
}

impl StreamingPayloadMetadata {
    /// Creates metadata for logical-replication insert tuple-value bytes.
    pub(crate) fn insert(insert_bytes: u64) -> Self {
        Self { insert_bytes: Some(insert_bytes), ..Self::default() }
    }

    /// Creates metadata for logical-replication update tuple-value bytes.
    pub(crate) fn update(update_bytes: u64) -> Self {
        Self { update_bytes: Some(update_bytes), ..Self::default() }
    }

    /// Creates metadata for logical-replication delete tuple-value bytes.
    pub(crate) fn delete(delete_bytes: u64) -> Self {
        Self { delete_bytes: Some(delete_bytes), ..Self::default() }
    }

    /// Returns represented operation labels and byte counts.
    fn by_event_type(self) -> impl Iterator<Item = (&'static str, u64)> {
        [
            ("insert", self.insert_bytes),
            ("update", self.update_bytes),
            ("delete", self.delete_bytes),
        ]
        .into_iter()
        .filter_map(|(event_type, bytes)| bytes.map(|bytes| (event_type, bytes)))
    }

    /// Returns the total bytes represented by this metadata.
    fn total_bytes(self) -> u64 {
        self.by_event_type().fold(0, |total, (_, bytes)| total.saturating_add(bytes))
    }

    /// Records represented source bytes as received from PostgreSQL.
    pub(crate) fn record_received(self) {
        record_received_bytes(self.by_event_type());
    }

    /// Records the row-size distribution for an individual streaming event.
    ///
    /// This method must only be called before merging, when this metadata
    /// represents one individual logical-replication row event.
    pub(crate) fn record_row_size(self) {
        record_row_size_metrics(self.by_event_type());
    }

    /// Records represented source bytes after a successful destination write.
    pub(crate) fn record_processed(self, destination_type: &'static str) {
        record_processed_metrics(
            self.by_event_type(),
            destination_type,
            "streaming",
            self.total_bytes(),
        );
    }

    /// Merges compatible source payload metadata into this metadata.
    pub(crate) fn merge(&mut self, other: Self) {
        merge_optional_bytes(&mut self.insert_bytes, other.insert_bytes);
        merge_optional_bytes(&mut self.update_bytes, other.update_bytes);
        merge_optional_bytes(&mut self.delete_bytes, other.delete_bytes);
    }
}

/// Merges an optional byte count while preserving whether it is represented.
fn merge_optional_bytes(current: &mut Option<u64>, other: Option<u64>) {
    let Some(other) = other else {
        return;
    };

    match current {
        Some(current) => *current = current.saturating_add(other),
        None => *current = Some(other),
    }
}

/// Records individual row-size observations at ingestion.
fn record_row_size_metrics(event_bytes: impl IntoIterator<Item = (&'static str, u64)>) {
    for (event_type, bytes) in event_bytes {
        histogram!(ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL => event_type).record(bytes as f64);
    }
}

/// Records represented source-payload byte counters at ingestion.
fn record_received_bytes(event_bytes: impl IntoIterator<Item = (&'static str, u64)>) {
    for (event_type, bytes) in event_bytes {
        counter!(ETL_BYTES_RECEIVED_TOTAL, EVENT_TYPE_LABEL => event_type).increment(bytes);
    }
}

/// Records represented source-payload metrics and nonzero billing after
/// acknowledgement.
fn record_processed_metrics(
    event_bytes: impl IntoIterator<Item = (&'static str, u64)>,
    destination_type: &'static str,
    processing_type: &'static str,
    bytes_sent: u64,
) {
    for (event_type, bytes) in event_bytes {
        counter!(ETL_BYTES_PROCESSED_TOTAL, EVENT_TYPE_LABEL => event_type).increment(bytes);
    }

    if bytes_sent == 0 {
        return;
    }

    #[cfg(feature = "egress")]
    crate::egress::log_processed_bytes(destination_type, processing_type, bytes_sent);

    #[cfg(not(feature = "egress"))]
    let _ = (destination_type, processing_type, bytes_sent);
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use metrics::{
        Counter, CounterFn, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
        SharedString, Unit, with_local_recorder,
    };

    use super::*;

    /// One captured counter increment.
    #[derive(Debug, PartialEq, Eq)]
    struct CounterIncrement {
        /// Metric name.
        metric: String,
        /// Event-type label value.
        event_type: String,
        /// Incremented value.
        value: u64,
    }

    /// Counter handle that records increments for assertions.
    struct CapturingCounter {
        /// Metric name.
        metric: String,
        /// Event-type label value.
        event_type: String,
        /// Shared captured increments.
        increments: Arc<Mutex<Vec<CounterIncrement>>>,
    }

    impl CounterFn for CapturingCounter {
        fn increment(&self, value: u64) {
            self.increments.lock().unwrap().push(CounterIncrement {
                metric: self.metric.clone(),
                event_type: self.event_type.clone(),
                value,
            });
        }

        fn absolute(&self, value: u64) {
            self.increment(value);
        }
    }

    /// One captured histogram observation.
    #[derive(Debug, PartialEq)]
    struct HistogramObservation {
        /// Metric name.
        metric: String,
        /// Event-type label value.
        event_type: String,
        /// Recorded value.
        value: f64,
    }

    /// Histogram handle that records observations for assertions.
    struct CapturingHistogram {
        /// Metric name.
        metric: String,
        /// Event-type label value.
        event_type: String,
        /// Shared captured observations.
        observations: Arc<Mutex<Vec<HistogramObservation>>>,
    }

    impl HistogramFn for CapturingHistogram {
        fn record(&self, value: f64) {
            self.observations.lock().unwrap().push(HistogramObservation {
                metric: self.metric.clone(),
                event_type: self.event_type.clone(),
                value,
            });
        }
    }

    /// Recorder that captures counters and histograms.
    #[derive(Default)]
    struct CapturingRecorder {
        /// Shared captured counter increments.
        increments: Arc<Mutex<Vec<CounterIncrement>>>,
        /// Shared captured histogram observations.
        observations: Arc<Mutex<Vec<HistogramObservation>>>,
    }

    impl Recorder for CapturingRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {
        }

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(
            &self,
            _key: KeyName,
            _unit: Option<Unit>,
            _description: SharedString,
        ) {
        }

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            let event_type = key
                .labels()
                .find(|label| label.key() == EVENT_TYPE_LABEL)
                .map(|label| label.value().to_owned())
                .unwrap_or_default();
            Counter::from_arc(Arc::new(CapturingCounter {
                metric: key.name().to_owned(),
                event_type,
                increments: Arc::clone(&self.increments),
            }))
        }

        fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            Gauge::noop()
        }

        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            let event_type = key
                .labels()
                .find(|label| label.key() == EVENT_TYPE_LABEL)
                .map(|label| label.value().to_owned())
                .unwrap_or_default();
            Histogram::from_arc(Arc::new(CapturingHistogram {
                metric: key.name().to_owned(),
                event_type,
                observations: Arc::clone(&self.observations),
            }))
        }
    }

    #[test]
    fn streaming_metadata_accumulates_without_losing_operation_breakdown() {
        let mut streaming_payload_metadata = StreamingPayloadMetadata::insert(3);
        streaming_payload_metadata.merge(StreamingPayloadMetadata::update(5));
        streaming_payload_metadata.merge(StreamingPayloadMetadata::delete(7));
        streaming_payload_metadata.merge(StreamingPayloadMetadata::insert(11));

        assert_eq!(streaming_payload_metadata.total_bytes(), 26);
        assert_eq!(
            streaming_payload_metadata.by_event_type().collect::<Vec<_>>(),
            [("insert", 14), ("update", 5), ("delete", 7)]
        );
    }

    #[test]
    fn streaming_metadata_preserves_absent_and_zero_byte_operations() {
        let mut streaming_payload_metadata = StreamingPayloadMetadata::default();
        assert!(streaming_payload_metadata.by_event_type().next().is_none());

        streaming_payload_metadata.merge(StreamingPayloadMetadata::insert(0));
        streaming_payload_metadata.merge(StreamingPayloadMetadata::update(5));
        streaming_payload_metadata.merge(StreamingPayloadMetadata::default());

        assert_eq!(streaming_payload_metadata.total_bytes(), 5);
        assert_eq!(
            streaming_payload_metadata.by_event_type().collect::<Vec<_>>(),
            [("insert", 0), ("update", 5)]
        );
    }

    #[test]
    fn optional_byte_merge_preserves_presence_and_saturates() {
        let cases = [
            (None, None, None),
            (None, Some(0), Some(0)),
            (Some(0), None, Some(0)),
            (Some(2), Some(3), Some(5)),
            (Some(u64::MAX), Some(1), Some(u64::MAX)),
        ];

        for (mut current, other, expected) in cases {
            merge_optional_bytes(&mut current, other);
            assert_eq!(current, expected);
        }
    }

    #[test]
    fn table_copy_batch_aggregates_counters_and_preserves_row_sizes() {
        let recorder = CapturingRecorder::default();

        with_local_recorder(&recorder, || {
            let first_metadata = TableCopyPayloadMetadata::new(3);
            let second_metadata = TableCopyPayloadMetadata::new(5);
            first_metadata.record_row_size();
            second_metadata.record_row_size();

            let mut table_copy_batch_metadata = TableCopyPayloadMetadata::default();
            table_copy_batch_metadata.merge(first_metadata);
            table_copy_batch_metadata.merge(second_metadata);
            table_copy_batch_metadata.record_received();
            table_copy_batch_metadata.record_processed("test_destination");
        });

        assert_eq!(
            *recorder.increments.lock().unwrap(),
            [
                CounterIncrement {
                    metric: ETL_BYTES_RECEIVED_TOTAL.to_owned(),
                    event_type: "copy".to_owned(),
                    value: 8,
                },
                CounterIncrement {
                    metric: ETL_BYTES_PROCESSED_TOTAL.to_owned(),
                    event_type: "copy".to_owned(),
                    value: 8,
                },
            ]
        );
        assert_eq!(
            *recorder.observations.lock().unwrap(),
            [
                HistogramObservation {
                    metric: ETL_ROW_SIZE_BYTES.to_owned(),
                    event_type: "copy".to_owned(),
                    value: 3.0,
                },
                HistogramObservation {
                    metric: ETL_ROW_SIZE_BYTES.to_owned(),
                    event_type: "copy".to_owned(),
                    value: 5.0,
                },
            ]
        );
    }

    #[test]
    fn received_and_processed_metadata_renders_to_distinct_metrics() {
        let recorder = CapturingRecorder::default();

        with_local_recorder(&recorder, || {
            let insert_metadata = StreamingPayloadMetadata::insert(3);
            insert_metadata.record_received();
            let update_metadata = StreamingPayloadMetadata::update(5);
            update_metadata.record_received();

            let mut streaming_payload_metadata = insert_metadata;
            streaming_payload_metadata.merge(update_metadata);
            streaming_payload_metadata.record_processed("test_destination");
        });

        assert_eq!(
            *recorder.increments.lock().unwrap(),
            [
                CounterIncrement {
                    metric: ETL_BYTES_RECEIVED_TOTAL.to_owned(),
                    event_type: "insert".to_owned(),
                    value: 3,
                },
                CounterIncrement {
                    metric: ETL_BYTES_RECEIVED_TOTAL.to_owned(),
                    event_type: "update".to_owned(),
                    value: 5,
                },
                CounterIncrement {
                    metric: ETL_BYTES_PROCESSED_TOTAL.to_owned(),
                    event_type: "insert".to_owned(),
                    value: 3,
                },
                CounterIncrement {
                    metric: ETL_BYTES_PROCESSED_TOTAL.to_owned(),
                    event_type: "update".to_owned(),
                    value: 5,
                },
            ]
        );
    }

    #[test]
    fn zero_byte_metadata_emits_only_represented_metrics() {
        let recorder = CapturingRecorder::default();

        with_local_recorder(&recorder, || {
            let table_copy_metadata = TableCopyPayloadMetadata::new(0);
            table_copy_metadata.record_received();
            table_copy_metadata.record_row_size();
            table_copy_metadata.record_processed("test_destination");

            let streaming_metadata = StreamingPayloadMetadata::insert(0);
            streaming_metadata.record_received();
            streaming_metadata.record_row_size();
            streaming_metadata.record_processed("test_destination");

            let absent_streaming_metadata = StreamingPayloadMetadata::default();
            absent_streaming_metadata.record_received();
            absent_streaming_metadata.record_row_size();
            absent_streaming_metadata.record_processed("test_destination");
        });

        assert_eq!(
            *recorder.increments.lock().unwrap(),
            [
                CounterIncrement {
                    metric: ETL_BYTES_RECEIVED_TOTAL.to_owned(),
                    event_type: "copy".to_owned(),
                    value: 0,
                },
                CounterIncrement {
                    metric: ETL_BYTES_PROCESSED_TOTAL.to_owned(),
                    event_type: "copy".to_owned(),
                    value: 0,
                },
                CounterIncrement {
                    metric: ETL_BYTES_RECEIVED_TOTAL.to_owned(),
                    event_type: "insert".to_owned(),
                    value: 0,
                },
                CounterIncrement {
                    metric: ETL_BYTES_PROCESSED_TOTAL.to_owned(),
                    event_type: "insert".to_owned(),
                    value: 0,
                },
            ]
        );
        assert_eq!(
            *recorder.observations.lock().unwrap(),
            [
                HistogramObservation {
                    metric: ETL_ROW_SIZE_BYTES.to_owned(),
                    event_type: "copy".to_owned(),
                    value: 0.0,
                },
                HistogramObservation {
                    metric: ETL_ROW_SIZE_BYTES.to_owned(),
                    event_type: "insert".to_owned(),
                    value: 0.0,
                },
            ]
        );
    }
}
