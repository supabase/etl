//! Source-side PostgreSQL row payload accounting.
//!
//! Initial-copy payload is the body of each backend `CopyData` message: the
//! text-format row including delimiters, escaping, and its terminating newline.
//! Streaming payload is the text or binary value data inside pgoutput tuples.
//! PostgreSQL message framing, tuple framing, TCP/TLS overhead, ETL metadata,
//! and destination encoding are excluded.

use metrics::{counter, histogram};

use crate::observability::{
    ETL_BYTES_PROCESSED_TOTAL, ETL_BYTES_RECEIVED_TOTAL, ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL,
};

/// Source-payload accounting shared by table copy and streaming.
///
/// A payload records source ingress when it is decoded and records successful
/// processing after the destination acknowledges the corresponding write. It
/// measures the PostgreSQL representation used for usage accounting, not the
/// decoded in-memory size reported by [`crate::data::SizeHint`].
pub(crate) trait SourcePayload: Sized {
    /// Records this payload as received from PostgreSQL.
    fn record_received(self);

    /// Records the row-size distribution for an individual source row event.
    ///
    /// This method must only be called before merging, when this payload
    /// represents one individual COPY row or logical-replication row event.
    fn record_row_size(self);

    /// Records this payload after a successful destination write result.
    fn record_processed(self, destination_type: &'static str);

    /// Merges another payload from the same source path into this payload.
    fn merge(&mut self, other: Self);
}

/// PostgreSQL COPY row-body bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TableCopyPayload {
    /// Initial-copy row bytes.
    copy_bytes: u64,
}

impl TableCopyPayload {
    /// Creates a payload for one PostgreSQL COPY `CopyData` row body.
    pub(crate) fn new(copy_bytes: u64) -> Self {
        Self { copy_bytes }
    }
}

impl SourcePayload for TableCopyPayload {
    fn record_received(self) {
        record_received_bytes([("copy", self.copy_bytes)]);
    }

    fn record_row_size(self) {
        record_row_size_metrics([("copy", self.copy_bytes)]);
    }

    fn record_processed(self, destination_type: &'static str) {
        record_processed_metrics(
            [("copy", self.copy_bytes)],
            destination_type,
            "table_copy",
            self.copy_bytes,
        );
    }

    fn merge(&mut self, other: Self) {
        self.copy_bytes = self.copy_bytes.saturating_add(other.copy_bytes);
    }
}

/// PostgreSQL logical-replication tuple-value bytes.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StreamingPayload {
    /// Insert new-tuple bytes.
    insert_bytes: u64,
    /// Update new- and old-identity-tuple bytes.
    update_bytes: u64,
    /// Delete old-identity-tuple bytes.
    delete_bytes: u64,
}

impl StreamingPayload {
    /// Creates a logical-replication insert tuple-value payload.
    pub(crate) fn insert(insert_bytes: u64) -> Self {
        Self { insert_bytes, ..Self::default() }
    }

    /// Creates a logical-replication update tuple-value payload.
    pub(crate) fn update(update_bytes: u64) -> Self {
        Self { update_bytes, ..Self::default() }
    }

    /// Creates a logical-replication delete tuple-value payload.
    pub(crate) fn delete(delete_bytes: u64) -> Self {
        Self { delete_bytes, ..Self::default() }
    }

    /// Returns operation labels with nonzero byte counts.
    fn by_event_type(self) -> impl Iterator<Item = (&'static str, u64)> {
        [
            ("insert", self.insert_bytes),
            ("update", self.update_bytes),
            ("delete", self.delete_bytes),
        ]
        .into_iter()
        .filter(|(_, bytes)| *bytes != 0)
    }

    /// Returns the total bytes represented by this payload.
    fn total_bytes(self) -> u64 {
        self.insert_bytes.saturating_add(self.update_bytes).saturating_add(self.delete_bytes)
    }
}

impl SourcePayload for StreamingPayload {
    fn record_received(self) {
        record_received_bytes(self.by_event_type());
    }

    fn record_row_size(self) {
        record_row_size_metrics(self.by_event_type());
    }

    fn record_processed(self, destination_type: &'static str) {
        record_processed_metrics(
            self.by_event_type(),
            destination_type,
            "streaming",
            self.total_bytes(),
        );
    }

    fn merge(&mut self, other: Self) {
        self.insert_bytes = self.insert_bytes.saturating_add(other.insert_bytes);
        self.update_bytes = self.update_bytes.saturating_add(other.update_bytes);
        self.delete_bytes = self.delete_bytes.saturating_add(other.delete_bytes);
    }
}

/// Records nonzero individual row-size observations at ingestion.
fn record_row_size_metrics(event_bytes: impl IntoIterator<Item = (&'static str, u64)>) {
    for (event_type, bytes) in event_bytes {
        if bytes == 0 {
            continue;
        }

        histogram!(ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL => event_type).record(bytes as f64);
    }
}

/// Records nonzero source-payload byte counters at ingestion.
fn record_received_bytes(event_bytes: impl IntoIterator<Item = (&'static str, u64)>) {
    for (event_type, bytes) in event_bytes {
        if bytes == 0 {
            continue;
        }

        counter!(ETL_BYTES_RECEIVED_TOTAL, EVENT_TYPE_LABEL => event_type).increment(bytes);
    }
}

/// Records nonzero source-payload metrics and billing after acknowledgement.
fn record_processed_metrics(
    event_bytes: impl IntoIterator<Item = (&'static str, u64)>,
    destination_type: &'static str,
    processing_type: &'static str,
    bytes_sent: u64,
) {
    for (event_type, bytes) in event_bytes {
        if bytes == 0 {
            continue;
        }

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
    fn streaming_payloads_accumulate_without_losing_operation_breakdown() {
        let mut total_streaming_payload = StreamingPayload::insert(3);
        total_streaming_payload.merge(StreamingPayload::update(5));
        total_streaming_payload.merge(StreamingPayload::delete(7));
        total_streaming_payload.merge(StreamingPayload::insert(11));

        assert_eq!(total_streaming_payload.total_bytes(), 26);
        assert_eq!(
            total_streaming_payload.by_event_type().collect::<Vec<_>>(),
            [("insert", 14), ("update", 5), ("delete", 7)]
        );
    }

    #[test]
    fn table_copy_batch_aggregates_counters_and_preserves_row_sizes() {
        let recorder = CapturingRecorder::default();

        with_local_recorder(&recorder, || {
            let first_table_copy_payload = TableCopyPayload::new(3);
            let second_table_copy_payload = TableCopyPayload::new(5);
            first_table_copy_payload.record_row_size();
            second_table_copy_payload.record_row_size();

            let mut total_table_copy_payload = TableCopyPayload::default();
            total_table_copy_payload.merge(first_table_copy_payload);
            total_table_copy_payload.merge(second_table_copy_payload);
            total_table_copy_payload.record_received();
            total_table_copy_payload.record_processed("test_destination");
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
    fn received_and_processed_payloads_render_to_distinct_metrics() {
        let recorder = CapturingRecorder::default();

        with_local_recorder(&recorder, || {
            let insert_streaming_payload = StreamingPayload::insert(3);
            insert_streaming_payload.record_received();
            let update_streaming_payload = StreamingPayload::update(5);
            update_streaming_payload.record_received();

            let mut total_streaming_payload = insert_streaming_payload;
            total_streaming_payload.merge(update_streaming_payload);
            total_streaming_payload.record_processed("test_destination");
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
    fn zero_byte_payloads_do_not_emit_metrics() {
        let recorder = CapturingRecorder::default();

        with_local_recorder(&recorder, || {
            let table_copy_payload = TableCopyPayload::new(0);
            table_copy_payload.record_received();
            table_copy_payload.record_row_size();
            table_copy_payload.record_processed("test_destination");

            let streaming_payload = StreamingPayload::insert(0);
            streaming_payload.record_received();
            streaming_payload.record_row_size();
            streaming_payload.record_processed("test_destination");
        });

        assert!(recorder.increments.lock().unwrap().is_empty());
        assert!(recorder.observations.lock().unwrap().is_empty());
    }
}
