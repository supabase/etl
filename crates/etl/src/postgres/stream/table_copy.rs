use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use metrics::{counter, histogram};
use pin_project_lite::pin_project;
use tokio_postgres::CopyOutStream;

use crate::{
    data::TableRow,
    error::EtlResult,
    observability::{
        ACTION_LABEL, ETL_BYTES_PROCESSED_TOTAL, ETL_BYTES_RECEIVED_TOTAL,
        ETL_EVENTS_PROCESSED_TOTAL, ETL_EVENTS_RECEIVED_TOTAL, ETL_ROW_SIZE_BYTES,
        EVENT_TYPE_LABEL, WORKER_TYPE_LABEL,
    },
    postgres::codec::parse_table_row_from_postgres_copy_bytes,
    schema::ColumnSchema,
};

/// Number of COPY rows to aggregate before emitting counter metrics.
///
/// Row-size histograms are still recorded per row so their distribution keeps
/// the same meaning.
const COPY_METRICS_FLUSH_ROWS: u64 = 1024;

/// Pending COPY metrics waiting to be emitted.
#[derive(Debug, Default)]
struct CopyMetricsBatch {
    /// Number of rows seen since the last flush.
    rows: u64,
    /// Number of raw COPY bytes seen since the last flush.
    bytes: u64,
}

impl CopyMetricsBatch {
    /// Records one COPY row and flushes counters when the batch is full.
    fn record_row(&mut self, row_size_bytes: u64) {
        self.rows = self.rows.saturating_add(1);
        self.bytes = self.bytes.saturating_add(row_size_bytes);

        histogram!(ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL => "copy").record(row_size_bytes as f64);

        if self.rows >= COPY_METRICS_FLUSH_ROWS {
            self.flush();
        }
    }

    /// Flushes pending metric deltas to the metrics recorder.
    fn flush(&mut self) {
        if self.rows == 0 {
            return;
        }

        counter!(
            ETL_EVENTS_RECEIVED_TOTAL,
            WORKER_TYPE_LABEL => "table_sync",
            ACTION_LABEL => "table_copy",
        )
        .increment(self.rows);

        counter!(
            ETL_EVENTS_PROCESSED_TOTAL,
            WORKER_TYPE_LABEL => "table_sync",
            ACTION_LABEL => "table_copy",
        )
        .increment(self.rows);

        counter!(ETL_BYTES_RECEIVED_TOTAL, EVENT_TYPE_LABEL => "copy").increment(self.bytes);
        counter!(ETL_BYTES_PROCESSED_TOTAL, EVENT_TYPE_LABEL => "copy").increment(self.bytes);

        self.rows = 0;
        self.bytes = 0;
    }
}

pin_project! {
    /// A stream that yields rows from a Postgres COPY operation.
    ///
    /// This stream wraps a [`CopyOutStream`] and converts each row into a [`TableRow`]
    /// using the provided column schemas. The conversion process handles both text and
    /// binary format data.
    #[must_use = "streams do nothing unless polled"]
    pub(crate) struct TableCopyStream {
        #[pin]
        stream: CopyOutStream,
        column_schemas: Vec<ColumnSchema>,
        metrics_batch: CopyMetricsBatch,
    }
}

impl TableCopyStream {
    /// Creates a new [`TableCopyStream`] from a [`CopyOutStream`] and column
    /// schemas.
    ///
    /// The column schemas are used to convert the raw Postgres data into
    /// [`TableRow`]s.
    pub(crate) fn wrap(stream: CopyOutStream, column_schemas: Vec<ColumnSchema>) -> Self {
        Self { stream, column_schemas, metrics_batch: CopyMetricsBatch::default() }
    }
}

impl Stream for TableCopyStream {
    type Item = EtlResult<TableRow>;

    /// Polls the stream for the next converted table row with comprehensive
    /// error handling.
    ///
    /// This method handles the complex process of converting raw Postgres COPY
    /// data into structured [`TableRow`] objects, with detailed error
    /// reporting for various failure modes.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.stream.poll_next(cx) {
            Poll::Pending => {
                this.metrics_batch.flush();

                Poll::Pending
            }
            // Row copy received.
            Poll::Ready(Some(Ok(row))) => {
                let row_size_bytes = row.len() as u64;

                this.metrics_batch.record_row(row_size_bytes);

                // Conversion step: transform raw bytes into structured TableRow.
                // This is where most errors occur due to data format or type issues.
                match parse_table_row_from_postgres_copy_bytes(&row, this.column_schemas.as_slice())
                {
                    Ok(row) => Poll::Ready(Some(Ok(row))),
                    Err(err) => {
                        this.metrics_batch.flush();

                        Poll::Ready(Some(Err(err)))
                    }
                }
            }
            // Postgres connection or protocol-level failure.
            Poll::Ready(Some(Err(err))) => {
                this.metrics_batch.flush();

                Poll::Ready(Some(Err(err.into())))
            }
            // Normal completion, no more rows available.
            Poll::Ready(None) => {
                this.metrics_batch.flush();

                Poll::Ready(None)
            }
        }
    }
}
