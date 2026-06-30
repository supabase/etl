use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::{Stream, ready};
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

pin_project! {
    /// A stream that yields rows from a Postgres COPY operation.
    ///
    /// This stream wraps a [`CopyOutStream`] and converts each row into a [`TableRow`]
    /// using the provided column schemas. The conversion process handles both text and
    /// binary format data.
    #[must_use = "streams do nothing unless polled"]
    pub(crate) struct TableCopyStream<I> {
        #[pin]
        stream: CopyOutStream,
        column_schemas: I,
    }
}

impl<I> TableCopyStream<I> {
    /// Creates a new [`TableCopyStream`] from a [`CopyOutStream`] and column
    /// schemas.
    ///
    /// The column schemas are used to convert the raw Postgres data into
    /// [`TableRow`]s.
    pub(crate) fn wrap(stream: CopyOutStream, column_schemas: I) -> Self {
        Self { stream, column_schemas }
    }

    /// Records source-side metrics for a COPY row received from Postgres.
    fn record_row_received(row_size_bytes: u64) {
        counter!(
            ETL_EVENTS_RECEIVED_TOTAL,
            WORKER_TYPE_LABEL => "table_sync",
            ACTION_LABEL => "table_copy",
        )
        .increment(1);

        counter!(ETL_BYTES_RECEIVED_TOTAL, EVENT_TYPE_LABEL => "copy").increment(row_size_bytes);
    }

    /// Records pipeline-side metrics for a COPY row read from the stream.
    fn record_row_processed(row_size_bytes: u64) {
        counter!(
            ETL_EVENTS_PROCESSED_TOTAL,
            WORKER_TYPE_LABEL => "table_sync",
            ACTION_LABEL => "table_copy",
        )
        .increment(1);

        counter!(ETL_BYTES_PROCESSED_TOTAL, EVENT_TYPE_LABEL => "copy").increment(row_size_bytes);

        histogram!(ETL_ROW_SIZE_BYTES, EVENT_TYPE_LABEL => "copy").record(row_size_bytes as f64);
    }
}

impl<'a, I> Stream for TableCopyStream<I>
where
    I: ExactSizeIterator<Item = &'a ColumnSchema> + Clone,
{
    type Item = EtlResult<TableRow>;

    /// Polls the stream for the next converted table row with comprehensive
    /// error handling.
    ///
    /// This method handles the complex process of converting raw Postgres COPY
    /// data into structured [`TableRow`] objects, with detailed error
    /// reporting for various failure modes.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.stream.poll_next(cx)) {
            // Row copy received.
            Some(Ok(row)) => {
                let row_size_bytes = row.len() as u64;

                Self::record_row_received(row_size_bytes);
                Self::record_row_processed(row_size_bytes);

                // Conversion step: transform raw bytes into structured TableRow.
                // This is where most errors occur due to data format or type issues.
                match parse_table_row_from_postgres_copy_bytes(&row, this.column_schemas.clone()) {
                    Ok(row) => Poll::Ready(Some(Ok(row))),
                    Err(err) => Poll::Ready(Some(Err(err))),
                }
            }
            // Postgres connection or protocol-level failure.
            Some(Err(err)) => Poll::Ready(Some(Err(err.into()))),
            // Normal completion, no more rows available.
            None => Poll::Ready(None),
        }
    }
}
