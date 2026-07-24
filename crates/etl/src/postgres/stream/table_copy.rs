use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures::Stream;
use pin_project_lite::pin_project;
use tokio_postgres::CopyOutStream;

use crate::{
    data::{SizeHint, TableRow},
    error::EtlResult,
    postgres::codec::parse_table_row_from_postgres_copy_bytes,
    schema::ColumnSchema,
    source_payload_metadata::TableCopyPayloadMetadata,
};

/// A decoded table-copy row paired with its PostgreSQL source metadata.
#[derive(Debug)]
pub(crate) struct TableCopyRow {
    /// Decoded row sent to the destination.
    row: TableRow,
    /// Metadata for the text-format row received from PostgreSQL.
    metadata: TableCopyPayloadMetadata,
}

impl TableCopyRow {
    /// Consumes the row and returns its decoded value and source metadata.
    pub(crate) fn into_parts(self) -> (TableRow, TableCopyPayloadMetadata) {
        (self.row, self.metadata)
    }
}

impl SizeHint for TableCopyRow {
    /// Returns the decoded row estimate used to control batching.
    ///
    /// The PostgreSQL COPY source metadata is retained separately for metrics
    /// and usage accounting.
    fn size_hint(&self) -> usize {
        self.row.size_hint()
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
    }
}

impl TableCopyStream {
    /// Creates a new [`TableCopyStream`] from a [`CopyOutStream`] and column
    /// schemas.
    ///
    /// The column schemas are used to convert the raw Postgres data into
    /// [`TableRow`]s.
    pub(crate) fn wrap(stream: CopyOutStream, column_schemas: Vec<ColumnSchema>) -> Self {
        Self { stream, column_schemas }
    }
}

impl Stream for TableCopyStream {
    type Item = EtlResult<TableCopyRow>;

    /// Polls the stream for the next converted table row with comprehensive
    /// error handling.
    ///
    /// This method handles the complex process of converting raw Postgres COPY
    /// data into structured [`TableRow`] objects, with detailed error
    /// reporting for various failure modes.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.stream.poll_next(cx) {
            Poll::Pending => Poll::Pending,
            // Row copy received.
            Poll::Ready(Some(Ok(row))) => {
                let metadata = TableCopyPayloadMetadata::new(row.len() as u64);

                // Conversion step: transform raw bytes into structured TableRow.
                // This is where most errors occur due to data format or type issues.
                match parse_table_row_from_postgres_copy_bytes(&row, this.column_schemas.as_slice())
                {
                    Ok(row) => Poll::Ready(Some(Ok(TableCopyRow { row, metadata }))),
                    Err(err) => Poll::Ready(Some(Err(err))),
                }
            }
            // Postgres connection or protocol-level failure.
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
            // Normal completion, no more rows available.
            Poll::Ready(None) => Poll::Ready(None),
        }
    }
}
