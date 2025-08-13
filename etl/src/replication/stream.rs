use etl_postgres::schema::ColumnSchema;
use etl_postgres::time::POSTGRES_EPOCH;
use futures::{Stream, ready};
use pin_project_lite::pin_project;
use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio_postgres::CopyOutStream;
use tokio_postgres::types::PgLsn;
use tracing::debug;

use crate::conversions::table_row::{TableRow, TableRowConverter};
use crate::error::EtlError;
use crate::error::{ErrorKind, EtlResult};
use crate::etl_error;

/// The amount of milliseconds between two consecutive status updates in case no forced update
/// is requested.
const STATUS_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

pin_project! {
    /// A stream that yields rows from a PostgreSQL COPY operation.
    ///
    /// This stream wraps a [`CopyOutStream`] and converts each row into a [`TableRow`]
    /// using the provided column schemas. The conversion process handles both text and
    /// binary format data.
    #[must_use = "streams do nothing unless polled"]
    pub struct TableCopyStream<'a> {
        #[pin]
        stream: CopyOutStream,
        column_schemas: &'a [ColumnSchema],
    }
}

impl<'a> TableCopyStream<'a> {
    /// Creates a new [`TableCopyStream`] from a [`CopyOutStream`] and column schemas.
    ///
    /// The column schemas are used to convert the raw PostgreSQL data into [`TableRow`]s.
    pub fn wrap(stream: CopyOutStream, column_schemas: &'a [ColumnSchema]) -> Self {
        Self {
            stream,
            column_schemas,
        }
    }
}

impl<'a> Stream for TableCopyStream<'a> {
    type Item = EtlResult<TableRow>;

    /// Polls the stream for the next converted table row with comprehensive error handling.
    ///
    /// This method handles the complex process of converting raw PostgreSQL COPY data into
    /// structured [`TableRow`] objects, with detailed error reporting for various failure modes.
    ///
    /// # Error Handling Scenarios
    ///
    /// ## PostgreSQL Protocol Errors
    /// - **Connection termination**: Network failures or PostgreSQL server shutdown
    /// - **Authentication issues**: Token expiration or permission changes during long operations
    /// - **Resource limits**: PostgreSQL memory/connection limits exceeded
    /// - **Protocol violations**: Malformed COPY protocol messages
    ///
    /// ## Data Conversion Errors
    /// - **Invalid format**: COPY data doesn't match expected text format specification
    /// - **Type conversion failures**: Data cannot be converted to expected column types
    /// - **Character encoding issues**: Invalid UTF-8 sequences in text data
    /// - **Schema mismatches**: Column count doesn't match provided schema
    ///
    /// ## Edge Cases and Recovery
    ///
    /// ### Empty Tables
    /// - Empty tables immediately return `None` without error
    /// - This is a normal completion scenario, not an error condition
    ///
    /// ### Partial Row Data
    /// - Incomplete rows due to connection interruption result in conversion errors
    /// - These errors preserve context about which column failed conversion
    /// - Recovery requires restarting the COPY operation from the beginning
    ///
    /// ### Large Tables
    /// - Memory pressure from large rows is handled by streaming conversion
    /// - Individual row size limits are enforced by PostgreSQL, not this stream
    /// - Back-pressure is automatically handled by the underlying stream implementation
    ///
    /// ### Schema Evolution
    /// - Column schema changes during COPY operations result in conversion errors
    /// - The schema parameter is static for the duration of the stream
    /// - Schema changes require creating a new stream with updated column information
    ///
    /// # Performance Considerations
    ///
    /// ## Conversion Overhead
    /// - Each row incurs parsing and type conversion costs
    /// - Text format parsing is optimized but still has CPU overhead
    /// - Binary format support (TODO) would reduce conversion costs significantly
    ///
    /// ## Memory Management
    /// - Rows are converted individually to minimize memory footprint
    /// - No batching occurs at this level - handled by higher-level batch streams
    /// - Failed conversions don't leak memory from partial parsing
    ///
    /// ## Error Propagation
    /// - Conversion errors immediately terminate the stream
    /// - No attempt is made to skip invalid rows to maintain data consistency
    /// - Error context includes column information for debugging
    ///
    /// # Future Enhancements
    ///
    /// The TODO comment indicates planned support for binary format conversion, which would:
    /// - Eliminate text parsing overhead for supported data types
    /// - Reduce CPU usage for large-scale data copying operations
    /// - Provide more accurate type preservation for complex data types
    /// - Enable pluggable conversion strategies based on data format detection
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.stream.poll_next(cx)) {
            // TODO: allow pluggable table row conversion based on if the data is in text or binary format.
            Some(Ok(row)) => {
                // CONVERSION PHASE: Transform raw bytes into structured TableRow
                // This is where most errors occur due to data format or type issues
                match TableRowConverter::try_from(&row, this.column_schemas) {
                    Ok(row) => Poll::Ready(Some(Ok(row))),
                    Err(err) => {
                        // CONVERSION ERROR: Preserve full error context for debugging
                        // These errors typically indicate schema mismatches or data corruption
                        Poll::Ready(Some(Err(err)))
                    }
                }
            }
            Some(Err(err)) => {
                // PROTOCOL ERROR: PostgreSQL connection or protocol-level failure
                // Convert tokio-postgres errors to ETL errors with additional context
                Poll::Ready(Some(Err(err.into())))
            }
            None => {
                // STREAM END: Normal completion - no more rows available
                // This is the success termination condition for table copying
                Poll::Ready(None)
            }
        }
    }
}

pin_project! {
    pub struct EventsStream {
        #[pin]
        stream: LogicalReplicationStream,
        last_update: Option<Instant>,
        last_flush_lsn: Option<PgLsn>,
        last_apply_lsn: Option<PgLsn>,
    }
}

impl EventsStream {
    /// Creates a new [`EventsStream`] from a [`LogicalReplicationStream`].
    pub fn wrap(stream: LogicalReplicationStream) -> Self {
        Self {
            stream,
            last_update: None,
            last_flush_lsn: None,
            last_apply_lsn: None,
        }
    }

    /// Sends a status update to the PostgreSQL server with comprehensive error handling and optimization.
    ///
    /// This method implements sophisticated status update logic that balances PostgreSQL's need for
    /// progress information with network efficiency and system performance. It handles multiple
    /// error scenarios and edge cases related to time synchronization and network communication.
    ///
    /// # Status Update Protocol
    ///
    /// PostgreSQL logical replication requires periodic status updates to:
    /// - **Progress tracking**: Inform the server how far the client has processed
    /// - **WAL cleanup**: Allow PostgreSQL to clean up old WAL files based on client progress
    /// - **Heartbeat mechanism**: Detect connection failures and client health
    /// - **Flow control**: Enable PostgreSQL to manage replication slot resource usage
    ///
    /// # LSN Position Semantics
    ///
    /// The three LSN values have distinct meanings in the replication protocol:
    /// - **write_lsn**: Last position received by the client (in-memory, not persisted)
    /// - **flush_lsn**: Last position durably written to destination storage
    /// - **apply_lsn**: Last position where all effects are visible to queries
    ///
    /// For async replication scenarios, `flush_lsn` and `apply_lsn` are typically identical,
    /// representing the point up to which data has been successfully replicated.
    ///
    /// # Update Frequency Optimization
    ///
    /// The method implements intelligent update frequency control to avoid overwhelming PostgreSQL:
    ///
    /// ## Forced Updates
    /// - Sent immediately when `force = true` (typically when PostgreSQL requests an update)
    /// - Used for immediate acknowledgment of specific replication milestones
    /// - Bypasses all frequency limiting logic
    ///
    /// ## Automatic Updates
    /// - Sent when meaningful progress has been made (`flush_lsn` or `apply_lsn` changes)
    /// - Rate-limited to avoid excessive network traffic (max once per 100ms)
    /// - Intentionally ignores `write_lsn` changes to focus on durable progress
    ///
    /// # Error Handling Scenarios
    ///
    /// ## Time Synchronization Errors
    /// - **System clock backwards**: Can occur due to NTP adjustments or manual time changes
    /// - **Clock overflow**: Extremely rare but possible with system clock issues
    /// - **PostgreSQL epoch calculation**: Handles conversion from system time to PostgreSQL format
    ///
    /// ## Network and Protocol Errors
    /// - **Connection failures**: Network interruption during status update transmission
    /// - **Authentication expiration**: Long-running replication sessions may face auth issues
    /// - **Protocol violations**: Malformed status update messages or protocol state issues
    /// - **PostgreSQL server errors**: Server-side resource limits or configuration issues
    ///
    /// ## Edge Cases and Recovery
    ///
    /// ### First Update
    /// - Initial status update when no previous state exists
    /// - All tracking variables are initialized after successful send
    /// - Failure during first update doesn't corrupt state tracking
    ///
    /// ### Large Time Gaps
    /// - Handles scenarios where system is suspended or heavily loaded
    /// - Time-based rate limiting accounts for arbitrary time gaps
    /// - No special handling needed for long delays between updates
    ///
    /// ### LSN Regression
    /// - LSN values are monotonically increasing in normal operation
    /// - Regression typically indicates serious replication or system issues
    /// - Updates proceed normally but should be investigated at higher levels
    ///
    /// # Performance Considerations
    ///
    /// ## Network Efficiency
    /// - Batches multiple progress updates into single status message
    /// - Avoids redundant updates when no meaningful progress occurred
    /// - Balances update frequency with PostgreSQL's cleanup needs
    ///
    /// ## CPU Overhead
    /// - Time calculations are minimal overhead (microsecond precision)
    /// - State tracking uses simple comparison operations
    /// - Error path handling is optimized for the common success case
    ///
    /// ## Memory Usage
    /// - Maintains minimal state for update tracking (timestamps and LSNs)
    /// - No allocation required for status update operations
    /// - Error handling doesn't introduce memory leaks
    ///
    /// # Integration Notes
    ///
    /// This method is typically called from the main apply loop at regular intervals
    /// and whenever PostgreSQL explicitly requests a status update. The calling code
    /// should handle returned errors appropriately, as status update failures may
    /// indicate serious connectivity or system issues requiring pipeline restart.
    pub async fn send_status_update(
        self: Pin<&mut Self>,
        write_lsn: PgLsn,
        flush_lsn: PgLsn,
        apply_lsn: PgLsn,
        force: bool,
    ) -> EtlResult<()> {
        let this = self.project();

        // If we are not forced to send an update, we can willingly do so based on a set of conditions.
        if !force
            && let (Some(last_update), Some(last_flush), Some(last_apply)) = (
                this.last_update.as_mut(),
                this.last_flush_lsn.as_mut(),
                this.last_apply_lsn.as_mut(),
            )
        {
            // The reason for only checking `flush_lsn` and `apply_lsn` is that if we are not
            // forced to send a status update to Postgres (when reply is requested), we want to just
            // notify it in case we actually durably flushed and persisted events, which is signalled via
            // the two aforementioned fields. The `write_lsn` field is mostly used by Postgres for
            // tracking what was received by the replication client but not what the client actually
            // safely stored.
            //
            // If we were to check `write_lsn` too, we would end up sending updates more frequently
            // when they are not requested, simply because the `write_lsn` is updated for every
            // incoming message in the apply loop.
            if flush_lsn == *last_flush
                && apply_lsn == *last_apply
                && last_update.elapsed() < STATUS_UPDATE_INTERVAL
            {
                return Ok(());
            }
        }

        // The client's system clock at the time of transmission, as microseconds since midnight
        // on 2000-01-01.
        let ts = POSTGRES_EPOCH
            .elapsed()
            .map_err(|e| {
                etl_error!(
                    ErrorKind::InvalidState,
                    "Invalid Postgres epoch",
                    e.to_string()
                )
            })?
            .as_micros() as i64;

        this.stream
            .standby_status_update(write_lsn, flush_lsn, apply_lsn, ts, 0)
            .await?;

        debug!(
            "status update successfully sent (write_lsn = {}, flush_lsn = {}, apply_lsn = {})",
            write_lsn, flush_lsn, apply_lsn
        );

        // Update the state after successful send.
        *this.last_update = Some(Instant::now());
        *this.last_flush_lsn = Some(flush_lsn);
        *this.last_apply_lsn = Some(apply_lsn);

        Ok(())
    }
}

impl Stream for EventsStream {
    type Item = EtlResult<ReplicationMessage<LogicalReplicationMessage>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some(Ok(item))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
