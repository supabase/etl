//! Concurrency primitives used by ETL workers and destinations.

mod shutdown;
mod stream;

pub(crate) use shutdown::{ShutdownResult, with_shutdown};
pub(crate) use stream::{
    MemoryBackpressureStream, MemoryBatchStream, apply_worker_apply_stream_id,
    table_sync_worker_apply_stream_id, table_sync_worker_copy_stream_id,
};
