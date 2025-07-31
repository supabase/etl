mod concurrency;
pub mod config;
mod conversions;
pub mod destination;
pub mod error;
#[cfg(feature = "failpoints")]
pub mod failpoints;
pub mod macros;
pub mod pipeline;
mod replication;
pub mod schema;
mod state;
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;
pub mod types;
mod workers;

// We export some internals when the feature is enabled since this might be useful for tests.
#[cfg(feature = "internals")]
pub mod internals {
    pub use crate::replication::{client::PgReplicationClient, slot::get_slot_name};
    pub use crate::state::store::notify::NotifyingStateStore;
    pub use crate::state::table::{TableReplicationPhase, TableReplicationPhaseType};
    pub use crate::workers::base::WorkerType;
}
