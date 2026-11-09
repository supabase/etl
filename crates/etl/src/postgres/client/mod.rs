mod child;
mod query;
mod raw;
mod slot_creation;
mod transaction;
mod types;
mod utils;

#[cfg(any(test, feature = "test-utils"))]
pub use child::ChildPgReplicationClient;
#[cfg(not(any(test, feature = "test-utils")))]
pub(crate) use child::ChildPgReplicationClient;
#[cfg(any(test, feature = "test-utils"))]
pub use raw::PgReplicationClient;
#[cfg(not(any(test, feature = "test-utils")))]
pub(crate) use raw::PgReplicationClient;
pub(crate) use slot_creation::SlotCreationMonitor;
#[cfg(any(test, feature = "test-utils"))]
pub use transaction::{PgChildReplicationTransaction, PgReplicationTransaction};
#[cfg(not(any(test, feature = "test-utils")))]
pub(crate) use transaction::{PgChildReplicationTransaction, PgReplicationTransaction};
#[cfg(any(test, feature = "test-utils"))]
pub use types::{CreateSlotResult, CtidPartition, GetSlotResult, SlotState};
#[cfg(not(any(test, feature = "test-utils")))]
pub(crate) use types::{CtidPartition, SlotState};
pub(crate) use types::{GetOrCreateSlotResult, PostgresConnectionUpdate};
