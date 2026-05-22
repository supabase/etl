//! Aggregate store capability for destination implementations.
//!
//! Destinations often need replication state, destination metadata, and
//! versioned schemas, but they should not need table lifecycle operations.
//! Narrower code should continue to depend on [`StateStore`] or [`SchemaStore`]
//! directly.

use crate::store::{schema::SchemaStore, state::StateStore};

/// Store capabilities commonly required by destination implementations.
///
/// This is a facade trait for destinations that need to read and update ETL
/// state and schema metadata while remaining cloneable and worker-safe.
pub trait DestinationStore: StateStore + SchemaStore + Clone + Send + Sync + 'static {}

impl<S> DestinationStore for S where S: StateStore + SchemaStore + Clone + Send + Sync + 'static {}
