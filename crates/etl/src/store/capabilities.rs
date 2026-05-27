//! Aggregate store capabilities for common runtime roles.
//!
//! These facade traits collect repeated bounds behind names that describe how
//! the store is used. Code that only needs one capability should continue to
//! depend on [`StateStore`], [`SchemaStore`], or [`TableLifecycleStore`]
//! directly.

use crate::store::{lifecycle::TableLifecycleStore, schema::SchemaStore, state::StateStore};

/// Store capabilities required by state-only worker code.
pub trait SharedStateStore: StateStore + Clone + Send + Sync + 'static {}

impl<S> SharedStateStore for S where S: StateStore + Clone + Send + Sync + 'static {}

/// Store capabilities commonly required by destination implementations.
///
/// This is a facade trait for destinations that need to read and update ETL
/// state and schema metadata while remaining cloneable and worker-safe.
pub trait DestinationStore: StateStore + SchemaStore + Clone + Send + Sync + 'static {}

impl<S> DestinationStore for S where S: StateStore + SchemaStore + Clone + Send + Sync + 'static {}

/// Store capabilities required by the pipeline runtime.
///
/// This is a facade trait for code that needs the full runtime store surface:
/// table state, versioned schemas, table lifecycle operations, and the
/// concurrency bounds required by worker tasks.
pub trait PipelineStore:
    StateStore + SchemaStore + TableLifecycleStore + Clone + Send + Sync + 'static
{
}

impl<S> PipelineStore for S where
    S: StateStore + SchemaStore + TableLifecycleStore + Clone + Send + Sync + 'static
{
}
