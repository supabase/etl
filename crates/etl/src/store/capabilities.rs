//! Aggregate store capabilities for common runtime roles.
//!
//! These facade traits collect repeated bounds behind names that describe how
//! the store is used. Code that only needs one capability should continue to
//! depend on that capability directly. Each capability already requires
//! [`crate::store::CachedStore`]; these facades add runtime sharing bounds.

use crate::store::{SchemaStore, StateStore, TableStateLifecycleStore};

/// Store capabilities required by state-only worker code.
pub trait SharedStateStore: StateStore + Clone + Send + Sync + 'static {}

impl<S> SharedStateStore for S where S: StateStore + Clone + Send + Sync + 'static {}

/// Store capabilities commonly required by destination implementations.
///
/// This is a facade trait for destinations that need to read and update ETL
/// state and schema metadata while remaining cloneable and worker-safe.
pub trait DestinationStore: SharedStateStore + SchemaStore {}

impl<S> DestinationStore for S where S: SharedStateStore + SchemaStore {}

/// Store capabilities required by the pipeline runtime.
///
/// This is a facade trait for code that needs the full runtime store surface:
/// cache preparation, table state, versioned schemas, table lifecycle
/// operations, and the concurrency bounds required by worker tasks.
pub trait PipelineStore: DestinationStore + TableStateLifecycleStore {}

impl<S> PipelineStore for S where S: DestinationStore + TableStateLifecycleStore {}
