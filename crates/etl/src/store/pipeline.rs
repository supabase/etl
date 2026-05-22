//! Aggregate store capability for pipeline runtime code.
//!
//! The pipeline runtime usually needs every store capability plus worker-safe
//! concurrency bounds. Narrower code should continue to depend on
//! [`StateStore`], [`SchemaStore`], or [`TableLifecycleStore`] directly.

use crate::store::{lifecycle::TableLifecycleStore, schema::SchemaStore, state::StateStore};

/// Store capabilities required by the pipeline runtime.
///
/// This is a facade trait for code that needs the full runtime store surface:
/// replication state, versioned schemas, table lifecycle operations, and the
/// concurrency bounds required by worker tasks. Code that only needs one
/// capability should depend on the narrower trait directly.
pub trait PipelineStore:
    StateStore + SchemaStore + TableLifecycleStore + Clone + Send + Sync + 'static
{
}

impl<S> PipelineStore for S where
    S: StateStore + SchemaStore + TableLifecycleStore + Clone + Send + Sync + 'static
{
}
