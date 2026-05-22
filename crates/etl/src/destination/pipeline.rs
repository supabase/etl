//! Aggregate destination capability for pipeline runtime code.
//!
//! The pipeline runtime needs destinations that can be cloned, shared across
//! worker tasks, and owned by spawned futures. Narrower code should continue to
//! depend on [`Destination`] plus only the additional bounds it actually needs.

use crate::destination::Destination;

/// Destination capabilities required by the pipeline runtime.
///
/// This is a facade trait for code that needs a destination to be cloneable,
/// shareable across worker tasks, and owned by spawned futures. Code that only
/// dispatches destination writes should depend on [`Destination`] plus the
/// narrowest additional bounds it actually needs.
pub trait PipelineDestination: Destination + Clone + Send + Sync + 'static {}

impl<D> PipelineDestination for D where D: Destination + Clone + Send + Sync + 'static {}
