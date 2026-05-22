//! Aggregate destination capabilities for common runtime roles.
//!
//! These facade traits collect repeated bounds behind names that describe how
//! the destination is used. Code that only dispatches destination writes should
//! depend on [`Destination`] plus only the additional bounds it actually needs.

use crate::destination::Destination;

/// Destination capabilities required by the pipeline runtime.
///
/// This is a facade trait for code that needs a destination to be cloneable,
/// shareable across worker tasks, and owned by spawned futures.
pub trait PipelineDestination: Destination + Clone + Send + Sync + 'static {}

impl<D> PipelineDestination for D where D: Destination + Clone + Send + Sync + 'static {}
