/// Unique identifier for an ETL pipeline instance.
///
/// [`PipelineId`] provides a simple numeric identifier to distinguish between
/// multiple pipeline instances running concurrently. This ID is used for logging,
/// monitoring, and coordinating shutdown operations across pipeline components.
pub type PipelineId = u64;
