//! Destination materialization helpers.

mod base;

pub use base::{
    CellMaterializationOutcome, CellMaterializationResult, DestinationMaterializer,
    MaterializationRules, MaterializedCell, TypeMaterializationResult, TypedCell,
};
pub use etl_config::shared::{DestinationMaterializationPolicy, TypeStrategy, ValueStrategy};
