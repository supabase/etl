//! Destination materialization helpers.

mod base;

pub use base::{
    CellMaterializationResult, DestinationMaterializer, MaterializationRules, MaterializedCell,
    TypeMaterializationResult, TypedCell,
};
pub use etl_config::shared::{DestinationTypeCompatibility, DestinationTypeCompatibilityMode};
