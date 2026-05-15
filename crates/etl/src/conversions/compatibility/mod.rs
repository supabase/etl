//! Destination compatibility conversion helpers.

mod base;

pub use base::{
    CellCarrier, CellCompatibilityResult, CompatibilityChecker, CompatibilityRules, CellCarrier,
    TypeCompatibilityResult,
};
pub use etl_config::shared::{DestinationTypeCompatibility, DestinationTypeCompatibilityMode};
