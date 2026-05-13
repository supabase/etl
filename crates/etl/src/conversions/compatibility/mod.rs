//! Destination compatibility conversion helpers.

mod base;

pub use base::{
    CellCompatibilityResult, CompatibilityChecker, CompatibilityRules, TypeCompatibilityResult,
};
pub use etl_config::shared::{DestinationTypeCompatibility, DestinationTypeCompatibilityMode};
