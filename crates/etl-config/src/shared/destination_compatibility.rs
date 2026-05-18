use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Controls how destinations handle source types that do not fit exactly.
///
/// The policy is destination-agnostic. Each destination decides which source
/// types are risky for the selected mode when it materializes its physical
/// schema and encodes values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DestinationTypeCompatibility {
    /// Selected compatibility mode.
    mode: DestinationTypeCompatibilityMode,
}

impl DestinationTypeCompatibility {
    /// Creates a new compatibility policy from a mode.
    pub const fn new(mode: DestinationTypeCompatibilityMode) -> Self {
        Self { mode }
    }

    /// Creates a policy that rejects values the destination may change.
    pub const fn strict() -> Self {
        Self::new(DestinationTypeCompatibilityMode::Strict)
    }

    /// Creates a policy that favors lossless destination representations.
    pub const fn lossless() -> Self {
        Self::new(DestinationTypeCompatibilityMode::Lossless)
    }

    /// Creates a policy that allows destination-native lossy conversions.
    pub const fn lossy() -> Self {
        Self::new(DestinationTypeCompatibilityMode::Lossy)
    }

    /// Returns the selected compatibility mode.
    pub const fn mode(&self) -> DestinationTypeCompatibilityMode {
        self.mode
    }

    /// Returns whether this policy rejects silent destination changes.
    pub const fn is_strict(&self) -> bool {
        matches!(self.mode, DestinationTypeCompatibilityMode::Strict)
    }

    /// Returns whether this policy should prefer exact string representations.
    pub const fn is_lossless(&self) -> bool {
        matches!(self.mode, DestinationTypeCompatibilityMode::Lossless)
    }

    /// Returns whether this policy allows the destination to coerce values.
    pub const fn is_lossy(&self) -> bool {
        matches!(self.mode, DestinationTypeCompatibilityMode::Lossy)
    }
}

impl Default for DestinationTypeCompatibility {
    fn default() -> Self {
        Self::lossy()
    }
}

/// Destination handling mode for source and destination type mismatches.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DestinationTypeCompatibilityMode {
    /// Preserve destination-native types and reject values that may be changed.
    Strict,
    /// Materialize risky source types using exact destination representations.
    Lossless,
    /// Allow destination-compatible coercions for values that do not fit
    /// exactly.
    ///
    /// This is the default because CDC pipelines often need compatible source
    /// and destination types to keep flowing when the destination can
    /// accept an approximate representation, such as rounding a value to
    /// the destination's supported precision.
    #[default]
    Lossy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_type_compatibility_defaults_to_lossy() {
        assert_eq!(
            DestinationTypeCompatibility::default().mode(),
            DestinationTypeCompatibilityMode::Lossy
        );
    }

    #[test]
    fn destination_type_compatibility_mode_helpers_match_selected_mode() {
        assert!(DestinationTypeCompatibility::strict().is_strict());
        assert!(DestinationTypeCompatibility::lossless().is_lossless());
        assert!(DestinationTypeCompatibility::lossy().is_lossy());
    }
}
