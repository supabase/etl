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

    /// Creates a policy that requires native destination types and exact
    /// values.
    pub const fn strict() -> Self {
        Self::new(DestinationTypeCompatibilityMode::Strict)
    }

    /// Creates a policy that favors native destination types with safe string
    /// fallbacks.
    pub const fn compatible() -> Self {
        Self::new(DestinationTypeCompatibilityMode::Compatible)
    }

    /// Creates a policy that favors exact source representations.
    pub const fn preserve() -> Self {
        Self::new(DestinationTypeCompatibilityMode::Preserve)
    }

    /// Creates a policy that allows destination-native value coercions.
    pub const fn coerce() -> Self {
        Self::new(DestinationTypeCompatibilityMode::Coerce)
    }

    /// Returns the selected compatibility mode.
    pub const fn mode(&self) -> DestinationTypeCompatibilityMode {
        self.mode
    }

    /// Returns whether this policy requires native destination types and exact
    /// values.
    pub const fn is_strict(&self) -> bool {
        matches!(self.mode, DestinationTypeCompatibilityMode::Strict)
    }

    /// Returns whether this policy should use safe destination-native types.
    pub const fn is_compatible(&self) -> bool {
        matches!(self.mode, DestinationTypeCompatibilityMode::Compatible)
    }

    /// Returns whether this policy should prefer exact source representations.
    pub const fn is_preserve(&self) -> bool {
        matches!(self.mode, DestinationTypeCompatibilityMode::Preserve)
    }

    /// Returns whether this policy allows the destination to coerce values.
    pub const fn is_coerce(&self) -> bool {
        matches!(self.mode, DestinationTypeCompatibilityMode::Coerce)
    }
}

impl Default for DestinationTypeCompatibility {
    fn default() -> Self {
        Self::compatible()
    }
}

/// Destination handling mode for source and destination type mismatches.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum DestinationTypeCompatibilityMode {
    /// Require native destination types and exact destination values.
    Strict,
    /// Prefer native destination types, fall back to exact strings when needed,
    /// and reject values that would change.
    #[default]
    Compatible,
    /// Preserve exact source representations, using strings for risky types.
    Preserve,
    /// Prefer native destination types and allow documented value changes.
    ///
    /// Many CDC pipelines benefit from this mode because they need queryable
    /// destination types to keep flowing when the destination can accept an
    /// approximate representation, such as rounding a value to the supported
    /// precision.
    Coerce,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_type_compatibility_defaults_to_compatible() {
        assert_eq!(
            DestinationTypeCompatibility::default().mode(),
            DestinationTypeCompatibilityMode::Compatible
        );
    }

    #[test]
    fn destination_type_compatibility_mode_helpers_match_selected_mode() {
        assert!(DestinationTypeCompatibility::strict().is_strict());
        assert!(DestinationTypeCompatibility::compatible().is_compatible());
        assert!(DestinationTypeCompatibility::preserve().is_preserve());
        assert!(DestinationTypeCompatibility::coerce().is_coerce());
    }
}
