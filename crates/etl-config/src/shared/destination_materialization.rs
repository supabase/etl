use serde::{Deserialize, Serialize};
#[cfg(feature = "utoipa")]
use utoipa::ToSchema;

/// Controls how destinations choose column types and handle values.
///
/// The policy is destination-agnostic. Each destination decides which source
/// types are risky for the selected type strategy and which value changes are
/// needed for the selected value strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DestinationMaterializationPolicy {
    /// Strategy for selecting destination column types.
    type_strategy: TypeStrategy,
    /// Strategy for handling values that do not fit the selected type.
    value_strategy: ValueStrategy,
}

impl DestinationMaterializationPolicy {
    /// Creates a new materialization policy from independent strategies.
    pub const fn new(type_strategy: TypeStrategy, value_strategy: ValueStrategy) -> Self {
        Self { type_strategy, value_strategy }
    }

    /// Creates a policy that requires native destination types and rejects
    /// value changes.
    pub const fn native_only_reject() -> Self {
        Self::new(TypeStrategy::NativeOnly, ValueStrategy::Reject)
    }

    /// Creates a policy that uses native destination types with safe string
    /// fallbacks and rejects value changes.
    pub const fn native_or_string_reject() -> Self {
        Self::new(TypeStrategy::NativeOrString, ValueStrategy::Reject)
    }

    /// Creates a policy that uses strings for risky types and preserves source
    /// representations.
    pub const fn string_if_risky_preserve() -> Self {
        Self::new(TypeStrategy::StringIfRisky, ValueStrategy::Preserve)
    }

    /// Creates a policy that uses native destination types with safe string
    /// fallbacks and normalizes values.
    pub const fn native_or_string_normalize() -> Self {
        Self::new(TypeStrategy::NativeOrString, ValueStrategy::Normalize)
    }

    /// Returns the type materialization strategy for this policy.
    pub const fn type_strategy(&self) -> TypeStrategy {
        self.type_strategy
    }

    /// Returns the value materialization strategy for this policy.
    pub const fn value_strategy(&self) -> ValueStrategy {
        self.value_strategy
    }

    /// Returns whether this policy uses [`TypeStrategy::NativeOnly`] and
    /// [`ValueStrategy::Reject`].
    pub const fn is_native_only_reject(&self) -> bool {
        matches!(self.type_strategy, TypeStrategy::NativeOnly)
            && matches!(self.value_strategy, ValueStrategy::Reject)
    }

    /// Returns whether this policy uses [`TypeStrategy::NativeOrString`]
    /// and [`ValueStrategy::Reject`].
    pub const fn is_native_or_string_reject(&self) -> bool {
        matches!(self.type_strategy, TypeStrategy::NativeOrString)
            && matches!(self.value_strategy, ValueStrategy::Reject)
    }

    /// Returns whether this policy uses [`TypeStrategy::StringIfRisky`] and
    /// [`ValueStrategy::Preserve`].
    pub const fn is_string_if_risky_preserve(&self) -> bool {
        matches!(self.type_strategy, TypeStrategy::StringIfRisky)
            && matches!(self.value_strategy, ValueStrategy::Preserve)
    }

    /// Returns whether this policy uses [`TypeStrategy::NativeOrString`]
    /// and [`ValueStrategy::Normalize`].
    pub const fn is_native_or_string_normalize(&self) -> bool {
        matches!(self.type_strategy, TypeStrategy::NativeOrString)
            && matches!(self.value_strategy, ValueStrategy::Normalize)
    }
}

impl Default for DestinationMaterializationPolicy {
    fn default() -> Self {
        Self::native_or_string_reject()
    }
}

/// Strategy for selecting destination column types.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum TypeStrategy {
    /// Require native destination types for every source type.
    NativeOnly,
    /// Use native destination types and fall back to strings when needed.
    #[default]
    NativeOrString,
    /// Use strings for unsupported source types and for source types whose
    /// native destination representation is risky.
    StringIfRisky,
}

/// Strategy for materializing values that do not fit the selected type.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "utoipa", derive(ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum ValueStrategy {
    /// Reject values that would be changed by the destination.
    #[default]
    Reject,
    /// Normalize values into the destination type's supported domain.
    Normalize,
    /// Preserve values without local validation or normalization.
    Preserve,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn destination_materialization_policy_defaults_to_native_or_string_reject() {
        assert_eq!(
            DestinationMaterializationPolicy::default(),
            DestinationMaterializationPolicy::new(
                TypeStrategy::NativeOrString,
                ValueStrategy::Reject
            )
        );
    }

    #[test]
    fn destination_materialization_policy_helpers_match_selected_strategies() {
        assert!(DestinationMaterializationPolicy::native_only_reject().is_native_only_reject());
        assert!(
            DestinationMaterializationPolicy::native_or_string_reject()
                .is_native_or_string_reject()
        );
        assert!(
            DestinationMaterializationPolicy::string_if_risky_preserve()
                .is_string_if_risky_preserve()
        );
        assert!(
            DestinationMaterializationPolicy::native_or_string_normalize()
                .is_native_or_string_normalize()
        );
    }
}
