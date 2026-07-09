use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use utoipa::ToSchema;

/// Represents an update field where the API distinguishes an omitted field
/// from an explicit JSON `null`.
///
/// Omitted fields preserve the stored value, explicit `null` clears or resets
/// it to the stored default, and non-null values replace it.
#[derive(Debug, Clone, Default, PartialEq, ToSchema)]
pub enum UpdateField<T> {
    /// Preserve the stored value.
    #[default]
    Preserve,
    /// Clear or reset the stored value.
    Clear,
    /// Replace the stored value.
    Set(T),
}

impl<T> UpdateField<T> {
    /// Returns whether the field preserves the stored value.
    pub(crate) fn is_preserve(&self) -> bool {
        matches!(self, Self::Preserve)
    }

    /// Converts an optional value into a set-or-clear update field.
    pub(crate) fn from_option(value: Option<T>) -> Self {
        match value {
            Some(value) => Self::Set(value),
            None => Self::Clear,
        }
    }

    /// Applies this update to an optional stored value.
    pub(crate) fn apply_to_option(self, stored: Option<T>) -> Option<T> {
        match self {
            Self::Preserve => stored,
            Self::Clear => None,
            Self::Set(value) => Some(value),
        }
    }

    /// Applies this update to a concrete stored value, using a default when
    /// the field is cleared.
    pub(crate) fn apply_to_value(self, stored: T, default: impl FnOnce() -> T) -> T {
        match self {
            Self::Preserve => stored,
            Self::Clear => default(),
            Self::Set(value) => value,
        }
    }

    /// Extracts a set value, treating preserve and clear as absent.
    pub(crate) fn into_option(self) -> Option<T> {
        match self {
            Self::Set(value) => Some(value),
            Self::Preserve | Self::Clear => None,
        }
    }

    /// Restores `key` from the original object when this field preserves it.
    pub(crate) fn restore_preserved_value(&self, original: &Value, updated: &mut Value, key: &str) {
        if !self.is_preserve() {
            return;
        }

        let Some(updated) = updated.as_object_mut() else {
            return;
        };

        match original.as_object().and_then(|object| object.get(key)) {
            Some(value) => {
                updated.insert(key.to_owned(), value.clone());
            }
            None => {
                updated.remove(key);
            }
        }
    }
}

impl<T> Serialize for UpdateField<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Preserve | Self::Clear => serializer.serialize_none(),
            Self::Set(value) => value.serialize(serializer),
        }
    }
}

impl<'de, T> Deserialize<'de> for UpdateField<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<T>::deserialize(deserializer).map(Self::from_option)
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;

    #[derive(Debug, Deserialize)]
    struct Patch {
        #[serde(default)]
        value: UpdateField<String>,
    }

    #[test]
    fn update_field_deserializes_missing_as_preserve() {
        let patch: Patch = serde_json::from_value(serde_json::json!({})).unwrap();

        assert_eq!(patch.value, UpdateField::Preserve);
    }

    #[test]
    fn update_field_deserializes_null_as_clear() {
        let patch: Patch = serde_json::from_value(serde_json::json!({ "value": null })).unwrap();

        assert_eq!(patch.value, UpdateField::Clear);
    }

    #[test]
    fn update_field_deserializes_value_as_set() {
        let patch: Patch =
            serde_json::from_value(serde_json::json!({ "value": "updated" })).unwrap();

        assert_eq!(patch.value, UpdateField::Set("updated".to_owned()));
    }
}
