use serde::{Deserialize, Deserializer, Serialize, Serializer};
use utoipa::{
    __dev::ComposeSchema,
    ToSchema,
    openapi::{RefOr, schema::Schema},
};

/// Represents an update field where the API distinguishes an omitted field
/// from an explicit JSON `null`.
///
/// Omitted fields preserve the stored value, explicit `null` clears optional
/// values or resets defaulted values, and non-null values replace the stored
/// value.
///
/// These semantics apply only at the field containing [`UpdateField`]. When
/// `T` is a structured configuration, a non-null value is deserialized as a
/// complete `T` and replaces the complete stored value. Members omitted inside
/// that value use `T`'s deserialization defaults; they do not preserve members
/// from the stored value. Nested member-by-member patching requires a dedicated
/// update type whose members are themselves [`UpdateField`] values.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum UpdateField<T> {
    /// Preserve the stored value.
    #[default]
    Preserve,
    /// Clear or reset the stored value.
    Clear,
    /// Replace the stored value.
    Set(T),
}

// Model the wire value as `null | T`; omission is represented by the enclosing
// object's absent field.
impl<T> ComposeSchema for UpdateField<T>
where
    T: ComposeSchema,
{
    fn compose(schemas: Vec<RefOr<Schema>>) -> RefOr<Schema> {
        <Option<T>>::compose(schemas)
    }
}

// Forward nested schema registration for composed `T` values.
impl<T> ToSchema for UpdateField<T>
where
    T: ComposeSchema + ToSchema,
{
    fn schemas(schemas: &mut Vec<(String, RefOr<Schema>)>) {
        T::schemas(schemas);
    }
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

    /// Applies this update to an optional stored value that has a default when
    /// omitted during creation.
    pub(crate) fn apply_to_defaulted_option(
        self,
        stored: Option<T>,
        default: impl FnOnce() -> T,
    ) -> Option<T> {
        match self {
            Self::Preserve => stored,
            Self::Clear => Some(default()),
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

    /// Applies this update to a required stored value, returning an error when
    /// the field is cleared.
    pub(crate) fn apply_to_required<E>(self, stored: T, cleared: E) -> Result<T, E> {
        match self {
            Self::Preserve => Ok(stored),
            Self::Clear => Err(cleared),
            Self::Set(value) => Ok(value),
        }
    }

    /// Resolves this update into a required value when no stored value exists.
    pub(crate) fn into_required<E>(self, missing: E, cleared: E) -> Result<T, E> {
        match self {
            Self::Preserve => Err(missing),
            Self::Clear => Err(cleared),
            Self::Set(value) => Ok(value),
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
    use serde_json::json;
    use utoipa::{OpenApi, ToSchema};

    use super::*;

    #[derive(Debug, Deserialize)]
    struct Patch {
        #[serde(default)]
        value: UpdateField<String>,
    }

    #[allow(dead_code)]
    #[derive(ToSchema)]
    struct PatchSchema {
        value: UpdateField<String>,
    }

    #[derive(OpenApi)]
    #[openapi(components(schemas(PatchSchema)))]
    struct ApiDoc;

    #[test]
    fn update_field_deserializes_missing_as_preserve() {
        let patch: Patch = serde_json::from_value(json!({})).unwrap();

        assert_eq!(patch.value, UpdateField::Preserve);
    }

    #[test]
    fn update_field_deserializes_null_as_clear() {
        let patch: Patch = serde_json::from_value(json!({ "value": null })).unwrap();

        assert_eq!(patch.value, UpdateField::Clear);
    }

    #[test]
    fn update_field_deserializes_value_as_set() {
        let patch: Patch = serde_json::from_value(json!({ "value": "updated" })).unwrap();

        assert_eq!(patch.value, UpdateField::Set("updated".to_owned()));
    }

    #[test]
    fn update_field_schema_matches_wire_format() {
        let openapi = serde_json::to_value(ApiDoc::openapi()).unwrap();
        let schema = &openapi["components"]["schemas"]["UpdateField_String"];

        assert_eq!(schema, &json!({ "oneOf": [{ "type": "null" }, { "type": "string" }] }));
    }

    #[test]
    fn update_field_applies_to_required_stored_value() {
        assert_eq!(UpdateField::Preserve.apply_to_required("stored", "cleared"), Ok("stored"));
        assert_eq!(UpdateField::Clear.apply_to_required("stored", "cleared"), Err("cleared"));
        assert_eq!(
            UpdateField::Set("updated").apply_to_required("stored", "cleared"),
            Ok("updated")
        );
    }

    #[test]
    fn update_field_resolves_required_value_without_stored_value() {
        assert_eq!(
            UpdateField::<&str>::Preserve.into_required("missing", "cleared"),
            Err("missing")
        );
        assert_eq!(UpdateField::<&str>::Clear.into_required("missing", "cleared"), Err("cleared"));
        assert_eq!(UpdateField::Set("updated").into_required("missing", "cleared"), Ok("updated"));
    }
}
