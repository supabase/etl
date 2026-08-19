use thiserror::Error;

/// Configuration validation errors.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// A field value violates a constraint.
    #[error("Field `{field}` {constraint}")]
    InvalidFieldValue {
        /// The name of the field that failed validation.
        field: String,
        /// Description of the constraint that was violated.
        constraint: String,
    },
}

/// Validates configuration values that can be checked without external state.
pub trait Validate {
    /// Validates this configuration.
    fn validate(&self) -> Result<(), ValidationError> {
        Ok(())
    }
}
