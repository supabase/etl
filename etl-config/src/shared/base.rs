use thiserror::Error;

/// Configuration validation errors.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// A field value violates a constraint.
    #[error("`{field}` {constraint}")]
    InvalidFieldValue {
        /// The name of the field that failed validation.
        field: String,
        /// Description of the constraint that was violated.
        constraint: String,
    },
}
