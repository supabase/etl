use crate::validation::ValidationFailure;

/// Formats validation failures for API error responses.
pub fn format_validation_failures(failures: Vec<ValidationFailure>) -> String {
    failures
        .into_iter()
        .map(|failure| failure.reason)
        .collect::<Vec<_>>()
        .join("; ")
}
