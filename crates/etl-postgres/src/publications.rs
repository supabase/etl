//! Naming helpers for ETL-created PostgreSQL publications.

/// Prefix used for publications created by Supabase ETL.
pub const ETL_PUBLICATION_PREFIX: &str = "supabase_etl_publication";

/// Returns the deterministic ETL publication name for a pipeline.
pub fn etl_publication_name(pipeline_id: i64) -> String {
    format!("{ETL_PUBLICATION_PREFIX}_{pipeline_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_name_contains_pipeline_id() {
        assert_eq!(etl_publication_name(42), "supabase_etl_publication_42");
        assert!(etl_publication_name(i64::MAX).len() <= 63);
    }
}
