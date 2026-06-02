/// Returns `true` if the error is a unique constraint violation error.
pub fn is_unique_constraint_violation_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            // 23505 is Postgres's unique constraint violation code.
            db_err.code().as_deref() == Some("23505")
        }
        _ => false,
    }
}
