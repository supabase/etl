//! Naming helpers for per-worker Postgres `application_name` values.
//!
//! Worker connections are tagged as `{base}:apply:{pipeline_id}` and
//! `{base}:table_sync:{pipeline_id}:{table_oid}` so they can be identified in
//! `pg_stat_activity` and targeted individually (e.g. by
//! `pg_terminate_backend` in tests). Connections not owned by a worker keep
//! the plain base name.

use tracing::warn;

use crate::schema::TableId;

/// Maximum length of a Postgres `application_name` in bytes.
///
/// Postgres silently truncates longer values, so names are clamped locally to
/// keep the worker suffix intact.
const MAX_APPLICATION_NAME_LENGTH: usize = 63;

/// Separator between the base application name and worker suffix parts.
///
/// `:` keeps suffixes unambiguous to parse, since base names contain `_`.
const SEPARATOR: char = ':';

/// Tag identifying apply worker connections.
pub const APPLY_WORKER_TAG: &str = "apply";
/// Tag identifying table sync worker connections.
pub const TABLE_SYNC_WORKER_TAG: &str = "table_sync";

/// Returns the `application_name` for an apply worker connection.
pub fn apply_worker_application_name(base: &str, pipeline_id: u64) -> String {
    with_worker_suffix(base, &format!("{SEPARATOR}{APPLY_WORKER_TAG}{SEPARATOR}{pipeline_id}"))
}

/// Returns the `application_name` for a table sync worker connection.
pub fn table_sync_worker_application_name(
    base: &str,
    pipeline_id: u64,
    table_id: TableId,
) -> String {
    with_worker_suffix(
        base,
        &format!(
            "{SEPARATOR}{TABLE_SYNC_WORKER_TAG}{SEPARATOR}{pipeline_id}{SEPARATOR}{}",
            table_id.into_inner()
        ),
    )
}

/// Appends a worker suffix to `base`, clamping `base` so the result fits the
/// Postgres limit with the suffix intact.
///
/// The suffix carries the worker identity that tests and operators match on,
/// so on overflow the base is truncated instead of letting Postgres cut the
/// tail. In-repo bases fit without clamping while pipeline id and table oid
/// stay within 15 digits combined; beyond that the suffix still survives.
fn with_worker_suffix(base: &str, suffix: &str) -> String {
    let max_allowed_len = MAX_APPLICATION_NAME_LENGTH.saturating_sub(suffix.len());
    if base.len() > max_allowed_len {
        warn!(base, suffix, "application_name base clamped to fit worker suffix");
    }

    let end = base.floor_char_boundary(max_allowed_len);

    format!("{}{suffix}", &base[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_worker_name_appends_tag_and_pipeline_id() {
        // --- GIVEN: a base name and pipeline id ---
        let name = apply_worker_application_name("supabase_etl_replicator_replication", 42);

        // --- THEN: the name carries the apply tag and pipeline id ---
        assert_eq!(name, "supabase_etl_replicator_replication:apply:42");
    }

    #[test]
    fn table_sync_worker_name_appends_tag_pipeline_id_and_table_oid() {
        // --- GIVEN: a base name, pipeline id, and table id ---
        let name = table_sync_worker_application_name(
            "supabase_etl_replicator_replication",
            42,
            TableId::new(16384),
        );

        // --- THEN: the name carries the table_sync tag, pipeline id, and table oid ---
        assert_eq!(name, "supabase_etl_replicator_replication:table_sync:42:16384");
    }

    #[test]
    fn name_fits_without_clamping_up_to_15_combined_id_digits() {
        // --- GIVEN: the longest in-repo base with pipeline id and table oid summing to
        // 15 digits, the documented no-clamp bound ---
        let base = "supabase_etl_replicator_replication";
        let name = table_sync_worker_application_name(base, 99_999, TableId::new(u32::MAX));

        // --- THEN: the name fits the Postgres limit with the base intact ---
        assert!(name.len() <= MAX_APPLICATION_NAME_LENGTH);
        assert!(name.starts_with(base));
    }

    #[test]
    fn name_beyond_no_clamp_bound_keeps_suffix_and_prefix_filterable_base() {
        // --- GIVEN: the longest in-repo base with ids one digit past the no-clamp
        // bound ---
        let base = "supabase_etl_replicator_replication";
        let name = table_sync_worker_application_name(base, 999_999, TableId::new(u32::MAX));

        // --- THEN: the base clamps but the suffix and the etl prefix survive ---
        assert_eq!(name.len(), MAX_APPLICATION_NAME_LENGTH);
        assert!(name.ends_with(&format!(":table_sync:999999:{}", u32::MAX)));
        assert!(name.starts_with("supabase_etl_"));
    }

    #[test]
    fn overlong_base_is_clamped_with_suffix_preserved() {
        // --- GIVEN: a base longer than the Postgres limit allows ---
        let base = "x".repeat(80);
        let name = table_sync_worker_application_name(&base, u64::MAX, TableId::new(u32::MAX));

        // --- THEN: the name fits and the full worker suffix survives ---
        assert_eq!(name.len(), MAX_APPLICATION_NAME_LENGTH);
        assert!(name.ends_with(&format!(":table_sync:{}:{}", u64::MAX, u32::MAX)));
    }

    #[test]
    fn clamping_respects_multibyte_char_boundaries() {
        // --- GIVEN: an overlong base of multibyte characters ---
        let base = "é".repeat(60);
        let name = apply_worker_application_name(&base, u64::MAX);

        // --- THEN: clamping does not split a character and the suffix survives ---
        assert!(name.len() <= MAX_APPLICATION_NAME_LENGTH);
        assert!(name.ends_with(&format!(":apply:{}", u64::MAX)));
    }
}
