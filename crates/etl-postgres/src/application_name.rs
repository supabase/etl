//! Naming helpers for per-worker Postgres `application_name` values.
//!
//! Worker connections are tagged as `{base}:apply:{pipeline_id}` and
//! `{base}:tsync:{pipeline_id}:{table_oid}` so they can be identified in
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
pub const TABLE_SYNC_WORKER_TAG: &str = "tsync";

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
/// tail. All in-repo bases fit without clamping; this is a backstop for
/// future name growth.
fn with_worker_suffix(base: &str, suffix: &str) -> String {
    let budget = MAX_APPLICATION_NAME_LENGTH.saturating_sub(suffix.len());
    if base.len() > budget {
        warn!(base, suffix, "application_name base clamped to fit worker suffix");
    }

    let mut end = base.len().min(budget);
    while !base.is_char_boundary(end) {
        end -= 1;
    }

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

        // --- THEN: the name carries the tsync tag, pipeline id, and table oid ---
        assert_eq!(name, "supabase_etl_replicator_replication:tsync:42:16384");
    }

    #[test]
    fn worst_case_realistic_name_fits_without_clamping() {
        // --- GIVEN: the longest in-repo base, a 10-digit pipeline id, and a max table
        // oid ---
        let base = "supabase_etl_replicator_replication";
        let name = table_sync_worker_application_name(base, 9_999_999_999, TableId::new(u32::MAX));

        // --- THEN: the name fits the Postgres limit with the base intact ---
        assert!(name.len() <= MAX_APPLICATION_NAME_LENGTH);
        assert!(name.starts_with(base));
    }

    #[test]
    fn overlong_base_is_clamped_with_suffix_preserved() {
        // --- GIVEN: a base longer than the Postgres limit allows ---
        let base = "x".repeat(80);
        let name = table_sync_worker_application_name(&base, u64::MAX, TableId::new(u32::MAX));

        // --- THEN: the name fits and the full worker suffix survives ---
        assert_eq!(name.len(), MAX_APPLICATION_NAME_LENGTH);
        assert!(name.ends_with(&format!(":tsync:{}:{}", u64::MAX, u32::MAX)));
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
