//! Replication-slot WAL retention recommendations.

use etl_config::shared::TableSyncCopyConfig;
use sqlx::{FromRow, PgPool, postgres::types::Oid};
use tracing::warn;

use crate::validation::ValidationFailure;

/// Number of bytes in one mebibyte.
const MEBIBYTE: u128 = 1024 * 1024;
/// Number of bytes in one gibibyte.
const GIBIBYTE: u128 = 1024 * MEBIBYTE;
/// Number of bytes in one tebibyte.
const TEBIBYTE: u128 = 1024 * GIBIBYTE;
/// Number of mebibytes in one gibibyte.
const MEBIBYTES_PER_GIBIBYTE: i64 = 1024;
/// Minimum practical per-slot WAL retention for logical replication.
const MIN_SLOT_WAL_KEEP_SIZE_MB: i64 = MEBIBYTES_PER_GIBIBYTE;
/// Initial sync throughput used to estimate how long a table pins WAL.
const ASSUMED_COPY_BYTES_PER_SECOND: u128 = 10 * MEBIBYTE;
/// Numerator of the largest-table fraction used without useful WAL history.
const TABLE_SIZE_FALLBACK_NUMERATOR: u128 = 15;
/// Denominator of the largest-table fraction used without useful WAL history.
const TABLE_SIZE_FALLBACK_DENOMINATOR: u128 = 100;
/// Numerator of the safety factor applied to an observed average WAL rate.
const HISTORICAL_WAL_SAFETY_NUMERATOR: u128 = 3;
/// Denominator of the safety factor applied to an observed average WAL rate.
const HISTORICAL_WAL_SAFETY_DENOMINATOR: u128 = 2;
/// Fixed time allowance for setup, durability barriers, and catchup
/// coordination.
const COPY_OVERHEAD_SECONDS: u128 = 2 * 60;
/// Minimum statistics age before the average WAL rate informs a recommendation.
const MIN_WAL_STATISTICS_SECONDS: u64 = 60 * 60;
/// Largest value pipeline preflight automatically recommends, in mebibytes.
const MAX_AUTOMATIC_RECOMMENDATION_MB: i64 = 1024 * MEBIBYTES_PER_GIBIBYTE;

/// Inputs used to recommend a bounded replication-slot WAL retention setting.
#[derive(Debug, FromRow)]
struct RecommendationInputs {
    /// Qualified name of the largest logical table selected for initial sync.
    largest_table_name: String,
    /// On-disk bytes used by the largest logical table, including TOAST but
    /// excluding indexes.
    largest_table_bytes: i64,
    /// Cluster-wide WAL bytes generated per second since statistics were reset.
    average_wal_bytes_per_second: f64,
    /// Seconds covered by the cluster-wide WAL statistics.
    wal_statistics_seconds: f64,
}

/// Source data used to calculate a slot WAL retention recommendation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecommendationBasis {
    /// At least one hour of WAL statistics was available.
    HistoricalWalRate,
    /// Usable WAL history was unavailable, so table size was used instead.
    TableSizeFallback,
}

impl RecommendationBasis {
    /// Explains the estimate's basis and its dependency on future write
    /// activity.
    fn workload_context(self) -> &'static str {
        match self {
            Self::HistoricalWalRate => {
                "The estimate uses the available historical average WAL rate for the source \
                 Postgres instance but cannot predict activity during initial sync. If the source \
                 remains idle, substantially less may be sufficient; if writes continue, use this \
                 value as a conservative baseline."
            }
            Self::TableSizeFallback => {
                "The estimate uses a table-size fallback because usable WAL history is \
                 unavailable. If the source remains idle during initial sync, substantially less \
                 may be sufficient; if writes continue, use this value as a conservative baseline."
            }
        }
    }
}

/// A replication-slot WAL retention recommendation.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct SlotWalKeepSizeRecommendation {
    /// Qualified name of the table which determined the initial sync duration.
    largest_table_name: String,
    /// Estimated on-disk table bytes used by the model.
    largest_table_bytes: u64,
    /// Recommended `max_slot_wal_keep_size` in megabytes.
    recommended_mb: i64,
    /// Whether the calculated recommendation exceeded the automatic cap.
    recommendation_was_capped: bool,
    /// Source data used to calculate the recommendation.
    basis: RecommendationBasis,
}

/// Best-effort estimates a slot WAL retention limit for initial sync.
///
/// Query errors, including source statement timeouts, return `None` because
/// this recommendation is advisory and must not prevent pipeline creation.
pub(super) async fn recommend_slot_wal_keep_size(
    source_pool: &PgPool,
    publication_name: &str,
    table_sync_copy: &TableSyncCopyConfig,
) -> Option<SlotWalKeepSizeRecommendation> {
    let (include_selected_table_ids, selected_table_ids): (Option<bool>, Vec<Oid>) =
        match table_sync_copy {
            TableSyncCopyConfig::IncludeAllTables => (None, Vec::new()),
            TableSyncCopyConfig::SkipAllTables => return None,
            TableSyncCopyConfig::IncludeTables { table_ids } => {
                (Some(true), table_ids.iter().copied().map(Oid).collect())
            }
            TableSyncCopyConfig::SkipTables { table_ids } => {
                (Some(false), table_ids.iter().copied().map(Oid).collect())
            }
        };

    // The catalog surface used below is shared by every supported PostgreSQL
    // version (14 through 18). PostgreSQL size functions inspect relation
    // storage without scanning table rows. The recursive inheritance tree
    // matches the initial sync `select` behavior: unless `only` is specified,
    // PostgreSQL scans both ordinary inheritance descendants and declarative
    // partitions. `pg_table_size` includes table forks and TOAST, which the
    // initial sync reads, but excludes indexes, which it does not read.
    let query = sqlx::query_as::<_, RecommendationInputs>(
        r#"
        with recursive selected_tables as (
            select distinct
                c.oid,
                format('%I.%I', n.nspname, c.relname) as table_name
            from pg_publication p
            cross join lateral pg_get_publication_tables(p.pubname) gpt
            join pg_class c on c.oid = gpt.relid
            join pg_namespace n on n.oid = c.relnamespace
            where p.pubname = $1
              and (
                  $2::boolean is null
                  or (c.oid = any($3::oid[])) = $2
              )
        ),
        relation_tree(root_oid, relation_oid) as (
            select oid, oid
            from selected_tables

            union

            select rt.root_oid, i.inhrelid
            from relation_tree rt
            join pg_inherits i on i.inhparent = rt.relation_oid
        ),
        table_sizes as (
            select
                st.oid,
                st.table_name,
                coalesce(sum(pg_table_size(rt.relation_oid)), 0)::bigint as table_bytes
            from selected_tables st
            join relation_tree rt on rt.root_oid = st.oid
            group by st.oid, st.table_name
        ),
        wal_statistics as (
            select
                wal_bytes::double precision as wal_bytes,
                greatest(
                    extract(epoch from statement_timestamp() - stats_reset),
                    0
                )::double precision as wal_statistics_seconds
            from pg_stat_wal
        )
        select
            ts.table_name as largest_table_name,
            ts.table_bytes as largest_table_bytes,
            coalesce(
                ws.wal_bytes / nullif(ws.wal_statistics_seconds, 0),
                0::double precision
            ) as average_wal_bytes_per_second,
            ws.wal_statistics_seconds
        from table_sizes ts
        cross join wal_statistics ws
        order by ts.table_bytes desc, ts.oid
        limit 1
        "#,
    )
    .bind(publication_name)
    .bind(include_selected_table_ids)
    .bind(&selected_table_ids);

    match query.fetch_optional(source_pool).await {
        Ok(inputs) => inputs.map(build_recommendation),
        Err(error) => {
            warn!(error = %error, "failed to estimate slot wal retention");
            None
        }
    }
}

/// Builds validation failures for a configured slot WAL retention limit.
pub(super) fn slot_wal_keep_size_failures(
    max_slot_wal_keep_size_mb: i64,
    recommendation: Option<&SlotWalKeepSizeRecommendation>,
) -> Vec<ValidationFailure> {
    match max_slot_wal_keep_size_mb {
        -1 => vec![ValidationFailure::warning(
            "Unlimited Slot WAL Retention",
            "`max_slot_wal_keep_size` is unlimited. A paused, disconnected, or stalled pipeline \
             can retain WAL until the source disk fills.\n\nSet a bounded value sized for \
             expected write volume, initial sync duration, downtime, and available disk. Change \
             it in `postgresql.conf` or the managed-service database parameter settings, then \
             reload Postgres.",
        )],
        0 => vec![ValidationFailure::critical(
            "Slot WAL Retention Disabled",
            "`max_slot_wal_keep_size` is 0 MB. A slot can be invalidated at a checkpoint as soon \
             as the pipeline falls behind, forcing slot recreation or a restart of the table's \
             initial sync.\n\nSet a positive value sized for expected write volume, downtime, and \
             available disk in `postgresql.conf` or the managed-service database parameter \
             settings, then reload Postgres.",
        )],
        configured_mb if configured_mb > 0 => match recommendation {
            Some(recommendation) if recommendation.recommendation_was_capped => {
                vec![capped_recommendation_warning(configured_mb, recommendation)]
            }
            Some(recommendation) if configured_mb < recommendation.recommended_mb => {
                vec![recommendation_warning(configured_mb, recommendation)]
            }
            None if configured_mb < MIN_SLOT_WAL_KEEP_SIZE_MB => {
                vec![ValidationFailure::warning(
                    "Low Slot WAL Retention",
                    format!(
                        "`max_slot_wal_keep_size` is {configured_mb} MB, which is below the \
                         pipeline's general planning floor of {MIN_SLOT_WAL_KEEP_SIZE_MB} MB. No \
                         source-specific estimate is available: required retention depends on WAL \
                         generated during initial sync or other pipeline downtime. A smaller \
                         value may work if the source stays idle, but active writes can exhaust \
                         it quickly.\n\nReview expected write activity and available disk. If \
                         needed, increase the setting in `postgresql.conf` or the managed-service \
                         database parameter settings, then reload Postgres."
                    ),
                )]
            }
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// Builds a moderate slot WAL retention recommendation from database data.
///
/// The model is:
///
/// `max(1 GiB, 15% * largest_table_bytes, 1.5 * average_wal_rate
/// * (largest_table_bytes / 10 MiB/s + 2 minutes))`.
///
/// The historical term is included only once the statistics cover at least one
/// hour. The result is rounded up to a whole GiB and capped at 1 TiB so an
/// advisory does not present an implausibly large automatic setting. Saturating
/// arithmetic keeps malformed or extreme catalog statistics from wrapping into
/// an unsafe small recommendation.
fn build_recommendation(inputs: RecommendationInputs) -> SlotWalKeepSizeRecommendation {
    let largest_table_bytes = u128::try_from(inputs.largest_table_bytes).unwrap_or(0);
    let average_wal_bytes_per_second = if inputs.average_wal_bytes_per_second.is_finite()
        && inputs.average_wal_bytes_per_second > 0.0
    {
        // A float-to-integer cast intentionally saturates implausibly large
        // statistics instead of allowing arithmetic to wrap.
        inputs.average_wal_bytes_per_second.ceil() as u64
    } else {
        0
    };
    let wal_statistics_seconds =
        if inputs.wal_statistics_seconds.is_finite() && inputs.wal_statistics_seconds > 0.0 {
            inputs.wal_statistics_seconds.floor() as u64
        } else {
            0
        };

    let estimated_copy_seconds = largest_table_bytes.div_ceil(ASSUMED_COPY_BYTES_PER_SECOND);
    let retention_seconds = estimated_copy_seconds.saturating_add(COPY_OVERHEAD_SECONDS);
    let size_based_retention_bytes = largest_table_bytes
        .saturating_mul(TABLE_SIZE_FALLBACK_NUMERATOR)
        .div_ceil(TABLE_SIZE_FALLBACK_DENOMINATOR);
    let historical_retention_bytes = u128::from(average_wal_bytes_per_second)
        .saturating_mul(retention_seconds)
        .saturating_mul(HISTORICAL_WAL_SAFETY_NUMERATOR)
        .div_ceil(HISTORICAL_WAL_SAFETY_DENOMINATOR);
    let has_usable_wal_history =
        wal_statistics_seconds >= MIN_WAL_STATISTICS_SECONDS && average_wal_bytes_per_second > 0;
    let basis = if has_usable_wal_history {
        RecommendationBasis::HistoricalWalRate
    } else {
        RecommendationBasis::TableSizeFallback
    };
    let size_based_recommendation_bytes = size_based_retention_bytes.max(GIBIBYTE);
    let raw_recommendation_bytes = if has_usable_wal_history {
        size_based_recommendation_bytes.max(historical_retention_bytes)
    } else {
        size_based_recommendation_bytes
    };
    let recommendation_gib = raw_recommendation_bytes.div_ceil(GIBIBYTE);
    let uncapped_recommended_mb = recommendation_gib.saturating_mul(GIBIBYTE / MEBIBYTE);
    let maximum_recommended_mb = TEBIBYTE / MEBIBYTE;
    let recommended_mb = i64::try_from(uncapped_recommended_mb.min(maximum_recommended_mb))
        .unwrap_or(MAX_AUTOMATIC_RECOMMENDATION_MB);

    SlotWalKeepSizeRecommendation {
        largest_table_name: inputs.largest_table_name,
        largest_table_bytes: u64::try_from(largest_table_bytes).unwrap_or(u64::MAX),
        recommended_mb,
        recommendation_was_capped: uncapped_recommended_mb > maximum_recommended_mb,
        basis,
    }
}

/// Builds the warning shown when a finite setting is below the recommendation.
fn recommendation_warning(
    configured_mb: i64,
    recommendation: &SlotWalKeepSizeRecommendation,
) -> ValidationFailure {
    let current_size = format_mebibytes(configured_mb);
    let recommended_size = format_mebibytes(recommendation.recommended_mb);
    let largest_table_size = format_bytes(u128::from(recommendation.largest_table_bytes));
    let workload_context = recommendation.basis.workload_context();

    ValidationFailure::warning(
        "Slot WAL Retention Below Recommendation",
        format!(
            "`max_slot_wal_keep_size` is {current_size}, below the pipeline's conservative \
             planning recommendation of {recommended_size}. During initial sync, the pipeline \
             will copy existing rows from `{}`, the largest selected table (approximately \
             {largest_table_size}), then catch up on retained WAL before ongoing \
             replication.\n\n{workload_context}\n\nIf Postgres removes the required WAL before \
             catch-up, the replication slot can become unusable and the table may need to run its \
             initial sync again. Increase the setting in `postgresql.conf` or the managed-service \
             database parameter settings, then reload Postgres and make sure the source has \
             enough free disk.",
            recommendation.largest_table_name,
        ),
    )
}

/// Builds the warning shown when the estimate exceeds the automatic cap.
fn capped_recommendation_warning(
    configured_mb: i64,
    recommendation: &SlotWalKeepSizeRecommendation,
) -> ValidationFailure {
    let current_size = format_mebibytes(configured_mb);
    let largest_table_size = format_bytes(u128::from(recommendation.largest_table_bytes));
    let workload_context = recommendation.basis.workload_context();

    ValidationFailure::warning(
        "Slot WAL Retention Requires Manual Planning",
        format!(
            "`max_slot_wal_keep_size` is {current_size}. The estimate exceeded the pipeline's 1 \
             TiB automatic recommendation cap. During initial sync, the pipeline will copy \
             existing rows from `{}`, the largest selected table (approximately \
             {largest_table_size}), then catch up on retained WAL before ongoing \
             replication.\n\n{workload_context}\n\nChoose a setting based on expected WAL \
             generation, initial sync duration, and available disk. If Postgres removes the \
             required WAL before catch-up, the replication slot can become unusable and the table \
             may need to run its initial sync again.",
            recommendation.largest_table_name,
        ),
    )
}

/// Formats a byte count using a conservative whole-unit display.
fn format_bytes(bytes: u128) -> String {
    if bytes >= GIBIBYTE {
        format!("{} GiB", bytes.div_ceil(GIBIBYTE))
    } else {
        format!("{} MiB", bytes.div_ceil(MEBIBYTE))
    }
}

/// Formats a PostgreSQL setting expressed in mebibytes.
fn format_mebibytes(mebibytes: i64) -> String {
    if mebibytes >= MEBIBYTES_PER_GIBIBYTE && mebibytes % MEBIBYTES_PER_GIBIBYTE == 0 {
        format!("{} GiB", mebibytes / MEBIBYTES_PER_GIBIBYTE)
    } else {
        format!("{mebibytes} MiB")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        GIBIBYTE, MAX_AUTOMATIC_RECOMMENDATION_MB, MEBIBYTE, MEBIBYTES_PER_GIBIBYTE,
        MIN_SLOT_WAL_KEEP_SIZE_MB, RecommendationInputs, SlotWalKeepSizeRecommendation,
        build_recommendation, slot_wal_keep_size_failures,
    };
    use crate::validation::FailureType;

    /// Builds a recommendation for a table and WAL history expressed in binary
    /// units.
    fn recommendation_for(
        table_gibibytes: u128,
        average_wal_mebibytes_per_second: f64,
        wal_statistics_seconds: f64,
    ) -> SlotWalKeepSizeRecommendation {
        build_recommendation(RecommendationInputs {
            largest_table_name: "public.events".to_owned(),
            largest_table_bytes: i64::try_from(table_gibibytes * GIBIBYTE).unwrap(),
            average_wal_bytes_per_second: average_wal_mebibytes_per_second * MEBIBYTE as f64,
            wal_statistics_seconds,
        })
    }

    /// Checks that a validation message uses the user-facing phase name.
    fn assert_uses_initial_sync_terminology(reason: &str) {
        assert!(reason.contains("initial sync"));
        assert!(!reason.contains("initial copy"));
        assert!(!reason.contains("copy phase"));
        assert!(!reason.contains("during the copy"));
        assert!(!reason.contains("whole copy"));
        assert!(!reason.contains("copy starting point"));
        assert!(!reason.contains("copy duration"));
    }

    #[test]
    fn recommendation_uses_moderate_size_fallback_without_wal_history() {
        let cases = [
            (1, 1, false),
            (10, 2, false),
            (50, 8, false),
            (100, 15, false),
            (500, 75, false),
            (1024, 154, false),
            (2048, 308, false),
            (5120, 768, false),
            (10_240, 1024, true),
        ];

        for (table_gibibytes, expected_recommendation_gibibytes, expected_capped) in cases {
            let recommendation = recommendation_for(table_gibibytes, 0.0, 0.0);

            assert_eq!(
                recommendation.recommended_mb,
                expected_recommendation_gibibytes * MEBIBYTES_PER_GIBIBYTE
            );
            assert_eq!(recommendation.recommendation_was_capped, expected_capped);
        }
    }

    #[test]
    fn recommendation_examples_cover_mature_wal_history() {
        let cases = [
            (1, 10.0, 4, false),
            (10, 5.0, 9, false),
            (50, 10.0, 77, false),
            (100, 50.0, 759, false),
            (500, 25.0, 1024, true),
            (1024, 5.0, 769, false),
        ];

        for (table_gibibytes, wal_mebibytes_per_second, expected_gibibytes, expected_capped) in
            cases
        {
            let recommendation =
                recommendation_for(table_gibibytes, wal_mebibytes_per_second, 24.0 * 60.0 * 60.0);

            assert_eq!(recommendation.recommended_mb, expected_gibibytes * MEBIBYTES_PER_GIBIBYTE);
            assert_eq!(recommendation.recommendation_was_capped, expected_capped);
        }
    }

    #[test]
    fn short_wal_history_does_not_raise_the_recommendation() {
        let recommendation = recommendation_for(50, 100.0, 30.0 * 60.0);

        assert_eq!(recommendation.recommended_mb, 8 * MEBIBYTES_PER_GIBIBYTE);
    }

    #[test]
    fn recommendation_is_monotonic_and_rounded_to_whole_gibibytes() {
        let table_sizes = [0, MEBIBYTE, GIBIBYTE, 50 * GIBIBYTE, 1024 * GIBIBYTE];
        let wal_rates = [0, MEBIBYTE, 5 * MEBIBYTE, 10 * MEBIBYTE, 100 * MEBIBYTE];

        let mut previous_recommendation = 0;
        for table_size in table_sizes {
            let recommendation = build_recommendation(RecommendationInputs {
                largest_table_name: "public.events".to_owned(),
                largest_table_bytes: i64::try_from(table_size).unwrap(),
                average_wal_bytes_per_second: 0.0,
                wal_statistics_seconds: 0.0,
            });

            assert!(recommendation.recommended_mb >= previous_recommendation);
            assert_eq!(recommendation.recommended_mb % MEBIBYTES_PER_GIBIBYTE, 0);
            previous_recommendation = recommendation.recommended_mb;
        }

        previous_recommendation = 0;
        for wal_rate in wal_rates {
            let recommendation = build_recommendation(RecommendationInputs {
                largest_table_name: "public.events".to_owned(),
                largest_table_bytes: i64::try_from(50 * GIBIBYTE).unwrap(),
                average_wal_bytes_per_second: wal_rate as f64,
                wal_statistics_seconds: 24.0 * 60.0 * 60.0,
            });

            assert!(recommendation.recommended_mb >= previous_recommendation);
            previous_recommendation = recommendation.recommended_mb;
        }
    }

    #[test]
    fn invalid_wal_statistics_use_the_size_fallback() {
        let expected = recommendation_for(50, 0.0, 24.0 * 60.0 * 60.0);

        for wal_rate in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -1.0] {
            let recommendation = build_recommendation(RecommendationInputs {
                largest_table_name: "public.events".to_owned(),
                largest_table_bytes: i64::try_from(50 * GIBIBYTE).unwrap(),
                average_wal_bytes_per_second: wal_rate,
                wal_statistics_seconds: 24.0 * 60.0 * 60.0,
            });

            assert_eq!(recommendation, expected);
        }
    }

    #[test]
    fn extreme_inputs_saturate_instead_of_wrapping() {
        let recommendation = build_recommendation(RecommendationInputs {
            largest_table_name: "public.events".to_owned(),
            largest_table_bytes: i64::MAX,
            average_wal_bytes_per_second: f64::MAX,
            wal_statistics_seconds: f64::MAX,
        });

        assert_eq!(recommendation.recommended_mb, MAX_AUTOMATIC_RECOMMENDATION_MB);
        assert!(recommendation.recommendation_was_capped);
    }

    #[test]
    fn finite_setting_warns_only_below_recommendation() {
        let recommendation = recommendation_for(50, 0.0, 0.0);

        let below =
            slot_wal_keep_size_failures(recommendation.recommended_mb - 1, Some(&recommendation));
        let equal =
            slot_wal_keep_size_failures(recommendation.recommended_mb, Some(&recommendation));
        let above =
            slot_wal_keep_size_failures(recommendation.recommended_mb + 1, Some(&recommendation));

        assert_eq!(below.len(), 1);
        assert_eq!(below[0].name, "Slot WAL Retention Below Recommendation");
        assert_eq!(below[0].failure_type, FailureType::Warning);
        assert!(below[0].reason.contains("8 GiB"));
        assert!(below[0].reason.contains("public.events"));
        assert!(below[0].reason.contains("table-size fallback"));
        assert!(below[0].reason.contains("usable WAL history is unavailable"));
        assert!(below[0].reason.contains("source remains idle during initial sync"));
        assert!(below[0].reason.contains("substantially less may be sufficient"));
        assert!(below[0].reason.contains("copy existing rows"));
        assert!(below[0].reason.contains("catch up on retained WAL"));
        assert!(below[0].reason.contains("before ongoing replication"));
        assert!(!below[0].reason.contains("ETL"));
        assert_uses_initial_sync_terminology(&below[0].reason);
        assert!(equal.is_empty());
        assert!(above.is_empty());
    }

    #[test]
    fn historical_recommendation_uses_the_simple_warning() {
        let recommendation = recommendation_for(50, 10.0, 24.0 * 60.0 * 60.0);

        let failures =
            slot_wal_keep_size_failures(8 * MEBIBYTES_PER_GIBIBYTE, Some(&recommendation));

        assert_eq!(failures.len(), 1);
        assert!(failures[0].reason.contains("77 GiB"));
        assert!(failures[0].reason.contains("historical average WAL rate for the source Postgres"));
        assert!(failures[0].reason.contains("cannot predict activity during initial sync"));
        assert!(failures[0].reason.contains("substantially less may be sufficient"));
        assert!(failures[0].reason.contains("copy existing rows"));
        assert!(failures[0].reason.contains("catch up on retained WAL"));
        assert!(!failures[0].reason.contains("MiB/s"));
        assert!(!failures[0].reason.contains("1.5x"));
        assert_uses_initial_sync_terminology(&failures[0].reason);
    }

    #[test]
    fn capped_model_always_explains_manual_planning() {
        let recommendation = recommendation_for(500, 100.0, 24.0 * 60.0 * 60.0);

        assert_eq!(recommendation.recommended_mb, MAX_AUTOMATIC_RECOMMENDATION_MB);
        assert!(recommendation.recommendation_was_capped);

        for configured_mb in [
            MAX_AUTOMATIC_RECOMMENDATION_MB - MEBIBYTES_PER_GIBIBYTE,
            MAX_AUTOMATIC_RECOMMENDATION_MB,
            MAX_AUTOMATIC_RECOMMENDATION_MB + MEBIBYTES_PER_GIBIBYTE,
        ] {
            let failures = slot_wal_keep_size_failures(configured_mb, Some(&recommendation));

            assert_eq!(failures.len(), 1);
            assert_eq!(failures[0].name, "Slot WAL Retention Requires Manual Planning");
            assert_eq!(failures[0].failure_type, FailureType::Warning);
            assert!(failures[0].reason.contains("1 TiB automatic recommendation cap"));
            assert!(failures[0].reason.contains("historical average WAL rate"));
            assert!(
                failures[0].reason.contains("Choose a setting based on expected WAL generation")
            );
            assert_uses_initial_sync_terminology(&failures[0].reason);
        }
    }

    #[test]
    fn unlimited_setting_warns_about_unbounded_disk_growth() {
        let failures = slot_wal_keep_size_failures(-1, None);

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].name, "Unlimited Slot WAL Retention");
        assert_eq!(failures[0].failure_type, FailureType::Warning);
        assert!(failures[0].reason.contains("until the source disk fills"));
        assert_uses_initial_sync_terminology(&failures[0].reason);
    }

    #[test]
    fn disabled_setting_remains_critical() {
        let failures = slot_wal_keep_size_failures(0, None);

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].name, "Slot WAL Retention Disabled");
        assert_eq!(failures[0].failure_type, FailureType::Critical);
        assert!(failures[0].reason.contains("a restart of the table's initial sync"));
        assert_uses_initial_sync_terminology(&failures[0].reason);
    }

    #[test]
    fn static_floor_is_used_when_no_dynamic_estimate_exists() {
        let below = slot_wal_keep_size_failures(MIN_SLOT_WAL_KEEP_SIZE_MB - 1, None);
        let equal = slot_wal_keep_size_failures(MIN_SLOT_WAL_KEEP_SIZE_MB, None);

        assert_eq!(below.len(), 1);
        assert_eq!(below[0].name, "Low Slot WAL Retention");
        assert_eq!(below[0].failure_type, FailureType::Warning);
        assert!(below[0].reason.contains("general planning floor"));
        assert!(below[0].reason.contains("No source-specific estimate is available"));
        assert!(below[0].reason.contains("smaller value may work if the source stays idle"));
        assert!(below[0].reason.contains("active writes can exhaust it quickly"));
        assert_uses_initial_sync_terminology(&below[0].reason);
        assert!(equal.is_empty());
    }
}
