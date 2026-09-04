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
/// Initial-copy throughput used to estimate how long a table pins WAL.
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
    /// Qualified name of the largest logical table selected for initial copy.
    largest_table_name: String,
    /// On-disk bytes used by the largest logical table, including TOAST but
    /// excluding indexes.
    largest_table_bytes: i64,
    /// Cluster-wide WAL bytes generated per second since statistics were reset.
    average_wal_bytes_per_second: f64,
    /// Seconds covered by the cluster-wide WAL statistics.
    wal_statistics_seconds: f64,
}

/// A replication-slot WAL retention recommendation.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct SlotWalKeepSizeRecommendation {
    /// Qualified name of the table which determined the copy duration.
    largest_table_name: String,
    /// Estimated on-disk table bytes used by the model.
    largest_table_bytes: u64,
    /// Recommended `max_slot_wal_keep_size` in megabytes.
    recommended_mb: i64,
    /// Whether the calculated recommendation exceeded the automatic cap.
    recommendation_was_capped: bool,
}

/// Best-effort estimates a slot WAL retention limit for initial copy.
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
    // storage without scanning table rows. `pg_partition_tree` makes one
    // published partition root a single logical table whose size is the sum
    // of its leaf partitions, including nested range, list, hash, and default
    // partitions. `pg_table_size` includes table forks and TOAST, which the
    // initial copy reads, but excludes indexes, which it does not read.
    let query = sqlx::query_as::<_, RecommendationInputs>(
        r#"
        with selected_tables as (
            select distinct
                c.oid,
                format('%I.%I', n.nspname, c.relname) as table_name,
                c.relkind
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
        table_sizes as (
            select
                st.oid,
                st.table_name,
                case st.relkind
                    when 'p' then coalesce(
                        (
                            select sum(pg_table_size(pt.relid))
                            from pg_partition_tree(st.oid) pt
                            where pt.isleaf
                        ),
                        0
                    )
                    else pg_table_size(st.oid)
                end::bigint as table_bytes
            from selected_tables st
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
            "`max_slot_wal_keep_size` is unlimited.\n\nLogical replication slots can retain WAL \
             indefinitely when the pipeline is paused, disconnected, or stuck on a table error. \
             This does not prevent the pipeline from starting, but an abandoned or stalled slot \
             can fill the source database disk.\n\nSet a bounded value large enough for your \
             write volume and longest expected initial copy in `postgresql.conf` or the \
             managed-service database parameter settings, then reload PostgreSQL. Make sure the \
             source has enough free disk for the configured retention.",
        )],
        0 => vec![ValidationFailure::critical(
            "Slot WAL Retention Disabled",
            "`max_slot_wal_keep_size` is 0 MB, leaving no per-slot WAL retention headroom.\n\nA \
             logical replication slot can be invalidated as soon as it falls behind at a \
             checkpoint, which can force the pipeline to restart table copies or require slot \
             recreation. Set `max_slot_wal_keep_size` to a positive value in `postgresql.conf` or \
             the managed-service database parameter settings, then reload the PostgreSQL \
             configuration. Size it for source write volume, available disk, and the longest \
             expected pipeline downtime.",
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
                         pipeline's recommended minimum of {MIN_SLOT_WAL_KEEP_SIZE_MB} \
                         MB.\n\nIncrease it in `postgresql.conf` or the managed-service database \
                         parameter settings, then reload PostgreSQL."
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

    ValidationFailure::warning(
        "Slot WAL Retention Below Recommendation",
        format!(
            "`max_slot_wal_keep_size` is {current_size}, below the pipeline's recommendation of \
             {recommended_size}. The largest table selected for initial copy is `{}` at \
             approximately {largest_table_size}.\n\nFor safety and consistency, PostgreSQL must \
             retain the WAL generated from the pipeline's copy starting point until the table \
             copy finishes. The pipeline then resumes streaming from that same point without \
             missing changes. If PostgreSQL removes the required WAL first, the replication slot \
             can become unusable and the table may need to be copied again.\n\nIncrease \
             `max_slot_wal_keep_size` in `postgresql.conf` or the managed-service database \
             parameter settings, then reload PostgreSQL. Make sure the source has enough free \
             disk for the additional WAL.",
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

    ValidationFailure::warning(
        "Slot WAL Retention Requires Manual Planning",
        format!(
            "`max_slot_wal_keep_size` is {current_size}. The estimate reached the pipeline's 1 \
             TiB automatic recommendation limit, so the required retention may be higher. The \
             largest table selected for initial copy is `{}` at approximately \
             {largest_table_size}.\n\nReview the source's WAL generation rate, expected copy \
             duration, and available disk before choosing a setting. PostgreSQL must retain the \
             WAL generated from the pipeline's copy starting point until the table copy finishes; \
             otherwise, the replication slot can become unusable and the table may need to be \
             copied again.",
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
        assert!(below[0].reason.contains("retain the WAL"));
        assert!(below[0].reason.contains("without missing changes"));
        assert!(!below[0].reason.contains("ETL"));
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
        assert!(failures[0].reason.contains("resumes streaming from that same point"));
        assert!(!failures[0].reason.contains("MiB/s"));
        assert!(!failures[0].reason.contains("1.5x"));
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
            assert!(failures[0].reason.contains("1 TiB automatic recommendation limit"));
            assert!(failures[0].reason.contains("Review the source's WAL generation rate"));
        }
    }

    #[test]
    fn unlimited_setting_warns_about_unbounded_disk_growth() {
        let failures = slot_wal_keep_size_failures(-1, None);

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].name, "Unlimited Slot WAL Retention");
        assert_eq!(failures[0].failure_type, FailureType::Warning);
        assert!(failures[0].reason.contains("fill the source database disk"));
    }

    #[test]
    fn disabled_setting_remains_critical() {
        let failures = slot_wal_keep_size_failures(0, None);

        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].name, "Slot WAL Retention Disabled");
        assert_eq!(failures[0].failure_type, FailureType::Critical);
    }

    #[test]
    fn static_floor_is_used_when_no_dynamic_estimate_exists() {
        let below = slot_wal_keep_size_failures(MIN_SLOT_WAL_KEEP_SIZE_MB - 1, None);
        let equal = slot_wal_keep_size_failures(MIN_SLOT_WAL_KEEP_SIZE_MB, None);

        assert_eq!(below.len(), 1);
        assert_eq!(below[0].name, "Low Slot WAL Retention");
        assert_eq!(below[0].failure_type, FailureType::Warning);
        assert!(equal.is_empty());
    }
}
