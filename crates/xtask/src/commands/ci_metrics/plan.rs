//! Pure planning rules for the CI metrics sync.
//!
//! The sync is level-triggered: each run compares the coverage it wants with
//! the stored per-day state and works off the difference. These functions
//! hold every ordering, freshness, settling and finality decision so they can
//! be tested without network or database access.

use std::collections::BTreeMap;

use chrono::{DateTime, Days, NaiveDate, TimeDelta, Utc};

/// Re-list a day younger than [`FINAL_AFTER_DAYS`] when its last listing is
/// older than this. Shorter than the hourly schedule, so every sync re-lists.
const RELIST_INTERVAL: TimeDelta = TimeDelta::minutes(50);
/// Days after which no run of a day can change: GitHub's 30-day re-run window
/// and 35-day maximum run lifetime have both passed.
pub(super) const FINAL_AFTER_DAYS: u64 = 35;
/// GitHub log and artifact retention for this repository.
pub(super) const ARTIFACT_RETENTION_DAYS: u64 = 90;
/// Non-final days this close to artifact expiry are synced first.
const EXPIRY_WARNING_DAYS: u64 = 10;
/// Recent days whose jobs are re-fetched in full once per [`REFETCH_INTERVAL`].
const REFETCH_WINDOW_DAYS: u64 = 7;
/// Interval of the full job re-fetch for recent days.
const REFETCH_INTERVAL: TimeDelta = TimeDelta::hours(24);
/// A run's details count as settled only when fetched this long after its
/// last update, which absorbs GitHub's eventual consistency.
pub(super) const SETTLE_MARGIN: TimeDelta = TimeDelta::hours(2);
/// First backoff after a failed day.
const BASE_BACKOFF: TimeDelta = TimeDelta::minutes(5);
/// Longest backoff after repeated failures of one day.
const MAX_BACKOFF: TimeDelta = TimeDelta::hours(24);

/// Stored sync state of one UTC day.
#[derive(Clone, Debug, Default)]
pub(super) struct DayState {
    pub(super) listed_at: Option<DateTime<Utc>>,
    pub(super) final_at: Option<DateTime<Utc>>,
    pub(super) detail_refetched_at: Option<DateTime<Utc>>,
    pub(super) retry_after: Option<DateTime<Utc>>,
}

/// One day to sync, in planned order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DayTask {
    pub(super) day: NaiveDate,
    /// Re-fetch jobs for every run of the day, not only changed ones.
    pub(super) refetch_details: bool,
}

/// Stored state of a run's latest attempt, used to skip unchanged runs.
#[derive(Clone, Debug)]
pub(super) struct StoredRun {
    pub(super) latest_attempt: i32,
    pub(super) updated_at: DateTime<Utc>,
    pub(super) status: String,
    pub(super) detail_fetched_at: Option<DateTime<Utc>>,
}

/// Orders the days a sync should process.
///
/// Today and yesterday come first so a large catch-up never freezes the
/// dashboard. Next come non-final days close to artifact expiry (oldest
/// first), because their manifests are the only data GitHub will drop. Then
/// recent days due for re-listing and finally older non-final days (newest
/// first), which also performs the initial backfill. Final days and days in
/// backoff are skipped.
pub(super) fn plan_days(
    now: DateTime<Utc>,
    history_since: NaiveDate,
    states: &BTreeMap<NaiveDate, DayState>,
) -> Vec<DayTask> {
    let today = now.date_naive();
    let default_state = DayState::default();
    let state = |day: NaiveDate| states.get(&day).unwrap_or(&default_state);
    let age = |day: NaiveDate| u64::try_from((today - day).num_days()).unwrap_or(0);
    let eligible = |day: NaiveDate| {
        let state = state(day);
        day >= history_since
            && day <= today
            && state.final_at.is_none()
            && state.retry_after.is_none_or(|retry_after| retry_after <= now)
    };
    let task = |day: NaiveDate| DayTask {
        day,
        refetch_details: age(day) < REFETCH_WINDOW_DAYS
            && state(day).detail_refetched_at.is_none_or(|at| at <= now - REFETCH_INTERVAL),
    };

    let mut tasks = Vec::new();
    let push = |tasks: &mut Vec<DayTask>, day: NaiveDate| {
        if eligible(day) && !tasks.iter().any(|task: &DayTask| task.day == day) {
            tasks.push(task(day));
        }
    };

    push(&mut tasks, today);
    if let Some(yesterday) = today.checked_sub_days(Days::new(1)) {
        push(&mut tasks, yesterday);
    }

    let days: Vec<NaiveDate> = history_since.iter_days().take_while(|day| *day <= today).collect();

    let expiry_start = ARTIFACT_RETENTION_DAYS - EXPIRY_WARNING_DAYS;
    for day in days.iter().copied() {
        if (expiry_start..ARTIFACT_RETENTION_DAYS).contains(&age(day)) {
            push(&mut tasks, day);
        }
    }

    for day in days.iter().rev().copied() {
        let due = state(day).listed_at.is_none_or(|at| at <= now - RELIST_INTERVAL);
        if age(day) <= FINAL_AFTER_DAYS && due {
            push(&mut tasks, day);
        }
    }

    for day in days.iter().rev().copied() {
        if age(day) > FINAL_AFTER_DAYS {
            push(&mut tasks, day);
        }
    }

    tasks
}

/// Returns whether a complete listing of `day` taken at `listed_at` makes the
/// day final.
pub(super) fn listing_finalizes(day: NaiveDate, listed_at: DateTime<Utc>) -> bool {
    day.checked_add_days(Days::new(FINAL_AFTER_DAYS + 1))
        .and_then(|end| end.and_hms_opt(0, 0, 0))
        .is_some_and(|threshold| listed_at.naive_utc() >= threshold)
}

/// Returns whether GitHub has dropped logs and artifacts of a run created at
/// `created_at`.
pub(super) fn artifacts_expired(now: DateTime<Utc>, created_at: DateTime<Utc>) -> bool {
    created_at
        .checked_add_days(Days::new(ARTIFACT_RETENTION_DAYS))
        .is_none_or(|expiry| now >= expiry)
}

/// Returns whether a listed run needs its jobs and manifests fetched.
///
/// New, changed and unsettled runs are fetched. A run is settled once it is
/// completed and its details were fetched at least [`SETTLE_MARGIN`] after
/// its last update; until then it is fetched on every sync.
pub(super) fn needs_details(
    stored: Option<&StoredRun>,
    run_attempt: i32,
    updated_at: DateTime<Utc>,
    refetch: bool,
) -> bool {
    let Some(stored) = stored else {
        return true;
    };
    let settled = stored.status == "completed"
        && stored
            .detail_fetched_at
            .is_some_and(|fetched_at| fetched_at >= stored.updated_at + SETTLE_MARGIN);
    refetch || run_attempt > stored.latest_attempt || updated_at > stored.updated_at || !settled
}

/// Returns the attempts below `latest` to fetch: those not stored yet, and
/// those stored before they completed. The run listing only returns the
/// latest attempt, so an earlier attempt would otherwise stay unfinished.
pub(super) fn attempts_to_refresh(stored: &[(i32, bool)], latest: i32) -> Vec<i32> {
    (1..latest)
        .filter(|attempt| {
            !stored.iter().any(|&(stored, completed)| stored == *attempt && completed)
        })
        .collect()
}

/// Returns whether an attempt below `latest` was stored before it completed.
///
/// Such a run is re-fetched even when its latest attempt is settled. Attempts
/// never stored are not counted: GitHub may no longer return them, and they
/// would otherwise be requested on every sync.
pub(super) fn has_unfinished_attempt(stored: &[(i32, bool)], latest: i32) -> bool {
    stored.iter().any(|&(attempt, completed)| attempt < latest && !completed)
}

/// Returns when a day that has now failed `failures` times may be retried.
pub(super) fn retry_after(now: DateTime<Utc>, failures: u32) -> DateTime<Utc> {
    let exponent = failures.saturating_sub(1).min(16);
    let backoff = BASE_BACKOFF
        .checked_mul(1_i32 << exponent)
        .map_or(MAX_BACKOFF, |backoff| backoff.min(MAX_BACKOFF));
    now + backoff
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }

    fn day(value: &str) -> NaiveDate {
        value.parse().unwrap()
    }

    fn days(tasks: &[DayTask]) -> Vec<NaiveDate> {
        tasks.iter().map(|task| task.day).collect()
    }

    #[test]
    fn empty_state_plans_today_then_every_day_newest_first() {
        let tasks = plan_days(at("2026-10-27T12:00:00Z"), day("2026-10-24"), &BTreeMap::new());

        assert_eq!(
            days(&tasks),
            [day("2026-10-27"), day("2026-10-26"), day("2026-10-25"), day("2026-10-24")]
        );
        assert!(tasks.iter().all(|task| task.refetch_details));
    }

    #[test]
    fn recently_listed_days_wait_but_today_and_yesterday_never_do() {
        let now = at("2026-10-27T12:00:00Z");
        let fresh =
            DayState { listed_at: Some(now - TimeDelta::minutes(10)), ..DayState::default() };
        let states = BTreeMap::from([
            (day("2026-10-27"), fresh.clone()),
            (day("2026-10-26"), fresh.clone()),
            (day("2026-10-25"), fresh),
        ]);

        let tasks = plan_days(now, day("2026-10-25"), &states);

        assert_eq!(days(&tasks), [day("2026-10-27"), day("2026-10-26")]);
    }

    #[test]
    fn final_and_backed_off_days_are_skipped() {
        let now = at("2026-10-27T12:00:00Z");
        let states = BTreeMap::from([
            (day("2026-10-26"), DayState { final_at: Some(now), ..DayState::default() }),
            (
                day("2026-10-25"),
                DayState { retry_after: Some(now + TimeDelta::hours(1)), ..DayState::default() },
            ),
            (
                day("2026-10-24"),
                DayState { retry_after: Some(now - TimeDelta::hours(1)), ..DayState::default() },
            ),
        ]);

        let tasks = plan_days(now, day("2026-10-24"), &states);

        assert_eq!(days(&tasks), [day("2026-10-27"), day("2026-10-24")]);
    }

    #[test]
    fn near_expiry_days_precede_ordinary_freshness_and_backfill() {
        let now = at("2026-10-27T12:00:00Z");
        let tasks = plan_days(now, day("2026-07-20"), &BTreeMap::new());
        let order = days(&tasks);

        // Days aged 80-89 come right after today and yesterday, oldest first.
        assert_eq!(order[2], day("2026-07-30"));
        assert_eq!(order[11], day("2026-08-08"));
        // Then the recent window, newest first.
        assert_eq!(order[12], day("2026-10-25"));
        // Older backfill last, newest first.
        assert_eq!(order.last(), Some(&day("2026-07-20")));
        assert_eq!(order.len(), 100);
    }

    #[test]
    fn only_recent_days_get_the_daily_full_refetch() {
        let now = at("2026-10-27T12:00:00Z");
        let refetched = DayState {
            listed_at: Some(now - TimeDelta::hours(2)),
            detail_refetched_at: Some(now - TimeDelta::hours(2)),
            ..DayState::default()
        };
        let states = BTreeMap::from([(day("2026-10-25"), refetched)]);

        let tasks = plan_days(now, day("2026-10-10"), &states);
        let refetch =
            |date: &str| tasks.iter().find(|task| task.day == day(date)).unwrap().refetch_details;

        assert!(refetch("2026-10-27"));
        assert!(!refetch("2026-10-25"));
        assert!(refetch("2026-10-21"));
        assert!(!refetch("2026-10-20"));
    }

    #[test]
    fn days_finalize_only_after_the_full_run_lifetime() {
        assert!(!listing_finalizes(day("2026-09-01"), at("2026-10-06T23:59:59Z")));
        assert!(listing_finalizes(day("2026-09-01"), at("2026-10-07T00:00:00Z")));
    }

    #[test]
    fn details_are_fetched_until_settled() {
        let updated_at = at("2026-10-27T10:00:00Z");
        let stored = |status: &str, fetched: Option<&str>| StoredRun {
            latest_attempt: 1,
            updated_at,
            status: status.to_owned(),
            detail_fetched_at: fetched.map(at),
        };

        assert!(needs_details(None, 1, updated_at, false));
        assert!(needs_details(
            Some(&stored("in_progress", Some("2026-10-27T13:00:00Z"))),
            1,
            updated_at,
            false
        ));
        assert!(needs_details(
            Some(&stored("completed", Some("2026-10-27T11:59:59Z"))),
            1,
            updated_at,
            false
        ));
        assert!(!needs_details(
            Some(&stored("completed", Some("2026-10-27T12:00:00Z"))),
            1,
            updated_at,
            false
        ));

        let settled = stored("completed", Some("2026-10-27T12:00:00Z"));
        assert!(needs_details(Some(&settled), 2, updated_at, false));
        assert!(needs_details(Some(&settled), 1, updated_at + TimeDelta::seconds(1), false));
        assert!(needs_details(Some(&settled), 1, updated_at, true));
        // An older listing never counts as a change.
        assert!(!needs_details(Some(&settled), 1, updated_at - TimeDelta::hours(1), false));
    }

    #[test]
    fn earlier_attempts_are_fetched_until_completed() {
        assert_eq!(attempts_to_refresh(&[], 1), Vec::<i32>::new());
        assert_eq!(attempts_to_refresh(&[(3, false)], 3), [1, 2]);
        assert_eq!(attempts_to_refresh(&[(1, true), (3, true)], 3), [2]);
        // An attempt stored while running is fetched again once a re-run
        // exists.
        assert_eq!(attempts_to_refresh(&[(1, false), (2, true)], 2), [1]);
        assert_eq!(attempts_to_refresh(&[(1, true), (2, false)], 2), Vec::<i32>::new());
        assert!(has_unfinished_attempt(&[(1, false), (2, true)], 2));
        // The latest attempt is not earlier; never-stored attempts do not
        // count.
        assert!(!has_unfinished_attempt(&[(1, true), (2, false)], 2));
        assert!(!has_unfinished_attempt(&[(3, true)], 3));
    }

    #[test]
    fn backoff_doubles_and_is_capped() {
        let now = at("2026-10-27T12:00:00Z");
        assert_eq!(retry_after(now, 1), now + TimeDelta::minutes(5));
        assert_eq!(retry_after(now, 3), now + TimeDelta::minutes(20));
        assert_eq!(retry_after(now, 20), now + TimeDelta::hours(24));
        assert_eq!(retry_after(now, u32::MAX), now + TimeDelta::hours(24));
    }

    #[test]
    fn artifacts_expire_after_retention() {
        let created = at("2026-07-29T12:00:00Z");
        assert!(!artifacts_expired(at("2026-10-27T11:59:59Z"), created));
        assert!(artifacts_expired(at("2026-10-27T12:00:00Z"), created));
    }
}
