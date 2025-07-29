use chrono::{DateTime, Utc};
use std::future::Future;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep_until};
use tracing::{debug, warn};

/// Converts a UTC datetime to a tokio Instant if the datetime is in the future.
/// Returns None if the datetime is in the past or conversion fails.
fn date_time_to_instant(date_time: DateTime<Utc>) -> Option<Instant> {
    let now = Utc::now();
    if date_time <= now {
        return None;
    }

    let duration = (date_time - now).to_std().ok()?;
    let instant = Instant::now() + duration;
    Some(instant)
}

/// Schedules a task to run at a specific UTC datetime.
///
/// If the datetime is in the past, the task will not be scheduled and a warning is logged.
/// Returns a [`JoinHandle`] that can be used to await or cancel the scheduled task.
pub fn schedule_at<F, Fut>(
    at: DateTime<Utc>,
    task_name: &'static str,
    block: F,
) -> Option<JoinHandle<()>>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send,
{
    let Some(instant) = date_time_to_instant(at) else {
        warn!(
            task_name = task_name,
            scheduled_time = %at,
            "could not schedule task - time is in the past or conversion failed"
        );
        return None;
    };

    let handle = tokio::task::spawn(async move {
        debug!(task_name = task_name, "waiting until scheduled time");
        sleep_until(instant).await;

        debug!(task_name = task_name, "executing scheduled task");
        block().await;

        debug!(task_name = task_name, "scheduled task completed");
    });

    Some(handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use std::sync::{Arc, Mutex};
    use tokio::time::Duration as TokioDuration;

    #[test]
    fn test_date_time_to_instant_future_time() {
        let future_time = Utc::now() + Duration::seconds(10);
        let result = date_time_to_instant(future_time);
        assert!(result.is_some());
    }

    #[test]
    fn test_date_time_to_instant_past_time() {
        let past_time = Utc::now() - Duration::seconds(10);
        let result = date_time_to_instant(past_time);
        assert!(result.is_none());
    }

    #[test]
    fn test_date_time_to_instant_current_time() {
        let now = Utc::now();
        let result = date_time_to_instant(now);
        assert!(result.is_none());
    }

    #[test]
    fn test_date_time_to_instant_edge_case_very_future() {
        let far_future = Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap();
        let result = date_time_to_instant(far_future);
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_schedule_at_executes_task() {
        let executed = Arc::new(Mutex::new(false));
        let executed_clone = executed.clone();

        let future_time = Utc::now() + Duration::milliseconds(50);

        let handle = schedule_at(future_time, "test_task", || async move {
            let mut guard = executed_clone.lock().unwrap();
            *guard = true;
        });

        assert!(handle.is_some());
        let _ = handle.unwrap().await;
        assert!(*executed.lock().unwrap());
    }

    #[tokio::test]
    async fn test_schedule_at_past_time_no_execution() {
        let executed = Arc::new(Mutex::new(false));
        let executed_clone = executed.clone();

        let past_time = Utc::now() - Duration::seconds(1);

        let handle = schedule_at(past_time, "past_task", || async move {
            let mut guard = executed_clone.lock().unwrap();
            *guard = true;
        });

        assert!(handle.is_none());
        tokio::time::sleep(TokioDuration::from_millis(10)).await;
        assert!(!*executed.lock().unwrap());
    }

    #[tokio::test]
    async fn test_schedule_at_multiple_tasks() {
        let counter = Arc::new(Mutex::new(0));
        let counter1 = counter.clone();
        let counter2 = counter.clone();

        let time1 = Utc::now() + Duration::milliseconds(50);
        let time2 = Utc::now() + Duration::milliseconds(100);

        let handle1 = schedule_at(time1, "task1", move || async move {
            let mut guard = counter1.lock().unwrap();
            *guard += 1;
        });

        let handle2 = schedule_at(time2, "task2", move || async move {
            let mut guard = counter2.lock().unwrap();
            *guard += 10;
        });

        assert!(handle1.is_some());
        assert!(handle2.is_some());

        let _ = handle1.unwrap().await;
        assert_eq!(*counter.lock().unwrap(), 1);

        let _ = handle2.unwrap().await;
        assert_eq!(*counter.lock().unwrap(), 11);
    }

    #[test]
    fn test_date_time_to_instant_extreme_future() {
        let extreme_future = Utc.with_ymd_and_hms(9999, 12, 31, 23, 59, 59).unwrap();
        let result = date_time_to_instant(extreme_future);
        assert!(result.is_some());
    }

    #[tokio::test]
    async fn test_schedule_at_task_cancellation() {
        let executed = Arc::new(Mutex::new(false));
        let executed_clone = executed.clone();

        let future_time = Utc::now() + Duration::seconds(10);

        let handle = schedule_at(future_time, "cancellable_task", || async move {
            let mut guard = executed_clone.lock().unwrap();
            *guard = true;
        });

        assert!(handle.is_some());
        let handle = handle.unwrap();

        handle.abort();

        tokio::time::sleep(TokioDuration::from_millis(50)).await;
        assert!(!*executed.lock().unwrap());
        assert!(handle.is_finished());
    }
}
