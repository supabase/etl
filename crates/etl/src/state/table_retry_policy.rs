use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Defines the retry strategy for a failed table replication.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TableRetryPolicy {
    /// No retry should be attempted, the system has to be fixed by hand.
    NoRetry,
    /// Retry after it was manually triggered.
    ManualRetry,
    /// Retry after the specified timestamp.
    TimedRetry {
        /// Timestamp at which a timed retry should next run.
        next_retry: DateTime<Utc>,
    },
}

impl TableRetryPolicy {
    /// Creates a timed retry policy relative to the current time.
    pub fn retry_in(duration: Duration) -> Self {
        Self::TimedRetry { next_retry: Utc::now() + duration }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::TableRetryPolicy;

    #[test]
    fn retry_policy_serialization() {
        let no_retry = TableRetryPolicy::NoRetry;
        let json = serde_json::to_value(&no_retry).unwrap();
        assert_eq!(json, serde_json::json!({"type": "no_retry"}));
        let deserialized: TableRetryPolicy = serde_json::from_value(json).unwrap();
        assert!(matches!(deserialized, TableRetryPolicy::NoRetry));

        let manual_retry = TableRetryPolicy::ManualRetry;
        let json = serde_json::to_value(&manual_retry).unwrap();
        assert_eq!(json, serde_json::json!({"type": "manual_retry"}));
        let deserialized: TableRetryPolicy = serde_json::from_value(json).unwrap();
        assert!(matches!(deserialized, TableRetryPolicy::ManualRetry));

        let timestamp = Utc::now();
        let timed_retry = TableRetryPolicy::TimedRetry { next_retry: timestamp };
        let json = serde_json::to_value(&timed_retry).unwrap();
        assert_eq!(json, serde_json::json!({"type": "timed_retry", "next_retry": timestamp}));
        let deserialized: TableRetryPolicy = serde_json::from_value(json).unwrap();
        if let TableRetryPolicy::TimedRetry { next_retry } = deserialized {
            assert_eq!(next_retry, timestamp);
        } else {
            panic!("Expected TimedRetry variant");
        }
    }
}
