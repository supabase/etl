use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::hash::Hash;
use std::time::Instant;
use tokio::time::sleep_until;

/// A timer registry that manages multiple named timers and returns the first one that expires.
///
/// This registry allows registering timers with associated identifiers. When a timer with an
/// existing identifier is registered, the old timer is cancelled and replaced with the new one.
///
/// The registry is designed to be reused across loop iterations - timers persist and
/// continue counting down between calls to `wait_first`.
#[derive(Debug)]
pub struct TimerRegistry<T> {
    /// Maps timer identifiers to their absolute deadlines.
    deadlines: HashMap<T, Instant>,
}

impl<T: Eq + Hash + Clone> TimerRegistry<T> {
    /// Creates a new empty timer registry.
    pub fn new() -> Self {
        Self {
            deadlines: HashMap::new(),
        }
    }

    /// Registers or updates a timer with the given identifier and deadline.
    ///
    /// If a timer with the same identifier already exists, it is cancelled and replaced.
    ///
    /// If the deadline has already passed, the timer will fire immediately when `wait_first` is called.
    pub fn register(&mut self, id: T, deadline: Instant) {
        self.deadlines.insert(id, deadline);
    }

    /// Cancels a timer with the given identifier.
    ///
    /// Returns `true` if a timer was cancelled, `false` if no timer with that identifier existed.
    pub fn cancel(&mut self, id: &T) -> bool {
        self.deadlines.remove(id).is_some()
    }

    /// Returns true if no timers are registered.
    pub fn is_empty(&self) -> bool {
        self.deadlines.is_empty()
    }

    /// Returns true if a timer with the given identifier is registered.
    pub fn has_timer(&self, id: &T) -> bool {
        self.deadlines.contains_key(id)
    }

    /// Returns the number of registered timers.
    pub fn len(&self) -> usize {
        self.deadlines.len()
    }
}

impl<T: Eq + Hash + Clone + Send + 'static> TimerRegistry<T> {
    /// Waits for the first timer to expire and returns its identifier.
    ///
    /// The expired timer is removed from the registry. Other timers remain registered
    /// and continue counting down.
    ///
    /// If no timers are registered, this method will wait forever (use with `tokio::select!`).
    ///
    /// This method uses [`FuturesUnordered`] to efficiently poll all registered timers
    /// concurrently and returns as soon as any timer expires.
    pub async fn wait_first(&mut self) -> T {
        if self.deadlines.is_empty() {
            // No timers, wait forever (caller should use with select!).
            std::future::pending::<()>().await;
            unreachable!()
        }

        // Create sleep futures for all registered deadlines.
        // We clone the IDs so we can return the winner and remove it from the map.
        let mut futures = FuturesUnordered::new();

        for (id, deadline) in self.deadlines.iter() {
            let id = id.clone();
            let deadline = *deadline;
            futures.push(async move {
                sleep_until(deadline.into()).await;
                id
            });
        }

        // Wait for the first timer to complete.
        let winner_id = futures.next().await.expect("futs is non-empty");

        // Remove the winner from the registry.
        self.deadlines.remove(&winner_id);

        // Other timers remain in the registry with their original deadlines.
        // They will be polled again on the next call to wait_first.

        winner_id
    }
}

impl<T: Eq + Hash + Clone> Default for TimerRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_empty_registry_does_not_complete() {
        let mut registry: TimerRegistry<&str> = TimerRegistry::new();

        // wait_first on empty registry should not complete
        let result = tokio::time::timeout(Duration::from_millis(50), registry.wait_first()).await;
        assert!(
            result.is_err(),
            "wait_first should timeout on empty registry"
        );
    }

    #[tokio::test]
    async fn test_single_timer() {
        let mut registry = TimerRegistry::new();
        registry.register("timer1", Instant::now() + Duration::from_millis(10));

        let result = registry.wait_first().await;
        assert_eq!(result, "timer1");
        assert!(registry.is_empty());
    }

    #[tokio::test]
    async fn test_earliest_timer_wins() {
        let mut registry = TimerRegistry::new();
        let now = Instant::now();

        registry.register("slow", now + Duration::from_millis(100));
        registry.register("fast", now + Duration::from_millis(10));
        registry.register("medium", now + Duration::from_millis(50));

        let result = registry.wait_first().await;
        assert_eq!(result, "fast");

        // Other timers should still be registered
        assert_eq!(registry.len(), 2);
        assert!(registry.has_timer(&"slow"));
        assert!(registry.has_timer(&"medium"));
    }

    #[tokio::test]
    async fn test_past_deadline_fires_immediately() {
        let mut registry = TimerRegistry::new();
        // Deadline in the past
        registry.register("past", Instant::now() - Duration::from_millis(100));

        let start = Instant::now();
        let result = registry.wait_first().await;
        let elapsed = start.elapsed();

        assert_eq!(result, "past");
        // Should complete almost immediately
        assert!(elapsed < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_register_replaces_existing_timer() {
        let mut registry = TimerRegistry::new();
        let now = Instant::now();

        // Register a timer with a long deadline
        registry.register("timer", now + Duration::from_secs(10));

        // Replace it with a short deadline
        registry.register("timer", now + Duration::from_millis(10));

        assert_eq!(registry.len(), 1);

        let start = Instant::now();
        let result = registry.wait_first().await;
        let elapsed = start.elapsed();

        assert_eq!(result, "timer");
        // Should complete quickly (the replacement timer), not after 10 seconds
        assert!(elapsed < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_cancel_timer() {
        let mut registry = TimerRegistry::new();
        let now = Instant::now();

        registry.register("timer1", now + Duration::from_millis(50));
        registry.register("timer2", now + Duration::from_millis(100));

        assert_eq!(registry.len(), 2);

        let cancelled = registry.cancel(&"timer1");
        assert!(cancelled);
        assert_eq!(registry.len(), 1);
        assert!(!registry.has_timer(&"timer1"));
        assert!(registry.has_timer(&"timer2"));

        // Cancelling non-existent timer returns false
        let cancelled = registry.cancel(&"nonexistent");
        assert!(!cancelled);
    }

    #[tokio::test]
    async fn test_timers_persist_across_waits() {
        let mut registry = TimerRegistry::new();
        let now = Instant::now();

        registry.register("first", now + Duration::from_millis(10));
        registry.register("second", now + Duration::from_millis(30));
        registry.register("third", now + Duration::from_millis(50));

        // First wait
        let result = registry.wait_first().await;
        assert_eq!(result, "first");
        assert_eq!(registry.len(), 2);

        // Second wait - remaining timers should still be there
        let result = registry.wait_first().await;
        assert_eq!(result, "second");
        assert_eq!(registry.len(), 1);

        // Third wait
        let result = registry.wait_first().await;
        assert_eq!(result, "third");
        assert!(registry.is_empty());
    }
}
