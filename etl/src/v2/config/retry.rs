use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_factor: f32,
}

impl RetryConfig {
    pub fn new(
        max_attempts: u32,
        initial_delay: Duration,
        max_delay: Duration,
        backoff_factor: f32,
    ) -> Self {
        Self {
            max_attempts,
            initial_delay,
            max_delay,
            backoff_factor,
        }
    }

    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let delay = self.initial_delay.as_secs_f32() * (self.backoff_factor.powi(attempt as i32));
        Duration::from_secs_f32(delay.min(self.max_delay.as_secs_f32()))
    }
}
