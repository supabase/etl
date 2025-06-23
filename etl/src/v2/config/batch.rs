use std::time::Duration;

#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_size: usize,
    pub max_fill: Duration,
}

impl BatchConfig {
    pub fn new(max_size: usize, max_fill: Duration) -> BatchConfig {
        BatchConfig { max_size, max_fill }
    }
}
