//! Shared configuration types used across ETL components.

mod batch;
mod connection;
mod heartbeat;
mod pipeline;
mod tls;
mod validation;

pub use batch::BatchConfig;
pub use connection::{PgConnectionConfig, PgConnectionConfigWithoutSecrets};
pub use heartbeat::{HeartbeatConfig, ETL_HEARTBEAT_OPTIONS};
pub use pipeline::{
    PipelineConfig, PipelineConfigWithoutSecrets, TableSyncCopyConfig,
};
pub use tls::TlsConfig;
pub use validation::ValidationError;
