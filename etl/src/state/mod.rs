//! Comprehensive state management for PostgreSQL logical replication operations.
//!
//! This module implements a sophisticated state management system that tracks the progress,
//! lifecycle, and coordination of table replication across the entire ETL pipeline. The state
//! system ensures consistency, enables recovery from failures, and coordinates complex
//! multi-worker operations while maintaining ACID properties.
//!
//! # State Management Architecture
//!
//! The state system is designed around several key principles:
//!
//! 1. **Persistence**: All critical state is persisted to survive restarts and failures
//! 2. **Consistency**: State transitions are atomic and respect dependency relationships
//! 3. **Observability**: Rich state information enables monitoring and debugging
//! 4. **Coordination**: State changes trigger appropriate worker actions and transitions
//!
//! # Replication Lifecycle States
//!
//! Each table in the ETL system progresses through a well-defined set of states:
//!
//! ```text
//! ┌─────────┐    ┌──────────┐    ┌─────────────┐    ┌──────────┐    ┌───────┐
//! │  Init   │ -> │ DataSync │ -> │FinishedCopy │ -> │SyncWait  │ -> │ Ready │
//! └─────────┘    └──────────┘    └─────────────┘    └──────────┘    └───────┘
//!      │              │               │                 │             │
//!      ▼              ▼               ▼                 ▼             ▼
//! ┌─────────┐    ┌─────────┐    ┌─────────┐       ┌─────────┐   ┌─────────┐
//! │ Errored │    │ Errored │    │ Errored │       │ Errored │   │ Errored │
//! └─────────┘    └─────────┘    └─────────┘       └─────────┘   └─────────┘
//! ```
//!
//! ## State Descriptions
//!
//! - **Init**: Table discovered but initial synchronization not yet started
//! - **DataSync**: Table sync worker is copying initial table data
//! - **FinishedCopy**: Initial data copy completed, preparing for continuous replication
//! - **SyncWait**: Waiting for apply worker to enable continuous replication
//! - **Ready**: Table is fully synchronized and receiving continuous updates
//! - **Errored**: Table encountered an error and requires intervention
//!
//! # State Coordination Patterns
//!
//! ## Worker Coordination
//!
//! The state system coordinates multiple types of workers:
//! - **Apply workers** monitor table states to determine when to start continuous replication
//! - **Table sync workers** update states as they progress through initial synchronization
//! - **Worker pools** use state information to manage worker lifecycle and resource allocation
//!
//! ## Failure Recovery
//!
//! State transitions are designed to enable robust failure recovery:
//! - **Idempotent operations**: Repeating state transitions produces consistent results
//! - **Checkpoint recovery**: State snapshots enable recovery from specific points
//! - **Error classification**: Different error types enable appropriate retry strategies
//! - **Manual intervention**: Some errors require operator action before retry
//!
//! ## Consistency Guarantees
//!
//! The state system provides several consistency guarantees:
//! - **Atomicity**: State transitions either complete fully or not at all
//! - **Ordering**: State changes respect dependency relationships between tables
//! - **Isolation**: Concurrent state changes don't interfere with each other
//! - **Durability**: State changes are persisted before being considered committed
//!
//! # Error Handling and Retry Policies
//!
//! The state system includes sophisticated error handling with configurable retry policies:
//!
//! ## Retry Policy Types
//! - **Automatic retry**: Errors that can be automatically retried after a delay
//! - **Manual retry**: Errors requiring operator intervention before retry
//! - **No retry**: Fatal errors that cannot be recovered from
//!
//! ## Error Context
//! Each error state includes:
//! - **Error description**: Human-readable description of what went wrong
//! - **Suggested solution**: Guidance for resolving the error condition
//! - **Retry policy**: Determines whether and how the operation should be retried
//! - **Timestamp**: When the error occurred for tracking and monitoring
//!
//! # Integration with Storage Systems
//!
//! The state management system integrates with various storage backends:
//! - **Memory stores**: For testing and development scenarios
//! - **PostgreSQL stores**: For production deployments requiring durability
//! - **Hybrid stores**: Combining memory caching with persistent storage
//!
//! # Monitoring and Observability
//!
//! The state system provides rich observability features:
//! - **State transition metrics**: Track table progress and identify bottlenecks
//! - **Error rate monitoring**: Detect systematic issues requiring attention
//! - **Performance metrics**: Monitor state operation latency and throughput
//! - **Health checks**: Validate state consistency and detect corruption
//!
//! # Usage Patterns
//!
//! ## Querying Table States
//! ```rust,no_run
//! use etl::state::table::{TableReplicationPhase, TableReplicationPhaseType};
//! use etl::store::state::StateStore;
//!
//! # async fn example<S: StateStore>(store: &S) -> etl::error::EtlResult<()> {
//! // Get all table states
//! let states = store.get_table_replication_states().await?;
//!
//! // Filter tables by state type
//! let ready_tables: Vec<_> = states
//!     .iter()
//!     .filter(|(_, state)| matches!(state.as_type(), TableReplicationPhaseType::Ready))
//!     .collect();
//! # Ok(())
//! # }
//! ```
//!
//! ## State Transitions
//! ```rust,no_run
//! use etl::state::table::TableReplicationPhase;
//! use etl::store::state::StateStore;
//! use etl_postgres::schema::TableId;
//!
//! # async fn example<S: StateStore>(store: &S, table_id: TableId) -> etl::error::EtlResult<()> {
//! // Transition table to data sync phase
//! store.update_table_replication_state(
//!     table_id,
//!     TableReplicationPhase::DataSync
//! ).await?;
//!
//! // Handle errors with retry policy
//! store.update_table_replication_state(
//!     table_id,
//!     TableReplicationPhase::Errored {
//!         reason: "Connection timeout".to_string(),
//!         solution: Some("Check network connectivity".to_string()),
//!         retry_policy: etl::state::table::RetryPolicy::ManualRetry,
//!     }
//! ).await?;
//! # Ok(())
//! # }
//! ```

pub mod table;
