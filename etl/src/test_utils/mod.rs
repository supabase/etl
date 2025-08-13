//! Comprehensive testing utilities for PostgreSQL logical replication ETL systems.
//!
//! This module provides a complete testing framework designed specifically for testing
//! complex ETL scenarios involving PostgreSQL logical replication, multiple workers,
//! and various destination systems. The utilities handle the intricacies of setting up
//! test databases, managing replication slots, coordinating worker lifecycles, and
//! validating data consistency across async operations.
//!
//! # Testing Architecture
//!
//! The testing framework is built around several key principles:
//!
//! 1. **Isolation**: Each test gets its own database and replication infrastructure
//! 2. **Determinism**: Utilities ensure reproducible test outcomes across different environments
//! 3. **Cleanup**: Automatic resource cleanup prevents test interference and resource leaks
//! 4. **Observability**: Rich debugging support for understanding test failures
//!
//! # Core Testing Patterns
//!
//! ## Database Lifecycle Management
//!
//! The [`database`] module provides utilities for creating isolated PostgreSQL databases
//! with proper replication configuration:
//!
//! ```rust,no_run
//! use etl::test_utils::database::TestDatabase;
//! use etl::prelude::*;
//!
//! # #[tokio::test]
//! # async fn example() -> EtlResult<()> {
//! // Create isolated test database with replication enabled
//! let test_db = TestDatabase::new().await?;
//!
//! // Database is automatically cleaned up when dropped
//! # Ok(())
//! # }
//! ```
//!
//! ## Pipeline Testing
//!
//! The [`pipeline`] module provides high-level utilities for testing complete ETL pipelines
//! including startup, data processing, and shutdown scenarios:
//!
//! ```rust,no_run
//! use etl::test_utils::pipeline::TestPipeline;
//! use etl::prelude::*;
//!
//! # #[tokio::test]
//! # async fn example() -> EtlResult<()> {
//! // Set up test pipeline with controlled data flow
//! let test_pipeline = TestPipeline::builder()
//!     .with_tables(&["users", "orders"])
//!     .build()
//!     .await?;
//!
//! // Test data replication scenarios
//! test_pipeline.insert_data("users", &[/* test data */]).await?;
//! test_pipeline.verify_replication().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Event Validation
//!
//! The [`event`] module provides utilities for validating replication events and data consistency:
//!
//! ```rust,no_run
//! use etl::test_utils::event::EventValidator;
//! use etl::conversions::event::Event;
//!
//! # #[tokio::test]
//! # async fn example() -> etl::error::EtlResult<()> {
//! let validator = EventValidator::new();
//!
//! // Validate event sequences and data integrity
//! validator.expect_insert("users", &[/* expected data */]);
//! validator.expect_update("users", &[/* expected changes */]);
//! validator.verify_event_sequence(&events).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Testing Complex Scenarios
//!
//! ## Multi-Worker Coordination
//!
//! Test utilities handle the complexity of coordinating multiple workers during testing:
//! - **Apply worker testing**: Validates main replication stream processing
//! - **Table sync worker testing**: Tests initial data copying scenarios
//! - **Worker pool testing**: Validates concurrent worker management
//! - **Coordination testing**: Tests inter-worker communication and state transitions
//!
//! ## Failure Scenario Testing
//!
//! The framework includes utilities for testing error conditions and recovery:
//! - **Connection failures**: Simulates network and database connectivity issues
//! - **Destination failures**: Tests handling of downstream system failures
//! - **Resource exhaustion**: Validates behavior under resource constraints
//! - **Partial failures**: Tests recovery from incomplete operations
//!
//! ## Performance Testing
//!
//! Utilities support performance and load testing scenarios:
//! - **Throughput testing**: Validates data processing rates under load
//! - **Latency testing**: Measures replication lag and response times
//! - **Resource usage**: Monitors memory and CPU usage during operations
//! - **Scalability testing**: Tests behavior with increasing data volumes
//!
//! # Module Organization
//!
//! - [`database`] - PostgreSQL test database setup and management
//! - [`pipeline`] - High-level pipeline testing utilities and builders
//! - [`event`] - Replication event validation and testing utilities
//! - [`table`] - Table creation, manipulation, and validation helpers
//! - [`test_destination_wrapper`] - Test destination implementations with observability
//! - [`test_schema`] - Schema generation and validation utilities
//! - [`materialize`] - Data materialization and comparison utilities
//! - [`notify`] - Async notification and waiting utilities for test coordination
//!
//! # Best Practices
//!
//! ## Test Organization
//! - Use descriptive test names that explain the scenario being tested
//! - Group related tests into modules that share setup/teardown logic
//! - Use builder patterns for complex test configuration
//! - Leverage parameterized tests for testing multiple scenarios
//!
//! ## Resource Management
//! - Always use RAII patterns for resource cleanup (databases, connections, workers)
//! - Set appropriate timeouts to prevent hanging tests
//! - Use test isolation to prevent interference between test cases
//! - Monitor resource usage to detect leaks early
//!
//! ## Debugging Support
//! - Enable detailed logging in test environments for debugging
//! - Use structured assertions that provide clear failure messages
//! - Capture system state on test failures for post-mortem analysis
//! - Provide utilities for dumping internal state during debugging
pub mod database;
pub mod event;
pub mod materialize;
pub mod notify;
pub mod pipeline;
pub mod table;
pub mod test_destination_wrapper;
pub mod test_schema;
