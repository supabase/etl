//! Common test utilities for etl API tests.
//!
//! This module provides shared functionality used across integration tests:
//!
//! - `test_app`: A test application wrapper that provides:
//!   - A running instance of the API server for testing
//!   - Helper methods for making authenticated HTTP requests
//!   - Request/response type definitions for all API endpoints
//!   - Methods to create, read, update, and delete resources
//!
//! - `database`: Database configuration utilities that:
//!   - Set up test databases with proper configuration
//!   - Handle database migrations
//!   - Provide connection pools for tests
//!
//! These utilities help maintain consistency across tests and reduce code duplication.
pub mod database;
pub mod k8s_client;
pub mod test_app;
