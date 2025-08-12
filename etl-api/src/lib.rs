//! ETL API service for managing data replication pipelines.
//!
//! Provides a REST API for configuring and managing ETL pipelines, including tenants,
//! sources, destinations, and replication monitoring. Includes authentication, encryption,
//! Kubernetes integration, and comprehensive OpenAPI documentation.

pub mod authentication;
pub mod config;
pub mod db;
pub mod encryption;
pub mod k8s_client;
pub mod metrics;
pub mod routes;
pub mod span_builder;
pub mod startup;
pub mod utils;
