//! Kubernetes integration for the ETL API.
//!
//! This module contains the abstractions and implementations used by the HTTP
//! API to manage Kubernetes resources required by replicators (secrets, config
//! maps, stateful sets, and pods). Consumers should depend on the trait
//! [`K8sClient`] and avoid relying on a specific transport.
//!
//! The default client, [`http::HttpK8sClient`], is backed by the [`kube`]
//! crate and talks to the cluster using the ambient configuration (in-cluster
//! or local `~/.kube/config`). Keeping the abstraction in [`base`] lets us
//! swap implementations in tests and non-Kubernetes environments.
//!
//! See [`base`] for errors, pod phase mapping, and the client trait.

mod base;
pub mod http;

pub use base::*;
