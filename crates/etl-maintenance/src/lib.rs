//! External maintenance coordination and runners for ETL.

mod coordination;

#[cfg(feature = "ducklake")]
pub mod ducklake;

pub use coordination::{
    ExternalMaintenanceOperationHistory, ExternalMaintenanceOperationPolicy,
    ExternalMaintenanceOperationRequest, ExternalMaintenanceOperationRun,
    ExternalMaintenanceOperations, ExternalMaintenancePause, ExternalMaintenanceReplicatorState,
    ExternalMaintenanceReplicatorStatus, ExternalMaintenanceRequestOutcome, ExternalMaintenanceRun,
    ExternalMaintenanceState, ExternalMaintenanceStore, ExternalMaintenanceWatcherConfig,
    PostgresExternalMaintenanceStore,
};

#[cfg(feature = "kubernetes")]
pub use coordination::KubernetesExternalMaintenanceStore;
