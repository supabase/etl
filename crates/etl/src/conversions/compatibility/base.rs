use super::DestinationTypeCompatibility;
use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{Cell, Type},
};

/// Destination-compatible cell together with its materialized type.
#[derive(Debug, Clone, PartialEq)]
pub struct CellCarrier<C, M> {
    /// Source-shaped type used for destination materialization.
    source_type: Type,
    /// Destination-compatible cell value.
    cell: C,
    metadata: M
}

impl<C, M> CellCarrier<C, M> {
    /// Creates a destination-compatible cell.
    pub const fn new(source_type: Type, cell: C, metadata: M) -> Self {
        Self { source_type, cell, metadata }
    }

    pub fn source_type(&self) -> &Type {
        &self.source_type
    }

    /// Returns the contained cell.
    pub fn into_parts(self) -> (C, M) {
        (self.cell, self.metadata)
    }
}

/// Result of applying a destination compatibility policy to a source type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeCompatibilityResult {
    /// The source type can be used unchanged for destination materialization.
    Unchanged(Type),
    /// The source type should be treated as another source-shaped type.
    Changed(Type),
    /// The source type cannot be represented by the destination.
    Invalid {
        /// Error kind to surface to callers.
        kind: ErrorKind,
        /// Human-readable reason for the incompatibility.
        reason: String,
    },
}

/// Result of applying a destination compatibility policy to a cell.
#[derive(Debug, Clone, PartialEq)]
pub enum CellCompatibilityResult<C, M> {
    /// The cell can be written unchanged.
    Unchanged(CellCarrier<C, M>),
    /// The cell value changed but the destination materialized type did not.
    ValueChanged(CellCarrier<C, M>),
    /// The cell changed to match a different destination materialized type.
    TypeChanged(CellCarrier<C, M>),
    /// The cell cannot be represented by the destination.
    Invalid {
        /// Error kind to surface to callers.
        kind: ErrorKind,
        /// Human-readable reason for the incompatibility.
        reason: String,
    },
}

/// Destination-specific rules for source types and cells.
pub trait CompatibilityRules {
    /// Destination-specific cell representation produced by compatibility.
    type CompatibleCell;

    /// Returns the source-shaped type to use for destination materialization.
    fn get_compatible_type(
        &self,
        source_type: &Type,
        compatibility: DestinationTypeCompatibility,
    ) -> TypeCompatibilityResult;

    /// Returns the cell to pass to destination encoding.
    fn get_compatible_cell<M>(
        &self,
        cell_carrier: CellCarrier<Cell, M>,
        compatibility: DestinationTypeCompatibility,
    ) -> CellCompatibilityResult<Self::CompatibleCell, M>;
}

/// Orchestrates compatibility checks for one destination.
pub struct CompatibilityChecker<C: CompatibilityRules> {
    /// Selected compatibility policy.
    type_compatibility: DestinationTypeCompatibility,
    /// Destination-specific compatibility implementation.
    compatibility: C,
}

impl<C: CompatibilityRules> CompatibilityChecker<C> {
    /// Creates a compatibility checker for a destination.
    pub fn new(type_compatibility: DestinationTypeCompatibility, compatibility: C) -> Self {
        Self { type_compatibility, compatibility }
    }

    /// Returns the configured compatibility policy.
    pub const fn compatibility(&self) -> DestinationTypeCompatibility {
        self.type_compatibility
    }

    /// Returns the destination-compatible type or an [`EtlError`].
    pub fn get_compatible_type(&self, typ: &Type) -> EtlResult<Type> {
        match self.compatibility.get_compatible_type(typ, self.type_compatibility) {
            TypeCompatibilityResult::Unchanged(typ) | TypeCompatibilityResult::Changed(typ) => {
                Ok(typ)
            }
            TypeCompatibilityResult::Invalid { kind, reason } => {
                Err(etl_error!(kind, "Type is incompatible with destination", reason))
            }
        }
    }

    /// Returns the destination-compatible cell or an [`EtlError`].
    pub fn get_compatible_cell<M>(
        &self,
        cell_carrier: CellCarrier<Cell, M>,
    ) -> EtlResult<CellCarrier<C::CompatibleCell, M>> {
        self.get_compatible_cell_with_type(cell_carrier)
    }

    /// Returns destination-compatible cell-carrying items.
    pub fn get_compatible_cells<M>(
        &self,
        cell_carriers: impl IntoIterator<Item = CellCarrier<Cell, M>>,
    ) -> EtlResult<Vec<CellCarrier<C::CompatibleCell, M>>>
    {
        cell_carriers
            .into_iter()
            .enumerate()
            .map(|(index, cell_carrier)| {
                self.get_compatible_cell_at_index(cell_carrier, index)
            })
            .collect()
    }

    /// Returns the destination-compatible cell with source type context.
    fn get_compatible_cell_with_type<M>(
        &self,
        cell_carrier: CellCarrier<Cell, M>,
    ) -> EtlResult<CellCarrier<C::CompatibleCell, M>> {
        let compatible_type = self.get_compatible_type(cell_carrier.source_type())?;
        match self.compatibility.get_compatible_cell(cell_carrier, self.type_compatibility) {
            CellCompatibilityResult::Unchanged(cell)
            | CellCompatibilityResult::ValueChanged(cell)
            | CellCompatibilityResult::TypeChanged(cell) => {
                if cell.source_type != compatible_type {
                    return Err(etl_error!(
                        ErrorKind::InvalidState,
                        "Cell compatibility type mismatch",
                        format!(
                            "Expected compatible type {}, got {}",
                            compatible_type.name(),
                            cell.source_type.name()
                        )
                    ));
                }

                Ok(cell)
            }
            CellCompatibilityResult::Invalid { kind, reason } => {
                Err(etl_error!(kind, "Cell is incompatible with destination", reason))
            }
        }
    }

    /// Returns a destination-compatible cell with index context on failures.
    fn get_compatible_cell_at_index<M>(
        &self,
        cell_carrier: CellCarrier<Cell, M>,
        index: usize,
    ) -> EtlResult<CellCarrier<C::CompatibleCell, M>> {
        self.get_compatible_cell_with_type(cell_carrier).map_err(|error| {
            let kind = error.kind();
            let detail = error.detail().map_or_else(
                || format!("Cell at index {index} failed compatibility"),
                |detail| format!("Cell at index {index} failed compatibility: {detail}"),
            );

            etl_error!(
                kind,
                "Cell compatibility check failed",
                detail,
                source: error
            )
        })
    }
}
