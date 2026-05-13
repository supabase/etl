use super::DestinationTypeCompatibility;
use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{Cell, TableRow, Type},
};

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
pub enum CellCompatibilityResult {
    /// The cell can be written unchanged.
    Unchanged(Cell),
    /// The cell value changed but the destination materialized type did not.
    ValueChanged(Cell),
    /// The cell changed to match a different destination materialized type.
    TypeChanged(Cell),
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
    /// Returns the source-shaped type to use for destination materialization.
    fn get_compatible_type(
        &self,
        typ: &Type,
        compatibility: DestinationTypeCompatibility,
    ) -> TypeCompatibilityResult;

    /// Returns the cell to pass to destination encoding.
    fn get_compatible_cell(
        &self,
        cell: Cell,
        compatibility: DestinationTypeCompatibility,
    ) -> CellCompatibilityResult;
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
    pub fn get_compatible_cell(&self, cell: Cell) -> EtlResult<Cell> {
        match self.compatibility.get_compatible_cell(cell, self.type_compatibility) {
            CellCompatibilityResult::Unchanged(cell)
            | CellCompatibilityResult::ValueChanged(cell)
            | CellCompatibilityResult::TypeChanged(cell) => Ok(cell),
            CellCompatibilityResult::Invalid { kind, reason } => {
                Err(etl_error!(kind, "Cell is incompatible with destination", reason))
            }
        }
    }

    /// Returns a destination-compatible [`TableRow`].
    pub fn get_compatible_table_row(&self, table_row: TableRow) -> EtlResult<TableRow> {
        let values = table_row
            .into_values()
            .into_iter()
            .enumerate()
            .map(|(index, cell)| self.get_compatible_cell_at_index(cell, index))
            .collect::<EtlResult<Vec<_>>>()?;

        Ok(TableRow::new(values))
    }

    /// Returns destination-compatible tagged cells.
    pub fn get_compatible_tagged_cells(
        &self,
        tagged_cells: impl IntoIterator<Item = (usize, Cell)>,
    ) -> EtlResult<Vec<(usize, Cell)>> {
        tagged_cells
            .into_iter()
            .map(|(index, cell)| {
                let zero_based_index = index.saturating_sub(1);
                self.get_compatible_cell_at_index(cell, zero_based_index).map(|cell| (index, cell))
            })
            .collect()
    }

    /// Returns a destination-compatible cell with index context on failures.
    fn get_compatible_cell_at_index(&self, cell: Cell, index: usize) -> EtlResult<Cell> {
        self.get_compatible_cell(cell).map_err(|error| {
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
