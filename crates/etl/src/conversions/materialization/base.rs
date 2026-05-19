use std::fmt;

use tracing::debug;

use super::DestinationTypeCompatibility;
use crate::{
    error::{ErrorKind, EtlResult},
    etl_error,
    types::{Cell, Type},
};

/// Cell plus the type used to interpret it at this conversion stage.
#[derive(Debug, PartialEq)]
pub struct TypedCell<T, C> {
    /// Type used to interpret the cell at this conversion stage.
    typ: T,
    /// Cell value for the current conversion stage.
    cell: C,
}

impl<T, C> TypedCell<T, C> {
    /// Creates a typed cell.
    pub const fn new(typ: T, cell: C) -> Self {
        Self { typ, cell }
    }

    /// Returns the type used to interpret the cell at this conversion stage.
    pub fn typ(&self) -> &T {
        &self.typ
    }

    /// Returns the type and contained cell.
    pub fn into_parts(self) -> (T, C) {
        (self.typ, self.cell)
    }
}

/// Result of applying a destination materialization policy to a type.
#[derive(Debug, PartialEq, Eq)]
pub enum TypeMaterializationResult<T> {
    /// The type can be used unchanged for destination materialization.
    Unchanged(T),
    /// The source type should be materialized as another destination type.
    Changed(T),
    /// The source type cannot be represented by the destination.
    Invalid {
        /// Error kind to surface to callers.
        kind: ErrorKind,
        /// Human-readable reason for the materialization failure.
        reason: String,
    },
}

/// Result of applying a destination materialization policy to a typed cell.
#[derive(Debug, PartialEq)]
pub enum CellMaterializationResult<T, C> {
    /// The cell can be written to the materialized destination type.
    Materialized {
        /// Materialized cell plus destination type.
        cell: TypedCell<T, C>,
        /// How the cell changed during materialization.
        outcome: MaterializationOutcome,
    },
    /// The cell cannot be represented by the destination.
    Invalid {
        /// Error kind to surface to callers.
        kind: ErrorKind,
        /// Human-readable reason for the materialization failure.
        reason: String,
    },
}

/// Successful cell materialization outcome.
#[derive(Debug, PartialEq, Eq)]
pub enum MaterializationOutcome {
    /// The cell can be written unchanged.
    Unchanged,
    /// The cell value changed but the destination materialized type did not.
    ValueChanged,
    /// The cell changed to match a different materialized type.
    TypeChanged,
}

impl MaterializationOutcome {
    /// Returns a stable log label for the outcome.
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Unchanged => "unchanged",
            Self::ValueChanged => "value_changed",
            Self::TypeChanged => "type_changed",
        }
    }
}

/// Destination-specific rules for source types and cells.
pub trait MaterializationRules {
    /// Destination-specific type produced by materialization.
    type MaterializedType;

    /// Destination-specific cell representation produced by materialization.
    type MaterializedCell;

    /// Returns the destination type to use for materialization.
    fn materialize_type(
        &self,
        typ: &Type,
        compatibility: DestinationTypeCompatibility,
    ) -> TypeMaterializationResult<Self::MaterializedType>;

    /// Returns the destination cell and its materialized destination type.
    ///
    /// The returned cell's type must match [`Self::materialize_type`] for the
    /// input cell's type under the same materialization policy.
    fn materialize_cell(
        &self,
        typed_cell: TypedCell<Type, Cell>,
        compatibility: DestinationTypeCompatibility,
    ) -> CellMaterializationResult<Self::MaterializedType, Self::MaterializedCell>;
}

/// Typed cell produced by a destination materialization implementation.
pub type MaterializedCell<R> = TypedCell<
    <R as MaterializationRules>::MaterializedType,
    <R as MaterializationRules>::MaterializedCell,
>;

/// Orchestrates materialization for one destination.
#[derive(Debug, Clone)]
pub struct DestinationMaterializer<C: MaterializationRules> {
    /// Selected type materialization policy.
    type_compatibility: DestinationTypeCompatibility,
    /// Destination-specific materialization rules.
    rules: C,
}

impl<C> DestinationMaterializer<C>
where
    C: MaterializationRules,
    C::MaterializedType: fmt::Display + PartialEq,
{
    /// Creates a materializer for a destination.
    pub fn new(type_compatibility: DestinationTypeCompatibility, rules: C) -> Self {
        Self { type_compatibility, rules }
    }

    /// Returns the configured type materialization policy.
    pub const fn type_compatibility(&self) -> DestinationTypeCompatibility {
        self.type_compatibility
    }

    /// Returns the destination materialized type or an [`EtlError`].
    pub fn materialize_type(&self, typ: &Type) -> EtlResult<C::MaterializedType> {
        match self.rules.materialize_type(typ, self.type_compatibility) {
            TypeMaterializationResult::Unchanged(typ) | TypeMaterializationResult::Changed(typ) => {
                Ok(typ)
            }
            TypeMaterializationResult::Invalid { kind, reason } => {
                Err(etl_error!(kind, "Type cannot be materialized for destination", reason))
            }
        }
    }

    /// Returns the destination materialized cell or an [`EtlError`].
    pub fn materialize_cell(
        &self,
        typed_cell: TypedCell<Type, Cell>,
    ) -> EtlResult<MaterializedCell<C>> {
        self.materialize_cell_with_type(typed_cell)
    }

    /// Returns destination materialized cells.
    pub fn materialize_cells<M>(
        &self,
        typed_cells: impl IntoIterator<Item = (M, TypedCell<Type, Cell>)>,
    ) -> EtlResult<Vec<(M, MaterializedCell<C>)>> {
        typed_cells
            .into_iter()
            .enumerate()
            .map(|(index, (metadata, typed_cell))| {
                self.materialize_cell_at_index(typed_cell, index).map(|cell| (metadata, cell))
            })
            .collect()
    }

    /// Returns the destination materialized cell and validates its type.
    fn materialize_cell_with_type(
        &self,
        typed_cell: TypedCell<Type, Cell>,
    ) -> EtlResult<MaterializedCell<C>> {
        let source_type = typed_cell.typ().clone();
        let materialized_type = self.materialize_type(&source_type)?;
        match self.rules.materialize_cell(typed_cell, self.type_compatibility) {
            CellMaterializationResult::Materialized { cell, outcome } => self
                .validate_and_log_materialized_cell(
                    &source_type,
                    &materialized_type,
                    outcome,
                    cell,
                ),
            CellMaterializationResult::Invalid { kind, reason } => {
                Err(etl_error!(kind, "Cell cannot be materialized for destination", reason))
            }
        }
    }

    /// Validates and logs a destination materialized cell.
    fn validate_and_log_materialized_cell(
        &self,
        source_type: &Type,
        materialized_type: &C::MaterializedType,
        materialization_outcome: MaterializationOutcome,
        cell: MaterializedCell<C>,
    ) -> EtlResult<MaterializedCell<C>> {
        if cell.typ() != materialized_type {
            return Err(etl_error!(
                ErrorKind::InvalidState,
                "Cell materialization type mismatch",
                format!("Expected materialized type {}, got {}", materialized_type, cell.typ())
            ));
        }

        debug!(
            source_type = source_type.name(),
            materialized_type = %materialized_type,
            compatibility_mode = ?self.type_compatibility.mode(),
            materialization_outcome = materialization_outcome.as_str(),
            "materialized destination cell"
        );

        Ok(cell)
    }

    /// Returns a destination materialized cell with index context on failures.
    fn materialize_cell_at_index(
        &self,
        typed_cell: TypedCell<Type, Cell>,
        index: usize,
    ) -> EtlResult<MaterializedCell<C>> {
        self.materialize_cell_with_type(typed_cell).map_err(|error| {
            let kind = error.kind();
            let detail = error.detail().map_or_else(
                || format!("Cell at index {index} failed materialization"),
                |detail| format!("Cell at index {index} failed materialization: {detail}"),
            );

            etl_error!(
                kind,
                "Cell materialization failed",
                detail,
                source: error
            )
        })
    }
}
