//! ETL value and row data model.

mod cell;
mod row;
mod size;

pub use cell::{ArrayCell, ArrayCellNonOptional, Cell, CellNonOptional};
pub use row::{OldTableRow, PartialTableRow, TableRow, UpdatedTableRow};
pub use size::SizeHint;
