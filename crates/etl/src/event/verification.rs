//! Kani proofs for the packed event position.
//!
//! Destinations such as ClickHouse keep the row version with the largest packed
//! position, so a packing that reorders positions keeps the wrong version.

use super::EventPosition;
use crate::schema::PgLsn;

/// Returns a position with any commit LSN and transaction ordinal.
fn any_position() -> EventPosition {
    EventPosition::new(PgLsn::from(kani::any::<u64>()), kani::any())
}

/// Returns the `(commit_lsn, tx_ordinal)` order key of `position`.
fn order_key(position: EventPosition) -> (u64, u64) {
    (u64::from(position.commit_lsn), position.tx_ordinal)
}

/// Packed positions order exactly like commit LSN, then transaction ordinal,
/// so distinct positions never share a packed value.
#[kani::proof]
fn packed_position_preserves_order() {
    let a = any_position();
    let b = any_position();

    assert_eq!(a.as_u128().cmp(&b.as_u128()), order_key(a).cmp(&order_key(b)));
}

/// The commit LSN and transaction ordinal are recoverable from the packed
/// value's high and low halves.
#[kani::proof]
fn packed_position_splits_into_its_parts() {
    let position = any_position();
    let packed = position.as_u128();

    assert_eq!(u64::try_from(packed >> 64).unwrap(), u64::from(position.commit_lsn));
    assert_eq!(u64::try_from(packed & u128::from(u64::MAX)).unwrap(), position.tx_ordinal);
}
