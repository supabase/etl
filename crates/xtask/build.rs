//! Rebuilds xtask when embedded SQL migrations change.
//!
//! `sqlx::migrate!` embeds migration files at compile time, but Cargo does not
//! track them on stable Rust. Without this, an edited or removed migration is
//! silently left out of the binary.

/// Registers the migrations directory as a build input.
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
