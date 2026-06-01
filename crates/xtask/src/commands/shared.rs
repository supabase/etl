use xshell::Cmd;

pub(crate) const DEFAULT_PG_SHARD_COUNT: u16 = 3;
pub(crate) const DEFAULT_BASE_PORT: u16 = 5430;
pub(crate) const READ_REPLICA_PORT_OFFSET: u16 = 1000;
pub(crate) const NIGHTLY_TOOLCHAIN: &str = "nightly-2026-04-15";

/// Returns whether sccache should be enabled for the spawned build.
///
/// Enabled when the `--sccache` flag is passed (`flag`) or when the
/// `ETL_SCCACHE` environment variable is set to `1`.
pub(crate) fn sccache_enabled(flag: bool) -> bool {
    flag || std::env::var("ETL_SCCACHE").is_ok_and(|value| value == "1")
}

/// Conditionally injects the sccache environment into a cargo command.
///
/// `CARGO_INCREMENTAL=0` is required because sccache cannot cache incremental
/// compilation. When disabled, the command is returned unchanged so the default
/// inner loop keeps incremental.
pub(crate) fn maybe_with_sccache(cmd: Cmd<'_>, flag: bool) -> Cmd<'_> {
    if !sccache_enabled(flag) {
        return cmd;
    }

    if !sccache_available() {
        eprintln!(
            "⚠️  sccache requested (--sccache / ETL_SCCACHE=1) but not found on PATH; continuing \
             without it. Install it with `brew install sccache`."
        );
        return cmd;
    }

    let cc = std::env::var("CC").unwrap_or_else(|_| "cc".into());
    let cxx = std::env::var("CXX").unwrap_or_else(|_| "c++".into());

    cmd.env("RUSTC_WRAPPER", "sccache")
        .env("CC", format!("sccache {cc}"))
        .env("CXX", format!("sccache {cxx}"))
        .env("CARGO_INCREMENTAL", "0")
        .env("SCCACHE_CACHE_SIZE", "20G")
}

/// Returns whether the `sccache` binary is callable on the current PATH.
fn sccache_available() -> bool {
    std::process::Command::new("sccache").arg("--version").output().is_ok()
}
