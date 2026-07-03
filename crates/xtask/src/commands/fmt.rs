use std::ffi::{OsStr, OsString};

use anyhow::Result;
use clap::Args;
use xshell::{Shell, cmd};

use crate::utils::NIGHTLY_TOOLCHAIN;

/// Name of the formatter subcommand in the raw process arguments.
const FMT_COMMAND: &str = "fmt";
/// Separator used by Cargo to forward trailing arguments to rustfmt.
const ARG_SEPARATOR: &str = "--";
/// Rustfmt check-mode argument.
const CHECK_ARG: &str = "--check";

/// Arguments for formatting the workspace with the pinned nightly rustfmt.
#[derive(Args)]
pub(crate) struct FmtArgs {
    /// Check formatting without modifying files.
    #[arg(long)]
    check: bool,

    /// Arguments passed through to `cargo fmt`.
    #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
    args: Vec<OsString>,
}

impl FmtArgs {
    /// Runs `cargo fmt --all` with the pinned nightly formatter toolchain.
    pub(crate) fn run(self) -> Result<()> {
        let sh = Shell::new()?;
        let FmtArgs { check, args: _ } = self;
        // Clap gives us typed access to `--check`, but it does not preserve the literal
        // `--` separator after parsing. Re-read the raw arguments so calls like `cargo
        // x fmt -- --config max_width=100` keep matching the deleted shell scripts'
        // passthrough behavior.
        let (cargo_fmt_args, rustfmt_args) = split_raw_passthrough_args(raw_fmt_args(), check);
        let toolchain = format!(
            "+{}",
            std::env::var("RUSTFMT_NIGHTLY_TOOLCHAIN")
                .unwrap_or_else(|_| NIGHTLY_TOOLCHAIN.to_owned())
        );

        let mut fmt = cmd!(sh, "cargo {toolchain} fmt --all").args(&cargo_fmt_args);
        if !rustfmt_args.is_empty() {
            // Rustfmt arguments must stay behind Cargo's `--` separator.
            fmt = fmt.arg(ARG_SEPARATOR).args(&rustfmt_args);
        }
        fmt.run()?;

        Ok(())
    }
}

/// Splits raw arguments into `cargo fmt` arguments and rustfmt arguments.
fn split_raw_passthrough_args(
    raw_args: Vec<OsString>,
    check: bool,
) -> (Vec<OsString>, Vec<OsString>) {
    let mut cargo_fmt_args = Vec::new();
    let mut rustfmt_args = Vec::new();
    let mut rustfmt_separator_seen = false;
    let mut check_arg_removed = false;

    for arg in raw_args {
        // Everything after `--` belongs to rustfmt, not `cargo fmt`.
        if !rustfmt_separator_seen && arg == OsStr::new(ARG_SEPARATOR) {
            rustfmt_separator_seen = true;
            continue;
        }

        // `cargo x fmt --check` is an xtask convenience flag, so remove it
        // from the Cargo side and inject it into the rustfmt side below.
        if !rustfmt_separator_seen && check && !check_arg_removed && arg == OsStr::new(CHECK_ARG) {
            check_arg_removed = true;
            continue;
        }

        if rustfmt_separator_seen {
            rustfmt_args.push(arg);
        } else {
            cargo_fmt_args.push(arg);
        }
    }

    if check {
        // Put `--check` first so user-provided rustfmt args keep their order.
        rustfmt_args.insert(0, OsString::from(CHECK_ARG));
    }

    (cargo_fmt_args, rustfmt_args)
}

/// Returns the raw process arguments after the `fmt` subcommand.
fn raw_fmt_args() -> Vec<OsString> {
    let mut args = std::env::args_os().skip(1);

    for arg in args.by_ref() {
        if arg == OsStr::new(FMT_COMMAND) {
            return args.collect();
        }
    }

    Vec::new()
}
