#!/usr/bin/env bash
set -euo pipefail

TOOLCHAIN="${RUSTFMT_NIGHTLY_TOOLCHAIN:-nightly-2026-04-15}"

exec cargo +"${TOOLCHAIN}" fmt --all -- --check "$@"
