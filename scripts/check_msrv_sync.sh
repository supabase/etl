#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "${script_dir}/.." && pwd)

cd "${repo_root}"

cargo_msrv=$(
  python - <<'PY'
import sys, tomllib

try:
    with open("Cargo.toml", "rb") as f:
        data = tomllib.load(f)
    print(data["workspace"]["package"]["rust-version"])
except Exception:
    sys.exit(1)
PY
)

if [[ -z "${cargo_msrv}" ]]; then
  echo "failed to parse workspace.package.rust-version from Cargo.toml" >&2
  exit 1
fi

toolchain_msrv=$(
  sed -nE 's/^channel = "([^"]+)"$/\1/p' rust-toolchain.toml | head -n 1
)

if [[ -z "${toolchain_msrv}" ]]; then
  echo "failed to parse channel from rust-toolchain.toml" >&2
  exit 1
fi

# Use a workspace member manifest so cargo-msrv can resolve the inherited rust-version.
resolved_msrv=$(cargo msrv show --manifest-path etl/Cargo.toml --output-format minimal)

if [[ -z "${resolved_msrv}" ]]; then
  echo "failed to read rust-version via cargo-msrv" >&2
  exit 1
fi

if [[ "${cargo_msrv}" != "${toolchain_msrv}" ]]; then
  echo "workspace rust-version (${cargo_msrv}) does not match rust-toolchain channel (${toolchain_msrv})" >&2
  exit 1
fi

if [[ "${cargo_msrv}" != "${resolved_msrv}" ]]; then
  echo "workspace rust-version (${cargo_msrv}) does not match cargo-msrv output (${resolved_msrv})" >&2
  exit 1
fi

echo "verified msrv sync at rust ${cargo_msrv}"
