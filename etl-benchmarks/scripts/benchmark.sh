#!/usr/bin/env bash
set -euo pipefail

echo "etl-benchmarks/scripts/benchmark.sh is deprecated; use cargo xtask benchmark." >&2
exec cargo xtask benchmark "$@"
