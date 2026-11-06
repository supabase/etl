#!/usr/bin/env bash
# Temporary instrumentation for the pooled-profile CI comparison.
set -euo pipefail

export COVERAGE_TIMING_DIR="$PWD/target/coverage-comparison"
mkdir -p "$COVERAGE_TIMING_DIR"
llvm_libdir="$(rustc --print target-libdir)"
COVERAGE_LLVM_BIN="$(dirname "$llvm_libdir")/bin"
export COVERAGE_LLVM_BIN

cat > "$COVERAGE_TIMING_DIR/llvm-wrapper" <<'WRAPPER'
#!/usr/bin/env bash
set -euo pipefail
name="$(basename "$0")"
if [[ "${1:-}" == merge || "${1:-}" == export ]]; then
  exec /usr/bin/time -f '%e' -o "$COVERAGE_TIMING_DIR/$name.seconds" \
    "$COVERAGE_LLVM_BIN/$name" "$@"
fi
exec "$COVERAGE_LLVM_BIN/$name" "$@"
WRAPPER
chmod +x "$COVERAGE_TIMING_DIR/llvm-wrapper"
ln -s llvm-wrapper "$COVERAGE_TIMING_DIR/llvm-profdata"
ln -s llvm-wrapper "$COVERAGE_TIMING_DIR/llvm-cov"
export LLVM_PROFDATA="$COVERAGE_TIMING_DIR/llvm-profdata"
export LLVM_COV="$COVERAGE_TIMING_DIR/llvm-cov"

python3 - <<'PYTHON'
import json
import os
import subprocess
from pathlib import Path

profiles = list(Path("target/llvm-cov-target").rglob("*.profraw"))
if not profiles:
    raise SystemExit("No coverage profiles were generated")
result = {
    "sample": os.environ.get("COVERAGE_EXPERIMENT", ""),
    "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    "cpus": int(subprocess.check_output(["nproc"], text=True)),
    "profile_count": len(profiles),
    "profile_bytes": sum(path.stat().st_size for path in profiles),
}
Path("target/coverage-comparison/result.json").write_text(json.dumps(result))
PYTHON

/usr/bin/time -f '%e' -o "$COVERAGE_TIMING_DIR/report.seconds" \
  cargo llvm-cov report --locked --lcov --output-path lcov.info

python3 - <<'PYTHON'
import json
from pathlib import Path

root = Path("target/coverage-comparison")
result = json.loads((root / "result.json").read_text())
for name in ("tests", "llvm-profdata", "llvm-cov", "report"):
    result[f"{name}_seconds"] = float((root / f"{name}.seconds").read_text().strip())
result["tests_and_report_seconds"] = result["tests_seconds"] + result["report_seconds"]
(root / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result, indent=2))
PYTHON
