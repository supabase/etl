#!/usr/bin/env bash
set -euo pipefail

# semver_bump.sh VERSION BUMP_KIND
# VERSION: MAJOR.MINOR.PATCH
# BUMP_KIND: patch | minor | major

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <MAJOR.MINOR.PATCH> <patch|minor|major>" >&2
  exit 1
fi

VERSION="$1"
BUMP_KIND="$2"

if [[ ! "${VERSION}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]]; then
  echo "invalid version: ${VERSION}" >&2
  exit 1
fi

MAJOR="${BASH_REMATCH[1]}"
MINOR="${BASH_REMATCH[2]}"
PATCH="${BASH_REMATCH[3]}"

case "${BUMP_KIND}" in
  patch)
    PATCH=$((PATCH + 1))
    ;;
  minor)
    MINOR=$((MINOR + 1))
    PATCH=0
    ;;
  major)
    MAJOR=$((MAJOR + 1))
    MINOR=0
    PATCH=0
    ;;
  *)
    echo "invalid bump kind: ${BUMP_KIND}" >&2
    exit 1
    ;;
esac

echo "${MAJOR}.${MINOR}.${PATCH}"

