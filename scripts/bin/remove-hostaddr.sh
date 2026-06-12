#!/bin/bash
# Remove the hostaddr field from a source configuration to force DNS resolution.
#
# Usage: ./scripts/bin/remove-hostaddr.sh --source-id <ID> [--tenant-id <TENANT_ID>] [--dry-run]
#
# Requires the same environment configuration as the etl-api.
#
# Examples:
#   # Dry-run: see what would change
#   ./scripts/bin/remove-hostaddr.sh --source-id 123 --dry-run
#
#   # Remove hostaddr for source 123
#   ./scripts/bin/remove-hostaddr.sh --source-id 123
#
#   # Remove hostaddr for source 123 in specific tenant
#   ./scripts/bin/remove-hostaddr.sh --source-id 123 --tenant-id abc123

set -euo pipefail

# Build and run the cargo xtask command
exec cargo xtask remove-hostaddr "$@"
