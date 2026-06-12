# Remove Hostaddr Script

This script removes the `hostaddr` field from a source configuration in the ETL API database, allowing the replicator to use DNS resolution instead of a hardcoded IP address.

## Problem

The replicator pod can fail with "Network is unreachable" when:
- A source is configured with a `hostaddr` field (e.g., an IPv6 address)
- The pod network cannot reach that address
- The DNS resolution would return a reachable address

By removing the `hostaddr` field, the replicator will use DNS resolution of the `host` field instead.

## Usage

### Using the shell script wrapper:

```bash
./scripts/bin/remove-hostaddr.sh --source-id <SOURCE_ID> [--tenant-id <TENANT_ID>] [--dry-run]
```

### Using cargo xtask directly:

```bash
cargo xtask remove-hostaddr --source-id <SOURCE_ID> [--tenant-id <TENANT_ID>] [--dry-run]
```

## Environment Variables

The script uses the same environment configuration as the etl-api. Ensure you have the standard etl-api environment variables set (typically `APP_ENVIRONMENT`, `APP_DATABASE_URL`, `APP_ENCRYPTION_KEY_CURRENT`, etc.).

## Examples

### Dry run first (recommended):

```bash
cargo xtask remove-hostaddr --source-id 123 --dry-run
```

### Remove hostaddr for a specific source:

```bash
# For source 123 in a specific tenant
cargo xtask remove-hostaddr --source-id 123 --tenant-id my-tenant

# For source 123 (any tenant)
cargo xtask remove-hostaddr --source-id 123
```

## Output

The script will:
1. Display the current source configuration
2. Show what will be changed (hostaddr removal)
3. In dry-run mode: show changes without updating
4. In live mode: update the database and confirm success

Example output:

```
[remove-hostaddr] starting
  source_id: 123
  tenant_id: my-tenant
  mode: live update

Found source:
  id: 123
  tenant_id: my-tenant
Current config:
  host: db.example.supabase.red
  hostaddr: 2a05:d014:a4e:5b02:771:7984:264b:9aff
  port: 5432
  name: postgres
  username: postgres

Updated config:
  hostaddr: None (removed)

[remove-hostaddr] successfully removed hostaddr from source 123
```

## Troubleshooting

### "Source with id X not found"
- Verify the source ID is correct
- If you specified `--tenant-id`, verify it matches the tenant that owns the source

### Database connection errors
- Verify `APP_DATABASE_URL` is correct and the database is accessible
- Ensure you have credentials to connect to the ETL API database

### Encryption errors
- Verify `APP_ENCRYPTION_KEY_CURRENT` matches the key used to encrypt the source config
- Check that the correct environment is being targeted

## Implementation Details

The script:
1. Loads the API configuration from environment variables
2. Builds the encryption keyring to decrypt/re-encrypt source configs
3. Queries the database for the source configuration
4. Decrypts the stored encrypted config
5. Removes the `hostaddr` field
6. Re-encrypts the updated config
7. Updates the database (unless `--dry-run` is set)

The source config is stored encrypted in the database, so the script must have access to the same encryption key used to store it.
