-- ============================================================================
-- DuckLake + PostgreSQL End-to-End Example (Pure SQL)
-- ============================================================================
-- Run this entire script in DuckDB CLI or DuckDB Studio
-- Example: duckdb < ducklake_postgres_example.sql
-- 
-- Prerequisites:
-- - PostgreSQL instance running with a 'ducklake_catalog' database created
-- - Valid PostgreSQL credentials
-- - Network access to PostgreSQL from your DuckDB process

-- ============================================================================
-- STEP 1: Configure writer session
-- ============================================================================
SET preserve_insertion_order = false;

-- ============================================================================
-- STEP 2: Install and load required extensions
-- ============================================================================
INSTALL ducklake;
INSTALL postgres;
INSTALL httpfs;
LOAD ducklake;
LOAD postgres;
LOAD httpfs;

-- ============================================================================
-- STEP 3: Configure S3 object store credentials
-- ============================================================================
-- Credentials sourced from pipeline-config.yaml destination.ducklake
SET enable_http_metadata_cache = true;
SET parquet_metadata_cache = true;
CREATE OR REPLACE SECRET ducklake_s3 (
  TYPE S3,
  KEY_ID 'minio-admin',
  SECRET 'minio-admin-password',
  ENDPOINT 'localhost:9010',
  URL_STYLE 'path',
  USE_SSL false,
  REGION 'us-east-1'
);

-- ============================================================================
-- STEP 4: Attach DuckLake catalog
-- ============================================================================
-- Catalog URL: postgres://postgres:postgres@host.docker.internal:5430/postgres
-- Data path: s3://dev-and-test/ducklake
ATTACH 'ducklake:postgres:user=''postgres'' password=''postgres'' host=''localhost'' port=5430 dbname=''postgres'''
  AS ducklake_catalog (
    DATA_PATH 's3://dev-and-test/ducklake',
    DATA_INLINING_ROW_LIMIT 100000,
    AUTOMATIC_MIGRATION true,
    METADATA_SCHEMA 'ducklake'
  );
USE ducklake_catalog;

-- ============================================================================
-- STEP 5: Configure Parquet settings
-- ============================================================================
CALL ducklake_catalog.set_option('parquet_compression', 'zstd');
CALL ducklake_catalog.set_option('parquet_row_group_size_bytes', '20MB');
CALL ducklake_catalog.set_option('parquet_version', 2);


-- ============================================================================
-- CONNECTION VERIFICATION - Test Queries
-- ============================================================================

-- List available tables in the ducklake catalog
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_catalog = 'ducklake_catalog'
ORDER BY table_schema, table_name;

-- ============================================================================
-- Maintenance ops
-- ============================================================================

CALL ducklake_flush_inlined_data('ducklake_catalog');
CALL ducklake_expire_snapshots('ducklake_catalog', 'etl_test', <timestamp_or_snapshot_id>);
CALL ducklake_cleanup_old_files('ducklake_catalog', 'your_table_name');


-- ============================================================================
-- Create test table
-- ============================================================================
CREATE TABLE users (
  id INTEGER,
  name VARCHAR,
  email VARCHAR,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Insert sample data
-- ============================================================================
INSERT INTO users (id, name, email) VALUES
  (1, 'Alice Johnson', 'alice@example.com'),
  (2, 'Bob Smith', 'bob@example.com'),
  (3, 'Charlie Brown', 'charlie@example.com');

-- ============================================================================
-- Test queries to verify connection and data integrity
-- ============================================================================

-- TEST 1: List all tables in the DuckLake catalog
SELECT '═══ TEST 1: List Tables ═══' as test;
SELECT database_name, schema_name, table_name 
FROM duckdb_tables() 
WHERE database_name = 'ducklake_catalog'
ORDER BY schema_name, table_name;

-- TEST 2: Query the sample data
SELECT '═══ TEST 2: Sample Data ═══' as test;
SELECT * FROM users ORDER BY id;

-- TEST 3: Get row count
SELECT '═══ TEST 3: Row Count ═══' as test;
SELECT COUNT(*) as total_rows FROM users;

-- TEST 4: View table schema
SELECT '═══ TEST 4: Table Schema ═══' as test;
DESCRIBE users;

-- TEST 5: Basic statistics
SELECT '═══ TEST 5: Data Statistics ═══' as test;
SELECT 
  COUNT(*) as total_rows, 
  MIN(id) as min_id, 
  MAX(id) as max_id,
  COUNT(DISTINCT name) as unique_names
FROM users;

-- ============================================================================
-- Demonstrate writes and updates
-- ============================================================================

-- TEST 6: Update a record
SELECT '═══ TEST 6: Update Test ═══' as test;
UPDATE users SET email = 'alice.johnson@example.com' WHERE id = 1;
SELECT 'Updated Alice''s email' as action;
SELECT * FROM users WHERE id = 1;

-- TEST 7: Insert a new record
SELECT '═══ TEST 7: Insert Test ═══' as test;
INSERT INTO users (id, name, email) VALUES (4, 'Diana Prince', 'diana@example.com');
SELECT 'Inserted new user Diana' as action;
SELECT COUNT(*) as total_rows_after_insert FROM users;

-- TEST 8: Delete operation
SELECT '═══ TEST 8: Delete Test ═══' as test;
DELETE FROM users WHERE id = 4;
SELECT 'Deleted Diana' as action;
SELECT COUNT(*) as total_rows_after_delete FROM users;

-- ============================================================================
-- Final verification
-- ============================================================================
SELECT '═══ FINAL VERIFICATION ═══' as test;
SELECT 
  'All tests completed successfully' as status,
  COUNT(*) as final_row_count
FROM users;

-- Display all final data
SELECT '═══ Final Data ═══' as test;
SELECT * FROM users ORDER BY id;

-- ============================================================================
-- CLEANUP (uncomment to run)
-- ============================================================================
-- DROP TABLE users;
-- DETACH ducklake_catalog;