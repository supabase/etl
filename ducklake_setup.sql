-- ============================================================================
-- DuckLake + PostgreSQL End-to-End Example (Pure SQL)
-- ============================================================================
-- Run this entire script in DuckDB CLI or DuckDB Studio
-- Example: duckdb < ducklake_postgres_example.sql
-- 
-- Prerequisites:
-- - PostgreSQL instance running with a 'ducklake' database created
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
  AS ducklake (
    DATA_PATH 's3://dev-and-test/ducklake',
    DATA_INLINING_ROW_LIMIT 100000,
    AUTOMATIC_MIGRATION true,
    METADATA_SCHEMA 'ducklake'
  );

-- ============================================================================
-- STEP 5: Configure Parquet settings
-- ============================================================================
CALL ducklake.set_option('parquet_compression', 'zstd');
CALL ducklake.set_option('parquet_row_group_size_bytes', '20MB');
CALL ducklake.set_option('parquet_version', 2);
