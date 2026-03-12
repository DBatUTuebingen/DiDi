-- DuckDB allocates data structures in main memory if possible
-- but is able to (partially) spill paged-sized table-like
-- data structures to secondary storage (SSD) if memory becomes tight.

SELECT current_setting('memory_limit');

-- ┌─────────────────────────────────┐
-- │ current_setting('memory_limit') │
-- │             varchar             │
-- ├─────────────────────────────────┤
-- │ 51.1 GiB                        │ ≈80% of 64 GB on Torsten's MacBook Pro
-- └─────────────────────────────────┘


-- Create a sizable table t (fits entirely in memory)
CREATE OR REPLACE TEMPORARY TABLE t AS
  -- SELECT 1 :: bigint                         -- optional: uncomment one of these
  SELECT (random() * 1_000_000) :: bigint       -- to see memory requirements adapt
                                                -- on on-disk temporary storage (compression)
  FROM generate_series(1, 1_000_000);

DESCRIBE t;
-- ┌─────────────────┬─────────────┬─────────┬─────────┬─────────┬─────────┐
-- │   column_name   │ column_type │  null   │   key   │ default │  extra  │
-- │     varchar     │   varchar   │ varchar │ varchar │ varchar │ varchar │
-- ├─────────────────┼─────────────┼─────────┼─────────┼─────────┼─────────┤
-- │ generate_series │ BIGINT      │ YES     │ ▢       │ ▢       │ ▢       │ -- 8 bytes per value
-- └─────────────────┴─────────────┴─────────┴─────────┴─────────┴─────────┘

-- Table t entirely fits into DuckDB's working memory:
FROM duckdb_memory();

-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │                  0 │                       0 │
-- │ HASH_TABLE          │                  0 │                       0 │
-- │ PARQUET_READER      │                  0 │                       0 │
-- │ CSV_READER          │                  0 │                       0 │
-- │ ORDER_BY            │                  0 │                       0 │
-- │ ART_INDEX           │                  0 │                       0 │
-- │ COLUMN_DATA         │                  0 │                       0 │
-- │ METADATA            │                  0 │                       0 │
-- │ OVERFLOW_STRINGS    │                  0 │                       0 │
-- │ IN_MEMORY_TABLE     │           11028480 │                       0 │ t requires ≈11 MB of RAM
-- │ ALLOCATOR           │                  0 │                       0 │ (≈11 bytes per row)
-- │ EXTENSION           │                  0 │                       0 │
-- │ TRANSACTION         │                  0 │                       0 │
-- │ EXTERNAL_FILE_CACHE │                  0 │                       0 │
-- ├─────────────────────┴────────────────────┴─────────────────────────┤
-- │ 14 rows                                                  3 columns │
-- └────────────────────────────────────────────────────────────────────┘


-- Now artificially constrain acceptable DuckDB's RAM usage to 4 MB
-- (this will not fit table t any longer).  Use directory 'spill/' to
-- spill data if needed:
SET temp_directory = 'spill';
SET memory_limit = '4MB';

-- Table t only partially fits into RAM, other pages of t have been
-- spilled to disk.
--
-- - Slightly less than 4 MB worth of pages of t remain in RAM,
--   all other pages have been spilled to disk.
-- - Recall: compression is applied to on-disk table data.
--
FROM duckdb_memory();

-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │                  0 │                       0 │
-- │ HASH_TABLE          │                  0 │                       0 │
-- │ PARQUET_READER      │                  0 │                       0 │
-- │ CSV_READER          │                  0 │                       0 │
-- │ ORDER_BY            │                  0 │                       0 │
-- │ ART_INDEX           │                  0 │                       0 │
-- │ COLUMN_DATA         │                  0 │                       0 │
-- │ METADATA            │                  0 │                       0 │
-- │ OVERFLOW_STRINGS    │                  0 │                       0 │
-- │ IN_MEMORY_TABLE     │            3950592 │                 3145728 │
-- │ ALLOCATOR           │                  0 │                       0 │
-- │ EXTENSION           │                  0 │                       0 │
-- │ TRANSACTION         │                  0 │                       0 │
-- │ EXTERNAL_FILE_CACHE │                  0 │                       0 │
-- ├─────────────────────┴────────────────────┴─────────────────────────┤
-- │ 14 rows                                                  3 columns │
-- └────────────────────────────────────────────────────────────────────┘


-- Data is spilled using fixed-size pages of various size categories,
-- table data is held in pages of 256KB, DuckDB's default page size:
--
--
FROM duckdb_temporary_files();

-- ┌─────────────────────────────────────────┬─────────┐
-- │                  path                   │  size   │
-- │                 varchar                 │  int64  │
-- ├─────────────────────────────────────────┼─────────┤
-- │ spill/duckdb_temp_storage_S32K-0.tmp    │  131072 │ 4 ×  32K pages
-- │ spill/duckdb_temp_storage_S64K-0.tmp    │  786432 │12 ×  64K pages
-- │ spill/duckdb_temp_storage_S96K-0.tmp    │  196608 │ 2 ×  96K pages
-- │ spill/duckdb_temp_storage_S128K-0.tmp   │  524288 │ 4 × 128K pages
-- │ spill/duckdb_temp_storage_DEFAULT-0.tmp │ 1835008 │ 7 × 256K pages
-- └─────────────────────────────────────────┴─────────┘

-- This should come close to or be slightly larger than the
-- temporary_storage_bytes shown by duckdb_memory():
--
SELECT sum(size)
FROM duckdb_temporary_files();

-- These temp files live in ordinary OS files under directory spill/
-- (will be automatically removed when the connection to the database
-- is closed):
.shell ls -l spill


-- Ending the session will clean-up directory spill/
.quit
