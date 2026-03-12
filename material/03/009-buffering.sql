-- Memory management in DuckDB
--
-- DuckDB attempts to make good use of primary memory (RAM) to save
-- I/O and speed up query processing.

-- Empty macOS file cache such that disk I/O needs to be performed
.shell sync
.shell sudo purge

-- Attach to TPC-H database of scale factor 10 (sf = 10), table
-- lineitem has cardinality 60 million rows. 'tpch-sf10.db' is hosted
-- on DuckDB's web server. We need to use a slow HTTPS connection
-- to access the table data:

ATTACH 'https://blobs.duckdb.org/data/tpch-sf10.db' AS tpch;
USE tpch;

-- Run query Q1 on table lineitem, measure runtime (do not display
-- the result table—otherwise, table display will dominate everything)
.timer on
.mode trash

SELECT l_orderkey, count(*)
FROM lineitem
GROUP BY l_orderkey;

-- Run Time (s): real 3.622 user 1.770295 sys 0.401456 (network latency)

-- After evaluation of Q1, pages of table lineitem are held in the
-- in-memory buffer (see tag BASE_TABLE):
.mode duckbox
FROM duckdb_memory();

-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │          130285568 │                       0 │ all rows of lineitem.l_orderkey buffered in RAM
-- │ HASH_TABLE          │                  0 │                       0 │
-- │ PARQUET_READER      │                  0 │                       0 │
-- │ CSV_READER          │                  0 │                       0 │
-- │ ORDER_BY            │                  0 │                       0 │
-- │ ART_INDEX           │                  0 │                       0 │
-- │ COLUMN_DATA         │                  0 │                       0 │
-- │ METADATA            │                  0 │                       0 │
-- │ OVERFLOW_STRINGS    │                  0 │                       0 │
-- │ IN_MEMORY_TABLE     │                  0 │                       0 │
-- │ ALLOCATOR           │                  0 │                       0 │
-- │ EXTENSION           │                  0 │                       0 │
-- │ TRANSACTION         │                  0 │                       0 │
-- │ EXTERNAL_FILE_CACHE │                  0 │                       0 │
-- ├─────────────────────┴────────────────────┴─────────────────────────┤
-- │ 14 rows                                                  3 columns │
-- └────────────────────────────────────────────────────────────────────┘

-- Rerunning query Q1 reads lineitem table data from the buffer:
.mode trash

SELECT l_orderkey, count(*)
FROM lineitem
GROUP BY l_orderkey;

-- Run Time (s): real 0.177 user 1.579138 sys 0.109408
--                    ^^^^^
--                    ~20-fold speedup

-- Pages of lineitem remain in the same buffer locations to serve
-- subsequent queries:
.mode duckbox
FROM duckdb_memory();
