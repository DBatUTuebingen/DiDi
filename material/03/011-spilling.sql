-- If intermediate data structures (e.g., aggregate hash tables) grow
-- large during query processing, store these on temporary storage.
-- This is known as "spilling."

-- Attach to the TPC-H (sf = 100) instance
ATTACH '../../databases/tpch-sf100.db' AS tpch (readonly);
USE tpch;

.timer on

-- By default, use 80% of available main memory to store table data
-- and intermediate data structures of query processing:
--
SELECT current_setting('memory_limit');

-- ┌─────────────────────────────────┐
-- │ current_setting('memory_limit') │
-- │             varchar             │
-- ├─────────────────────────────────┤
-- │ 51.1 GiB                        │ ≈80% of 64 GB on Torsten's MacBook Pro
-- └─────────────────────────────────┘


-- Run query Q1:
-- - Creates MANY (150 million) groups, but aggregation will still
--   fit in the large main memory.
-- - There still is plenty of space to buffer the relevant pages of table
--   lineitem (only column l_orderkey is required to process Q1).
--
.mode trash
SELECT l_orderkey, count(*)
FROM lineitem
GROUP BY l_orderkey;


-- Run Time (s): real 1.900 user 16.699751 sys 1.692981

.mode duckbox
FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │      1,295,777,792 │                       0 │ all rows of lineitem.l_orderkey
-- │ HASH_TABLE          │                  0 │                       0 │ placed in buffer (uses 1.3 GB)
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


-- Rerunning query Q1 does not change memory usage
-- (all rows of lineitem already buffered)
--
.mode trash
SELECT l_orderkey, count(*)
FROM lineitem
GROUP BY l_orderkey;

-- Run Time (s): real 1.721 user 16.564913 sys 0.926055 (good //ization)

.mode duckbox
FROM duckdb_memory();


-- Only column l_orderkey is accessed during the scan of table lineitem:
--
EXPLAIN ANALYZE
SELECT l_orderkey, count(*)
FROM lineitem
GROUP BY l_orderkey;

-- └─────────────┬─────────────┘
-- ┌─────────────┴─────────────┐
-- │         SEQ_SCAN          │
-- │    ────────────────────   │
-- │      Table: lineitem      │
-- │   Type: Sequential Scan   │
-- │                           │
-- │        Projections:       │
-- │         l_orderkey        │
-- │                           │
-- │     ~600,037,902 rows     │
-- └───────────────────────────┘

-- Accessing more columns enlarges the memory footprint of query Q1
-- (buffer space for columns l_orderkey AND l_partkey is allocated):
--
.mode trash
SELECT l_orderkey, any_value(l_partkey), count(*)
FROM lineitem
GROUP BY l_orderkey;

.mode duckbox
FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │         3733192704 │                       0 │ all rows of lineitem.l_orderkey and
-- │ HASH_TABLE          │                  0 │                       0 │ lineitem.l_partkey in buffer (3.7 GB)

-----------------------------------------------------------------------

-- Now constrain main memory usage (~1.8 GB)
SET memory_limit = '2GB';
SELECT current_setting('memory_limit');

-- If required, spill temporary data structures into OS filesystem
-- directory spill/:
SET temp_directory = 'spill';

-- Rerun query Q1, the aggregate hash table will not fit into memory
-- any longer and will spill to disk (in files in directory spill/)
-- - now uses about 4s
--
.mode trash
SELECT l_orderkey, count(*)
FROM lineitem
GROUP BY l_orderkey;

-- Run Time (s): real 3.961 user 31.868395 sys 6.676574
--                    ^^^^^


.mode duckbox
FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │                  0 │                       0 │ no rows of lineitem buffered
-- │ HASH_TABLE          │                  0 │             23485186048 │ hash table held on SSD storage
-- │ PARQUET_READER      │                  0 │                       0 │
-- │ CSV_READER          │                  0 │                       0 │


-- Rerunning query Q1 adds to temporary HASH_TABLE storage each time
-- (if max_temp_directory_size is approached, DuckDB cleans up)
--
.mode trash
SELECT l_orderkey, count(*)
FROM lineitem
GROUP BY l_orderkey;

.mode duckbox
FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │                  0 │                       0 │
-- │ HASH_TABLE          │                  0 │             59565539328 │


-- Now also constrain size of temporary storage in directory spill/ to 2 GB
-- (this will probably not suffice to hold the large aggregate hash table
-- which appeared to occupy ~23GB)
SET max_temp_directory_size = '2GB';

-- Rerun query Q1 (⚠️ this will be aborted):
--
.mode trash
SELECT l_orderkey, count(*)
FROM lineitem
GROUP BY l_orderkey;

-- Out of Memory Error:
-- failed to offload data block of size 256.0 KiB (1.8 GiB/1.8 GiB used).
-- This limit was set by the 'max_temp_directory_size' setting.
-- By default, this setting utilizes the available disk space on the drive where the 'temp_directory' is located.
-- You can adjust this setting, by using (for example) PRAGMA max_temp_directory_size='10GiB'

-- Possible solutions:
-- * Reducing the number of threads (SET threads=X)
-- * Disabling insertion-order preservation (SET preserve_insertion_order=false)
-- * Increasing the memory limit (SET memory_limit='...GB')

-- See also https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads


-- Back to generous main memory usage
.mode duckbox
SET memory_limit = '50GB';
SELECT current_setting('memory_limit');

-- Rerun query Q1
-- - Now uses about 1.7 seconds again
--
.mode trash
SELECT l_orderkey, count(*)
FROM lineitem
GROUP BY l_orderkey;

.mode duckbox
FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │         1280311296 │                       0 │ buffering is possible again
-- │ HASH_TABLE          │                  0 │             70595969024 │


.quit

-----------------------------------------------------------------------

-- Replace query Q1 by query Q2 with SIGNIFICANTLY smaller number of groups
-- (3 groups) and thus smaller hash table:
-- - Uses 0.3s.

ATTACH '../../databases/tpch-sf100.db' AS tpch (readonly);
USE tpch;

.timer on

SELECT l_returnflag, count(*)
FROM lineitem
GROUP BY l_returnflag;

-- ┌──────────────┬──────────────┐
-- │ l_returnflag │ count_star() │
-- │   varchar    │    int64     │
-- ├──────────────┼──────────────┤
-- │ A            │    148047881 │
-- │ N            │    303922760 │
-- │ R            │    148067261 │
-- └──────────────┴──────────────┘

-- Run Time (s): real 0.287 user 2.792068 sys 0.287598

FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │         1295777792 │                       0 │ buffering of lineitem.l_orderkey


-- Now SEVERELY constrain main memory usage: ~3.8 MB(!)
SET memory_limit = '4MB';
SELECT current_setting('memory_limit');

-- Rerun query Q2
-- - Still uses about 0.3s (fully //ized).

SELECT l_returnflag, count(*)
FROM lineitem
GROUP BY l_returnflag;


-- Indeed, *before* query execution DuckDB used statistics for
-- column l_returnflag to detect that it's active domain merely
-- has size 3.  It thus plans the grouping operation accordingly
-- (PERFECT_HASH_GROUP_BY):
--
EXPLAIN                         -- EXPLAIN: query plan annotated *before*
SELECT l_returnflag, count(*)   --          query execution
FROM lineitem
GROUP BY l_returnflag;

-- Statistics for table lineitem (in particular "approx_unique" for
-- column "l_returnflag")
--
SET memory_limit = '50GB';

SELECT column_name, column_type, min, max, approx_unique, null_percentage
FROM   (SUMMARIZE lineitem);

-- ┌───────────────────────────┐
-- │         PROJECTION        │
-- │    ────────────────────   │
-- │__internal_decompress_strin│
-- │           g(#0)           │
-- │             #1            │
-- │                           │
-- │          ~3 rows          │ <-- prediction: the grouping result
-- └─────────────┬─────────────┘     will only have 3 rows
-- ┌─────────────┴─────────────┐
-- │   PERFECT_HASH_GROUP_BY   │ <-- a variant of HASH_GROUP_BY that
-- │    ────────────────────   │     allocates a tiny hash table and
-- │         Groups: #0        │     will not need to handle collisions
-- │                           │
-- │        Aggregates:        │
-- │        count_star()       │
-- └─────────────┬─────────────┘





-- The three-row hash table uses negligible RAM, can use almost of all the
-- 3.8 MB to buffer column lineitem.l_orderkey:

FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │            3407872 │                       0 │ buffering lineitem.l_orderkey (3.4 MB)
-- │ HASH_TABLE          │                  0 │                       0 │
