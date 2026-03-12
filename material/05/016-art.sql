-- DuckDB implements Adaptive Radix Trees (ART) to support
-- the evaluation of simple lookup predicates on tables.


-- To experiment with ART indexes, copy data from a sizable
-- TPC-H instance (sf = 100) in memory (do not measure the
-- impact of maintaining indexes on disk).  We only refer
-- to TPC-H table "orders" of 150,000,000 rows below.

ATTACH '../../databases/tpch-sf100.db' AS tpch (readonly);
COPY FROM DATABASE tpch TO memory;

DESCRIBE orders;

SELECT count(*)
FROM   orders;

SELECT o_clerk
FROM   orders
LIMIT  10;

.timer on

-- Create an ART index on non-unique text column orders(o_clerk),
-- this is in itself a costly operation
CREATE INDEX orders_clerk_idx ON orders(o_clerk);

-- An index consumes a significant amount of working memory (≈ 3 GB)
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
-- │ ART_INDEX           │         3237216256 │                       0 │
-- │ COLUMN_DATA         │                  0 │                       0 │
-- │ METADATA            │                  0 │                       0 │
-- │ OVERFLOW_STRINGS    │                  0 │                       0 │
-- │ IN_MEMORY_TABLE     │        49703608320 │             76488900608 │


-- DuckDB configuration: when will the DBMS consider to use an
-- ART index when it performs a TABLE_SCAN over table t?
--
--   # expected rows <= MAX(index_scan_max_count, index_scan_percentage * |t|)
--                                                └───────────────────┘
--                                            maximum acceptable selectivity
--
SELECT current_setting('index_scan_percentage');  -- default: 0.001
SELECT current_setting('index_scan_max_count');   -- default: 2048

-- Only use index lookup for TPC-H table orders if it is expected to yield
-- less than this many rows:
SELECT greatest(current_setting('index_scan_max_count'),
                current_setting('index_scan_percentage')
                * (SELECT count(*) FROM orders)) AS "max # of expected rows";

-- Query Q1 yields less rows than the threshold, we expect DuckDB
-- to use an Index Scan
--
-- Q1:
SELECT count(*)
FROM   orders AS o
WHERE  o.o_clerk = 'Clerk#000051887';

-- Indeed, TABLE_SCAN uses Index Scan:
EXPLAIN ANALYZE
SELECT count(*)
FROM   orders AS o
WHERE  o.o_clerk = 'Clerk#000051887';

-- ┌─────────────┴─────────────┐
-- │    UNGROUPED_AGGREGATE    │
-- │    ────────────────────   │
-- │        Aggregates:        │
-- │        count_star()       │
-- │                           │
-- │           1 Rows          │
-- │          (0.00s)          │
-- └─────────────┬─────────────┘
-- ┌─────────────┴─────────────┐
-- │         TABLE_SCAN        │
-- │    ────────────────────   │
-- │       Table: orders       │
-- │      Type: Index Scan     │  Index Scan ≡ TABLE_SCAN implemented
-- │                           │  in terms of an ART index lookup
-- │          Filters:         │
-- │ o_clerk='Clerk#000051887' │
-- │                           │
-- │         1518 Rows         │
-- │          (0.00s)          │
-- └───────────────────────────┘

-- Benchmark Q1 with and without index support
-- (repeat this run a few times to obtain stable timings of about 0.004s)

-- Will use Index Scan
SELECT count(*)
FROM   orders AS o
WHERE  o.o_clerk = 'Clerk#000051887';
-- Run Time (s): real 0.004 user 0.008222 sys 0.000550

.timer off

-- Now effectively disable index usage (threshold will be 0)
SET index_scan_max_count  = 0;
SET index_scan_percentage = 0.0;

-- Indeed: TABLE_SCAN now uses Sequential Scan for Q1
EXPLAIN ANALYZE
SELECT count(*)
FROM   orders AS o
WHERE  o.o_clerk = 'Clerk#000051887';

-- ┌─────────────┴─────────────┐
-- │         TABLE_SCAN        │
-- │    ────────────────────   │
-- │       Table: orders       │
-- │   Type: Sequential Scan   │  ART index ignored
-- │                           │
-- │          Filters:         │
-- │ o_clerk='Clerk#000051887' │
-- │                           │
-- │         1,518 rows        │
-- │          (1.32s)          │
-- └───────────────────────────┘

-- Repeat timing for Q1

.timer on

SELECT count(*)
FROM   orders AS o
WHERE  o.o_clerk = 'Clerk#000051887';
-- Run Time (s): real 0.109 user 1.022465 sys 0.114519

.timer off

-- Turn ART index support back on

RESET index_scan_max_count;
RESET index_scan_percentage;


-----------------------------------------------------------------------
-- How does index maintenance affect insertion/update performance?

.timer on

-- Index size before update
--
FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ ART_INDEX           │         3237216256 │                       0 │

-- Update: add postfix to indexed key, takes ~120s (no parallelism)
UPDATE orders
SET    o_clerk = o_clerk || o_orderkey
WHERE  o_orderpriority = '1-URGENT';
-- Run Time (s): real 117.047 user 123.273431 sys 16.160956

-- Index has grown in size after update (key size has grown)
--
FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ ART_INDEX           │         5006426112 │                       0 │


-- Now drop the index: we will save index maintenance on future updates
DROP INDEX orders_clerk_idx;

-- Repeat the update, no index maintenance required (the # of rows
-- with o_orderpriority = '3-MEDIUM' or '1-URGENT' is almost identical)
--
-- This now only takes ~8s (some parallelism)
UPDATE orders
SET    o_clerk = o_orderkey || o_clerk
WHERE  o_orderpriority = '3-MEDIUM';
-- Run Time (s): real 7.557 user 14.713685 sys 6.183807

.timer off


-----------------------------------------------------------------------
-- Presence of duplicates in a non-unique index has an impact on index
-- scan performance.  Since index leaves store row IDs that point into
-- column chunks, collecting the rows for a key leads to "jumping"
-- in the data file.
--
-- Based on experiments described in YouTube video https://www.youtube.com/watch?v=RywT9_K4QWg
-- "Indexes are (not) all you need: Common DuckDB pitfalls and how to find them"


-- 1. Create table t of 100 million rows
--
CREATE OR REPLACE TABLE t AS
  SELECT (i * 9_876_983_769_044 :: hugeint % 100_000_000) :: bigint AS i
  FROM   range(100_000_000) AS _(i);

-- Few duplicates: each i occurs four times
SELECT avg(occurs)
FROM   (SELECT count(i)
        FROM   t
        GROUP BY i) AS _(occurs);

-- Create non-unique index on t(i)
CREATE INDEX t_i_idx ON t(i);


-- Measure table scan performance (ignore index)

-- Disable index usage (threshold will be 0)
SET index_scan_max_count  = 0;
SET index_scan_percentage = 0.0;

.timer on

SELECT count(i)
FROM   t
WHERE  i = 1904;
-- Run Time (s): real 0.011 user 0.064499 sys 0.005875

-- Now use index t_i_idx on table to perform the key lookup

RESET index_scan_max_count;
RESET index_scan_percentage;

-- Index usage speeds up query evaluation.  Good.
--
SELECT count(i)
FROM   t
WHERE  i = 1904;
-- Run Time (s): real 0.001 user 0.001301 sys 0.000614

.timer off

-- 2. Recreate table t, many duplicates: now each i occurs 40,000 times
--
CREATE OR REPLACE TABLE t AS
  SELECT (i * 9_876_983_769_044 :: hugeint % 10_000) :: bigint AS i
  FROM   range(100_000_000) AS _(i);

SELECT avg(occurs)
FROM   (SELECT count(i)
        FROM   t
        GROUP BY i) AS _(occurs);

-- Create non-unique index on t(i)
CREATE INDEX t_i_idx ON t(i);

-- Measure table scan performance (ignore index)

-- Disable index usage (threshold will be 0)
SET index_scan_max_count  = 0;
SET index_scan_percentage = 0.0;

.timer on

SELECT count(i)
FROM   t
WHERE  i = 1904;
-- Run Time (s): real 0.011 user 0.102831 sys 0.001775
--                    ^^^^^

RESET index_scan_max_count;
RESET index_scan_percentage;


-- Use index t_i_idx on table to perform the key lookup: once
-- we reach the index leaf, dereference about 40,000 row IDs
-- into the column chunks for table t.  In effect, index usage
-- SLOWS DOWN the query. :-/
EXPLAIN ANALYZE
SELECT count(i)
FROM   t
WHERE  i = 1904;
-- Run Time (s): real 0.028 user 0.068331 sys 0.139631
--                    ^^^^^
--
-- ┌─────────────┴─────────────┐
-- │         TABLE_SCAN        │
-- │    ────────────────────   │
-- │          Table: t         │
-- │      Type: Index Scan     │
-- │       Projections: i      │
-- │      Filters: i=1904      │
-- │                           │
-- │        40,000 rows        │
-- │          (0.25s)          │
-- └───────────────────────────┘
