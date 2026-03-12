-- How performant is DuckDB when we compute the sum of all quantities
-- (5th column "l_quantity") in the TPC-H lineitem table?

-- Report query time for all following queries
--
-- (For all such "dot commands" in the DuckDB CLI, see .help or
--  https://duckdb.org/docs/stable/clients/cli/dot_commands)
.timer on


-- Remember the TPC-H lineitem CSV file
-- (l_quantity is in the 5th column "column04")
SELECT count(*)
FROM   '../../databases/lineitem.csv';

FROM '../../databases/lineitem.csv'
LIMIT 10;

-- Supply TPC-H column names
FROM read_csv('lineitem.csv',
              header = false,
              names = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                       'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag',
                       'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate',
                       'l_shipinstruct', 'l_shipmode', 'l_comment'])
LIMIT 10;

-- Benchmark query (sum of all quantities)
SELECT sum(l_quantity)
FROM   read_csv('../../databases/lineitem.csv',
                header = false,
                names = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                         'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag',
                         'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate',
                         'l_shipinstruct', 'l_shipmode', 'l_comment']);

------------------------------------------------------------------------

-- The reported query time discloses that DuckDB uses thread-based
-- parallelism internally:
--
--     Run Time (s): real 0.460 user 3.259277 sys 0.148414
--                          |          |
--      elapsed wall-clock time       overall CPU time used by DuckDB

-- To verify, let us temporarily disable parallelism and
-- re-run the benchmark query (i.e., force T = 1):
SET threads = 1;

SELECT sum(l_quantity)
FROM   read_csv('../../databases/lineitem.csv',
                header = false,
                names = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                         'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag',
                         'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate',
                         'l_shipinstruct', 'l_shipmode', 'l_comment']);

-- Report query time now:
--
--     Run Time (s): real 2.837 user 2.662403 sys 0.171905
--
-- We got rid of thread-related overhead (e.g., partitioning), but the overall
-- query time is significantly worse.

-- Re-enable thread-based parallelism:
RESET threads;
SELECT current_setting('threads');  -- # of threads (T) used by DuckDB

------------------------------------------------------------------------
-- ⚠ SLIDES

-- Less than 0.5s query time is great for a generic CSV reader that has
-- to detect CSV dialects, handle/report errors, perform type conversion, etc.

-- SQL syntax:
--
-- Use modifier EXPLAIN [ANALYZE] to learn how DuckDB chooses to
-- evaluate a query and where time goes:
--
--   EXPLAIN                   -- do NOT run ‹query› but explain how
--   ‹query›                   -- ‹query› WOULD be run
--                             -- (reports estimated row counts)
--
--   EXPLAIN ANALYZE           -- RUN ‹query›, measure actual row counts
--   ‹query›                   -- and query times, then explain how
--                             -- ‹query› ACTUALLY WAS executed
--
-- Also see https://duckdb.org/docs/stable/sql/statements/profiling

-- NB.
-- - Read such plans bottom to top (rows flow "upwards")
-- - Row count estimate are off
-- - Table function read_csv() knows that l_quantity is the only relevant column
--   (this is called PROJECTION PUSHDOWN)
EXPLAIN
SELECT sum(l_quantity)
FROM   read_csv('../../databases/lineitem.csv',
                header = false,
                names = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                         'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag',
                         'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate',
                         'l_shipinstruct', 'l_shipmode', 'l_comment']);


-- NB.
-- - Now reports accurate row counts (aggregate sum() returns a single row)
-- - ALMOST ALL of the time is spent in read_csv(),
--   summing basically is "for free"
EXPLAIN ANALYZE
SELECT sum(l_quantity)
FROM   read_csv('../../databases/lineitem.csv',
                header = false,
                names = ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity',
                        'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag',
                        'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate',
                        'l_shipinstruct', 'l_shipmode', 'l_comment']);

/*
┌────────────────────────────────────────────────┐
│┌──────────────────────────────────────────────┐│
││              Total Time: 0.478s              ││
│└──────────────────────────────────────────────┘│
└────────────────────────────────────────────────┘
┌───────────────────────────┐
│           QUERY           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      EXPLAIN_ANALYZE      │
│    ────────────────────   │
│           0 Rows          │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│    UNGROUPED_AGGREGATE    │
│    ────────────────────   │
│    Aggregates: sum(#0)    │
│                           │
│           1 Rows          │
│          (0.01s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│         l_quantity        │
│                           │
│        6001215 Rows       │
│          (0.00s)          │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         TABLE_SCAN        │
│    ────────────────────   │
│     Function: READ_CSV    │
│                           │
│        Projections:       │
│         l_quantity        │
│                           │
│        6001215 Rows       │
│          (3.82s)          │
└───────────────────────────┘
*/

------------------------------------------------------------------------

-- Re-reading, parsing, the CSV 6+ million rows of CSV file lineitem.csv
-- is wasteful.
-- Idea:
-- 1. Read the file once, copy its contents into a DuckDB table in DRAM.
-- 2. All future queries refer to the table (not the CSV file).

-- 1. Create the lineitem table, populate it from the CSV file using COPY
--    (will need to read the entire CSV file, no projection pushdown
--     for column l_quantity).
CREATE OR REPLACE TABLE lineitem (
  l_orderkey      int,
  l_partkey       int,
  l_suppkey       int,
  l_linenumber    int,
  l_quantity      int,
  l_extendedprice float,
  l_discount      float,
  l_tax           float,
  l_returnflag    text,
  l_linestatus    text,
  l_shipdate      date,
  l_commitdate    date,
  l_receiptdate   date,
  l_shipinstruct  text,
  l_shipmode      text,
  l_comment       text
);

COPY lineitem FROM '../../databases/lineitem.csv';

-- 2. Run the benchmark query on the lineitem table
SELECT sum(l_quantity)
FROM   lineitem;

-- WOAH... that was fast (0.002s on Torsten's MacBook Pro M2 Max).
--
-- This is 20 times faster than our threaded C program and appears
-- to read the CSV data with a throughput of ~377 GB/s.  This exceeds
-- DRAM bandwidth by far.  IMPOSSIBLE!.
--
-- Explain yourself, DuckDB:

-- NB.
-- - Scanning the table takes no significant time (no parsing, no type conversion,
--   we directly scan integers)
-- - ⚠️ The PROJECTION on l_quantity HAS BEEN PUSHED DOWN:
--   DuckDB only reads the relevant column, no other columns were touched.
-- - Aggregate function sum() knows that addition will not overflow
--   the value range of type int128 (less error checking)
EXPLAIN ANALYZE
SELECT sum(l_quantity)
FROM   lineitem;

-- To obtain an interactive visualization of query plans, use
--
--     EXPLAIN (ANALYZE, FORMAT json)
--     ‹query›
--
-- and paste the resulting DuckDB output into the "Plan (JSON)" box at
-- https://db.cs.uni-tuebingen.de/explain/ (the DuckDB EXPLAIN Visualizer)

-- Query time reports no longer needed
.timer off
