-- When DuckDB uses parallel threads to evaluate queries, query
-- results will exhibit random row order (pipeline sinks combine the
-- result of multiple threads whenever they are finished processing
-- a batch of rows).

-- We only refer to TPC-H table orders below (sf = 1 suffices)
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;

DESCRIBE orders;

-- NB: in base table orders, column o_orderkey is sorted ascendingly
--     (the TPC-H data generator emits rows in ascending o_orderkey order).
FROM orders
LIMIT 20;

-- Sample query (cheap orders):
-- Identify the cheap orders and collect their order keys.
--
-- NB. When cheap_orders is referenced below, its defining
--     query is inlined (and thus re-evaluated).
CREATE OR REPLACE VIEW cheap_orders AS
  SELECT list(o.o_orderkey) AS o_orderkeys
  FROM   orders AS o
  WHERE  o.o_totalprice < 1000;


EXPLAIN ANALYZE
FROM cheap_orders;

/*
┌─────────────┴─────────────┐
│       HASH_GROUP_BY       │ sink
│    ────────────────────   │
│    Aggregates: list(#0)   │
│                           │
│           1 row           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │ operator
│    ────────────────────   │
│         o_orderkey        │
│                           │
│          160 rows         │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         TABLE_SCAN        │ source
│    ────────────────────   │
│       Table: orders       │
│   Type: Sequential Scan   │
│                           │
│        Projections:       │
│         o_orderkey        │
│                           │
│          Filters:         │
│    o_totalprice<1000.00   │
│                           │
│          160 rows         │
└───────────────────────────┘
*/


-- ⚠️ Evaluate the SQL queries below TWICE
--    and compare the outputs of CTE "ranked".
--
-- 1. Single-threaded execution
SET threads = 1;
-- 2. Multi-threaded execution
SET threads = 4;

-- List the 160 cheap orders.
--
-- - Note whether (how) the list aggregate o_orderkeys is sorted.
-- - Run the query multiple times: does list order change?
FROM cheap_orders;


-- In the "ranked" CTE below, pair the o_orderkeys with their rank in the
-- list (smallest o_orderkey has rank 1, next larger has rank 2, ...)
--
-- (For list_grade_up,
-- see https://duckdb.org/docs/stable/sql/functions/list#list_grade_uplist-col1-col2):
--
--    SELECT list_grade_up([10,20,30]);
--    SELECT list_grade_up([20,30,10]);
--
WITH
ranked(o_orderkey, rank) AS (
  SELECT unnest(c.o_orderkeys), unnest(list_grade_up(c.o_orderkeys)) AS ranks
  FROM   cheap_orders AS c
)
SELECT r.o_orderkey, r.rank, bar(r.rank, 0, 160, 80) AS visual
FROM ranked AS r
ORDER BY r.o_orderkey;


/*  Try to explain the generated bar chart

┌────────────┬───────┬────────────────────────────────────────────────────┐
│ o_orderkey │ rank  │                        visual                      │
│   int64    │ int64 │                       varchar                      │
├────────────┼───────┼────────────────────────────────────────────────────┤
│       8354 │     1 │ ▌                                                  │
│      34338 │     2 │ █                                                  │
│      70048 │     3 │ █▌                                                 │
│      90880 │     4 │ ██                                                 │
│      90978 │     5 │ ██▌                                                │
│     106983 │     6 │ ███                                                │
│     197380 │     7 │ ███▌                                               │
│     245159 │     8 │ ████                                               │
│     266663 │     9 │ ████▌                                              │
[...]

*/

DROP VIEW cheap_orders;
