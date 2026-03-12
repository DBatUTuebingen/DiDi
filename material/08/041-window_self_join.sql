-- SQL's window clause (OVER ...) forms frames of related rows which
-- can then be jointly processed by a window function.  Given that
--
-- 1. frames are unordered partitions of rows and
-- 2. the window function is an aggregate function (e.g., sum, max, count)
--
-- window clauses can be equivalently rewritten into efficient self-joins
-- over the underlying table.

-- Attach to a TPC-H instance, scale factor sf = 1
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;

-- We are looking at optimized plans
PRAGMA explain_output = 'optimized_only';

-- Show canonical plans
-- PRAGMA disable_optimizer;

-- Show optimized plans
PRAGMA enable_optimizer;

-----------------------------------------------------------------------

.timer on

-- Sample query Q:
-- Find the orders of customers who have placed a single order only.

FROM orders AS o
QUALIFY count(*) OVER (PARTITION BY o.o_custkey) = 1;
-- Run Time (s): real 0.031 user 0.152005 sys 0.011332


-- Equivalent rewrite of Q on the SQL level
--
-- NB. Performance is very close to the original Q

WITH
frame(key, agg) AS (
  SELECT o.o_custkey AS key, count(*) AS agg
  FROM   orders AS o
  GROUP BY o.o_custkey
)
FROM orders AS o SEMI JOIN frame
     ON (o.o_custkey IS NOT DISTINCT FROM frame.key AND frame.agg = 1);
-- Run Time (s): real 0.028 user 0.128951 sys 0.010039

-----------------------------------------------------------------------

-- Let DuckDB rewrite the window function in Q into a self-join

EXPLAIN ANALYZE
FROM orders AS o
QUALIFY count(*) OVER (PARTITION BY o.o_custkey) = 1;


/* NB. Plan slightly simplified

┌────────────────────────┴───────────────────────┐
│                    HASH_JOIN                   │
│ ────────────────────────────────────────────── │
│                 Join Type: INNER               │
│  Conditions: o_custkey IS NOT DISTINCT FROM #0 │
│                                                ├──────────────┐
│                     17 rows                    │              │
│                      0.01s                     │              │
└────────────────────────┬───────────────────────┘              │
┌────────────────────────┴───────────────────────┐┌─────────────┴─────────────┐
│                    TABLE_SCAN                  ││         PROJECTION        │
│ ────────────────────────────────────────────── ││ ───────────────────────── │
│             Table: tpch.main.orders            ││             #0            │
│              Type: Sequential Scan             ││                           │
│                                                ││                           │
│                   Projections:                 ││                           │
│                    o_custkey                   ││                           │
│                    o_orderkey                  ││           0.00s           │
│                  o_orderstatus                 ││                           │
│                   o_totalprice                 ││                           │
│                   o_orderdate                  ││                           │
│                 o_orderpriority                ││                           │
│                     o_clerk                    ││                           │
│                  o_shippriority                ││                           │
│                    o_comment                   ││                           │
│                                                ││                           │
│                  1,500,000 rows                ││          17 rows          │
│                      0.05s                     ││          (0.00s)          │
└────────────────────────────────────────────────┘└─────────────┬─────────────┘
                                                  ┌─────────────┴─────────────┐
                                                  │           FILTER          │
                                                  │ ───────────────────────── │
                                                  │          (#1 = 1)         │
                                                  │                           │
                                                  │          17 rows          │
                                                  │           0.00s           │
                                                  └─────────────┬─────────────┘
                                                  ┌─────────────┴─────────────┐
                                                  │       HASH_GROUP_BY       │
                                                  │ ───────────────────────── │
                                                  │         Groups: #0        │
                                                  │    Aggregates: count()    │
                                                  │                           │
                                                  │        99,996 rows        │
                                                  │           0.09s           │
                                                  └─────────────┬─────────────┘
                                                  ┌─────────────┴─────────────┐
                                                  │         TABLE_SCAN        │
                                                  │ ───────────────────────── │
                                                  │  Table: tpch.main.orders  │
                                                  │   Type: Sequential Scan   │
                                                  │   Projections: o_custkey  │
                                                  │                           │
                                                  │       1,500,000 rows      │
                                                  │           0.00s           │
                                                  └───────────────────────────┘
*/

-----------------------------------------------------------------------

-- Re-evaluating Q with pass window_self_join disabled shows a
-- significant performance degradation:

SET disabled_optimizers = 'window_self_join';

FROM orders AS o
QUALIFY count(*) OVER (PARTITION BY o.o_custkey) = 1;
-- Run Time (s): real 0.078 user 0.572611 sys 0.047158
--                    ^^^^^
--                    0.031 with window_self_join enabled

RESET disabled_optimizers;
