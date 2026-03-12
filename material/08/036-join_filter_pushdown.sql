-- DuckDB relocates FILTER predicates in plans to restrict intermediate
-- table cardinality as early as possible.
--
-- Pass join_filter_pushdown prepares a *dynamic* optimization that
-- filter the probe input of a join based on the values read
-- from the build input.
--
-- ⚠️ This dynamic optimization (performed *during* plan execution)
-- can only be observed using EXPLAIN ANALYZE.

-- Attach to a TPC-H instance, scale factor sf = 1
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;

-- Here we are looking at plans only
-- (optimization does not affect query results)
PRAGMA explain_output = 'optimized_only';
.mode trash

-- Show canonical plans
-- PRAGMA disable_optimizer;

-- Show optimized plans
PRAGMA enable_optimizer;

-----------------------------------------------------------------------

-- For now focus on regular (non-dynamic) filter pullup and pushdown,
-- thus disable join_filter_pushdown
SET disabled_optimizers = 'join_filter_pushdown';

-- (1) Predicate l.l_orderkey BETWEEN 100 AND 200 on table lineitem
--     will be pulled UP above the join.
-- (2) DuckDB then uses the equivalence l_orderkey = o_orderkey to
--     also restrict table orders.
EXPLAIN
SELECT l.l_orderkey, o.o_totalprice
FROM lineitem AS l JOIN orders AS o ON (l.l_orderkey = o.o_orderkey)
WHERE l.l_orderkey BETWEEN 100 AND 200;

/*

AFTER OPTIMIZER filter_pullup

┌────────────────────┴────────────────────┐
│                PROJECTION               │
│ ─────────────────────────────────────── │
│               Expressions:              │
│                l_orderkey               │
│               o_totalprice              │
└────────────────────┬────────────────────┘
┌────────────────────┴────────────────────┐
│                  FILTER                 │
│ ─────────────────────────────────────── │
│               Expressions:              │
│           (l_orderkey >= 100)           │ (1) pulled up predicates
│           (l_orderkey <= 200)           │
└────────────────────┬────────────────────┘
┌────────────────────┴────────────────────┐
│             COMPARISON_JOIN             │
│ ─────────────────────────────────────── │
│             Join Type: INNER            ├───────────────────┐
│  Conditions: (l_orderkey = o_orderkey)  │                   │
└────────────────────┬────────────────────┘                   │
┌────────────────────┴────────────────────┐┌──────────────────┴───────────────────┐
│                 SEQ_SCAN                ││               SEQ_SCAN               │
│ ─────────────────────────────────────── ││ ──────────────────────────────────── │
│        Table: tpch.main.lineitem        ││       Table: tpch.main.orders        │
│          Type: Sequential Scan          ││        Type: Sequential Scan         │
└─────────────────────────────────────────┘└──────────────────────────────────────┘


FINAL OPTIMIZED PLAN

┌──────────────────────────────────────────────────┐
│                    PROJECTION                    │
│ ──────────────────────────────────────────────── │
│                   Expressions:                   │
│                    l_orderkey                    │
│                   o_totalprice                   │
└────────────────────────┬─────────────────────────┘
┌────────────────────────┴─────────────────────────┐
│                 COMPARISON_JOIN                  │
│ ──────────────────────────────────────────────── │
│                 Join Type: INNER                 │
│      Conditions: (l_orderkey = o_orderkey)       ├─────────────────────────┐
└────────────────────────┬─────────────────────────┘                         │
┌────────────────────────┴─────────────────────────┐┌────────────────────────┴─────────────────────────┐
│                     SEQ_SCAN                     ││                     SEQ_SCAN                     │
│ ──────────────────────────────────────────────── ││ ──────────────────────────────────────────────── │
│   Filters: l_orderkey>=100 AND l_orderkey<=200   ││   Filters: o_orderkey>=100 AND o_orderkey<=200   │ (2) pushed down predicates
│            Table: tpch.main.lineitem             ││             Table: tpch.main.orders              │
│              Type: Sequential Scan               ││              Type: Sequential Scan               │
└──────────────────────────────────────────────────┘└──────────────────────────────────────────────────┘

*/

-----------------------------------------------------------------------

-- Now re-enable dynamic join filter pushdown (no optimizer pass disabled)
SET disabled_optimizers = '';

-- EXPLAIN ANALYZE will show the physical plan used during plan execution
PRAGMA explain_output = 'physical_only';

-- After filter pushdown, only a small set of o_orderkey values
-- {100, 101, ..., 199} remain relevant on the build side
SELECT DISTINCT o.o_orderkey
FROM orders AS o
WHERE o.o_orderkey BETWEEN 100 AND 200;

-- The min/max values in this set are used to construct a dynamic range
-- predicate to pre-filter the probe side.
--
-- Use SET dynamic_or_filter_threshold = n to control whether DuckDB
-- also generates a precise dynamic IN (100, 101, ..., 199) filter
EXPLAIN ANALYZE
SELECT l.l_orderkey, o.o_totalprice
FROM lineitem AS l JOIN orders AS o ON (l.l_orderkey = o.o_orderkey)
WHERE l.l_orderkey BETWEEN 100 AND 200;


/*

PHYSICAL PLAN AFTER OPTIMIZATION

┌──────────────────────────────┴──────────────────────────────┐
│                          HASH_JOIN                          │
│    ──────────────────────────────────────────────────────   │
│                       Join Type: INNER                      │
│             Conditions: l_orderkey = o_orderkey             │
│                                                             ├───────────────────────────────┐
│                                                             │                               │
│                                                             │                               │
│                           116 rows                          │                               │
│                            0.00s                            │                               │
└──────────────────────────────┬──────────────────────────────┘                               │
┌──────────────────────────────┴──────────────────────────────┐┌──────────────────────────────┴──────────────────────────────┐
│                          TABLE_SCAN                         ││                          TABLE_SCAN                         │
│    ──────────────────────────────────────────────────────   ││    ──────────────────────────────────────────────────────   │
│                  Table: tpch.main.lineitem                  ││                   Table: tpch.main.orders                   │
│                    Type: Sequential Scan                    ││                    Type: Sequential Scan                    │
│                   Projections: l_orderkey                   ││                                                             │
│         Filters: l_orderkey>=100 AND l_orderkey<=200        ││                         Projections:                        │
│                                                             ││                          o_orderkey                         │
│                       Dynamic Filters:                      ││                         o_totalprice                        │
│ optional: l_orderkey IN (100, 101, 102, 103, 128, 129, 130, ││                                                             │
│  131, 132, 133, 134, 135, 160, 161, 162, 163, 164, 165, 166,││         Filters: o_orderkey>=100 AND o_orderkey<=200        │
│  167, 192, 193, 194, 195, 196, 197, 198, 199) AND optional: ││                                                             │
│         l_orderkey>=100 AND optional: l_orderkey<=199       ││                                                             │
│                                                             ││                                                             │
│                                                             ││                                                             │
│                                                             ││                            0.00s                            │
│                           116 rows                          ││                           28 rows                           │
│                            0.00s                            ││                           (0.00s)                           │
└─────────────────────────────────────────────────────────────┘└─────────────────────────────────────────────────────────────┘

*/
