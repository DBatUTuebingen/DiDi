-- SQL translation may yield canonical plans that contain identical
-- plan fragments (subplans) multiple times.  Use DuckDB's existing
-- CTE infrastructure to save repeated subplan evaluation effort:
--
-- 1. Evaluate the subplan once, materialize under a system-generated
--    pseudo CTE.
-- 2. Use operator CTE_SCAN to scan the materialized result multiple
--    times.

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

-- TPC-H Query 11 repeats the markedSQL clauses  [...] albeit with
-- syntactic variation (tables reordered in FROM clause, predicates
-- in WHERE clause jumbled). Also note that these clauses appear in two
-- differing query contexts. Yet, both clauses lead to two identical
-- plan fragments in the (partially optimized) plan.
--
-- TPC-H Query Q11
-- Identify the parts that represent a significant percentage of
-- the total value of all parts

EXPLAIN
SELECT ps.ps_partkey, sum(ps.ps_supplycost * ps.ps_availqty) AS value
FROM   partsupp AS ps, nation AS n, supplier AS s            -- [1 pns]
WHERE  n.n_name = 'GERMANY'                                  -- [4]
AND    ps.ps_suppkey = s.s_suppkey                           -- [2]
AND    s.s_nationkey = n.n_nationkey                         -- [3]
GROUP BY ps_partkey
HAVING sum(ps.ps_supplycost * ps.ps_availqty) > (
        SELECT sum(ps.ps_supplycost * ps.ps_availqty) * 0.0001000000
        FROM   partsupp AS ps, supplier AS s, nation AS n    -- [1 psn]
        WHERE  ps.ps_suppkey = s.s_suppkey                   -- [2]
        AND    s.s_nationkey = n.n_nationkey                 -- [3]
        AND    n.n_name = 'GERMANY')                         -- [4]
ORDER BY value DESC;


/* COMMON SUBPLAN

┌───────────────────┴────────────────────┐
│                  CTE                   │
│ ────────────────────────────────────── │
│      CTE Name: __common_subplan_1      ├─── refer to common subplan
│            Table Index: 21             │    multiple times                                                                                                                                                           │
└───────────────────┬────────────────────┘
┌───────────────────┴────────────────────┐
│            COMPARISON_JOIN             │
│ ────────────────────────────────────── │
│            Join Type: INNER            │
│  Conditions: (ps_suppkey = s_suppkey)  │
│                                        │
│                                        ├──────────────────────┐
│                                        │                      │
│                                        │                      │
│                                        │                      │
│              ~35,924 rows              │                      │
└───────────────────┬────────────────────┘                      │
┌───────────────────┴────────────────────┐┌─────────────────────┴─────────────────────┐
│                SEQ_SCAN                ││              COMPARISON_JOIN              │
│ ────────────────────────────────────── ││ ───────────────────────────────────────── │
│       Table: tpch.main.partsupp        ││              Join Type: INNER             │
│         Type: Sequential Scan          ││  Conditions: (s_nationkey = n_nationkey)  │
│                                        ││                                           │
│                                        ││                                           ├───────────────────────┐
│                                        ││                                           │                       │
│                                        ││                                           │                       │
│                                        ││                                           │                       │
│                                        ││                                           │                       │
│             ~800,000 rows              ││                 ~384 rows                 │                       │
└────────────────────────────────────────┘└─────────────────────┬─────────────────────┘                       │
                                         ┌──────────────────────┴─────────────────────┐┌──────────────────────┴───────────────────────┐
                                         │                   SEQ_SCAN                 ││                   SEQ_SCAN                   │
                                         │    ─────────────────────────────────────   ││ ──────────────────────────────────────────   │
                                         │          Table: tpch.main.supplier         ││          Filters: n_name='GERMANY'           │
                                         │            Type: Sequential Scan           ││           Table: tpch.main.nation            │
                                         │                                            ││            Type: Sequential Scan             │
                                         │                                            ││                                              │
                                         │                 ~10,000 rows               ││                    ~1 row                    │
                                         └────────────────────────────────────────────┘└──────────────────────────────────────────────┘
*/


-- Use an explicit CTE to express the same subplan re-use on
-- the SQL (as opposed to the plan) level:

EXPLAIN
WITH subplan AS (
  FROM   partsupp AS ps, supplier AS s, nation AS n
  WHERE  ps.ps_suppkey = s.s_suppkey
  AND    s.s_nationkey = n.n_nationkey
  AND    n.n_name = 'GERMANY'
)
SELECT q1.ps_partkey, sum(q1.ps_supplycost * q1.ps_availqty) AS value
FROM   subplan AS q1                                 -- use #1
GROUP BY q1.ps_partkey
HAVING sum(q1.ps_supplycost * q1.ps_availqty) > (
        SELECT sum(q2.ps_supplycost * q2.ps_availqty) * 0.0001000000
        FROM   subplan AS q2)                        -- use #2
ORDER BY value DESC;


-----------------------------------------------------------------------

-- To identify common subplans, pass common_subplan performs bottom-up
-- recursion (starting at the plan leaves). Operators are (temporarily)
-- converted into a unified representation and then serialized in binary
-- for comparison.
--
-- If we disable an optimizer pass that runs *before* common_subplan, we
-- may miss common subplans since their canonical (unoptimized) forms differ.
--
-- Example: disabling join_order will miss the common subplan in Q11
-- (note that the table orders in clauses [1 pns] and [1 psn] differ):

SET disabled_optimizers = 'join_order';

EXPLAIN
SELECT ps.ps_partkey, sum(ps.ps_supplycost * ps.ps_availqty) AS value
FROM   partsupp AS ps, nation AS n, supplier AS s            -- [1 pns]
WHERE  n.n_name = 'GERMANY'                                  -- [4]
AND    ps.ps_suppkey = s.s_suppkey                           -- [2]
AND    s.s_nationkey = n.n_nationkey                         -- [3]
GROUP BY ps_partkey
HAVING sum(ps.ps_supplycost * ps.ps_availqty) > (
        SELECT sum(ps.ps_supplycost * ps.ps_availqty) * 0.0001000000
        FROM   partsupp AS ps, supplier AS s, nation AS n    -- [1 psn]
        WHERE  ps.ps_suppkey = s.s_suppkey                   -- [2]
        AND    s.s_nationkey = n.n_nationkey                 -- [3]
        AND    n.n_name = 'GERMANY')                         -- [4]
ORDER BY value DESC;

RESET disabled_optimizers;

-----------------------------------------------------------------------

-- Common subplans that refer to volatile functions (like random())
-- are not considered for re-use.  DuckDB builds on the logic used
-- during CTE inlining (pass cte_inlining).

EXPLAIN
SELECT ps.ps_partkey, sum(ps.ps_supplycost * ps.ps_availqty) AS value
FROM   partsupp AS ps, nation AS n, supplier AS s            -- [1 pns]
WHERE  random() > 0.5                                        -- [4 volatile]
AND    ps.ps_suppkey = s.s_suppkey                           -- [2]
AND    s.s_nationkey = n.n_nationkey                         -- [3]
GROUP BY ps_partkey
HAVING sum(ps.ps_supplycost * ps.ps_availqty) > (
        SELECT sum(ps.ps_supplycost * ps.ps_availqty) * 0.0001000000
        FROM   partsupp AS ps, supplier AS s, nation AS n    -- [1 psn]
        WHERE  ps.ps_suppkey = s.s_suppkey                   -- [2]
        AND    s.s_nationkey = n.n_nationkey                 -- [3]
        AND    random() > 0.5)                               -- [4 volatile]
ORDER BY value DESC;
