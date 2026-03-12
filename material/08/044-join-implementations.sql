-- DuckDB implements a family of join operators all of which specialize
-- in the evaluation of specific kinds of join predicate (equality,
-- inequality, complex).


-- Attach to a TPC-H instance, scale factor sf = 1
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;

-- Here we are looking at optimized plans
PRAGMA explain_output = 'optimized_only';

-- Show canonical plans
-- PRAGMA disable_optimizer;

-- Show optimized plans
PRAGMA enable_optimizer;


-----------------------------------------------------------------------
-- *Before* DuckDB chooses a specific join implementation, joins in
-- the plans are generic COMPARISON_JOINs.

EXPLAIN
SELECT c.c_custkey, c.c_name AS "spammer?"
FROM   customer AS c,
       (VALUES  ('26-451-451-3382'),
                ('12-595-957-9150'),
                ('10-263-501-9162'),
                ('27-363-772-2776')) AS spam(phone)
WHERE  c.c_phone <> spam.phone;  -- <>: complex predicate

/*

┌────────────────┴─────────────────┐
│         COMPARISON_JOIN          │
│ ──────────────────────────────── │
│         Join Type: INNER         │
│  Conditions: (c_phone != phone)  ├─
│                                  │
│            ~615 rows             │
└────────────────┬─────────────────┘

*/

-- In the sequel show physical plans
-- (only then has the choice of join implementation been made)
PRAGMA explain_output = 'physical_only';


-- NESTED_LOOP_JOIN (BLOCKWISE_NL_JOIN)

-- Nested loop join
-- (used if smaller table has cardinality < nested_loop_join_threshold [default  5]
--  or if complex predicate that is neither = nor <, <=, >=, >)
SELECT current_setting('nested_loop_join_threshold');

-- List customers, ignore spam callers
EXPLAIN
SELECT c.c_custkey, c.c_name AS "spammer?"
FROM   customer AS c,
       (VALUES  ('26-451-451-3382'),
                ('12-595-957-9150'),
                ('10-263-501-9162'),
                ('27-363-772-2776')) AS spam(phone)
WHERE  c.c_phone <> spam.phone;  -- <>: complex predicate

-- Change <> into <: DuckDB chooses PIECEWISE_MERGE_JOIN (< is simple).

/*

┌──────────────────────────────┴──────────────────────────────┐
│                       NESTED_LOOP_JOIN                      │
│    ──────────────────────────────────────────────────────   │
│                       Join Type: INNER                      │
│                 Conditions: c_phone != phone                ├─
│                                                             │
│                          ~295 rows                          │
└──────────────────────────────┬──────────────────────────────┘

*/


-- Will use NESTED_LOOP_JOIN unless more row(s) are added to inline
-- table delay [e.g. (15, 'critical')] then switches to PIECEWISE_MERGE_JOIN
-- (see nested_loop_join_threshold).
--
-- Categorize order processing based on duration in days
EXPLAIN
SELECT l.l_orderkey, arg_max(delay.status, delay.days) AS status
FROM   lineitem AS l,
       (VALUES ( 0, 'great'),
               ( 5, 'ok'),
               (10, 'slow'),
               -- (15, 'critical'),
               (20, 'failed')) AS delay(days,status)
WHERE  l.l_receiptdate - l.l_shipdate >= delay.days  -- inequality (simple)
GROUP BY l.l_orderkey
ORDER BY l.l_orderkey
LIMIT 20;

/*
┌──────────────────────────────┴──────────────────────────────┐
│                       NESTED_LOOP_JOIN                      │
│    ──────────────────────────────────────────────────────   │
│                       Join Type: INNER                      │
│                                                             │
│                         Conditions:                         ├─
│     (l_receiptdate - l_shipdate) >= CAST(days AS BIGINT)    │
│                                                             │
│                        ~130,144 rows                        │
└──────────────────────────────┬──────────────────────────────┘
*/

-----------------------------------------------------------------------
-- IE_JOIN

-- Find pairs of parts in which the first is cheaper/smaller and of
-- similar type.
--
-- Leads to IE_JOIN (2+ inequality join conditions: <=)
EXPLAIN
SELECT p1.p_partkey, p2.p_partkey
FROM part AS p1, part AS p2
WHERE p1.p_retailprice <= p2.p_retailprice  -- inequality #1
AND   p1.p_size <= p2.p_size                -- inequality #2
AND   contains(p2.p_type, p1.p_type)
AND   p1.p_partkey <> p2.p_partkey;

/*

┌──────────────────────────────┴──────────────────────────────┐
│                           IE_JOIN                           │
│    ──────────────────────────────────────────────────────   │
│                       Join Type: INNER                      │
│                                                             │
│                         Conditions:                         │
│                p_retailprice <= p_retailprice               ├─
│                       p_size <= p_size                      │
│                    p_partkey != p_partkey                   │
│                   contains(p_type, p_type)                  │
│                                                             │
│                       ~4,608,997 rows                       │
└──────────────────────────────┬──────────────────────────────┘
*/
