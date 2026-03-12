-- Sum aggregates of the form sum(e + const) lead to repeated additions
-- of the (possibly expensive) constant expression const.  Avoid the
-- repeated effort and rewrite the aggregate into
--
--   sum(e) + const * count(e)
--
-- exploiting the commutativity and associativity of integral addition.

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


-- Required box size for all boxed parts plus some slack

EXPLAIN
--               e      + const
--         ┌─────┴─────┐  │
SELECT sum(2 * p.p_size + 3) AS shipping_size
FROM   part AS p
WHERE  p.p_container = 'WRAP BOX';

/*

┌────────────────────────────────────────────────────────┐
│                       PROJECTION                       │
│ ────────────────────────────────────────────────────── │
│ "+"(#0, "*"(CAST(#1 AS HUGEINT), CAST(3 AS HUGEINT)))  │ expression
│                                                        │ fabricated by
│                         ~1 row                         │ the optimizer
└───────────────────────────┬────────────────────────────┘
┌───────────────────────────┴────────────────────────────┐
│                  UNGROUPED_AGGREGATE                   │
│ ────────────────────────────────────────────────────── │
│                      Aggregates:                       │
│                  sum_no_overflow(#0)                   │ evaluate
│                       count(#1)                        │ aggregates
└───────────────────────────┬────────────────────────────┘
┌───────────────────────────┴────────────────────────────┐
│                       PROJECTION                       │
│ ────────────────────────────────────────────────────── │
│                           #0                           │ duplicate e
│                           #0                           │ into column
│                                                        │ slots #0 and #1
│                      ~5,406 rows                       │
└───────────────────────────┬────────────────────────────┘ [Hey DuckDB: required?]
┌───────────────────────────┴────────────────────────────┐
│                       PROJECTION                       │
│ ────────────────────────────────────────────────────── │
│                      (2 * p_size)                      │ evaluate expression e
│                                                        │ only once, put into
│                      ~5,406 rows                       │ column slot #0
└───────────────────────────┬────────────────────────────┘
┌───────────────────────────┴────────────────────────────┐
│                        SEQ_SCAN                        │
│ ────────────────────────────────────────────────────── │
│                 Table: tpch.main.part                  │
│                 Type: Sequential Scan                  │
│                  Projections: p_size                   │
│            Filters: p_container='WRAP BOX'             │
│                                                        │
│                      ~5,406 rows                       │
└────────────────────────────────────────────────────────┘

*/
