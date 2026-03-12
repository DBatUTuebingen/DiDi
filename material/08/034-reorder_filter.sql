-- Reorder the clauses in complex AND/OR predicates from cheap to
-- costly.  Rationale: Boolean shortcut evaluation will be complete
-- before the costly clauses are encountered.
--
-- Expression evaluation cost is determined heuristically.

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

-- Comparison of int values is cheaper than text value comparison
EXPLAIN
FROM lineitem AS l
WHERE l.l_linestatus = 'O' OR l.l_linenumber = 1;   -- reorder clauses

-- Casting has its costs (unless type T is cast to T itself)
-- (a cast to text does not fail: safe to reorder)
EXPLAIN
FROM lineitem AS l
WHERE l.l_linenumber :: text = '1' OR l.l_linestatus = 'O';  -- reorder clauses

-- ~~ (LIKE) is considered even more costly than CASE WHEN ELSE END ...
EXPLAIN
FROM lineitem AS l
WHERE ll_comment ~~ '%furiously%'              -- most costly
   OR CASE l.l_shipmode WHEN 'AIR'  THEN true
                        WHEN 'SHIP' THEN true
                        ELSE false
      END
   OR l.l_extendedprice < 1000.0;              -- cheapest

-- ... and a scalar subquery?
-- (see the topmost FILTER in plan for the reordered predicate)
EXPLAIN
FROM lineitem AS l
WHERE l.l_comment ~~ '%furiously%'                -- reorder clauses
   OR (SELECT count(*)
       FROM lineitem AS l1
       WHERE l1.l_orderkey = l.l_orderkey) > 5;

-- ┌───────────────────────────┴───────────────────────────┐
-- │                         FILTER                        │
-- │  ──────────────────────────────────────────────────── │
-- │                      Expressions:                     │
-- │  ((SUBQUERY > 5) OR contains(l_comment, 'furiously')) │
-- │                                                       │
-- │                    ~6,001,215 rows                    │
-- └───────────────────────────┬───────────────────────────┘


-----------------------------------------------------------------------

-- ⚠️ DO NOT reorder clauses if any can possibly fail

-- Cast to int is costly but could fail: no reorder
EXPLAIN
FROM lineitem AS l
WHERE l.l_linestatus :: int = 0 OR l.l_linenumber = 1;
