-- Rewrite scalar expressions that can be (partially) evaluated without
-- access to the base data (access to the schema, e.g., types, NULL
-- constraints, suffices).
--
-- Attach to a TPC-H instance, scale factor sf = 1
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;

-- Here we are looking at plans only
-- (optimization does not affect query results)
PRAGMA explain_output = 'optimized_only';
.mode trash

-- Show canonical plans
PRAGMA disable_optimizer;

EXPLAIN
SELECT 42 // 1;

-- Show optimized plans
PRAGMA enable_optimizer;

EXPLAIN
SELECT 42 // 1;

-----------------------------------------------------------------------
-- Arithmetic simplifcation
--
-- const ‹op› const
-- e + 0, e - 0, e // 1, e * 1, e ‹op› NULL

EXPLAIN
SELECT p.p_size * 1     -- evaluated (p_size :: int , 1 :: int)
FROM part AS p;

EXPLAIN
SELECT p.p_retailprice * 1  -- not evaluated (p_retailprice :: decimal)
FROM part AS p;

EXPLAIN
FROM part AS p
WHERE p.p_size * 2 = 10;    -- simplified (and pushed down into SEQ_SCAN)

EXPLAIN
SELECT p.p_size * 2 = 11    -- simplified to constant_or_null(false, p_size)
FROM part AS p;

-- constant_or_null(false, p_size) detected as always false or NULL
-- => EMPTY_RESULT
EXPLAIN
FROM part AS p
WHERE p.p_size * 2 = 11;    --  simplified to constant_or_null(false, p_size)

-----------------------------------------------------------------------
-- CASE simplification

-- Evaluate constant guards, prune always false/unreachable branches
EXPLAIN
SELECT CASE WHEN 2 < 1 THEN 'branch #1'
            WHEN 2 > 1 THEN 'branch #2'
            ELSE            'branch #3'
       END;

-----------------------------------------------------------------------
-- Boolean simplifcation
--
-- NOT const, const ‹op› const
-- e ‹op› {false, true}, e ‹op› NULL

EXPLAIN
SELECT NOT (p.p_size > 42)       -- NOT > => <=
FROM part AS p;

EXPLAIN
FROM lineitem AS l
WHERE l.l_shipdate = l.l_commitdate                        -- equivalent to
OR    (l.l_shipdate IS NULL AND l.l_commitdate IS NULL);   -- IS NOT DISTINCT FROM

-- Distributivity: (e AND p) OR (e AND q) => e AND (p OR q)
--                 (e OR p) AND (e OR q)  => e OR (p AND q)
EXPLAIN
FROM lineitem AS l
WHERE (l.l_linestatus = 'F' AND l.l_shipmode = 'AIR')
   OR (l.l_linestatus = 'F' AND l.l_shipmode = 'SHIP');
-- After distributivity rewrite, filter l.linestatus = 'F' is
-- pushed down into the SEQ_SCAN, residual OR predicate evaluated
-- in post-FILTER


-----------------------------------------------------------------------
-- LIKE and regular expression match simplifications

EXPLAIN
FROM lineitem AS l
WHERE l.l_shipmode LIKE 'AIR%';   -- prefix test simplified into range predicate
                                  -- (and pushed down into SEQ_SCAN)
EXPLAIN
FROM lineitem AS l
WHERE l.l_shipmode LIKE '%AIR';    -- suffix test rewritten into suffix(...,'AIR')

EXPLAIN
FROM lineitem AS l
WHERE l.l_shipmode LIKE '%AIR%';   -- contains test rewritten into contains(...,'AIR')

-- Some regular expression matches can be rewritten into
-- computationally more lightweight LIKE (~~) predicates
--
-- NB. regexp_matches(e, pattern, 's') matches if e contains pattern
--     (option 's': newline is like any other charcater)
EXPLAIN
FROM lineitem AS l
WHERE regexp_matches(l.l_shipmode, 'A.R', 's');

EXPLAIN
FROM lineitem AS l
WHERE regexp_matches(l.l_shipmode, 'A.*R', 's');
