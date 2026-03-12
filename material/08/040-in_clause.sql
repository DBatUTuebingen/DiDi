-- The evaluation of predicates of the form e IN (c₁,...,cₘ) has complexity
-- O(n * m). Avoid this cost if m is large and rewrite the predicate into a
-- hash-based MARK join which will evaluate with complexity O(n + m).

-- Attach to a TPC-H instance, scale factor sf = 1
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;

-- Here we are looking at plans only
-- (optimization does not affect query results)
PRAGMA explain_output = 'physical_only';
.mode trash

-- Show canonical plans
-- PRAGMA disable_optimizer;

-- Show optimized plans
PRAGMA enable_optimizer;

-----------------------------------------------------------------------

-- Check the sizes of parts packaged in the five given containter types
--
-- An IN clause of five values just meets DuckDB's default threshold
-- for the MARK join rewrite.

EXPLAIN ANALYZE
SELECT p.p_name, p.p_size
FROM   part AS p
WHERE  p.p_container IN ('MED BOX', 'MED CAN', 'LG PACK', 'JUMBO CAN', 'WRAP BOX');

/*

┌─────────────────────────────────┐
│           PROJECTION            │
│ ─────────────────────────────── │
│               #0                │
│               #1                │
│                                 │
│           25,222 rows           │
└───────────────┬─────────────────┘
┌───────────────┴─────────────────┐
│             FILTER              │
│ ─────────────────────────────── │
│            IN (...)             │ refers to the Boolean mark column
│                                 │ (does NOT re-evaluate the IN clause)
│           25,222 rows           │
└───────────────┬─────────────────┘
┌───────────────┴─────────────────┐
│           HASH_JOIN             │ MARK join
│ ─────────────────────────────── │ (outputs p_name, p_size, Boolean mark)
│        Join Type: MARK          │
│  Conditions: p_container = #0   ├───────────┐
│                                 │           │
│          200,000 rows           │           │
└───────────────┬─────────────────┘           │
┌───────────────┴─────────────────┐┌──────────┴───────────┐
│            SEQ_SCAN             ││   COLUMN_DATA_SCAN   │
│ ─────────────────────────────── ││ ──────────────────── │
│     Table: tpch.main.part       ││                      │
│     Type: Sequential Scan       ││                      │ delivers values
│                                 ││                      │
│          Projections:           ││                      │ 'MED BOX'
│          p_container            ││                      │ 'MED CAN'
│             p_name              ││                      │ 'LG PACK'
│             p_size              ││                      │ 'JUMBO CAN'
│                                 ││                      │ 'WRAP BOX'
│          200,000 rows           ││        5 rows        │
└─────────────────────────────────┘└──────────────────────┘

*/


-- For small IN clauses with less than five items simple generate
-- a regular disjunction of equality comparison s(for NOT IN:
-- conjunction of inequalities).

EXPLAIN ANALYZE
SELECT p.p_name, p.p_size
FROM   part AS p
WHERE  p.p_container IN ('MED BOX', 'WRAP BOX');  -- rewrites into disjunction


-----------------------------------------------------------------------


-- TPC-H Query 16
-- How many suppliers can supply parts with the given properties?

EXPLAIN ANALYZE
SELECT p_brand, p_type, p_size,
       count(DISTINCT ps_suppkey) AS supplier_cnt
FROM partsupp, part
WHERE p_partkey = ps_partkey
  AND p_brand <> 'Brand#45'
  AND p_type NOT LIKE 'MEDIUM POLISHED%'
  AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9) -- 8 items: MARK join
  AND ps_suppkey NOT IN (
        SELECT s_suppkey
        FROM supplier
        WHERE s_comment LIKE '%Customer%Complaints%')
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size;
