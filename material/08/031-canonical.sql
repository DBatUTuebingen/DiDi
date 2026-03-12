-- SQL is a declarative query language that REQUIRES a
-- query optimizer to turn initial (or: canonical) query plans
-- into equivalent plans that reduce execution time and resource (e.g.,
-- CPU and memory) usage.
--
-- Disabling the DuckDB's query optimizer leaves us with canonical
-- plans that do deliver the correct result but may very well be
-- impractical.

-- Use TPC-H instance of scale factor sf = 1 (most canonical plans fail
-- to terminate in reasonable time for sf = 1 let alone larger scale factors)
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;

-- We are interested in the query plan and timings only, ignore the query result
.mode trash
.timer on

-- ⚠️ Disable the query optimizer, display and execute canonical plans
PRAGMA disable_optimizer;
PRAGMA explain_output = 'optimized_only';

-----------------------------------------------------------------------
-- TPC-H Query Q11 (most important subset of suppliers' stock in a
-- given nation)
--
EXPLAIN
SELECT ps_partkey, sum(ps_supplycost * ps_availqty) AS value
FROM   partsupp, supplier, nation
WHERE  ps_suppkey = s_suppkey
AND    s_nationkey = n_nationkey
AND    n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING sum(ps_supplycost * ps_availqty) > (
    SELECT sum(ps_supplycost * ps_availqty) * 0.0001000000
    FROM   partsupp, supplier, nation
    WHERE  ps_suppkey = s_suppkey
    AND    s_nationkey = n_nationkey
    AND    n_name = 'GERMANY')
ORDER BY value DESC;


/* CANONICAL PLAN for Q11

   Notes:

   - FROM clauses translates into plan subtrees of CROSS_PRODUCTs that
     use SEQ_SCAN to access tables.
   - Sequences of tables in cross product trees follows the user-specified
     FROM clause.
   - Join conditions sit *above* all cross products in separate FILTER
     nodes.
   - Expressions (e.g., in PROJECTION, FILTER) are not simplified.

┌───────────────────────────┐
│          ORDER_BY         │
│    ────────────────────   │
│  sum((tpch.main.partsupp  │
│ .ps_supplycost * tpch.main│
│  .partsupp.ps_availqty))  │
│            DESC           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│         ps_partkey        │
│           value           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│      (CAST(sum((CAST      │
│ (ps_supplycost AS DECIMAL │
│ (34,2)) * CAST(ps_availqty│
│   AS DECIMAL(34,0)))) AS  │
│      DECIMAL(38,12)) >    │
│          SUBQUERY)        │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│       CROSS_PRODUCT       ├────────────────────────────────────────────────────────────────────────┐
└─────────────┬─────────────┘                                                                        │
┌─────────────┴─────────────┐                                                          ┌─────────────┴─────────────┐
│       HASH_GROUP_BY       │                                                          │         PROJECTION        │
│    ────────────────────   │                                                          │    ────────────────────   │
│         Groups: #0        │                                                          │ CASE  WHEN ((#1 > 1)) THEN│
│    Aggregates: sum(#1)    │                                                          │   ("error"('More than one │
│                           │                                                          │      row returned by a    │
│                           │                                                          │     subquery used as an   │
│                           │                                                          │     expression - scalar   │
│                           │                                                          │     subqueries can only   │
│                           │                                                          │    return a single row.   │
│                           │                                                          │          Use "SET         │
│                           │                                                          │ scalar_subquery_error_on_m│
│                           │                                                          │   ultiple_rows=false" to  │
│                           │                                                          │     revert to previous    │
│                           │                                                          │   behavior of returning a │
│                           │                                                          │ random row.')) ELSE #0 END│
└─────────────┬─────────────┘                                                          └─────────────┬─────────────┘
┌─────────────┴─────────────┐                                                          ┌─────────────┴─────────────┐
│         PROJECTION        │                                                          │    UNGROUPED_AGGREGATE    │
│    ────────────────────   │                                                          │    ────────────────────   │
│         ps_partkey        │                                                          │        Aggregates:        │
│   (CAST(ps_supplycost AS  │                                                          │        "first"(#0)        │
│    DECIMAL(34,2)) * CAST  │                                                          │        count_star()       │
│ (ps_availqty AS DECIMAL(34│                                                          │                           │
│           ,0)))           │                                                          │                           │
└─────────────┬─────────────┘                                                          └─────────────┬─────────────┘
┌─────────────┴─────────────┐                                                          ┌─────────────┴─────────────┐
│           FILTER          │                                                          │         PROJECTION        │
│    ────────────────────   │                                                          │    ────────────────────   │
│ ((ps_suppkey = s_suppkey) │                                                          │             #0            │
│     AND (s_nationkey =    │                                                          │                           │
│  n_nationkey) AND (n_name │                                                          │                           │
│    = CAST('GERMANY' AS    │                                                          │                           │
│         VARCHAR)))        │                                                          │                           │
└─────────────┬─────────────┘                                                          └─────────────┬─────────────┘
┌─────────────┴─────────────┐                                                          ┌─────────────┴─────────────┐
│       CROSS_PRODUCT       │                                                          │         PROJECTION        │
│                           │                                                          │    ────────────────────   │
│                           │                                                          │  (sum((CAST(ps_supplycost │
│                           │                                                          │  AS DECIMAL(34,2)) * CAST │
│                           ├───────────────────────────────────────────┐              │ (ps_availqty AS DECIMAL(34│
│                           │                                           │              │ ,0)))) * CAST(0.0001000000│
│                           │                                           │              │     AS DECIMAL(38,10)))   │
└─────────────┬─────────────┘                                           │              └─────────────┬─────────────┘
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│       CROSS_PRODUCT       │                             │         SEQ_SCAN          ││    UNGROUPED_AGGREGATE    │
│                           │                             │    ────────────────────   ││    ────────────────────   │
│                           │                             │       Table: nation       ││    Aggregates: sum(#0)    │
│                           ├──────────────┐              │   Type: Sequential Scan   ││                           │
└─────────────┬─────────────┘              │              └───────────────────────────┘└─────────────┬─────────────┘
┌─────────────┴─────────────┐┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│         SEQ_SCAN          ││         SEQ_SCAN          │                             │         PROJECTION        │
│    ────────────────────   ││    ────────────────────   │                             │    ────────────────────   │
│      Table: partsupp      ││      Table: supplier      │                             │   (CAST(ps_supplycost AS  │
│   Type: Sequential Scan   ││   Type: Sequential Scan   │                             │    DECIMAL(34,2)) * CAST  │
│                           ││                           │                             │ (ps_availqty AS DECIMAL(34│
│                           ││                           │                             │           ,0)))           │
└───────────────────────────┘└───────────────────────────┘                             └─────────────┬─────────────┘
                                                                                       ┌─────────────┴─────────────┐
                                                                                       │           FILTER          │
                                                                                       │    ────────────────────   │
                                                                                       │ ((ps_suppkey = s_suppkey) │
                                                                                       │     AND (s_nationkey =    │
                                                                                       │  n_nationkey) AND (n_name │
                                                                                       │    = CAST('GERMANY' AS    │
                                                                                       │         VARCHAR)))        │
                                                                                       └─────────────┬─────────────┘
                                                                                       ┌─────────────┴─────────────┐
                                                                                       │       CROSS_PRODUCT       ├───────────────────────────────────────────┐
                                                                                       └─────────────┬─────────────┘                                           │
                                                                                       ┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
                                                                                       │       CROSS_PRODUCT       │                             │         SEQ_SCAN          │
                                                                                       │                           │                             │    ────────────────────   │
                                                                                       │                           │                             │       Table: nation       │
                                                                                       │                           ├──────────────┐              │   Type: Sequential Scan   │
                                                                                       └─────────────┬─────────────┘              │              └───────────────────────────┘
                                                                                       ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
                                                                                       │         SEQ_SCAN          ││         SEQ_SCAN          │
                                                                                       │    ────────────────────   ││    ────────────────────   │
                                                                                       │      Table: partsupp      ││      Table: supplier      │
                                                                                       │   Type: Sequential Scan   ││   Type: Sequential Scan   │
                                                                                       └───────────────────────────┘└───────────────────────────┘

*/


-- Execute Q11 (unoptimized)
--
PRAGMA tpch(11);
-- Run Time (s): real 21.862 user 175.204150 sys 7.810077
--                    ^^^^^^

-----------------------------------------------------------------------
-- TPC-H Query Q17 (average yearly revenue lost if orders were no
-- longer filled for small quantities of given parts)
--
EXPLAIN
SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
FROM   lineitem, part
WHERE  p_partkey = l_partkey
AND    p_brand = 'Brand#23'
AND    p_container = 'MED BOX'
AND    l_quantity < (
    SELECT 0.2 * avg(l_quantity)
    FROM   lineitem
    WHERE  l_partkey = p_partkey);    -- correlated subquery


-- ⚠️ Unoptimized Q17 will not finish in reasonable time and requires
--     LOTS OF TEMP STORAGE on disk (see progress bar of file system
--     activity)
--
--     Use ^C (Control-C) to interrupt execution.
PRAGMA tpch(17);


-----------------------------------------------------------------------
-- TPC-H Query Q19 (discounted revenue attributed to the sale of
-- specifically selected parts)
--
EXPLAIN
SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM   lineitem, part
WHERE  (p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1
        AND l_quantity <= 1 + 10
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
OR     (p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10
        AND l_quantity <= 10 + 10
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
OR     (p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20
        AND l_quantity <= 20 + 10
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON');


/* CANONICAL PLAN for Q19

   Notes:
   - Massive cross product between tables lineitem (6 mio rows) and
     part (200,000 rows).
   - Complicated FILTER expression, non-simplified. (This expression
     includes the join conditions.)

┌───────────────────────────┐
│    UNGROUPED_AGGREGATE    │
│    ────────────────────   │
│    Aggregates: sum(#0)    │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│ (l_extendedprice * (CAST(1│
│     AS DECIMAL(16,2)) -   │
│        l_discount))       │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│ (((p_partkey = l_partkey) │
│    AND (p_brand = CAST(   │
│  'Brand#12' AS VARCHAR))  │
│  AND (p_container IN (CAST│
│  ('SM CASE' AS VARCHAR),  │
│  CAST('SM BOX' AS VARCHAR)│
│    , CAST('SM PACK' AS    │
│   VARCHAR), CAST('SM PKG' │
│     AS VARCHAR))) AND     │
│  (l_quantity >= CAST(1 AS │
│    DECIMAL(15,2))) AND    │
│  (l_quantity <= CAST((1 + │
│   10) AS DECIMAL(15,2)))  │
│  AND ((p_size >= CAST(1 AS│
│  INTEGER)) AND (p_size <= │
│  CAST(5 AS INTEGER))) AND │
│ (l_shipmode IN (CAST('AIR'│
│   AS VARCHAR), CAST('AIR  │
│   REG' AS VARCHAR))) AND  │
│  (l_shipinstruct = CAST(  │
│   'DELIVER IN PERSON' AS  │
│  VARCHAR))) OR ((p_partkey│
│  = l_partkey) AND (p_brand│
│    = CAST('Brand#23' AS   │
│       VARCHAR)) AND       │
│ (p_container IN (CAST('MED│
│   BAG' AS VARCHAR), CAST( │
│   'MED BOX' AS VARCHAR),  │
│  CAST('MED PKG' AS VARCHAR│
│   ), CAST('MED PACK' AS   │
│       VARCHAR))) AND      │
│ (l_quantity >= CAST(10 AS │
│    DECIMAL(15,2))) AND    │
│ (l_quantity <= CAST((10 + │
│   10) AS DECIMAL(15,2)))  │
│  AND ((p_size >= CAST(1 AS│
│  INTEGER)) AND (p_size <= │
│  CAST(10 AS INTEGER))) AND│
│  (l_shipmode IN (CAST('AIR│
│  ' AS VARCHAR), CAST('AIR │
│   REG' AS VARCHAR))) AND  │
│  (l_shipinstruct = CAST(  │
│   'DELIVER IN PERSON' AS  │
│  VARCHAR))) OR ((p_partkey│
│  = l_partkey) AND (p_brand│
│    = CAST('Brand#34' AS   │
│       VARCHAR)) AND       │
│ (p_container IN (CAST('LG │
│  CASE' AS VARCHAR), CAST( │
│ 'LG BOX' AS VARCHAR), CAST│
│  ('LG PACK' AS VARCHAR),  │
│  CAST('LG PKG' AS VARCHAR)│
│ )) AND (l_quantity >= CAST│
│ (20 AS DECIMAL(15,2))) AND│
│  (l_quantity <= CAST((20 +│
│   10) AS DECIMAL(15,2)))  │
│  AND ((p_size >= CAST(1 AS│
│  INTEGER)) AND (p_size <= │
│  CAST(15 AS INTEGER))) AND│
│  (l_shipmode IN (CAST('AIR│
│  ' AS VARCHAR), CAST('AIR │
│   REG' AS VARCHAR))) AND  │
│  (l_shipinstruct = CAST(  │
│   'DELIVER IN PERSON' AS  │
│         VARCHAR))))       │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│       CROSS_PRODUCT       ├──────────────┐
└─────────────┬─────────────┘              │
┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         SEQ_SCAN          ││         SEQ_SCAN          │
│    ────────────────────   ││    ────────────────────   │
│      Table: lineitem      ││        Table: part        │
│   Type: Sequential Scan   ││   Type: Sequential Scan   │
└───────────────────────────┘└───────────────────────────┘
*/

-- ⚠️ Unoptimized Q19 will not finish in reasonable time although
--     ALL CPU CORES participate in parallel (see progress bar or
--     htop).
--
--     Single-pipeline plan plan with UNGROUPED_AGGREGATE sink,
--     thus high degree of //ism with minimal memory requirements.
--
--     Use ^C (Control-C) to interrupt execution.
PRAGMA tpch(19);


-----------------------------------------------------------------------
-- Now enable the query optimizer and re-evaluate Q11, Q17, and Q19
-- (We will study optimized plans in the rest of the chapter.)

PRAGMA enable_optimizer;

PRAGMA tpch(11);
-- Run Time (s): real 0.034 user 0.027727 sys 0.005655 (speed-up of factor 640)

PRAGMA tpch(17);
-- Run Time (s): real 0.029 user 0.067094 sys 0.009504 (speed-up ∞)

PRAGMA tpch(19);
-- Run Time (s): real 0.038 user 0.141652 sys 0.006443 (speed-up ∞)
