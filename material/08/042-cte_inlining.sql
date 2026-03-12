-- Shall CTE definitions be
-- (inl) inlined (and thus re-evaluated) when they are referenced or
-- (mat) evaluated and materialized once to avoid recompuation.
--
-- Option (inl) incurs repeated evaluation effort but the inlined CTE
-- definition can be optimized with the surrounding query.  Option (mat)
-- promises to save runtime but requires space for the materialization.
--
-- If the CTE definition is not explicitly annotated by [NOT] MATERIALIZED
-- DuckDB follows heuristics to decide between (inl) and (mat).
-- (If such an annotation is provided, DuckDB tries to respect it.)


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

-- Sample query Q:
-- Identify customers (probably a call center?) that were spam calling
-- and compute overall revenue lost due to fraudulent orders.

-- Two CTE references: expect CTE materialization
EXPLAIN
WITH        -- ┌─────────────────── add NOT MATERIALIZED to observe
spam_callers AS (                -- DuckDB evaluating the CTE twice
  FROM customer AS c
  WHERE c.c_phone LIKE '12-758-%'
)
SELECT sum(o.o_totalprice) AS fraud_volume,
       (SELECT list(c.c_name)
        FROM   spam_callers AS c) AS customers
FROM   orders AS o
WHERE  o.o_custkey IN (
  SELECT c.c_custkey
  FROM   spam_callers AS c);


/* PLAN TOPOLOGY

┌──────────────────────────────────────────────────────┐
│                         CTE                          │  CTE evaluated once and then materialized
│ ──────────────────────────────────────────────────── │  using CTE index 0.  Materialized table may be
│                CTE Name: spam_callers                │  re-read using CTE_SCAN.
│                    Table Index: 0                    ├───────────────────────────────┐
│                                                      │                               │
│                        ~1 row                        │                               │
└──────────────────────────┬───────────────────────────┘
┌──────────────────────────┴───────────────────────────┐                   Plan for top-level query
│                      PROJECTION                      │                   does contain >1 references
│ ──────────────────────────────────────────────────── │                   to the CTE with index 0:
│                     Expressions:                     │
│                      c_custkey                       │                      ┌────────┴──────────┐
│                        c_name                        │                      │     CTE_SCAN      │
│                      c_address                       │                      │ ───────────────── │
│                     c_nationkey                      │                      │   CTE Index: 0    │
│                       c_phone                        │                      │                   │
│                      c_acctbal                       │                      │   ~30,000 rows    │
│                     c_mktsegment                     │                      └───────────────────┘
│                      c_comment                       │
│                                                      │
│                     ~30,000 rows                     │
└──────────────────────────┬───────────────────────────┘
┌──────────────────────────┴───────────────────────────┐
│                       SEQ_SCAN                       │
│ ──────────────────────────────────────────────────── │
│  Filters: c_phone>='12-758-' AND c_phone<'12-758.'   │
│              Table: tpch.main.customer               │
│                Type: Sequential Scan                 │
│                                                      │
│                     ~30,000 rows                     │
└──────────────────────────────────────────────────────┘

*/


-- Single CTE reference: expect CTE inlining
EXPLAIN
WITH        -- ┌─────────────────── add MATERIALIZED to observe
spam_callers AS (                -- DuckDB materializing the CTE
  FROM customer AS c
  WHERE c.c_phone LIKE '12-758-%'
)
SELECT sum(o.o_totalprice) AS fraud_volume
FROM   orders AS o
WHERE  o.o_custkey IN (
  SELECT c.c_custkey
  FROM   spam_callers AS c);

-----------------------------------------------------------------------

-- ⚠️ DuckDB respects a NOT MATERIALIZED annotation, even if
--     the query semantics turn out to be questionable.
--
-- Enforce inlining (and thus re-evaluation) of a CTE that invokes
-- volatile built-in function random().  Volatility lets the query
-- observe whether materialization has (not) taken place. :-/

.mode duckbox

WITH
spam_callers AS NOT MATERIALIZED (
  FROM customer AS c
  WHERE c.c_phone LIKE '12-758-%' OR random() > 0.5  -- ⚠️
)
FROM spam_callers        -- result may randomly be non-empty
  EXCEPT                 -- due to  re-evaluation of CTE spam_callers
FROM spam_callers;

-----------------------------------------------------------------------

-- Demonstrate DuckDB optimization pass cte_filter_pusher:
--
-- If all references to the CTE filter its result, combine
-- those filters (using OR) and push them down into the CTE
-- definition:

EXPLAIN
WITH
spam_callers AS (
  FROM customer AS c
  WHERE c.c_phone LIKE '12-758-%'
)
SELECT sum(o.o_totalprice) AS fraud_volume,
       (SELECT list(c.c_name)
        FROM   spam_callers AS c
        WHERE  c.c_address ~~ '%Fake City%') AS customers
FROM   orders AS o
WHERE  o.o_custkey IN (
  SELECT c.c_custkey
  FROM   spam_callers AS c
  WHERE  c.c_address ~~ '%Call Center%');


/* PLAN EXCERPT (CTE evaluation and materialization shown only)

┌───────────────────────────────────────────────────┐
│                        CTE                        │
│ ───────────────────────────────────────────────── │
│               CTE Name: spam_callers              │
│                   Table Index: 0                  ├─
│                       ~1 row                      │
└─────────────────────────┬─────────────────────────┘
┌─────────────────────────┴─────────────────────────┐
│                     PROJECTION                    │
│ ───────────────────────────────────────────────── │
│                    Expressions:                   │
│                     c_custkey                     │
│                       c_name                      │
│                     c_address                     │
│                    c_nationkey                    │
│                      c_phone                      │
│                     c_acctbal                     │
│                    c_mktsegment                   │
│                     c_comment                     │
│                                                   │
│                    ~6,000 rows                    │
└─────────────────────────┬─────────────────────────┘
┌─────────────────────────┴─────────────────────────┐
│                       FILTER                      │
│ ───────────────────────────────────────────────── │
│                    Expressions:                   │  combined filter pushed
│      (contains(c_address, 'Call Center') OR       │  down into CTE definition
│       contains(c_address, 'Fake City'))           │  by pass cte_filter_pusher
│                                                   │
│                    ~6,000 rows                    │
└─────────────────────────┬─────────────────────────┘
┌─────────────────────────┴─────────────────────────┐
│                      SEQ_SCAN                     │
│ ───────────────────────────────────────────────── │
│ Filters: c_phone>='12-758-' AND c_phone<'12-758.' │
│             Table: tpch.main.customer             │
│               Type: Sequential Scan               │
│                                                   │
│                    ~30,000 rows                   │
└───────────────────────────────────────────────────┘

*/
