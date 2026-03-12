-- DuckDB decorrelates nested SQL subqueries to avoid a nested-loops
-- strategy that would re-evaluate dependent subplans again an again.

-- Attach to a TPC-H instance, scale factor sf = 1 suffices to
-- demonstrate the query decorrelation optimization
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;

-- Table cardinalities:
SELECT count(*) AS "|orders|"
FROM orders;

SELECT count(*) AS "|lineitem|"
FROM lineitem;

-- Here we study query plans, not results
.mode trash

-- ⚠️ To see the initial/non-optimized query plan for Q1, feed this
--     query into a patched version of DuckDB that prints the plan
--     *before* FlattenDependentJoins::DecorrelateIndependent(...)
--     is invoked.  See function
--
--        void Planner::CreatePlan(SQLStatement &statement)
--
--     in duckdb/src/planner/planner.cpp:
--
--       this->plan->Print();  // DiDi: print initial plan
--       this->plan = FlattenDependentJoins::DecorrelateIndependent(*this->binder, std::move(this->plan));

-- Query Q1 (all orders along with the linenumber of their cheapest lineitem)
--
SELECT o.o_orderkey, o.o_orderdate, l.l_linenumber
FROM orders AS o, lineitem AS l
WHERE o.o_orderkey = l.l_orderkey
AND   l.l_extendedprice = (SELECT min(i.l_extendedprice)
                            FROM  lineitem AS i
                            WHERE i.l_orderkey = o.o_orderkey);


-- ➊ ------------------------------------------------------------------

/* Non-optimized plan for Q1. Contains DEPENDENT_JOIN and cannot be
   executed in this form by DuckDB.

   (This plan is the result of a canonical SQL-to-plan translation.
    Note the CROSS_PRODUCT between tables orders and lineitems:
    this initial plan is infeasible for execution.)

   - 🠵 marks the correlation (column reference to outer query).

   - Annotation "Join Type: SINGLE": each row of the lhs input will
     join with one row in the rhs input.


   ┌─────────────────────────────────┐
   │            PROJECTION           │
   │  ────────────────────────────── │
   │           Expressions:          │
   │            o_orderkey           │
   │           o_orderdate           │
   │           l_linenumber          │
   └────────────────┬────────────────┘
   ┌────────────────┴────────────────┐
   │              FILTER             │
   │  ────────────────────────────── │
   │           Expressions:          │
   │    (o_orderkey = l_orderkey)    │
   │   (l_extendedprice = SUBQUERY)  │
   └────────────────┬────────────────┘
   ┌────────────────┴────────────────┐
   │          DEPENDENT_JOIN         │
   │  ────────────────────────────── ├──────────────────────────────────────────────────┐
   │        Join Type: SINGLE        │                                                  │
   └────────────────┬────────────────┘                                                  │
   ┌────────────────┴────────────────┐                               ┌─────────────────┴─────────────────┐
   │          CROSS_PRODUCT          │                               │             PROJECTION            │
   │  ────────────────────────────── ├───────────────┐               │ ───────────────────────────────── │
   │                                 │               │               │ Expressions: min(l_extendedprice) │
   └────────────────┬────────────────┘               │               └─────────────────┬─────────────────┘
   ┌────────────────┴────────────────┐┌──────────────┴──────────────┐┌─────────────────┴─────────────────┐
   │             SEQ_SCAN            ││           SEQ_SCAN          ││             AGGREGATE             │
   │  ────────────────────────────── ││ ─────────────────────────── ││ ───────────────────────────────── │
   │     Table: tpch.main.orders     ││  Table: tpch.main.lineitem  ││ Expressions: min(l_extendedprice) │
   │      Type: Sequential Scan      ││    Type: Sequential Scan    ││                                   │
   └─────────────────────────────────┘└─────────────────────────────┘└─────────────────┬─────────────────┘
                                                                   ┌───────────────────┴────────────────────┐
                                                                   │                 FILTER                 │
                                                                   │ ────────────────────────────────────── │
                                                                   │ Expressions: (l_orderkey = o_orderkey) │
                                                                   └───────────────────┬────────🠵───────────┘
                                                                   ┌───────────────────┴────────────────────┐
                                                                   │                SEQ_SCAN                │
                                                                   │ ────────────────────────────────────── │
                                                                   │       Table: tpch.main.lineitem        │
                                                                   │         Type: Sequential Scan          │
                                                                   └────────────────────────────────────────┘
*/

-- ➋ ------------------------------------------------------------------

-- Disable finalizing optimzations so that we can inspect the
-- result of decorrelation
PRAGMA disable_optimizer;
PRAGMA explain_output = 'optimized_only';

EXPLAIN
SELECT o.o_orderkey, o.o_orderdate, l.l_linenumber
FROM   orders AS o, lineitem AS l
WHERE  o.o_orderkey = l.l_orderkey
AND    l.l_extendedprice = (SELECT min(i.l_extendedprice)
                            FROM   lineitem AS i
                            WHERE  i.l_orderkey = o.o_orderkey);

/* Query plan for Q1 after decorrelation (but before more finalizing
   optimizations have been performed).  Contains no DEPENDENT_JOIN.

   - DELIM_JOIN has been introduced in decorrelation step 󰎤:

     1. Performs the lookup in the lookup table T.
     2. Implicitly computes the set D of distinct parameters for
        the subquery plan.

   - DELIM_GET reads set D in the subquery plan.

 NB. Superfluous COMPARISON_JOIN with D removed for clarity.

┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│        Expressions:       │
│         o_orderkey        │
│        o_orderdate        │
│        l_linenumber       │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│        Expressions:       │
│ (o_orderkey = l_orderkey) │
│     (l_extendedprice =    │
│          SUBQUERY)        │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         DELIM_JOIN        │
│    ────────────────────   │
│     Join Type: SINGLE     │
│                           ├─────────────────────────────────────┐
│        Conditions:        │                                     │
│     (o_orderkey IS NOT    │                                     │
│  DISTINCT FROM o_orderkey)│                                     │
└─────────────┬─────────────┘                                     │
┌─────────────┴─────────────┐                         ┌───────────┴───────────┐
│       CROSS_PRODUCT       │                         │       PROJECTION      │
│    ────────────────────   │                         │  ──────────────────── │
│                           ├────────────┐            │      Expressions:     │
│                           │            │            │  min(l_extendedprice) │
│                           │            │            │       o_orderkey      │
└─────────────┬─────────────┘            │            └───────────┬───────────┘
┌─────────────┴─────────────┐┌───────────┴───────────┐┌───────────┴───────────┐
│          SEQ_SCAN         ││        SEQ_SCAN       ││       AGGREGATE       │
│    ────────────────────   ││  ──────────────────── ││  ──────────────────── │
│       Table: orders       ││    Table: lineitem    ││   Groups: o_orderkey  │
│   Type: Sequential Scan   ││ Type: Sequential Scan ││                       │
│                           ││                       ││      Expressions:     │
│                           ││                       ││  min(l_extendedprice) │
└───────────────────────────┘└───────────────────────┘└───────────┬───────────┘
                                                    ┌─────────────┴─────────────┐
                                                    │           FILTER          │
                                                    │    ────────────────────   │
                                                    │        Expressions:       │
                                                    │ (l_orderkey = o_orderkey) │
                                                    └─────────────┬─────────────┘
                                                    ┌─────────────┴─────────────┐
                                                    │       CROSS_PRODUCT       │
                                                    │    ────────────────────   ├───────────┐
                                                    └─────────────┬─────────────┘           │
                                                    ┌─────────────┴─────────────┐┌──────────┴──────────┐
                                                    │          SEQ_SCAN         ││      DELIM_GET      │
                                                    │    ────────────────────   ││  ─────────────────  │
                                                    │      Table: lineitem      ││                     │
                                                    │   Type: Sequential Scan   ││                     │
                                                    └───────────────────────────┘└─────────────────────┘
*/


-- ➌ ------------------------------------------------------------------

-- Re-enable all optimzations done after decorrelation
-- (filter pushed down into, decoupling)
PRAGMA enable_optimizer;

EXPLAIN
SELECT o.o_orderkey, o.o_orderdate, l.l_linenumber
FROM   orders AS o, lineitem AS l
WHERE  o.o_orderkey = l.l_orderkey
AND    l.l_extendedprice = (SELECT min(i.l_extendedprice)
                            FROM   lineitem AS i
                            WHERE  i.l_orderkey = o.o_orderkey);


/* Final query plan for Q1 after decorrelation and post-optimizations
   (filter pushed down into joins, decoupling of outer and subquery).

   - NB. This is EXPLAIN (not: EXPLAIN ANALYZE) output: the row
     counts are estimates and thus are partly off
     (the grouped AGGREGATE returns 1,500,000 rows).

┌───────────────────────────┐
│         PROJECTION        │
│    ────────────────────   │
│        Expressions:       │
│         o_orderkey        │
│        o_orderdate        │
│        l_linenumber       │
│                           │
│      ~1,207,209 rows      │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│        Expressions:       │
│     (l_extendedprice =    │
│          SUBQUERY)        │
│                           │
│      ~1,207,209 rows      │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      COMPARISON_JOIN      │
│    ────────────────────   │
│      Join Type: LEFT      │
│                           │
│        Conditions:        ├───────────────────────────────────────────┐
│     (o_orderkey IS NOT    │                                           │
│  DISTINCT FROM o_orderkey)│                                           │
│                           │                                           │
│      ~6,036,047 rows      │                                           │
└─────────────┬─────────────┘                                           │
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│         PROJECTION        │                             │         PROJECTION        │
│    ────────────────────   │                             │    ────────────────────   │
│        Expressions:       │                             │        Expressions:       │
│             #0            │                             │    min(l_extendedprice)   │
│             #1            │                             │         o_orderkey        │
│__internal_decompress_integ│                             │                           │
│     ral_bigint(#2, 1)     │                             │                           │
│             #3            │                             │                           │
│                           │                             │                           │
│      ~6,036,047 rows      │                             │       ~600,121 rows       │
└─────────────┬─────────────┘                             └─────────────┬─────────────┘
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│      COMPARISON_JOIN      │                             │         PROJECTION        │
│    ────────────────────   │                             │    ────────────────────   │
│      Join Type: INNER     │                             │        Expressions:       │
│                           │                             │__internal_decompress_integ│
│        Conditions:        ├──────────────┐              │     ral_bigint(#0, 1)     │
│ (l_orderkey = o_orderkey) │              │              │             #1            │
│                           │              │              │                           │
│      ~6,036,047 rows      │              │              │       ~600,121 rows       │
└─────────────┬─────────────┘              │              └─────────────┬─────────────┘
┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         PROJECTION        ││         PROJECTION        ││         AGGREGATE         │
│    ────────────────────   ││    ────────────────────   ││    ────────────────────   │
│        Expressions:       ││        Expressions:       ││     Groups: o_orderkey    │
│__internal_compress_integra││__internal_compress_integra││                           │
│     l_uinteger(#0, 1)     ││     l_uinteger(#0, 1)     ││        Expressions:       │
│             #1            ││             #1            ││    min(l_extendedprice)   │
│             #2            ││                           ││                           │
│                           ││                           ││                           │
│      ~6,001,215 rows      ││      ~1,500,000 rows      ││       ~600,121 rows       │
└─────────────┬─────────────┘└─────────────┬─────────────┘└─────────────┬─────────────┘
┌─────────────┴─────────────┐┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│          SEQ_SCAN         ││          SEQ_SCAN         ││         PROJECTION        │
│    ────────────────────   ││    ────────────────────   ││    ────────────────────   │
│      Table: lineitem      ││       Table: orders       ││        Expressions:       │
│   Type: Sequential Scan   ││   Type: Sequential Scan   ││__internal_compress_integra│
│                           ││                           ││     l_uinteger(#0, 1)     │
│                           ││                           ││             #1            │
│                           ││                           ││                           │
│      ~6,001,215 rows      ││      ~1,500,000 rows      ││      ~6,001,215 rows      │
└───────────────────────────┘└───────────────────────────┘└─────────────┬─────────────┘
                                                          ┌─────────────┴─────────────┐
                                                          │          SEQ_SCAN         │
                                                          │    ────────────────────   │
                                                          │      Table: lineitem      │
                                                          │   Type: Sequential Scan   │
                                                          │                           │
                                                          │      ~6,001,215 rows      │
                                                          └───────────────────────────┘

*/


-- Now evaluate the fully optimized query Q1
.mode trash
.timer on

SELECT o.o_orderkey, o.o_orderdate, l.l_linenumber
FROM   orders AS o, lineitem AS l
WHERE  o.o_orderkey = l.l_orderkey
AND    l.l_extendedprice = (SELECT min(i.l_extendedprice)
                            FROM   lineitem AS i
                            WHERE  i.l_orderkey = o.o_orderkey);

-- Run Time (s): real 0.075 user 0.643478 sys 0.036738

-- Compare this to a manual SQL re-formulation of Q1 that aims
-- to directly mimic the optimized query plan ➌.
--
-- (Note: You can obtain such decorrelated SQL queries using Umbra's
--
--    EXPLAIN (SQL)
--    ‹query›
--
-- See Umbra's Web interface at https://umbra-db.com/interface/.)

WITH
lookup(d,m) AS (
  SELECT i.l_orderkey AS d, min(i.l_extendedprice) AS m
  FROM   lineitem AS i
  GROUP BY i.l_orderkey
)
SELECT o.o_orderkey, o.o_orderdate, l.l_linenumber
FROM   orders AS o, lineitem AS l, lookup AS subq
WHERE  o.o_orderkey = l.l_orderkey
AND    subq.d = o.o_orderkey AND l.l_extendedprice = subq.m;

-- Run Time (s): real 0.073 user 0.567859 sys 0.040673
