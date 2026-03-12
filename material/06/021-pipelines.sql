-- SQL queries of non-trivial complexity lead to execution plans
-- that are assembled from multiple pipelines.  Pipelines are
-- stitched together at the pipeline breakers (in the query below:
-- HASH_GROUP_BY, HASH_JOIN).

-- Use TPC-H instance of some arbitrary scale factor (e.g., sf = 1)
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;


-- Enable and configure DuckDB's profiling facility to dump a
-- compact variant of the query plan
PRAGMA enable_profiling = 'query_tree';
PRAGMA custom_profiling_settings =
  '{ "OPERATOR_TYPE": "true", "EXTRA_INFO": "true", "OPERATOR_CARDINALITY": "true" }';
PRAGMA explain_output = 'physical_only';


-- Q1: Checks for violations of TPC-H constraint (Section 4.2.3):
--   O_ORDERSTATUS set to the following value:
--    - "F" if all lineitems of this order have L_LINESTATUS set to "F".
--    - "O" if all lineitems of this order have L_LINESTATUS set to "O".
--    - "P" otherwise.
SELECT DISTINCT o.o_orderkey AS violation
FROM   lineitem AS l, orders AS o
WHERE  l.l_orderkey = o.o_orderkey
AND    o.o_orderstatus IN ('O', 'F')
AND    l.l_linestatus <> o.o_orderstatus;


/*

NB. In the plan diagram, we have *manually* marked the pipeline breakers.

                     ┯━━━━━━━━━━━━━━━━━━━━━━━━━━━┯
                     │           QUERY           │   󰎪 QUERY constructs the query result
                     └─────────────┬─────────────┘      based on all incoming rows
                     ┌─────────────┴─────────────┐
                     │         PROJECTION        │
                     │    ────────────────────   │
                     │__internal_decompress_integ│
                     │     ral_bigint(#0, 1)     │
                     │                           │
                     │           0 rows          │
                     └─────────────┬─────────────┘
󰎪 The hash table    ┌─────────────┴─────────────┐
   entries are read  │       HASH_GROUP_BY       │
   and pushed upwards│    ────────────────────   │
                     ┿━━━━━━━━━ Groups: #0 ━━━━━━┿
                     │                           │  󰎧 HASH_GROUP_BY builds an aggregate hash table
                     │           0 rows          │     that supports the evaluation of the DISTINCT
                     └─────────────┬─────────────┘     clause
                     ┌─────────────┴─────────────┐
                     │         PROJECTION        │
                     │    ────────────────────   │
                     │         violation         │
                     │                           │
                     │           0 rows          │
                     └─────────────┬─────────────┘
                     ┌─────────────┴─────────────┐
                     │         PROJECTION        │
                     │    ────────────────────   │
                     │__internal_compress_integra│
                     │     l_uinteger(#0, 1)     │
                     │                           │
                     │           0 rows          │
                     └─────────────┬─────────────┘
                     ┌─────────────┴─────────────┐
                     │         PROJECTION        │
                     │    ────────────────────   │
                     │         violation         │
                     │                           │
                     │           0 rows          │
                     └─────────────┬─────────────┘
                     ┌─────────────┴───────────╂─┐
                     │         HASH_JOIN       ┃ │
                     │    ──────────────────── ┃ │
󰎧 HASH_JOIN probes  │      Join Type: INNER   ┃ │  󰎤 HASH_JOIN builds a hash table that holds
   incoming lineitem │                         ┃ │     1,461,457 (filtered) rows of table orders
   rows against the  │        Conditions:      ┃ │
   hash table        │  l_orderkey = o_orderkey┃ ├──────────────┐
                     │      l_linestatus !=    ┃ │              │
                     │        o_orderstatus    ┃ │              │
                     │                         ┃ │              │
                     │           0 rows        ┃ │              │
                     └─────────────┬───────────╂─┘              │
                     ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
                     │         TABLE_SCAN        ││           FILTER          │
                     │    ────────────────────   ││    ────────────────────   │
                     │      Table: lineitem      ││ ((o_orderstatus = 'O') OR │
                     │   Type: Sequential Scan   ││   (o_orderstatus = 'F'))  │
                     │                           ││                           │
                     │        Projections:       ││                           │
                     │         l_orderkey        ││                           │
                     │        l_linestatus       ││                           │
                     │                           ││                           │
                     │       6,001,215 rows      ││       1,461,457 rows      │
                     └───────────────────────────┘└─────────────┬─────────────┘
                                                  ┌─────────────┴─────────────┐
                                                  │         TABLE_SCAN        │
                                                  │    ────────────────────   │
                                                  │       Table: orders       │
                                                  │   Type: Sequential Scan   │
                                                  │                           │
                                                  │        Projections:       │
                                                  │         o_orderkey        │
                                                  │       o_orderstatus       │
                                                  │                           │
                                                  │          Filters:         │
                                                  │ optional: o_orderstatus IN│
                                                  │         ('O', 'F')        │
                                                  │                           │
                                                  │       1,500,000 rows      │
                                                  └───────────────────────────┘
*/


-- Q2: Find the 10 most expensive orders
--
-- QUIZ: Study the plan, identify the pipeline breakers, and
--       identify the pipelines and their dependencies.
--
EXPLAIN ANALYZE
SELECT o.o_orderkey, o.o_totalprice
FROM   orders AS o
ORDER BY o.o_totalprice DESC NULLS FIRST
LIMIT 10;


/*

NB. The TABLE_SCAN on the lhs probe side of the HASH_JOIN
only reads a fraction of the rows of table orders.  This is
due to DuckDB's join_filter_pushdown optimization.  We will
focus on DuckDB's query optimizers in Chapter 08.

┌───────────────────────────┐
│           QUERY           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│__internal_decompress_integ│
│     ral_bigint(#0, 1)     │
│             #1            │
│          10 rows          │
│                           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│          ORDER_BY         │
│    ────────────────────   │
│    o.o_totalprice DESC    │
│          10 rows          │
│                           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         PROJECTION        │
│    ────────────────────   │
│__internal_compress_integra│
│     l_uinteger(#0, 1)     │
│             #1            │
│          10 rows          │
│                           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│         HASH_JOIN         │
│    ────────────────────   │
│      Join Type: SEMI      │
│                           │
│        Conditions:        ├──────────────┐
│       rowid = rowid       │              │
│          10 rows          │              │
│                           │              │
└─────────────┬─────────────┘              │
┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│         TABLE_SCAN        ││           TOP_N           │
│    ────────────────────   ││    ────────────────────   │
│       Table: orders       ││          Top: 10          │
│   Type: Sequential Scan   ││                           │
│                           ││         Order By:         │
│        Projections:       ││    o.o_totalprice DESC    │
│         o_orderkey        ││                           │
│        o_totalprice       ││                           │
│        17,729 rows        ││          10 rows          │
│                           ││                           │
└───────────────────────────┘└─────────────┬─────────────┘
                             ┌─────────────┴─────────────┐
                             │         TABLE_SCAN        │
                             │    ────────────────────   │
                             │       Table: orders       │
                             │   Type: Sequential Scan   │
                             │                           │
                             │        Projections:       │
                             │        o_totalprice       │
                             │       1,500,000 rows      │
                             │                           │
                             └───────────────────────────┘
*/


-- Q3 (TPC-H Query 20)
--
-- From the TPC-H specification:
--
-- "The Returned Item Reporting Query finds the top 20 customers,
--  in terms of their effect on lost revenue for a given
--  quarter, who have returned parts. The query considers only parts
--  that were ordered in the specified quarter. Customers are listed
--  in descending order of lost revenue."
--
-- QUIZ: Study the plan, identify the pipeline breakers, and
--       identify the pipelines and their dependencies.
--
-- Only output operator names, suppress all other plan annotations
PRAGMA custom_profiling_settings = '{ "OPERATOR_TYPE": "true" }';

SELECT c.c_custkey, c.c_name, c.c_acctbal, c.c_address, c.c_phone, c.c_comment, n.n_name,
       sum(l.l_extendedprice * (1 - l.l_discount)) as lost_revenue
FROM   customer AS c, orders AS o, lineitem AS l, nation AS n
WHERE  c.c_custkey = o.o_custkey
AND    l.l_orderkey = o.o_orderkey
AND    o.o_orderdate >= date '1994-08-01'
AND    o.o_orderdate < date '1994-08-01' + interval '3' month
AND    l.l_returnflag = 'R'
AND    c.c_nationkey = n.n_nationkey
GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, c.c_address, c.c_comment, n.n_name
ORDER BY lost_revenue DESC
LIMIT 20;


/*

┌───────────────────┐
│       QUERY       │
└─────────┬─────────┘
┌─────────┴─────────┐
│       TOP_N       │
└─────────┬─────────┘
┌─────────┴─────────┐
│     PROJECTION    │
└─────────┬─────────┘
┌─────────┴─────────┐
│     PROJECTION    │
└─────────┬─────────┘
┌─────────┴─────────┐
│   HASH_GROUP_BY   │
└─────────┬─────────┘
┌─────────┴─────────┐
│     PROJECTION    │
└─────────┬─────────┘
┌─────────┴─────────┐
│     PROJECTION    │
└─────────┬─────────┘
┌─────────┴─────────┐
│     HASH_JOIN     ├──────────┐
└─────────┬─────────┘          │
┌─────────┴─────────┐┌─────────┴─────────┐
│     TABLE_SCAN    ││     HASH_JOIN     ├───────────────────────────────┐
└───────────────────┘└─────────┬─────────┘                               │
                             ┌─────────┴─────────┐                     ┌─────────┴─────────┐
                             │     HASH_JOIN     ├──────────┐          │     TABLE_SCAN    │
                             └─────────┬─────────┘          │          └───────────────────┘
                             ┌─────────┴─────────┐┌─────────┴─────────┐
                             │     TABLE_SCAN    ││     TABLE_SCAN    │
                             └───────────────────┘└───────────────────┘
*/
