-- DuckDB's join order optimizer (based on dynamic programming algorithm
-- DPhyp, https://dl.acm.org/doi/10.1145/1376616.1376672) is hypergraph-aware
-- which helps to understand joins that incorporate predicates spanning
-- multiple tables as joins that span two hypernodes (table subsets).

-- Here we are looking at physical plans
PRAGMA explain_output = 'optimized_only';
.mode trash
.timer on

-- Show canonical plans
-- PRAGMA disable_optimizer;

-- Show optimized plans
PRAGMA enable_optimizer;

-----------------------------------------------------------------------
-- Create playground of six empty tables (with identical schemata)

CREATE TABLE T1 (
  a int,
  b int,
  c int,
  d int,
  e int);

CREATE TABLE T2 AS FROM T1;
CREATE TABLE T3 AS FROM T1;
CREATE TABLE T4 AS FROM T1;
CREATE TABLE T5 AS FROM T1;
CREATE TABLE T6 AS FROM T1;

-----------------------------------------------------------------------


EXPLAIN
FROM T1, T2, T3, T4, T5, T6
WHERE T1.a = T2.a AND T2.b = T3.b      -- T1--T2--T3  (hypernode)
AND   T4.c = T5.c AND T5.d = T6.d      -- T4--T5--T6  (hypernode)
AND   T1.e + T2.e + T3.e = T4.e + T5.e + T6.e;
--    └──────────────────────────────────────┘
--  induces hyperedge (T1--T2--T3)╌╌╌(T4--T5--T6)


/* OPTIMIZED JOIN TREE

   - The tree is bushy (not left-deep).
   - No cross products generated.
   - The hyper edge (T1--T2--T3)╌╌╌(T4--T5--T6) led to creation of
     the top-most join.

┌─────────────┴─────────────┐
│      COMPARISON_JOIN      │
│    ────────────────────   │
│      Join Type: INNER     │
│                           │
│        Conditions:        ├────────────────────────────────────────────────────────────────────────┐
│ (((e + e) + e) = ((e + e) │                                                                        │
│           + e))           │                                                                        │
└─────────────┬─────────────┘                                                                        │
┌─────────────┴─────────────┐                                                          ┌─────────────┴─────────────┐
│      COMPARISON_JOIN      │                                                          │      COMPARISON_JOIN      │
│    ────────────────────   │                                                          │    ────────────────────   │
│      Join Type: INNER     │                                                          │      Join Type: INNER     │
│    Conditions: (a = a)    ├───────────────────────────────────────────┐              │    Conditions: (c = c)    ├───────────────────────────────────────────┐
└─────────────┬─────────────┘                                           │              └─────────────┬─────────────┘                                           │
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│      COMPARISON_JOIN      │                             │          SEQ_SCAN         ││      COMPARISON_JOIN      │                             │          SEQ_SCAN         │
│    ────────────────────   │                             │    ────────────────────   ││    ────────────────────   │                             │    ────────────────────   │
│      Join Type: INNER     │                             │         Table: T1         ││      Join Type: INNER     │                             │         Table: T4         │
│    Conditions: (b = b)    ├──────────────┐              │   Type: Sequential Scan   ││    Conditions: (d = d)    ├──────────────┐              │   Type: Sequential Scan   │
└─────────────┬─────────────┘              │              └───────────────────────────┘└─────────────┬─────────────┘              │              └───────────────────────────┘
┌─────────────┴─────────────┐┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│          SEQ_SCAN         ││          SEQ_SCAN         │                             │          SEQ_SCAN         ││          SEQ_SCAN         │
│    ────────────────────   ││    ────────────────────   │                             │    ────────────────────   ││    ────────────────────   │
│         Table: T2         ││         Table: T3         │                             │         Table: T5         ││         Table: T6         │
│   Type: Sequential Scan   ││   Type: Sequential Scan   │                             │   Type: Sequential Scan   ││   Type: Sequential Scan   │
└───────────────────────────┘└───────────────────────────┘                             └───────────────────────────┘└───────────────────────────┘

*/

-----------------------------------------------------------------------
-- Temporarily switching off the join_order optimizer leads to a
-- left-deep join tree that contains a cross product.  The hyperedge
-- predicate is evaluated in a post-FILTER.

SET disabled_optimizers = 'join_tree';

EXPLAIN
FROM T1, T2, T3, T4, T5, T6
WHERE T1.a = T2.a AND T2.b = T3.b
AND   T4.c = T5.c AND T5.d = T6.d
AND   T1.e + T2.e + T3.e = T4.e + T5.e + T6.e;

/* NON-OPTIMIZED LEFT-DEEP JOIN TREE

┌─────────────┴─────────────┐
│           FILTER          │
│    ────────────────────   │
│        Expressions:       │
│ (((e + e) + e) = ((e + e) │
│           + e))           │
└─────────────┬─────────────┘
┌─────────────┴─────────────┐
│      COMPARISON_JOIN      │
│    ────────────────────   │
│      Join Type: INNER     ├──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│    Conditions: (d = d)    │                                                                                                                                  │
└─────────────┬─────────────┘                                                                                                                                  │
┌─────────────┴─────────────┐                                                                                                                    ┌─────────────┴─────────────┐
│      COMPARISON_JOIN      │                                                                                                                    │          SEQ_SCAN         │
│    ────────────────────   │                                                                                                                    │    ────────────────────   │
│      Join Type: INNER     │                                                                                                                    │         Table: T6         │
│    Conditions: (c = c)    ├─────────────────────────────────────────────────────────────────────────────────────────────────────┐              │   Type: Sequential Scan   │
│                           │                                                                                                     │              │                           │
│          ~0 rows          │                                                                                                     │              │                           │
└─────────────┬─────────────┘                                                                                                     │              └───────────────────────────┘
┌─────────────┴─────────────┐                                                                                       ┌─────────────┴─────────────┐
│       CROSS_PRODUCT       │                                                                                       │          SEQ_SCAN         │
│    ────────────────────   │                                                                                       │    ────────────────────   │
│                           ├────────────────────────────────────────────────────────────────────────┐              │         Table: T5         │
│          ~0 rows          │                                                                        │              │   Type: Sequential Scan   │
└─────────────┬─────────────┘                                                                        │              └───────────────────────────┘
┌─────────────┴─────────────┐                                                          ┌─────────────┴─────────────┐
│      COMPARISON_JOIN      │                                                          │          SEQ_SCAN         │
│    ────────────────────   │                                                          │    ────────────────────   │
│      Join Type: INNER     │                                                          │         Table: T4         │
│    Conditions: (b = b)    ├───────────────────────────────────────────┐              │   Type: Sequential Scan   │
│                           │                                           │              │                           │
│          ~0 rows          │                                           │              │                           │
└─────────────┬─────────────┘                                           │              └───────────────────────────┘
┌─────────────┴─────────────┐                             ┌─────────────┴─────────────┐
│      COMPARISON_JOIN      │                             │          SEQ_SCAN         │
│    ────────────────────   │                             │    ────────────────────   │
│      Join Type: INNER     │                             │         Table: T3         │
│    Conditions: (a = a)    ├──────────────┐              │   Type: Sequential Scan   │
│                           │              │              │                           │
│          ~0 rows          │              │              │                           │
└─────────────┬─────────────┘              │              └───────────────────────────┘
┌─────────────┴─────────────┐┌─────────────┴─────────────┐
│          SEQ_SCAN         ││          SEQ_SCAN         │
│    ────────────────────   ││    ────────────────────   │
│         Table: T1         ││         Table: T2         │
│   Type: Sequential Scan   ││   Type: Sequential Scan   │
└───────────────────────────┘└───────────────────────────┘

*/


-----------------------------------------------------------------------
-- Clean up

RESET disabled_optimizers;

DROP TABLE T1;
DROP TABLE T2;
DROP TABLE T3;
DROP TABLE T4;
DROP TABLE T5;
DROP TABLE T6;
