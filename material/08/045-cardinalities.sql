-- The associativity of join gives DuckDB the freedom to pick one
-- of a (possibly wide!) range of equivalent join tree shapes.
--
-- We demonstrate that picking a good join tree is essential for
-- the runtime performance of join-heavy SQL queries.
--
-- You will find that DuckDB PICKS THE JOIN TREE SHAPE THAT PROMISES
-- TO PRODUCE THE INTERMEDIATE RESULTS OF SMALLEST CARDINALITY.


-- Attach to a TPC-H instance, scale factor sf = 10
ATTACH '../../databases/tpch-sf10.db' AS tpch;
USE tpch;

-- Here we are looking at physical plans
PRAGMA explain_output = 'physical_only';
.mode trash
.timer on

-- Show canonical plans
-- PRAGMA disable_optimizer;

-- Show optimized plans
PRAGMA enable_optimizer;

-----------------------------------------------------------------------

-- The FROM clause of the query below syntactically suggests
-- a join order of (l ⋈ o) ⋈ c.  We temporarily disable the
-- join_order optimizer to force DuckDB to pick just that
-- join order.
SET disabled_optimizers = 'join_order';

-- Parts in the orders of customer #000000001
EXPLAIN ANALYZE
SELECT l.l_partkey, l.l_quantity, l.l_extendedprice
FROM   lineitem AS l JOIN orders AS o
           ON (l.l_orderkey = o.o_orderkey)
       JOIN customer AS c
           ON (o.o_custkey = c.c_custkey)
WHERE  c.c_name = 'Customer#000000001';


/* Join tree shape and intermediate cardinalities:

               |
            53 ⋈ ────────────────────┐
               |                     σ 1
    59,986,052 ⋈ ─────────┐          |
               |          |       customer
           lineitem     orders   1,500,000
          59,986,052  15,000,000

*/

-- Evaluate the query with join tree shape (l ⋈ o) ⋈ c

SELECT l.l_partkey, l.l_quantity, l.l_extendedprice
FROM   lineitem AS l JOIN orders AS o
           ON (l.l_orderkey = o.o_orderkey)
       JOIN customer AS c
           ON (o.o_custkey = c.c_custkey)
WHERE  c.c_name = 'Customer#000000001';
-- Run Time (s): real 0.202 user 1.884446 sys 0.114630


-----------------------------------------------------------------------

-- Re-enable the join_order optimizer.  DuckDB will consider other
-- join tree shapes.  DuckDB v1.5 now chooses l ⋈ (o ⋈ c).
RESET disabled_optimizers;

-- Disable the dynamic join_filter_pushdown since it will benefit
-- the plan unfairly at runtime.  Currently we discuss the static
-- join_order optimization only.
SET disabled_optimizers = 'join_filter_pushdown';

EXPLAIN ANALYZE
SELECT l.l_partkey, l.l_quantity, l.l_extendedprice
FROM   lineitem AS l JOIN orders AS o
           ON (l.l_orderkey = o.o_orderkey)
       JOIN customer AS c
           ON (o.o_custkey = c.c_custkey)
WHERE  c.c_name = 'Customer#000000001';


/* Join tree shape and intermediate cardinalities (drastically reduced):

       |
    53 ⋈ ──────────┐
       |        15 ⋈ ──────────┐
   lineitem        |           σ 1
   59,986,052    orders        |
               15,000,000   customer
                           1,500,000
*/


SELECT l.l_partkey, l.l_quantity, l.l_extendedprice
FROM   lineitem AS l JOIN orders AS o
           ON (l.l_orderkey = o.o_orderkey)
       JOIN customer AS c
           ON (o.o_custkey = c.c_custkey)
WHERE  c.c_name = 'Customer#000000001';
-- Run Time (s): real 0.045 user 0.364396 sys 0.007549
--                    ^^^^^
--                 was: 0.202


-- ⚠️ DuckDB picks the join tree shape that promises to produce the
--    INTERMEDIATE RESULTS OF SMALLEST CARDINALITY.

-----------------------------------------------------------------------

-- Re-enable join_filter_pushdown to see DuckDB operate at
-- full speed.

RESET disabled_optimizers;

SELECT l.l_partkey, l.l_quantity, l.l_extendedprice
FROM   lineitem AS l JOIN orders AS o
           ON (l.l_orderkey = o.o_orderkey)
       JOIN customer AS c
           ON (o.o_custkey = c.c_custkey)
WHERE  c.c_name = 'Customer#000000001';
-- Run Time (s): real 0.013 user 0.042944 sys 0.003995


/* Join tree shape and intermediate cardinalities with dynamic
   join_filter_pushdown:

       |
    53 ⋈ ──────────┐
       |        15 ⋈ ──────────┐
   lineitem        |           σ 1
    dyn:862      orders        |
                 dyn:15     customer
                           1,500,000
*/
