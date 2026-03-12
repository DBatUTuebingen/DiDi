-- Attach to a TPC-H instance, scale factor sf = 10
ATTACH '../../databases/tpch-sf10.db' AS tpch;
USE tpch;

-- Here we are looking at physical plans
PRAGMA explain_output = 'physical_only';

-- Show canonical plans
-- PRAGMA disable_optimizer;

-- Show optimized plans
PRAGMA enable_optimizer;

-----------------------------------------------------------------------

-- Cardinalities of tables customer (1.5 million) and nation (25) differ
-- significantly:
SELECT count(*) AS "|customer|"
FROM   customer;

SELECT count(*) AS "|nation|"
FROM   nation;

-- Heuristic  in pass build_side_probe_side:
--
-- Place the smaller table on the rhs (build) side of joins such
-- that the hash table build by HASH_JOIN uses neglible space.

-- Customers along with nation details
EXPLAIN ANALYZE                       -- SQL syntax suggests
FROM nation AS n, customer AS c       -- lhs: nation, rhs: customer
WHERE c.c_nationkey = n.n_nationkey;

/* PHYSICAL PLAN AFTER build_side_probe_side pass (lhs/rhs swapped)

┌───────────────────┴───────────────────┐
│               HASH_JOIN               │
│ ───────────────────────────────────── │
│            Join Type: INNER           │
│ Conditions: c_nationkey = n_nationkey │
│                                       ├─────────────┐
│             1,500,000 rows            │             │
└───────────────────┬───────────────────┘             │
┌───────────────────┴───────────────────┐┌────────────┴────────────┐
│               TABLE_SCAN              ││        TABLE_SCAN       │
│ ───────────────────────────────────── ││ ─────────────────────── │
│       Table: tpch.main.customer       ││ Table: tpch.main.nation │
│         Type: Sequential Scan         ││  Type: Sequential Scan  │
│                                       ││                         │
│             1,500,000 rows            ││         25 rows         │
└───────────────────────────────────────┘└─────────────────────────┘

*/


-- The query remains executable even with restrictive memory limit
-- and spilling disabled:

SET memory_limit = '500MB';
SET temp_directory = '';

.timer on
.mode trash

FROM nation AS n, customer AS c
WHERE c.c_nationkey = n.n_nationkey;


-- Without build_side_probe_side, large table customer on the rhs
-- (build) side requires a hash table that does not fit within
-- the specified memory limit.

SET disabled_optimizers = 'build_side_probe_side';

FROM nation AS n, customer AS c
WHERE c.c_nationkey = n.n_nationkey;


-- Re-enable pass build_side_probe_side, back to default memory limit

RESET disabled_optimizers;
RESET memory_limit;

-----------------------------------------------------------------------

-- Heuristic in pass build_side_probe_side if cardinalities of
-- both inputs are the same:
--
-- Place the input that passes more columns downstreams on the
-- lhs (probe) side.  Tuples on the rhs (build) side can be made
-- narrower via pass unused_columns and will thus contribute a
-- lighter payload during hash table construction.


-- Customers that share addresses
EXPLAIN ANALYZE
SELECT c1.*                           -- SQL syntax suggests
FROM customer AS c2, customer AS c1   -- customer c2: lhs, customer c1: rhs
WHERE c1.c_address = c2.c_address
AND   c1.c_custkey <> c2.c_custkey;

/* PHYSICAL PLAN (narrow input on the rhs thanks to pass unused_columns)


┌──────────────┴──────────────┐
│          HASH_JOIN          │
│ ─────────────────────────── │
│       Join Type: INNER      │
│                             │
│         Conditions:         │
│    c_address = c_address    │
│    c_custkey != c_custkey   ├───────────────┐
│                             │               │
│            0 rows           │               │
└──────────────┬──────────────┘               │
┌──────────────┴──────────────┐┌──────────────┴──────────────┐
│          TABLE_SCAN         ││          TABLE_SCAN         │
│ ─────────────────────────── ││ ─────────────────────────── │
│  Table: tpch.main.customer  ││  Table: tpch.main.customer  │
│    Type: Sequential Scan    ││    Type: Sequential Scan    │
│                             ││                             │
│         Projections:        ││         Projections:        │
│          c_address          ││          c_address          │ narrow payload
│          c_custkey          ││          c_custkey          │ after analysis of
│            c_name           ││                             │ unused columns
│         c_nationkey         ││                             │
│           c_phone           ││                             │
│          c_acctbal          ││                             │
│         c_mktsegment        ││            0.02s            │
│          c_comment          ││                             │
│                             ││                             │
│        1,500,000 rows       ││        1,500,000 rows       │ cardinalities identical
└─────────────────────────────┘└─────────────────────────────┘
               c1                             c2
*/
