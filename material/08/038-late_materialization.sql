-- Plan execution benefits if rows are narrow (contain few columns):
-- 1. Push down projections into table scan to access relevant columns
--    only.
-- 2. Only *late in the plan* join the rows resulting from 1. with
--    the (potentially many) columns required to build the final
--    result.  Use rowid as the join criterion to identify the
--    relevant rows.  Make use of DuckDB's TABLE_SCAN ability to
--    only access those rows identified by a set of given rowids.

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
-- Find the 10 oldest orders that are not completed yet marked urgent.
--
-- The "heavy work" relies on only three (of nine) columns in table
-- orders. If we push down the filters into TABLE_SCAN this leaves
-- only the single column o_orderdate relevant for the ORDER BY + LIMIT.
--
-- Join in the full rows only once the ORDER BY + LIMIT (TOP_N) has
-- cut down cardinality considerably.

EXPLAIN ANALYZE
SELECT o.*                               -- wide result (9 columns)
FROM  orders AS o
WHERE o.o_orderstatus <> 'F' AND o.o_orderpriority <= '2'  -- heavy
ORDER BY o.o_orderdate                                     -- work
LIMIT 10;


/* Notes:

   - The ORDER_BY is reapplied at the top of the plan since
     the HASH_JOIN potentially destroys the order established
     by TOP_N.  This second ORDER_BY sorts 10 rows only, though.

   - The TABLE_SCAN 󰎧 only accesses the rows found in the list
     of row IDs provided by the rhs build side ("rowid pushdown").


┌──────────────┸───────────────┐
│           ORDER_BY           │
│ ──────────────────────────── │
│      o.o_orderdate ASC       │
│                              │
│           10 rows            │
│            0.00s             │
└──────────────┰───────────────┘
┌──────────────┸───────────────┐
│          PROJECTION          │
│ ──────────────────────────── │
│          o_orderkey          │
│          o_custkey           │
│        o_orderstatus         │
│         o_totalprice         │
│         o_orderdate          │
│       o_orderpriority        │
│           o_clerk            │
│        o_shippriority        │
│          o_comment           │
│                              │
│           10 rows            │
│            0.00s             │
└──────────────┰───────────────┘
┌──────────────┸───────────────┐
│          HASH_JOIN           │
│ ──────────────────────────── │
│       Join Type: SEMI        │
│  Conditions: rowid = rowid   │
│                              ├─────────────────────┐
│           10 rows            │                     │
│            0.00s             │                     │
└──────────────┰───────────────┘                     │
           wide rows                                 │
┌──────────────┸───────────────┐┌────────────────────┴─────────────────────┐
│          TABLE_SCAN 󰎧       ││                  TOP_N                   │
│ ──────────────────────────── ││ ──────────────────────────────────────── │
│   Table: tpch.main.orders    ││                 Top: 10                  │
│    Type: Sequential Scan     ││       Order By: o.o_orderdate ASC        │
│                              ││                                          │
│         Projections:         ││                                          │
│        o_orderstatus         ││                                          │
│       o_orderpriority        ││                                          │
│          o_orderkey          ││                  0.00s                   │
│          o_custkey           ││                                          │
│         o_totalprice         ││                                          │
│         o_orderdate          ││                                          │
│           o_clerk            ││                                          │
│        o_shippriority        ││                                          │
│          o_comment           ││                                          │
│                              ││                                          │
│  rowid   12 rows             ││                 10 rows                  │
│ pushdown  0.00s              ││                 (0.00s)                  │
└──────────────────────────────┘└────────────────────┬─────────────────────┘
                                                narrow rows
                                ┌────────────────────┴─────────────────────┐
                                │                TABLE_SCAN 󰎤             │
                                │ ──────────────────────────────────────── │
                                │         Table: tpch.main.orders          │
                                │          Type: Sequential Scan           │
                                │         Projections: o_orderdate         │ narrow rows
                                │                                          │
                                │                 Filters:                 │
                                │            o_orderstatus!='F'            │
                                │           o_orderpriority<='2'           │
                                │  optional: Dynamic Filter (o_orderdate)  │
                                │                                          │
                                │               154,200 rows               │
                                │                  0.02s                   │
                                └──────────────────────────────────────────┘

*/


-- If the build side returns more than late_materialization_max_rows
-- (default: 50), save the HASH_JOIN and scattered reading of row IDs
-- in TABLE_SCAN 󰎧. Instead, simply work on wide rows throughout.

.mode duckbox
SELECT current_setting('late_materialization_max_rows');

EXPLAIN ANALYZE
SELECT o.*
FROM  orders AS o
WHERE o.o_orderstatus <> 'F' AND o.o_orderpriority <= '2'
ORDER BY o.o_orderdate
LIMIT 100;             -- too many rows (> 50), no late materialization
