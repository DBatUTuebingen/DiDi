-- DuckDB can scan the row groups of a table in two defined orders
-- based on their (minᵢ, Maxᵢ) zonemap entries:
--
-- 1. in ascending minᵢ order
-- 2. in descending Maxᵢ order
--
-- Use this ordered scanning to read less row groups when processing
-- ORDER BY + LIMIT SQL queries (plan operator TOP_N).


-- Attach to a TPC-H instance, scale factor sf = 1
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;

-- Here we are looking at plans only
-- (optimization does not affect query results)
PRAGMA explain_output = 'optimized_only';

-- Show canonical plans
-- PRAGMA disable_optimizer;

-- Show optimized plans
PRAGMA enable_optimizer;

-----------------------------------------------------------------------
-- Zonemap for column orders(col) (recall Chapter 05)
--
CREATE OR REPLACE MACRO zonemap(tbl,col) AS TABLE
  SELECT t.row_group_id                                          AS "row group",
         sum(t.count)                                            AS "# rows",
         min(regexp_extract(t.stats, 'Min: (\d+)', 1) :: bigint) AS "min",
         max(regexp_extract(t.stats, 'Max: (\d+)', 1) :: bigint) AS "Max",
         "Max" - "min"                                           AS span
  FROM   pragma_storage_info(tbl) AS t
  WHERE  t.column_name = col AND t.segment_type <> 'VALIDITY'
  GROUP  BY t.row_group_id
  ORDER  BY t.row_group_id;

-----------------------------------------------------------------------

-- The TPC-H data generator emits the rows of table orders in ascending
-- o_orderkey order.  If the table has not been updated yet, this o_orderkey
-- order corresponds with the physical order of rows in their row groups
-- (DB lingo: table orders is *clustered*).


SELECT rowid, o.o_orderkey
FROM orders AS o
USING SAMPLE 20 ROWS
ORDER BY o.o_orderkey;

-- In consequence, the zonemap entries for table orders DO NOT OVERLAP.
-- A quick check:

FROM zonemap('orders','o_orderkey')
ORDER BY Max;

-- ┌───────────┬────────┬─────────┬─────────┬────────┐
-- │ row group │ # rows │   min   │   Max   │  span  │
-- ├───────────┼────────┼─────────┼─────────┼────────┤
-- │ 0         │ 122880 │ 1       │ 491520  │ 491519 │
-- │ 1         │ 122880 │ 491521  │ 983040  │ 491519 │
-- │ 2         │ 122880 │ 983041  │ 1474560 │ 491519 │
-- │ 3         │ 122880 │ 1474561 │ 1966080 │ 491519 │
-- │ 4         │ 122880 │ 1966081 │ 2457600 │ 491519 │
-- │ 5         │ 122880 │ 2457601 │ 2949120 │ 491519 │
-- │ 6         │ 122880 │ 2949121 │ 3440640 │ 491519 │
-- │ 7         │ 122880 │ 3440641 │ 3932160 │ 491519 │
-- │ 8         │ 122880 │ 3932161 │ 4423680 │ 491519 │
-- │ 9         │ 122880 │ 4423681 │ 4915200 │ 491519 │
-- │ 10        │ 122880 │ 4915201 │ 5406720 │ 491519 │
-- │ 11        │ 122880 │ 5406721 │ 5898240 │ 491519 │
-- │ 12        │ 25440  │ 5898241 │ 6000000 │ 101759 │
-- └───────────┴────────┴─────────┴─────────┴────────┘

-----------------------------------------------------------------------

-- Sample query Q1: Show the last 100 orders.
--
-- With pass row_group_pruner enabled: NO OVERLAP, so only read
-- row group #12 with maximum o_orderkey value. The group will
-- return R = 25,440 > 100 = N rows, stop scanning.
--
-- (Use EXPLAIN ANALYZE to see the true number of rows read.)

EXPLAIN ANALYZE
FROM orders
ORDER BY o_orderkey
DESC LIMIT 100;

/*

┌────────────────────────┴─────────────────────────┐
│                      TOP_N                       │
│ ──────────────────────────────────────────────── │
│                     Top: 100                     │
│    Order By: tpch.main.orders.o_orderkey DESC    │
│                                                  │
│                     100 rows                     │
│                      0.00s                       │
└────────────────────────┬─────────────────────────┘
┌────────────────────────┴─────────────────────────┐
│                    TABLE_SCAN                    │
│ ──────────────────────────────────────────────── │
│             Table: tpch.main.orders              │
│              Type: Sequential Scan               │
│                                                  │
│  Filters: optional: Dynamic Filter (o_orderkey)  │
│                                                  │
│                   25,440 rows                    │ only read row group #12
│                      0.00s                       │
└──────────────────────────────────────────────────┘

*/

-- Increase N to observe that more row groups are read

EXPLAIN ANALYZE
FROM orders
ORDER BY o_orderkey
DESC LIMIT 30000;   -- 30,000 > 25,440: read row groups #12 + #11

/*

┌──────────────────────────────┴──────────────────────────────┐
│                       STREAMING_LIMIT                       │
│    ──────────────────────────────────────────────────────   │
│                                                             │
│                         30,000 rows                         │
│                            0.00s                            │
└──────────────────────────────┬──────────────────────────────┘
                               ⋮
┌──────────────────────────────┴──────────────────────────────┐
│                           ORDER_BY                          │
│    ──────────────────────────────────────────────────────   │
│               tpch.main.orders.o_orderkey DESC              │
│                                                             │
│                         30,720 rows                         │ return only 15 vectors of
│                            0.01s                            │ sorted rows (15 × 2,048 = 30,720)
└──────────────────────────────┬──────────────────────────────┘
                               ⋮
┌──────────────────────────────┴──────────────────────────────┐
│                          TABLE_SCAN                         │
│    ──────────────────────────────────────────────────────   │
│                   Table: tpch.main.orders                   │
│                    Type: Sequential Scan                    │
│                                                             │
│                         148,320 rows                        │ read row groups #12 + #11
│                            0.01s                            │ (25,440 + 122,880 rows)
└─────────────────────────────────────────────────────────────┘

*/

-----------------------------------------------------------------------

-- The zonemap for column o_totalprice indicates OVERLAP of row groups
-- (table orders is clustered regarding o_orderkey, not o_totalprice):

FROM zonemap('orders', 'o_totalprice')
ORDER BY min;

-- ┌───────────┬────────┬───────┬────────┬────────┐
-- │ row group │ # rows │  min  │  Max   │  span  │
-- │   int64   │ int128 │ int64 │ int64  │ int64  │
-- ├───────────┼────────┼───────┼────────┼────────┤
-- │         4 │ 122880 │   857 │ 522720 │ 521863 │ read
-- │         3 │ 122880 │   866 │ 555285 │ 554419 │ read
-- │         1 │ 122880 │   870 │ 508668 │ 507798 │ read
-- │        10 │ 122880 │   875 │ 477117 │ 476242 │ read
-- │         7 │ 122880 │   884 │ 522644 │ 521760 │ read
-- │         8 │ 122880 │   884 │ 502906 │ 502022 │
-- │         2 │ 122880 │   896 │ 508010 │ 507114 │
-- │         6 │ 122880 │   908 │ 530604 │ 529696 │
-- │         5 │ 122880 │   912 │ 498810 │ 497898 │
-- │        11 │ 122880 │   916 │ 499753 │ 498837 │
-- │         0 │ 122880 │   917 │ 496620 │ 495703 │
-- │         9 │ 122880 │   925 │ 544089 │ 543164 │
-- │        12 │  25440 │   947 │ 494398 │ 493451 │
-- └───────────┴────────┴───────┴────────┴────────┘

-- Sample query Q2: Show the 5 cheapest orders (based on o_totalprice).
--
-- With pass row_group_pruner enabled: THERE IS OVERLAP, so read
-- N = 5 row groups in ascending order of min zonemap entries for
-- column o_totalprice.
--
-- (Use EXPLAIN ANALYZE to see the true number of rows read.)

-- Temporarily disable pass late_materialization so that we can observe
-- the effect of row_group_pruner in isolation
SET disabled_optimizers = 'late_materialization';

EXPLAIN ANALYZE
FROM orders
ORDER BY o_totalprice ASC
LIMIT 5;

/*

┌────────────────────────┴─────────────────────────┐
│                      TOP_N                       │
│ ──────────────────────────────────────────────── │
│                      Top: 5                      │
│   Order By: tpch.main.orders.o_totalprice ASC    │
│                                                  │
│                      5 rows                      │
│                      0.00s                       │
└────────────────────────┬─────────────────────────┘
┌────────────────────────┴─────────────────────────┐
│                    TABLE_SCAN                    │
│ ──────────────────────────────────────────────── │
│             Table: tpch.main.orders              │
│              Type: Sequential Scan               │
│                                                  │
│ Filters: optional: Dynamic Filter (o_totalprice) │
│                                                  │
│                   614,400 rows                   │ read N = 5 row groups only
│                      0.02s                       │ (5 × 122,880 rows)
└──────────────────────────────────────────────────┘

*/

RESET disabled_optimizers;

-----------------------------------------------------------------------

-- Temporarily disable pass row_group_pruner to observe that
-- ALMOST ALL rows of table orders have to be scanned.

SET disabled_optimizers = 'late_materialization, row_group_pruner';

-- Rerun Q2 with row_group_pruner disabled

EXPLAIN ANALYZE
FROM orders
ORDER BY o_totalprice ASC
LIMIT 5;

/*

┌─────────────────────────┴─────────────────────────┐
│                       TOP_N                       │
│ ───────────────────────────────────────────────── │
│                       Top: 5                      │
│    Order By: tpch.main.orders.o_totalprice ASC    │
│                                                   │
│                       5 rows                      │
│                       0.00s                       │
└─────────────────────────┬─────────────────────────┘
┌─────────────────────────┴─────────────────────────┐
│                     TABLE_SCAN                    │
│ ───────────────────────────────────────────────── │
│              Table: tpch.main.orders              │
│               Type: Sequential Scan               │
│                                                   │
│  Filters: optional: Dynamic Filter (o_totalprice) │ see below (***)
│                                                   │
│                   1,474,560 rows                  │ scan almost all rows
│                       0.06s                       │ of table orders
└───────────────────────────────────────────────────┘
*/

-----------------------------------------------------------------------
-- (***)

-- Dynamic Filter optimization:
--
--   DURING EXECUTION of operator TOP_N, update the Dynamic Filter
--   placed in the upstream TABLE_SCAN (this query: ORDER BY c ASC LIMIT N):
--
--   If we have found N rows already among which the maximum c value
--   is V, set Dynamic Filter to: c < V.


RESET disabled_optimizers;
