-- DuckDB implements efficient sorting routines that
-- - aim to use all available CPU cores,
-- - can sort larger-than-memory tables, and
-- - can adapt to pre-sorted data.

-- Directory spill/ will hold temporary files if sorting operations
-- require them
SET temp_directory = 'spill';

.timer on

-- Attach to a TPCH-H instance of medium size (sf = 10),
-- below we sort the rows of table lineitem
ATTACH '../../databases/tpch-sf10.db' AS tpch (readonly);
USE tpch;

DESCRIBE lineitem;

-- Table lineitem holds about 60 million rows
SELECT count(*)
FROM   lineitem;

-----------------------------------------------------------------------

-- Plan operator ORDER_BY implements sorting
--
-- The __internal_[de]compress_* routines aim to reduce the memory
-- pressure due to the wide 15-column payload.
--
EXPLAIN
FROM lineitem
ORDER BY l_shipdate DESC NULLS FIRST;

-- ┌───────────────────────────┐
-- │         PROJECTION        │
-- │    ────────────────────   │
-- │__internal_decompress_integ│
-- │     ral_bigint(#0, 1)     │
-- │__internal_decompress_integ│
-- │     ral_bigint(#1, 1)     │
-- │__internal_decompress_integ│
-- │     ral_bigint(#2, 1)     │
-- │__internal_decompress_integ│
-- │     ral_bigint(#3, 1)     │
-- │             #4            │
-- │             #5            │
-- │             #6            │
-- │             #7            │
-- │__internal_decompress_strin│
-- │           g(#8)           │
-- │__internal_decompress_strin│
-- │           g(#9)           │
-- │            #10            │
-- │            #11            │
-- │            #12            │
-- │            #13            │
-- │__internal_decompress_strin│
-- │           g(#14)          │
-- │            #15            │
-- │                           │
-- │          ~0 rows          │
-- └─────────────┬─────────────┘
-- ┌─────────────┴─────────────┐
-- │          ORDER_BY         │
-- │    ────────────────────   │
-- │    memory.main.lineitem   │
-- │      .l_shipdate DESC     │ NULLS FIRST removed (no NULL values in column l_shipdate)
-- └─────────────┬─────────────┘
-- ┌─────────────┴─────────────┐
-- │         PROJECTION        │
-- │    ────────────────────   │
-- │__internal_compress_integra│ l_orderkey :: bigint
-- │     l_uinteger(#0, 1)     │
-- │__internal_compress_integra│ l_partkey :: bigint
-- │     l_uinteger(#1, 1)     │
-- │__internal_compress_integra│ l_suppkey :: bigint
-- │     l_uinteger(#2, 1)     │
-- │__internal_compress_integra│ l_linenumber :: bigint ∊ {1...7}
-- │     l_utinyint(#3, 1)     │
-- │             #4            │ l_quantity :: decimal(15,2)
-- │             #5            │ l_extendedprice :: decimal(15,2)
-- │             #6            │ l_discount :: decimal(15,2)
-- │             #7            │ l_tax :: decimal(15,2)
-- │__internal_compress_string_│ l_returnflag :: text ∊ {N,A,R}
-- │        utinyint(#8)       │
-- │__internal_compress_string_│ l_linestatus :: text ∊ {F,O}
-- │        utinyint(#9)       │
-- │            #10            │ l_shipdate :: date
-- │            #11            │ l_commitdate :: date
-- │            #12            │ l_receiptdate :: date
-- │            #13            │ l_shipinstruct :: text (4 distinct strings, but max length > 16 bytes)
-- │__internal_compress_string_│ l_shipmode :: text ∊ {RAIL,SHIP,TRUCK,...,MAIL} (7 distinct short strings)
-- │        ubigint(#14)       │
-- │            #15            │ l_comment
-- │                           │
-- │      ~59,986,052 rows     │
-- └─────────────┬─────────────┘
-- ┌─────────────┴─────────────┐
-- │         SEQ_SCAN          │
-- │    ────────────────────   │
-- │      Table: lineitem      │
-- │   Type: Sequential Scan   │
-- │                           │
-- │        Projections:       │ -- project all columns
-- │         l_orderkey        │ -- (wide payload)
-- │         l_partkey         │
-- │         l_suppkey         │
-- │        l_linenumber       │
-- │         l_quantity        │
-- │      l_extendedprice      │
-- │         l_discount        │
-- │           l_tax           │
-- │        l_returnflag       │
-- │        l_linestatus       │
-- │         l_shipdate        │
-- │        l_commitdate       │
-- │       l_receiptdate       │
-- │       l_shipinstruct      │
-- │         l_shipmode        │
-- │         l_comment         │
-- │                           │
-- │      ~59,986,052 rows     │
-- └───────────────────────────┘


-- DuckDB CLI: do not display result rows
.mode trash

-- Order the 60,000,000 rows of lineitem, returning those items that
-- need to ship first at the top, right after those with undefined
-- shipping date
--
FROM lineitem
ORDER BY l_shipdate DESC NULLS FIRST;
-- Run Time (s): real 2.621 user 16.351566 sys 3.247508

-- The cost of row comparisons has a significant effect on sorting time
-- (wider sorting key, most significant colum is of type text)
--
FROM lineitem
ORDER BY l_comment, l_shipdate DESC NULLS FIRST;
-- Run Time (s): real 5.687 user 44.197254 sys 3.334067

-----------------------------------------------------------------------
-- Reducing the number of available CPU threads affects
-- the performance of DuckDB's parallel sorting strategy

.mode duckbox
SELECT current_setting('threads');
-- ┌────────────────────────────┐
-- │ current_setting('threads') │
-- │           int64            │
-- ├────────────────────────────┤
-- │             12             │  Torsten's Apple MacBook Pro M2 Max
-- └────────────────────────────┘


.mode trash

SET threads = 2;

FROM lineitem
ORDER BY l_shipdate DESC NULLS FIRST;
-- Run Time (s): real 7.435 user 12.215209 sys 1.999928
--                    ^^^^^

SET threads = 1;

FROM lineitem
ORDER BY l_shipdate DESC NULLS FIRST;
-- Run Time (s): real 13.712 user 11.570236 sys 2.118189
--                    ^^^^^^      ^^^^^^^^^

RESET threads;

-----------------------------------------------------------------------
-- DuckDB can sort tables that are larger than memory (memory_limit)
-- in terms of disk spilling.  Performance suffers due to I/O cost.

-- After decompression, all columns of table lineitem amount to 9.3 GB
-- (see "result_set_size" in JSON profiling output)
set enable_profiling = 'json';
EXPLAIN ANALYZE
FROM lineitem;
set enable_profiling = 'no_output';

-- 1GB will not hold the sort criterion plus the payload columns
SET memory_limit = '1GB';

FROM lineitem
ORDER BY l_shipdate DESC NULLS FIRST;
-- Run Time (s): real 7.377 user 52.236161 sys 11.987300
--                    ^^^^^

SET memory_limit = '50GB';

-----------------------------------------------------------------------
-- DuckDB can detect and benefit if table rows are pre-sorted by
-- the sorting key (below: column i)

USE memory;

-- Create table of 100 million rows with ascending column i
CREATE OR REPLACE TABLE ascending100m AS
    SELECT range AS i FROM range(100_000_000);

-- Identical table, but shuffle its rows randomly
CREATE OR REPLACE TABLE random100m AS
    SELECT range AS i FROM range(100_000_000) ORDER BY random();

.mode duckbox
FROM ascending100m
LIMIT 10;

FROM random100m
LIMIT 10;

.mode trash

FROM ascending100m
ORDER BY i;
-- Run Time (s): real 0.400 user 2.913557 sys 0.397602

FROM random100m
ORDER BY i;
-- Run Time (s): real 0.862 user 7.005605 sys 0.534683
