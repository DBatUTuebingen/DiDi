-- - Zonemaps are created automatically for all columns of
--   non-aggregate data types (no zonemaps for list/array/map/struct columns)
-- - In this file: no table constraints, no CREATE INDEX, thus no ART index.

CREATE OR REPLACE TABLE t (
  c1 bigint,
  c2 bigint
);

INSERT INTO t(c1,c2)
  SELECT i & ~3 AS c1, (i * 9_876_983_769_044 :: hugeint % 100_000_000) :: bigint AS c2
  FROM   range(100_000_000) AS _(i);

-- Peek into t
-- - virtual column rowid reflects the physical/storage order of rows
-- - sort order in column c1 coincides with table storage order (c2 odes not)
SELECT rowid, t.*
FROM t
LIMIT 20;

.timer on

-- The evaluation of the predicate c1/2 = 500_000 is "pushed down"
-- into the Sequential Scan over t: during scanning, we try to
-- skip row groups that are guaranteed to contribute no rows whatsoever
--
-- Both queries return 4 rows.  Timing differs (by factor about 10).
EXPLAIN ANALYZE
SELECT t.c1, t.c2
FROM   t
WHERE  t.c1 = 500_000;
-- Run Time (s): real 0.001 user 0.001576 sys 0.004683
--                    ^^^^^

EXPLAIN ANALYZE
SELECT t.c2, t.c1
FROM   t
WHERE  t.c2 = 500_000;
-- Run Time (s): real 0.010 user 0.088613 sys 0.001817
--                    ^^^^^


-- Now use DuckDB's builtin function pragma_storage_info(‹table name›)
-- to explore the row groups of table t.

-- What does pragma_storage_info() provide?
-- See https://duckdb.org/docs/stable/configuration/pragmas
--
SELECT row_group_id, column_name, segment_id, start, count, compression, stats, has_updates, persistent
FROM   pragma_storage_info('t')
WHERE  segment_type = 'BIGINT'
LIMIT 20;


-- Table storage is indeed divided by column (vertical) and into row groups
-- of 120K rows each (horizontal):
--
SELECT t.column_name, t.row_group_id, sum(t.count) AS "# rows"
FROM   pragma_storage_info('t') AS t
WHERE  segment_type = 'BIGINT'
GROUP BY t.column_name, t.row_group_id
ORDER BY row_group_id, column_name;


-- Zonemap for column col
--
CREATE OR REPLACE MACRO zonemap(col) AS TABLE
  SELECT t.row_group_id                                          AS "row group",
         min(t.start)                                            AS "first rowid",
         sum(t.count)                                            AS "# rows",
         min(regexp_extract(t.stats, 'Min: (\d+)', 1) :: bigint) AS "min",
         max(regexp_extract(t.stats, 'Max: (\d+)', 1) :: bigint) AS "Max",
         "Max" - "min"                                           AS span
  FROM   pragma_storage_info('t') AS t
  WHERE  t.column_name = col AND t.segment_type = 'BIGINT'
  GROUP  BY t.row_group_id
  ORDER  BY t.row_group_id;


-- # of row groups for t
-- (each row group can hold 120 * 1024 = 122880 rows)
SELECT count(*) AS "# row groups"
FROM   zonemap('c1');
-- ┌──────────────┐
-- │ # row groups │
-- │    int64     │
-- ├──────────────┤
-- │     814      │
-- └──────────────┘


  -- zonemap for column c1
FROM zonemap('c1')
WHERE "row group" <= 10;
-- ┌───────────┬─────────────┬────────┬─────────┬─────────┬────────┐
-- │ row group │ first rowid │ # rows │   min   │   Max   │  span  │
-- ├───────────┼─────────────┼────────┼─────────┼─────────┼────────┤
-- │ 0         │ 0           │ 122880 │ 0       │ 122876  │ 122876 │
-- │ 1         │ 122880      │ 122880 │ 122880  │ 245756  │ 122876 │
-- │ 2         │ 245760      │ 122880 │ 245760  │ 368636  │ 122876 │
-- │ 3         │ 368640      │ 122880 │ 368640  │ 491516  │ 122876 │
-- │ 4         │ 491520      │ 122880 │ 491520  │ 614396  │ 122876 │
-- │ 5         │ 614400      │ 122880 │ 614400  │ 737276  │ 122876 │
-- │ 6         │ 737280      │ 122880 │ 737280  │ 860156  │ 122876 │
-- │ 7         │ 860160      │ 122880 │ 860160  │ 983036  │ 122876 │
-- │ 8         │ 983040      │ 122880 │ 983040  │ 1105916 │ 122876 │
-- │ 9         │ 1105920     │ 122880 │ 1105920 │ 1228796 │ 122876 │
-- │ 10        │ 1228800     │ 122880 │ 1228800 │ 1351676 │ 122876 │
-- └───────────┴─────────────┴────────┴─────────┴─────────┴────────┘
--                                |        |         |         |
--                         row groups    ordering in column c1 leads
--                   are fully packed    to narrow (min,Max) intervals
--                                       that inform the Sequential Scan
--                                       and allow row group skipping

-- zonemap for column c2
FROM zonemap('c2')
WHERE "row group" <= 10;
-- ┌───────────┬─────────────┬────────┬──────┬──────────┬──────────┐
-- │ row group │ first rowid │ # rows │ min  │   Max    │   span   │
-- ├───────────┼─────────────┼────────┼──────┼──────────┼──────────┤
-- │ 0         │ 0           │ 122880 │ 0    │ 99999524 │ 99999524 │
-- │ 1         │ 122880      │ 122880 │ 380  │ 99999904 │ 99999524 │
-- │ 2         │ 245760      │ 122880 │ 1236 │ 99998952 │ 99997716 │
-- │ 3         │ 368640      │ 122880 │ 284  │ 99999808 │ 99999524 │
-- │ 4         │ 491520      │ 122880 │ 664  │ 99998380 │ 99997716 │
-- │ 5         │ 614400      │ 122880 │ 188  │ 99999712 │ 99999524 │
-- │ 6         │ 737280      │ 122880 │ 92   │ 99997808 │ 99997716 │
-- │ 7         │ 860160      │ 122880 │ 1424 │ 99999616 │ 99998192 │
-- │ 8         │ 983040      │ 122880 │ 472  │ 99999996 │ 99999524 │
-- │ 9         │ 1105920     │ 122880 │ 852  │ 99999044 │ 99998192 │
-- │ 10        │ 1228800     │ 122880 │ 376  │ 99999900 │ 99999524 │
-- └───────────┴─────────────┴────────┴──────┴──────────┴──────────┘
--                                |        |         |         |
--                         row groups    unordered column c2 leads to
--                   are fully packed    wiiiide (min,Max) intervals that
--                                       each almost span the entire
--                                       active  domain of column c2
--                                       => each row group may potentially
--                                          contain search value 500_000 :-/
--                                          (no skipping)

-- # of skippable row groups for predicate c1 = 500_000
SELECT count(*) AS "may be skipped"
FROM zonemap('c1')
WHERE 500_000 NOT BETWEEN "min" AND "Max";  -- ≡ 500_000 < "min" OR 500_000 > "Max"
-- ┌────────────────┐
-- │ may be skipped │
-- ├────────────────┤
-- │ 813            │ all but one row group
-- └────────────────┘

-- # of skippable row groups for predicate c2 = 500_000
SELECT count(*) AS "may be skipped"
FROM zonemap('c2')
WHERE 500_000 NOT BETWEEN "min" AND "Max";
-- ┌────────────────┐
-- │ may be skipped │
-- ├────────────────┤
-- │ 0              │ ugh... :-/
-- └────────────────┘


------------------------------------------------------------------------
-- Zonemaps and table updates
--
-- When a table column (say c1: UPDATE t SET c1 = ...) is updated,
-- either...
-- (A) eagerly update the zonemap associated with column c1 as well
--     (costly on update)
-- (B) ignore the existing (now outdated zonemap)
--     (costly on scans: no skipping possible)
-- (C) create new zonemap entries for the affected/updated rowgroups
--     and switch between existing and new zonemap entries as needed
--     (implemented DuckDB, see below)


-- Perform a HEAVY update: negate all entries in column c1
UPDATE t SET c1 = -c1;

SELECT t.*
FROM t
LIMIT 20;

-- The evaluation of the predicate c1 = -500_000 still is "pushed down"
-- into the Sequential Scan over t.  Query runtime suggests that
-- zonemaps are still used, but performance appears to suffer slightly:
SELECT t.c1, t.c2
FROM   t
WHERE  t.c1 = -500_000;
-- Run Time (s): real 0.004 user 0.026735 sys 0.001540
--                    ^^^^^
--                    (before UPDATE: 0.001, see above)


-- In DuckDB's table storage, the row groups for column c1 are marked
-- as being updated (see column "has_updates").  The min/max stats
-- are still unchanged, however:
--
SELECT row_group_id, column_name, segment_id, stats, has_updates, persistent
FROM   pragma_storage_info('t')
WHERE  segment_type = 'BIGINT'
LIMIT 20;

-- During a Sequential Scan over t, the "has_updates" flag is checked:
-- - if false, perform row group skipping based on existing zonemap entry
--   as before,
-- - if true, use the new zonemap entry for the group created when the
--   UPDATE was performed (option (C) above).
--
-- These new zonemap entries are kept in extra update statistics storage.
-- Accessing this extra storage (see TRANSACTION below) to the performance
-- loss we saw above:

FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │                  0 │                       0 │
-- │ HASH_TABLE          │                  0 │                       0 │
-- │ PARQUET_READER      │                  0 │                       0 │
-- │ CSV_READER          │                  0 │                       0 │
-- │ ORDER_BY            │                  0 │                       0 │
-- │ ART_INDEX           │                  0 │                       0 │
-- │ COLUMN_DATA         │                  0 │                       0 │
-- │ METADATA            │                  0 │                       0 │
-- │ OVERFLOW_STRINGS    │                  0 │                       0 │
-- │ IN_MEMORY_TABLE     │         2133889024 │                       0 │
-- │ ALLOCATOR           │                  0 │                       0 │
-- │ EXTENSION           │                  0 │                       0 │
-- │ TRANSACTION         │         1280049152 │                       0 │ <-- update statistics are stored along with
-- │ EXTERNAL_FILE_CACHE │                  0 │                       0 │     a log of database changes
-- ├─────────────────────┴────────────────────┴─────────────────────────┤
-- │ 14 rows                                                  3 columns │
-- └────────────────────────────────────────────────────────────────────┘


-- DuckDB can merge the updates into the base table, making the extra
-- update statistic obsolete.  At that point, the table's zonemaps are
-- updated.

-- Copying table t to a different database merges the updates into the
-- table data and updates the zonemap:
ATTACH 'scratch.db' AS scratch;
COPY FROM DATABASE memory TO scratch;

USE scratch;

-- In the persistent scratch database, the updates have been merged into
-- table t.  The zonemap of c1 has been updated:
SELECT row_group_id, column_name, segment_id, stats, has_updates, persistent
FROM   pragma_storage_info('t')
WHERE  segment_type = 'BIGINT'
LIMIT 20;


-- The query now is as fast as before:
SELECT t.c1, t.c2
FROM   t
WHERE  t.c1 = -500_000;
-- Run Time (s): real 0.001 user 0.001299 sys 0.002816
--                    ^^^^^

-- The update statistics were only relevant in the in-memory database.
-- If we detach it, these statistics will be deleted:
DETACH memory;

FROM duckdb_memory();
-- ┌─────────────────────┬────────────────────┬─────────────────────────┐
-- │         tag         │ memory_usage_bytes │ temporary_storage_bytes │
-- │       varchar       │       int64        │          int64          │
-- ├─────────────────────┼────────────────────┼─────────────────────────┤
-- │ BASE_TABLE          │          427294720 │                       0 │
-- │ HASH_TABLE          │                  0 │                       0 │
-- │ PARQUET_READER      │                  0 │                       0 │
-- │ CSV_READER          │                  0 │                       0 │
-- │ ORDER_BY            │                  0 │                       0 │
-- │ ART_INDEX           │                  0 │                       0 │
-- │ COLUMN_DATA         │                  0 │                       0 │
-- │ METADATA            │                  0 │                       0 │
-- │ OVERFLOW_STRINGS    │                  0 │                       0 │
-- │ IN_MEMORY_TABLE     │                  0 │                       0 │
-- │ ALLOCATOR           │                  0 │                       0 │
-- │ EXTENSION           │                  0 │                       0 │
-- │ TRANSACTION         │                  0 │                       0 │ <-- update statistics removed
-- │ EXTERNAL_FILE_CACHE │                  0 │                       0 │
-- ├─────────────────────┴────────────────────┴─────────────────────────┤
-- │ 14 rows                                                  3 columns │
-- └────────────────────────────────────────────────────────────────────┘


-- Clean up
.shell rm scratch.db
