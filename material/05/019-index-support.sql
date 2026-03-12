-- DuckDB implements basic support for ART indexes but will the indexes
-- only to evaluate simple (non-composite) predicates.
--
-- 1. DuckDB uses ART to evaluate selective predicates p of the form
--    c = val (index lookup) or c > val (index traversal with leaf scan).
--
-- 2. DuckDB does not use ARTs to evaluate conjunctions (disjunctions)
--    p AND q (p OR q) even if p and/or q could be evaluated using ARTs.
--
-- 3. DuckDB does not use composite indexes for predicate evaluation
--    (but uses such indexes for checking multi-column PRIMARY KEY
--    or UNIQUE constraints).
--
-- 4. DuckDB does not use ARTs to evaluate LIKE-style predicates
--    (internally these are rewritten into conjunctive predicates, see 2.)
--
-- 5. DuckDB does not use indexes to evaluate ORDER BY.
--
-- 6. DuckDB does not use indexes to evaluate min/max aggregates.


-- To experiment with ART indexes, attach to a sizable TPC-H
-- instance (sf = 100).
ATTACH '../../databases/tpch-sf100.db' AS tpch;
USE tpch;

DESCRIBE orders;

SELECT o_clerk, o_orderkey
FROM   orders
LIMIT  10;

SELECT count(*)
FROM   orders;

.timer on

-- Create three indexes
--
CREATE INDEX orders_clerk_idx    ON orders(o_clerk);             -- key length: 15 bytes
CREATE INDEX orders_orderkey_idx ON orders(o_orderkey);          -- key length: 8 bytes
CREATE INDEX composite_idx       ON orders(o_clerk, o_orderkey); -- key length: 15+8 bytes

-- DuckDB only considers Index Scan if the number of expected rows
-- is limited:
--
SELECT greatest(current_setting('index_scan_max_count'),
                    current_setting('index_scan_percentage')
                    * (SELECT count(*) FROM orders)) AS "max # of expected rows";
-- ┌────────────────────────┐
-- │ max # of expected rows │
-- ├────────────────────────┤
-- │        150000.0        │
-- └────────────────────────┘

-- 1. DuckDB uses Index Scan for selective simple predicates
--
EXPLAIN ANALYZE
SELECT count(*)
FROM   orders AS o
WHERE  o.o_clerk = 'Clerk#000051887';

EXPLAIN ANALYZE
SELECT count(*)
FROM   orders AS o
WHERE  o.o_orderkey < 600001;  -- the cutoff is between 600001|600002


-- 2.+3. Conjunctive/disjunctive predicates are "not simple" and never
--       considered for an Index Scan (but the predicate is pushed down
--       into the Sequential Scan where zonemaps are applied)
--
EXPLAIN ANALYZE
SELECT count(*)
FROM   orders AS o
WHERE  o.o_clerk = 'Clerk#000051887' AND o.o_orderkey < 6000001;
--                                   ^^^
--                               conjunction

-- Index-based plan alternatives:
--
-- A. Use index orders(o_clerk), then FILTER on o_orderkey < 6000001
-- B. Use index orders(o_orderkey), then FILTER on o_clerk = 'Clerk#000051887'
-- C. Use composite index orders(o_clerk, o_orderkey) with a lookup
--    on the concatenated key 'Clerk#000051887|6000001', then scan
--    leaves leftwards
-- D. Use indexes orders(o_clerk) and orders(o_orderkey), obtain two
--    sets of rowids, perform rowid set INTERSECTION (PostgreSQL implements
--    this based by bit-wise AND on bitmaps that represent the rowid sets)


-- Disjunctions are trickier to evaluate (zonemaps for two columns have
-- to be considered to identify the irrelevant row groups).  DuckDB
-- does not push the disjunction into Sequential Scan but uses a
-- post-FILTER step
--
EXPLAIN ANALYZE
SELECT count(*)
FROM   orders AS o
WHERE  o.o_clerk = 'Clerk#000051887' OR o.o_orderkey < 6000001;
--                                   ^^
--                               disjunction

-- Index-based plan alternatives:
--
-- A., B., C. (see above) will not work
-- D. Use indexes orders(o_clerk) and orders(o_orderkey), obtain two
--    sets of rowids, then perform rowid set UNION


-- 4. LIKE-style patterns (%: match any [empty] substring, _: match any
--    character) are translated into conjunctive predicates behind the
--    scenes.  In consequence, Index Scan is not applied :-/

-- DuckDB rewrites the pattern match into (pushed down into Sequential Scan)
--
--   o_clerk>='Clerk#00005188' AND o_clerk<'Clerk#00005189'
--
EXPLAIN ANALYZE
SELECT count(*)
FROM   orders AS o
WHERE  o.o_clerk LIKE 'Clerk#00005188%';

-- DuckDB rewrites this pattern match into (~~ is a synonym for LIKE)
--
--   o_clerk>='Clerk#00005' AND o_clerk<'Clerk#00006' [pushed down]
--                       AND
--            o_clerk ~~ 'Clerk#00005_888'            [post FILTER]
--
-- The pushed down predicates reduces row volume from 150,000,000
-- to 15,000,000 rows to save 90% work that would hit ~~ otherwise.
--
EXPLAIN ANALYZE
SELECT count(*)
FROM   orders AS o
WHERE  o.o_clerk LIKE 'Clerk#00005_888';


-- 5. DuckDB does not exploit that ARTs are ordered tree structures and
--    can scan the leaves in forward/backward fashion

EXPLAIN ANALYZE
SELECT o.o_orderkey       -- narrow SELECT clause, scanning the leaves
FROM   orders AS o        -- would be all that is needed (this is
ORDER BY o.o_orderkey;    -- a so-called "index-only query")


-- 6. Traversing the very left (right) branch of an ART leads to the
--    minimum (maximum) value.  DuckDB does not exploit that.

EXPLAIN ANALYZE
SELECT min(o.o_clerk)
FROM   orders AS o;


-- Clean up (remove indexes)
DROP INDEX orders_clerk_idx;
DROP INDEX orders_orderkey_idx;
DROP INDEX composite_idx;
