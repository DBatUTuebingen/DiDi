-- Propagating basic statistics from base tables into the upstream plan
-- helps to statically identify predicates that will be ALWWAYS TRUE
-- or ALWAYS FALSE.  This, in turn, can be used to further simplify plans.

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

-- In TPC-H table part, column p_size ∊ [1,50], covered in DuckDB's
-- min/max value statistics for numeric column p_size
.mode duckbox
SUMMARIZE part;
.mode trash

EXPLAIN
FROM  part AS p
WHERE p.p_size > 64;   -- 64 ∊̷ [1,50] => predicate will always be false

EXPLAIN
FROM  part AS p
WHERE p.p_size < 64;    -- predicate will always be true


-----------------------------------------------------------------------

-- Statistics propagation:
-- - At 󰎤:    p_size ∊ [1,50] (base table statistics)
-- - After 󰎤: p_size ∊ [10,30] (propagate statistics through FILTER)
-- - At 󰎧:    p_size >= 5 always true, remove; only keep p_size <= 20
EXPLAIN
SELECT p.p_partkey, p.p_size BETWEEN 5 AND 20 AS small_part  -- 󰎧
FROM   part AS p
WHERE  p.p_size BETWEEN 10 AND 30;                           -- 󰎤


-- ⚠️ Pass ordering problem: pass statistics_propagation runs *after*
-- pass expression_rewriter.  DuckDB thus misses the opportunity
-- to simplify the CASE. (Re-running expression_rewriter would fix this.)
EXPLAIN
SELECT CASE WHEN p.p_size >=  5 THEN 'branch #1'  -- 󰎧 always picked since p_size ∊ [10,30]
            WHEN p.p_size <= 20 THEN 'branch #2'
            ELSE                     'branch #3'
       END
FROM   part AS p
WHERE  p.p_size BETWEEN 10 AND 30;               -- 󰎤

-----------------------------------------------------------------------

-- Since o.o_orderstatus has no NULL values, COALESCE will never pick 'UNKNOWN'
EXPLAIN
SELECT o.o_orderkey, COALESCE(o.o_orderstatus, 'UNKNOWN') AS status
FROM   orders AS o;

-- Since o.o_orderstatus has no NULL values, IS NULL always is false
-- (Pass ordering: DuckDB misses the opportunity to not scan o_orderstatus,
-- since statistics_propagation runs late and after unused_columns)
EXPLAIN ANALYZE
SELECT o.o_orderkey, o.o_orderstatus IS NULL  -- always false
FROM   orders AS o;
