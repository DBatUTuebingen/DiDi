-- If possible, DuckDB executes queries in a streaming fashion, operating
-- vector-by-vector.  This allows to process (simple) queries over
-- larger-than-memory tables with minimal memory requirements.


-- Attach to the large TPC-H instance of scale factor 100 (sf = 100),
-- table lineitem contains 600 millions rows.

ATTACH '../../databases/tpch-sf100.db' AS tpch (readonly);
USE tpch;

-- Artificially constrain the RAM available to DuckDB to less than 2MB
SET memory_limit = '2MB';
SELECT current_setting('memory_limit');

-- No spilling to disk allowed
SET max_temp_directory_size = '0MB';

-- Restrict parallel processing to further reduce memory usage
-- (each thread allocates thread-local intermediates)
SET threads = 1;

-- Do not render query results in the CLI (besides query runtime)
.mode trash
.timer on

-- Playground of streaming queries below...

-- Simple filtering+projection
SELECT l_quantity * 10
FROM   lineitem;

SELECT 'open'
FROM   lineitem
WHERE  l_linestatus = 'O';

.mode duckbox

-- The more table columns become relevant, the more vectors need to flow
-- through memory at an instant
-- (⚠️ Using * in the SELECT clause leads to OOM)
SELECT l_orderkey, l_partkey, l_suppkey
FROM   lineitem
WHERE  l_linestatus = 'O'
LIMIT  10;

-- Ungrouped aggregation
SELECT count(l_linestatus)  -- ⚠️ count(DISTINCT l_linestatus) gives OOM
FROM   lineitem;

SELECT sum(l_quantity)
FROM   lineitem;

-- Grouped aggregation producing only two groups
SELECT l_linestatus, count(*)
FROM   lineitem
GROUP BY l_linestatus;

-- Grouped aggregation producing lots of groups (20,000,000 groups)
-- (⚠️ Fails with OOM)
SELECT l_partkey, count(*)
FROM   lineitem
GROUP BY l_partkey;


-- Copying from one data source to a sink

-- Increase memory limit slightly
-- (Parquet format is complex due to, e.g., compression)
SET memory_limit = '128MB';

-- (1) Export 10 million rows from lineitem to a CSV file
COPY (FROM lineitem LIMIT 10_000_000) TO 'lineitem.csv' (header true);

-- (2) The resulting CSV file has size ~1.2 GB >> 128 MB memory_limit
.shell ls -lh lineitem.csv

-- (3) Read CSV source file, write to Parquet sink
COPY (FROM 'lineitem.csv') TO 'lineitem.parquet';

-- (4) The resulting Parquet file has size 367 MB (compression)
.shell ls -lh lineitem.parquet

-- Clean up
.shell rm lineitem.csv lineitem.parquet
