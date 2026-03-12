-- Generate and persist TPC-H instances that DiDi
-- will use repeatedly throughout the course. Files are
-- placed in the current working directory:
--
-- - Three TPC-H instances of scale factors 1, 10, 100, saved
--   as DuckDB database files tpch-sf{1,10,100}.db
--
-- - A |-delimited CSV file lineitem.csv for TPC-H table
--   lineitem (scale factor 1).
--
-- Alternatively, load the required TPCH-H instances from duckdb.org
-- at the following URLs:
--
-- - https://blobs.duckdb.org/data/tpch-sf1.db
-- - https://blobs.duckdb.org/data/tpch-sf10.db
-- - https://blobs.duckdb.org/data/tpch-sf100.db

.bail on

-----------------------------------------------------------------------
-- Generate TPC-H instances (sf = 1, 10, 100)
INSTALL tpch;
LOAD tpch;

.print Generating TPC-H instance (scale factor 1)
.shell rm -f tpch-sf1.db
ATTACH OR REPLACE 'tpch-sf1.db' AS tpch;
USE tpch;
CALL dbgen(sf = 1);
.tables

.print Generating TPC-H instance (scale factor 10)
.shell rm -f tpch-sf10.db
ATTACH OR REPLACE 'tpch-sf10.db' AS tpch;
CALL dbgen(sf = 10);

.print Generating TPC-H instance (scale factor 100)
.shell rm -f tpch-sf100.db
ATTACH OR REPLACE 'tpch-sf100.db' AS tpch;
CALL dbgen(sf = 100, children = 10, step = 0);
CALL dbgen(sf = 100, children = 10, step = 1);
CALL dbgen(sf = 100, children = 10, step = 2);
CALL dbgen(sf = 100, children = 10, step = 3);
CALL dbgen(sf = 100, children = 10, step = 4);
CALL dbgen(sf = 100, children = 10, step = 5);
CALL dbgen(sf = 100, children = 10, step = 6);
CALL dbgen(sf = 100, children = 10, step = 7);
CALL dbgen(sf = 100, children = 10, step = 8);
CALL dbgen(sf = 100, children = 10, step = 9);

-----------------------------------------------------------------------
-- Generate lineitem.csv from instance TPC-H (sf = 1) instance

.print Generating lineitem CSV file (TPC-H scale factor 1)
.shell rm -f lineitem.csv
ATTACH OR REPLACE 'tpch-sf1.db' AS tpch;
COPY (SELECT * REPLACE (l_quantity :: bigint AS l_quantity)
      FROM tpch.lineitem)
TO 'lineitem.csv' (delimiter '|', header false);

USE memory;
DETACH tpch;

.print Done
.quit
