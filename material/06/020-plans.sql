-- DuckDB translates the text of any SQL query Q into tree-shaped
-- (or DAG-shaped) execution plans.  These plans describe how rows
-- flow from node  to node (the operators) along the upwards-pointing
-- edges.  Rows flowing out of the top-most root node represent the
-- result of Q.

-- Use TPC-H instance of some arbitrary scale factor (e.g., sf = 1)
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;


-- If Q is complex, the plans can become intricate and/or large.  DuckDB
-- comes with a variety of tuning knobs that control the level of detail
-- at which plans are rendered and nodes are annotated.

-- Enable and configure DuckDB's profiling facility to dump a
-- compact variant of the query plan
PRAGMA enable_profiling = 'query_tree';
PRAGMA custom_profiling_settings = '{ "OPERATOR_TYPE": "true", "EXTRA_INFO": "false" }';
PRAGMA explain_output = 'physical_only';

-- We are interested in the query plan only, ignore the query result
.mode trash

-- TPC-H Query Q2 (an example of a rather complex query Q)
--
SELECT s.s_acctbal, s.s_name, n.n_name, p.p_partkey, p.p_mfgr,
       s.s_address, s.s_phone, s.s_comment
FROM   part AS p, supplier AS s, partsupp AS ps, nation AS n, region AS r
WHERE  p.p_partkey = ps.ps_partkey
       AND s.s_suppkey = ps.ps_suppkey
       AND p.p_size = 15
       AND p.p_type LIKE '%BRASS'
       AND s.s_nationkey = n.n_nationkey
       AND n.n_regionkey = r.r_regionkey
       AND r.r_name = 'EUROPE'
       AND ps.ps_supplycost = (
            SELECT min(ps.ps_supplycost)
            FROM  partsupp AS ps, supplier AS s, nation AS n, region AS r
            WHERE p.p_partkey = ps.ps_partkey
                  AND s.s_suppkey = ps.ps_suppkey
                  AND s.s_nationkey = n.n_nationkey
                  AND n.n_regionkey = r.r_regionkey
                  AND r.r_name = 'EUROPE')
ORDER BY s.s_acctbal DESC, n.n_name, s.s_name, p.p_partkey
LIMIT 100;

/*
┌─────────────────┐
│      QUERY      │
└────────┬────────┘
┌────────┴────────┐
│      TOP_N      │
└────────┬────────┘
┌────────┴────────┐
│    PROJECTION   │
└────────┬────────┘
┌────────┴────────┐
│    PROJECTION   │
└────────┬────────┘
┌────────┴────────┐
│      FILTER     │
└────────┬────────┘
┌────────┴────────┐
│ LEFT_DELIM_JOIN ├─────────────────────────────────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
└────────┬────────┘                                                                                     │                                                                                                                 │
┌────────┴────────┐                                                                            ┌────────┴────────┐                                                                                               ┌────────┴────────┐
│    HASH_JOIN    ├────────────────────────────┐                                               │    HASH_JOIN    ├─────────┐                                                                                     │  HASH_GROUP_BY  │
└────────┬────────┘                            │                                               └────────┬────────┘         │                                                                                     └─────────────────┘
┌────────┴────────┐                   ┌────────┴────────┐                                      ┌────────┴────────┐┌────────┴────────┐
│    HASH_JOIN    ├─────────┐         │    HASH_JOIN    ├─────────┐                            │ COLUMN_DATA_SCAN││    PROJECTION   │
└────────┬────────┘         │         └────────┬────────┘         │                            └─────────────────┘└────────┬────────┘
┌────────┴────────┐┌────────┴────────┐┌────────┴────────┐┌────────┴────────┐                                      ┌────────┴────────┐
│    TABLE_SCAN   ││    TABLE_SCAN   ││    TABLE_SCAN   ││    HASH_JOIN    ├─────────┐                            │    PROJECTION   │
└─────────────────┘└─────────────────┘└─────────────────┘└────────┬────────┘         │                            └────────┬────────┘
                                                         ┌────────┴────────┐┌────────┴────────┐                   ┌────────┴────────┐
                                                         │    TABLE_SCAN   ││    TABLE_SCAN   │                   │  HASH_GROUP_BY  │
                                                         └─────────────────┘└─────────────────┘                   └────────┬────────┘
                                                                                                                  ┌────────┴────────┐
                                                                                                                  │    PROJECTION   │
                                                                                                                  └────────┬────────┘
                                                                                                                  ┌────────┴────────┐
                                                                                                                  │    PROJECTION   │
                                                                                                                  └────────┬────────┘
                                                                                                                  ┌────────┴────────┐
                                                                                                                  │    HASH_JOIN    ├────────────────────────────┐
                                                                                                                  └────────┬────────┘                            │
                                                                                                                  ┌────────┴────────┐                   ┌────────┴────────┐
                                                                                                                  │    HASH_JOIN    ├─────────┐         │    HASH_JOIN    ├─────────┐
                                                                                                                  └────────┬────────┘         │         └────────┬────────┘         │
                                                                                                                  ┌────────┴────────┐┌────────┴────────┐┌────────┴────────┐┌────────┴────────┐
                                                                                                                  │    TABLE_SCAN   ││    DELIM_SCAN   ││    TABLE_SCAN   ││    HASH_JOIN    ├─────────┐
                                                                                                                  └─────────────────┘└─────────────────┘└─────────────────┘└────────┬────────┘         │
                                                                                                                                                                           ┌────────┴────────┐┌────────┴────────┐
                                                                                                                                                                           │    TABLE_SCAN   ││    TABLE_SCAN   │
                                                                                                                                                                           └─────────────────┘└─────────────────┘

*/
