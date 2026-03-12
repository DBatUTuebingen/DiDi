-- DuckDB's optimizer proceeds in a pre-determined order of passes
-- to guarantee deterministic optimization times.


-- Use TPC-H instance of scale factor sf = 1 (arbitrary)
ATTACH '../../databases/tpch-sf1.db' AS tpch;
USE tpch;

-- We are interested in the query plan and
-- optimizer timings only, ignore the query result
.timer on
.mode trash

-- Make sure the optimizer is enabled
PRAGMA enable_optimizer;

-- Start DuckDB's query profiler,
-- record individual optimization pass durations and overall optimizer time
PRAGMA enable_profiling = 'json';
PRAGMA custom_profiling_settings = '{ "ALL_OPTIMIZERS": true, "CUMULATIVE_OPTIMIZER_TIMING": true }';

-- Optimize any TPC-H query
PRAGMA tpch(10);

/*

{
    "all_optimizers": 0.001575957,                   -- optimizer runtime: ≈1.5 ms
    "cumulative_optimizer_timing": 0.0007178320000000001,
    "optimizer_window_self_join": 0.000001,          -- pass duration: 1 µs (not applicable)
    "optimizer_join_elimination": 0.000015459,       -- pass duration: 15 µs
    "optimizer_common_subplan": 0.000067208,         -- pass duration: 67 µs
    "optimizer_cte_inlining": 0.000002125,
    "optimizer_late_materialization": 0.000004833,
    "optimizer_sum_rewriter": 0.000009667,
    "optimizer_materialized_cte": 0.0,
    "optimizer_extension": 0.0,
    "optimizer_join_filter_pushdown": 0.000019166,
    "optimizer_sampling_pushdown": 6.67e-7,
    "optimizer_reorder_filter": 0.000004875,
    "optimizer_duplicate_groups": 0.000009708,
    "optimizer_compressed_materialization": 0.0,
    "optimizer_top_n_window_elimination": 0.000003,
    "optimizer_top_n": 0.000002958,
    "optimizer_row_group_pruner": 0.000001083,
    "optimizer_limit_pushdown": 0.000001,
    "optimizer_build_side_probe_side": 0.000015208,
    "optimizer_column_lifetime": 0.000031458,
    "optimizer_common_aggregate": 0.000006584,
    "optimizer_common_subexpressions": 0.000008875,
    "optimizer_statistics_propagation": 0.000105708,
    "optimizer_unused_columns": 0.000026583,
    "optimizer_unnest_rewriter": 0.00000125,
    "optimizer_deliminator": 0.000001292,
    "optimizer_join_order": 0.000138875,
    "optimizer_in_clause": 0.000003833,
    "optimizer_regex_range": 0.000003,
    "optimizer_cte_filter_pusher": 0.000003083,
    "optimizer_empty_result_pullup": 0.00000175,
    "optimizer_filter_pushdown": 0.0001265,
    "optimizer_filter_pullup": 0.000005,
    "optimizer_expression_rewriter": 0.000096084,
    "children": [
        {
            "operator_name": "TOP_N",
            ...

*/
