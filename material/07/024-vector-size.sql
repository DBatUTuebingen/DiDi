-- Run this SQL file on DuckDB variants compiled with different
-- settings for STANDARD_VECTOR_SIZE to observe the effect of
-- vector size on DuckDB query performance.
--
-- The SQL code below aims to mimic the computation performed by
-- C program 023-intermediates.c.

-- paging in CLI available starting with DuckDB 1.5
.pager off
.timer on

CREATE OR REPLACE TABLE lineitem (
  l_shipdate      int,
  l_returnflag    text,
  l_discount      double,
  l_extendedprice double
);

-- (1) Create a "fake" TPC-H lineitem table
INSERT INTO lineitem
  SELECT i                                     AS l_shipdate,
         ['A','N','R','N'][1 + i % 4]          AS l_returnflag,
         random() * 0.12                       AS l_discount,
         900.0 + random() * (100000.0 - 900.0) AS l_extendedprice
  FROM   generate_series(1, 10_000_000) AS _(i);

-- (2) Evaluate a simplified variant of TPC-H Query Q1
SELECT l_returnflag, sum(l_extendedprice * (1.0 - l_discount)) AS sum_disc_price
FROM   lineitem
WHERE  l_shipdate < 9_800_000 -- |lineitem| * 0.98
GROUP BY l_returnflag;
