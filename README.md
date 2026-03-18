
# **Di**ssecting the **D**uck's **I**nnards (*DiDi*)

A DuckDB-based course on the Design and Implementation of Database
System Internals.

## Welcome!

This lecture material has been developed by [Torsten Grust](https://db.cs.uni-tuebingen.de/grust/)
to support a 15-week course (coined *DiDi*) for undergraduate students of
the [Database Research Group](https://db.cs.uni-tuebingen.de) at
University of Tübingen (Germany).  You are welcome to use this
material in any way you may see fit: skim it, study it, send suggestions
or corrections, or tear it apart to build your own lecture material
based on it.  I would be delighted to hear from you in any case:

- E-Mail: [torsten.grust@uni-tuebingen.de](mailto:torsten.grust@uni-tuebingen.de)
- Web: https://db.cs.uni-tuebingen.de/grust/
- Bluesky: https://bsky.app/profile/teggy.org

## A Tour Through DuckDB's Internals

The course treads on a path through selected internals of the
[DuckDB](https://duckdb.org/) relational database system.  15 weeks
do not suffice to exhaustively discuss all interesting bits and pieces of the
DuckDB kernel.  I still hope that I managed to paint a characteristic
picture of what makes DuckDB a capable and very efficient
SQL database system that can
[crunch billions of rows on commodity laptops](https://blobs.duckdb.org/merch/duckdb-2024-big-data-on-your-laptop-poster.pdf).
A future *DiDi* may see chapters added, merged, or removed but as of
March 2026, the chapter layout reads as follows:

1. Welcome & Setup
2. The Query Performance Spectrum
3. Managing Memory + Grouped Aggregation
4. Sorting Large Tables
5. The ART of Indexing
6. Query Execution Plans and Pipelining
7. Vectorized Query Execution
8. Query Rewriting and Optimization

Here at U Tübingen, I walk students through these chapters front to
back but I am positive that chapters 4–8 could be read in any order.

You will need basic SQL skills to follow DiDi's red thread and
auxiliary material.  There are few queries that go beyond the core
`SELECT`-`FROM`-`WHERE`-`GROUP BY`-`HAVING` block, however.  Should
you require an introduction to the tabular data model and its
query language SQL, you may find the companion course
[*TaDa*](https://github.com/DBatUTuebingen/TaDa) helpful.  *TaDa*, too,
revolves around DuckDB.

## *DiDi* = Slides + Auxiliary Material

Chapter ‹N› of *DiDi* comes with a slide set in file `slides/DiDi-‹N›.pdf`
(see the hierachy of relevant files below).  Note that these slide sets
literally only tell half of the story.

The other half is found in about 50
auxiliary files—mostly SQL scripts, but also code written in C,
Python, and awk—collected in directory `material/‹N›/` for Chapter ‹N›.
The slides contain tags `📄#‹nnn›` whenever a file
named `‹nnn›-*` contains relevant supporting material. Beyond code, these
files contains plenty of commentary—**you absolutely *need* to study (and ideally run,
modify, play with) these files in `material/` to obtain the intended and complete
*DiDi* picture.**

To run these files, change into the `material/‹N›/` directory and invoke
DuckDB, your Python/awk interpreter, or C compiler there:

~~~
$ cd material/02
$ ./002-sum-quantity.py ../../databases/lineitem.csv
$ duckdb -f 008-sum-quantity.sql
$ duckdb
D .read 008-sum-quantity.sql
~~~

I have found that students make best use of the SQL scripts when
they cut & paste individual SQL commands and queries from the `*.sql`
files right into a [DuckDB CLI](https://duckdb.org/docs/current/clients/cli/overview) session.

### Generating Sample Database Instances

Most of the SQL scripts operate over instances of the [TPC-H benchmark](https://www.tpc.org/tpch/)
and assume that these databases can be accessed in directory
`databases/`.  You can generate the required DuckDB databases (and
an accompanying CSV file) using the `generate-databases.sql` script:

~~~
$ cd databases
$ duckdb -f generate-databases.sql
~~~

NB. This will place three TPC-H instances for scale factors 1, 10, 100
in DuckDB database files `tpch-sf{1,10,100}.db`, respectively.  We use
DuckDB's own [`tpch` extension](https://duckdb.org/docs/current/core_extensions/tpch), but be
patient: this will take its time (on the order of 20 minutes).  Alternatively, script
`generate-databases.sql` contains pointers to canned DuckDB database
files that you can download instead.

## Credits

The *DiDi* material stands on the shoulders of

- a variety of scientific papers (which we mention and link to on the slides),
- the DuckDB documentation at https://duckdb.org/docs/,
- blog posts (mostly found on https://duckdb.org/news/),
- an exploration of DuckDB's C++ code base at https://github.com/duckdb/duckdb,
- discussions on the friendly DuckDB Discord (https://discord.duckdb.org/),
- personal communication (over Discord and beers) with the awesome
  bunch of DuckDB developers at [DuckDB Labs](https://duckdblabs.com),
- SQL references/standards,
- experience, and best practices.

Chapter 02 (The Query Performance Spectrum) is an adaptation and
extension of a discussion found in Thomas Neumann's fabulous lecture notes
on [Foundations in Data Engineering](https://db.in.tum.de/teaching/ws2425/foundationsde/?lang=en).

The slides were authored using (a heavily modified version of) Morgan McGuire's
Markdown dialect [Markdeep](https://casual-effects.com/markdeep/).
I used Fabrizio Schiavi's fixed-width [Pragmata Pro](https://fsd.it/shop/fonts/pragmatapro/) fonts
for typesetting.


## *DiDi* File Layout

~~~
.
├── slides
│   ├── DiDi-01.pdf
│   ├── DiDi-02.pdf
│   ├── DiDi-03.pdf
│   ├── DiDi-04.pdf
│   ├── DiDi-05.pdf
│   ├── DiDi-06.pdf
│   ├── DiDi-07.pdf
│   └── DiDi-08.pdf
├── material
│   ├── 01
│   │   └── no-material-here
│   ├── 02
│   │   ├── 001-sum-quantity.awk
│   │   ├── 002-sum-quantity.py
│   │   ├── 003-sum-quantity.c
│   │   ├── 004-sum-quantity-mmap.c
│   │   ├── 005-bit-twiddling.c
│   │   ├── 006-sum-quantity-mmap-block.c
│   │   ├── 007-sum-quantity-mmap-threads.c
│   │   └── 008-sum-quantity.sql
│   ├── 03
│   │   ├── 009-buffering.sql
│   │   ├── 010-streaming.sql
│   │   ├── 011-spilling.sql
│   │   └── 012-adaptive.sql
│   ├── 04
│   │   ├── 013-sorting.sql
│   │   └── 014-key-normalization.sql
│   ├── 05
│   │   ├── 015-zonemaps.sql
│   │   ├── 016-art.sql
│   │   ├── 017-encode-float.c
│   │   ├── 018-simd-compare.c
│   │   └── 019-index-support.sql
│   ├── 06
│   │   ├── 020-plans.sql
│   │   ├── 021-pipelines.sql
│   │   └── 022-parallelism.sql
│   ├── 07
│   │   ├── 023-intermediates.c
│   │   ├── 024-vector-size.sql
│   │   ├── 025-vectors.sql
│   │   ├── 026-unroll.c
│   │   ├── 026-unroll.README
│   │   ├── 027-duffs-device.c
│   │   ├── 027-duffs-device.README
│   │   ├── 028-prefetch.c
│   │   ├── 028-prefetch.README
│   │   ├── 029-branch-prediction.c
│   │   ├── 029-branch-prediction.README
│   │   ├── 030-mixed-mode-conjunction.c
│   │   └── 030-mixed-mode-conjunction.README
│   └── 08
│       ├── 031-canonical.sql
│       ├── 032-pass-duration.sql
│       ├── 033-expression_rewriter.sql
│       ├── 034-reorder_filter.sql
│       ├── 035-statistics-propagation.sql
│       ├── 036-join_filter_pushdown.sql
│       ├── 037-row_group_pruner.sql
│       ├── 038-late_materialization.sql
│       ├── 039-sum_rewriter.sql
│       ├── 040-in_clause.sql
│       ├── 041-window_self_join.sql
│       ├── 042-cte_inlining.sql
│       ├── 043-common_subplan.sql
│       ├── 044-join-implementations.sql
│       ├── 045-cardinalities.sql
│       ├── 046-hypergraph.sql
│       ├── 047-build_side_probe_side.sql
│       └── 048-decorrelation.sql
├── databases
│   └── generate-databases.sql
├── README.md
└── LICENSE
~~~
