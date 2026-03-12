-- To speed up row comparison, DuckDB normalize sort keys and specifiers
-- (like ASC/DESC, NULL FIRST/LAST) to a byte sequence such that operator <
-- implements the desired row ordering.
--
-- Key normalization is available as a user-level scalar UDF
-- (key1 dominates the lexicographic ordering):
--
--   create_sort_key(key1, specifier1, key2, specfifier2, ...)

-- A table of pioneers of SQL and the relational data model
CREATE OR REPLACE TABLE pioneers  (
  id int8,
  c1 text,
  c2 text
);

INSERT INTO pioneers(id,c1,c2) VALUES
  (-1,   'Ray',    'Boyce'),
  ( 0,   'Ted',    'Codd'),
  ( 1,   'Donald', 'Chamberlin'),
  ( 2,   'Ray',    NULL),
  ( 3,   'Ray',    'Ray'),
  (NULL, NULL,     NULL);

FROM pioneers;

-----------------------------------------------------------------------

-- Order the pioneers
--
FROM pioneers AS p
ORDER BY p.c1 ASC NULLS LAST, p.c2 DESC NULLS FIRST;


-- Show sort key values after normalization
--
SELECT p.id,
       p.c1, create_sort_key(p.c1, 'ASC  NULLS LAST' ) AS 'c1 🡑 NULLS LAST',
       p.c2, create_sort_key(p.c2, 'DESC NULLS FIRST') AS 'c2 🡓 NULLS FIRST'
FROM   pioneers AS p;

-- ┌───────┬─────────┬─────────────────┬────────────┬──────────────────────────────────────────────────┐
-- │  id   │   c1    │ c1 🡑 NULLS LAST │     c2     │                 c2 🡓 NULLS FIRST                 │
-- │ int64 │ varchar │      blob       │  varchar   │                       blob                       │
-- ├───────┼─────────┼─────────────────┼────────────┼──────────────────────────────────────────────────┤
-- │    -1 │ Ray     │ \x01Sbz\x00     │ Boyce      │ \x02\xBC\x8F\x85\x9B\x99\xFF                     │
-- │     0 │ Ted     │ \x01Ufe\x00     │ Codd       │ \x02\xBB\x8F\x9A\x9A\xFF                         │
-- │     1 │ Donald  │ \x01Epobme\x00  │ Chamberlin │ \x02\xBB\x96\x9D\x91\x9C\x99\x8C\x92\x95\x90\xFF │
-- │     2 │ Ray     │ \x01Sbz\x00     │ ▢          │ \x01                                             │
-- │     3 │ Ray     │ \x01Sbz\x00     │ Ray        │ \x02\xAC\x9D\x85\xFF                             │
-- │     ▢ │ ▢       │ \x02            │ ▢          │ \x01                                             │
-- └───────┴─────────┴─────────────────┴────────────┴──────────────────────────────────────────────────┘

-- NB.
-- - NULLS FIRST: NULL maps to \x01, prepend \x02 to non-NULL values
--   NULLS LAST : NULL maps to \x02, prepend \x01 to non-NULL values
--
-- - DESC: Invert the bits in a byte (almost two's complement):
--         'S' = 01010011 -> \xAC = 10101100
--
-- - Encode text character-by-character by shifting bytes by 1
--   to also encode \x00:
--     create_sort_key('R', 'ASC NULLS LAST') = '\x01S\x00' :: blob
--     create_sort_key('æ', 'ASC NULLS LAST') = '\x01\xC4\xA7\x00' :: blob
--                                                   􀇃──􀇄───􀇅
--                                        'æ' has UTF8 encoding C3 A6


-- Lexicographic ordering is implemented in terms of concatentation
-- of normalized keys.
--
-- These normalized keys implement the proper row ordering.
SELECT p.*,
       create_sort_key(p.c1, 'ASC  NULLS LAST', p.c2, 'DESC NULLS FIRST') AS normalized
FROM   pioneers AS p
ORDER BY normalized;

-- Normalized keys can be compared via single < operation
-- (is normalized 'R' less than normalized 'T'?)
--
SELECT '\x01Sbz\x00\x02\xBC\x8F\x85\x9B\x99\xFF' :: blob < '\x01Ufe\x00\x02\xBB\x8F\x9A\x9A\xFF' :: blob;


-- The "small" type int8 also maps to a byte sequence.
-- (⚠️ But do we want that?)
--
SELECT p.id,
       create_sort_key(p.id, 'ASC  NULLS FIRST') AS "🡑 NULLS FIRST",
       create_sort_key(p.id, 'DESC NULLS LAST')  AS "🡓 NULLS LAST"
FROM pioneers AS p;

-- ┌───────┬──────────────────────────────────────┬──────────────────────────────────────┐
-- │  id   │            🡑 NULLS FIRST             │             🡓 NULLS LAST             │
-- │ int64 │                 blob                 │                 blob                 │
-- ├───────┼──────────────────────────────────────┼──────────────────────────────────────┤
-- │    -1 │ \x02\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF │ \x01\x80\x00\x00\x00\x00\x00\x00\x00 │
-- │     0 │ \x02\x80\x00\x00\x00\x00\x00\x00\x00 │ \x01\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF │
-- │     1 │ \x02\x80\x00\x00\x00\x00\x00\x00\x01 │ \x01\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFE │
-- │     2 │ \x02\x80\x00\x00\x00\x00\x00\x00\x02 │ \x01\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFD │
-- │     3 │ \x02\x80\x00\x00\x00\x00\x00\x00\x03 │ \x01\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFC │
-- │     ▢ │ \x01\x00\x00\x00\x09\x00\x00\x00\x00 │ \x02\x00\x00\x00\x09\x00\x00\x00\x00 │
-- └───────┴──────────────────────────────────────┴──────────────────────────────────────┘
--
-- NB.
-- - First bit flipped to preserve order between positive and negative integers:
--   -1 -> \x02\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF
--    1 -> \x02\x80\x00\x00\x00\x00\x00\x00\x01
--               |
--             first bit flipped

-- Is normalized -1 less than normalized 1?
--
SELECT '\x02\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF' :: blob < '\x02\x80\x00\x00\x00\x00\x00\x00\x01' :: blob;
