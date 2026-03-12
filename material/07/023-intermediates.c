/* Demonstrate the impact of different intermediate sizes N on plan execution
 * performance:
 *
 * - N = 1:         pass individual rows
 * - N = 2048:      pass data chunks (DuckDB's STANDARD_VECTOR_SIZE)
 * - N = 600000000: pass entire columns (cardinality of table lineitem)
 *
 * The C code below implements a simplified variant of TPC-H Query Q1:
 *
 *   SELECT l_returnflag, sum(l_extendedprice * (1.0 - l_discount)) AS sum_disc_price
 *   FROM   lineitem
 *   WHERE  l_shipdate < '1998-09-03' :: date
 *   GROUP BY l_returnflag;
 *
 * We will come back to this file later on.  Only focus on function Q1() for now.
 *
 * Compile via: cc -O2 -Wall -DN=‹intermediate size› 023-intermediates.c -o 023-intermediates
 */

#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

/* intermediate size */
#ifndef N
 #define N 2048
#endif

/* cardinality of table lineitem (SF = 100) */
#define LINEITEM 600000000

clock_t start, end;
double millisecs;

#define MIN(x, y) ((x) < (y) ? (x) : (y))

/* intermediates to hold the relevant columns of table lineitem */
int*           __restrict__ l_shipdate;
unsigned char* __restrict__ l_returnflag;
double*        __restrict__ l_discount;
double*        __restrict__ l_extendedprice;
/* hash table entry (GROUP BY char / sum(double)) */
struct hash_entry {
  bool          valid;      /* hash table slot occupied? */
  unsigned char returnflag; /* grouping key */
  double        sum;        /* aggregate */
};
typedef struct hash_entry hash_entry_t;
hash_entry_t* __restrict__ hash_table;


int AGGR_sum_double_col(int n, hash_entry_t* __restrict__ ht,
                        int* __restrict__ hash, double* __restrict__ col, int*  __restrict__ sel)
{
  if (sel) {
    for (int j = 0; j < n; j += 1) {
      int i = sel[j];
      ht[hash[i]].sum += col[i];
    }
  } else {
    for (int i = 0; i < n; i += 1) {
      ht[hash[i]].sum += col[i];
    }
  }

  return n;
}

int HASH_direct_char_col(
  int n, int* __restrict__ res,
  hash_entry_t* __restrict__ ht, unsigned char* __restrict__ col, int* __restrict__ sel)
{
  int key;

  if (sel) {
    for (int j = 0; j < n; j += 1) {
      int i = sel[j];
      key = (int)col[i]; /* direct hashing */
      res[i] = key;
      ht[key].valid      = true;
      ht[key].returnflag = col[i];
    }
  } else {
    for (int i = 0; i < n; i += 1) {
      key = (int)col[i]; /* direct hashing */
      res[i] = key;
      ht[key].valid      = true;
      ht[key].returnflag = col[i];
    }
  }

  return n;
}

int FILTER_lt_date_col_date_val(
  int n, int* __restrict__ res,
  int* __restrict__ col, int val, int* __restrict__ sel)
{
  int o = 0;

  if (sel) {
    /* only process entries contained in selection vector */
    for (int j = 0; j < n; j += 1) {
      int i = sel[j];
      if (col[i] < val) {
        res[o] = i;
        o += 1;
      }
    }
  } else {
    /* no selection vector, process all entries of intermediate col */
    for (int i = 0; i < n; i += 1) {
      if (col[i] < val) {
        res[o] = i;
        o += 1;
      }
    }
  }

  return o;
}

int PROJECT_sub_double_val_double_col(
  int n, double* __restrict__ res,
  double* __restrict__ col, double val, int* __restrict__ sel)
{
  if (sel) {
    /* only process entries contained in selection vector */
    for (int j = 0; j < n; j += 1) {
      int i = sel[j];
      res[i] = val - col[i];
    }
  } else {
    /* no selection vector, process all entries of intermediate col */
    for (int i = 0; i < n; i += 1) {
      res[i] = val - col[i];
    }
  }

  return n;
}

int PROJECT_mul_double_col_double_col(
  int n, double* __restrict__ res,
  double* __restrict__ col1, double* __restrict__ col2, int* __restrict__ sel)
{
  if (sel) {
    /* only process entries contained in selection vector */
    for (int j = 0; j < n; j += 1) {
      int i = sel[j];
      res[i] = col1[i] * col2[i];
    }
  } else {
    /* no selection vector, process all entries of intermediates col1, col2 */
    for (int i = 0; i < n; i += 1) {
      res[i] = col1[i] * col2[i];
    }
  }

  return n;
}

int SCAN_shipdate(int n, int* __restrict__ *res)
{
  static int idx = 0;         /* state of scan */
  int rows = LINEITEM - idx;  /* # rows left to emit */

  if (rows <= 0)
    return 0;

  *res = l_shipdate + idx;
  idx += n;

  return MIN(n, rows);
}

int SCAN_returnflag(int n, unsigned char* __restrict__ *res)
{
  static int idx = 0;         /* state of scan */
  int rows = LINEITEM - idx;  /* # rows left to emit */

  if (rows <= 0)
    return 0;

  *res = l_returnflag + idx;
  idx += n;

  return MIN(n, rows);
}

int SCAN_discount(int n, double* __restrict__ *res)
{
  static int idx = 0;         /* state of scan */
  int rows = LINEITEM - idx;  /* # rows left to emit */

  if (rows <= 0)
    return 0;

  *res = l_discount + idx;
  idx += n;

  return MIN(n, rows);
}

int SCAN_extendedprice(int n, double* __restrict__ *res)
{
  static int idx = 0;         /* state of scan */
  int rows = LINEITEM - idx;  /* # rows left to emit */

  if (rows <= 0)
    return 0;

  *res = l_extendedprice + idx;
  idx += n;

  return MIN(n, rows);
}

void Q1()
{
  int n = 0;
  /* intermediates */
  int*           __restrict__ shipdates = {};
  unsigned char* __restrict__ returnflags = {};
  double*        __restrict__ discounts = {};
  double*        __restrict__ extendedprices = {};
  double* vec0 = malloc(N * sizeof(double));
  double* vec1 = malloc(N * sizeof(double));
  int* vec2    = malloc(N * sizeof(int));
  /* intermediate selection vector */
  int* sel = malloc(N * sizeof(int));

  do {
    /* SCAN */
    n = SCAN_shipdate(N, &shipdates);
    SCAN_returnflag(N, &returnflags);
    SCAN_discount(N, &discounts);
    SCAN_extendedprice(N, &extendedprices);

    /* FILTER */
    int m = FILTER_lt_date_col_date_val(n, sel, shipdates, (int)LINEITEM * 0.98, NULL);

    /* PROJECT */
    PROJECT_sub_double_val_double_col(m, vec0, discounts, 1.0, sel);
    PROJECT_mul_double_col_double_col(m, vec1, vec0, extendedprices, sel);

    /* AGGREGATE */
    HASH_direct_char_col(m, vec2, hash_table, returnflags, sel);
    AGGR_sum_double_col(m, hash_table, vec2, vec1, sel);
  } while (n > 0);
}

int main()
{

  /* initialize a fake TPC-H lineitem table */

  /* allocate source table */
  l_shipdate      = calloc(LINEITEM, sizeof(int));
  l_returnflag    = calloc(LINEITEM, sizeof(unsigned char));
  l_discount      = calloc(LINEITEM, sizeof(double));
  l_extendedprice = calloc(LINEITEM, sizeof(double));

  for (int i = 0; i < LINEITEM; i+= 1) {
    l_shipdate[i]      = i;
    l_returnflag[i]    = (unsigned char[]) { 'A', 'N', 'R', [3]='N' }[i % 4];
    l_discount[i]      = ((double)rand() / RAND_MAX) * 0.12;
    l_extendedprice[i] = 900.0 + ((double)rand() / RAND_MAX) * (100000.0 - 900.0);
  }

  /* allocate + initialize hash table sized to support
   * direct hashing of unsigned char column returnflag
   */
  hash_table = calloc(UCHAR_MAX, sizeof(hash_entry_t));
  for (int i = 0; i < UCHAR_MAX; i += 1)
    hash_table[i] = (hash_entry_t) { .valid = false };

  /* start processing of Q1 */
  start = clock();

  Q1();

  end = clock();

  /* dump query result */
  for (int i = 0; i < UCHAR_MAX; i += 1)
    if (hash_table[i].valid)
      printf("%c | %f\n", hash_table[i].returnflag, hash_table[i].sum);

  /* timing for Q1 */
  millisecs = ((double) (end - start)) / CLOCKS_PER_SEC * 1000;
  printf("Q1 with chunk size %d: %f ms\n", N, millisecs);

  return 0;
}
