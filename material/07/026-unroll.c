/* Demonstrate the effect of loop vectorization and unrolling
 *
 * Compile via
 *
 *   cc -O2 -fno-vectorize -fno-unroll-loops 026-unroll.c -o 026-unroll
 *
 * Execute via
 *
 *   ./026-unroll  or  ./026-unroll -u (← peforms unrolling)
 */
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/time.h>

#define MICROSECS(t) (1000000 * (t).tv_sec + (t).tv_usec)
#define REPETITIONS 1000000

#define STANDARD_VECTOR_SIZE 2048

void PROJECT_sub_int_col_int_col(int *col1, int *col2, int *res)
{
  int i;

  for (i = 0; i < STANDARD_VECTOR_SIZE; i += 1) {
      res[i] = col1[i] - col2[i];
  }
}

void PROJECT_sub_int_col_int_col_unrolled(int *col1, int *col2, int *res)
{
  int i;

  for (i = 0; i + 3 < STANDARD_VECTOR_SIZE; i += 4) {
      res[i  ] = col1[i  ] - col2[i  ];
      res[i+1] = col1[i+1] - col2[i+1];
      res[i+2] = col1[i+2] - col2[i+2];
      res[i+3] = col1[i+3] - col2[i+3];
  }
}

int main(int argc, char **argv)
{
  int *v1, *v2, *v3;
  struct timeval t0, t1;
  unsigned long duration;

  /* option -u: perform unrolling */
  int unroll = 0;
  unroll = getopt(argc, argv, "u") == 'u';

  v1 = malloc(STANDARD_VECTOR_SIZE * sizeof(int));
  v2 = malloc(STANDARD_VECTOR_SIZE * sizeof(int));
  v3 = malloc(STANDARD_VECTOR_SIZE * sizeof(int));

  for (int i = 0; i < STANDARD_VECTOR_SIZE; i += 1)
      v1[i] = v2[i] = v3[i] = 42;

  gettimeofday(&t0, NULL);
  if (unroll)
    for (int i = 0; i < REPETITIONS; i += 1)
      PROJECT_sub_int_col_int_col_unrolled(v1, v2, v3);
  else
    for (int i = 0; i < REPETITIONS; i += 1)
      PROJECT_sub_int_col_int_col(v1, v2, v3);
  gettimeofday(&t1, NULL);

  duration = MICROSECS(t1) - MICROSECS(t0);
  printf("time: %luμs (v3[42] = %d)\n", duration, v3[42]);

  return 0;
}
