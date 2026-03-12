/*
 * Compile via
 *
 *   cc -Wall -O2 028-prefetch.c -o 028-prefetch
 */

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sys/time.h>
#include <stdint.h>

#define MICROSECS(t) (1000000 * (t).tv_sec + (t).tv_usec)

/* process int vector of 256M elements >> Apple M2 Max L2 cache of 32M */
#define SIZE (256 * 1024 * 1024)

/* prefetch how many iterations ahead? */
#define LOOKAHEAD 128


/* linearly scan the vector, add elements
   (no manual prefetching, but CPU will detect the linear memory access
   pattern and automatically issue prefetching operations) */
int linear(int *vector)
{
  int sum = 0;

  for (int i = 0; i < SIZE; i += 1)
    sum = sum + vector[i];

  return sum;
}

/* randomly bounce around the vector (no manual or CPU prefetching) */
int bounce(int *vector)
{
  int sum = 0;

  /* initialize deterministic random number sequence */
  srand(42);

  for (int i = 0; i < SIZE; i += 1)
    sum = sum + vector[rand() % SIZE];

  return sum;
}

/* randomly bounce around the vector, but explicitly prefetch the address
   needed in LOOKAHEAD iterations from now: hide memory access latency */
int prefetching_bounce(int *vector)
{
  int sum = 0;
  int locations[LOOKAHEAD];

  /* initialize deterministic random number sequence */
  srand(42);

  /* prime a ring buffer of prefetching addresses neeed in future iterations
     (simulates that we know about our future memory access pattern) */
  for (int l = 0; l < LOOKAHEAD; l += 1)
    /* can also prefetch these locations — but makes no measurable difference */
    locations[l] = rand() % SIZE;

  for (int i = 0, l = 0; i < SIZE; i += 1) {
      sum = sum + vector[locations[l]];

      locations[l] = rand() % SIZE;
      /* prefetch memory needed in LOOKAHEAD iterations from now */
      __builtin_prefetch(&vector[locations[l]]);
      l = (l + 1) % LOOKAHEAD;
  }

  return sum;
}

int main()
{
  int *vector, sum;
  struct timeval t0, t1;
  unsigned long duration;

  vector = malloc(SIZE * sizeof(int));
  assert(vector);
  for (int i = 0; i < SIZE; i += 1)
    vector[i] = i % 10;

  /* ➊ linear scan */
  gettimeofday(&t0, NULL);
  sum = linear(vector);
  gettimeofday(&t1, NULL);

  duration = MICROSECS(t1) - MICROSECS(t0);
  printf("time (linear): %luμs (sum = %d)\n", duration, sum);

  /* ➋ bounce, no prefetch */
  gettimeofday(&t0, NULL);
  sum = bounce(vector);
  gettimeofday(&t1, NULL);

  duration = MICROSECS(t1) - MICROSECS(t0);
  printf("time (bounce): %luμs (sum = %d)\n", duration, sum);

  /* ➌ bounce with prefetch */
  gettimeofday(&t0, NULL);
  sum = prefetching_bounce(vector);
  gettimeofday(&t1, NULL);

  duration = MICROSECS(t1) - MICROSECS(t0);
  printf("time (bounce with prefetch): %luμs (sum = %d)\n", duration, sum);

  return 0;
}
