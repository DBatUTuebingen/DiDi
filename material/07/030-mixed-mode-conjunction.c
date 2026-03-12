/* Demonstrate alternatives for the implementation of
 * conjunctive predicate col < val ∧ col % 2 = 0:
 *
 * (A) branch-less selection (via & and +=)
 * (B) mixed mode selection (via if [varying selectivity] and +=)
 * (C) mixed mode selection (via if [unpredictable] and +=)
 *
 * Compile via (no -O2 optimization for dramatic effect):
 *
 *   cc -Wall 030-mixed-mode-conjunction.c -o 030-mixed-mode-conjunction
 */
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <assert.h>

#define MILLISECS(t) ((1000000 * (t).tv_sec + (t).tv_usec) / 1000)

#define SIZE (32 * 1024 * 1024)

/* experiment: increase selectivity from 0% to 100% in STEPS steps */
#define STEPS 11

/* comparison of a, b (only used in qsort) */
int cmp (const void *a, const void *b)
{
    return *((int*) a) - *((int*) b);
}

int main()
{
    int *col;     /* column vector */
    int *res;     /* selection vector */

    int i, o;
    float selectivity;

    struct timeval t0, t1;
    unsigned long duration1, duration2, duration3;

    /* allocate memory */
    col = calloc(SIZE, sizeof(int));
    assert(col);
    res = calloc(SIZE, sizeof(int));
    assert(res);

    /* initialize column with (pseudo) random values in interval 0...RAND_MAX */
    srand(42);
    for (int i = 0; i < SIZE; i += 1)
        col[i] = rand();

    /* Quiz: how will sorting the column affect run time?
    */
    // qsort(col, SIZE, sizeof (*col), cmp);

    printf("\tsel\tA\tmixed B\tmixed C\n");

    for (int step = 0; step < STEPS; step += 1) {

        /* v grows linearly 0...RAND_MAX in STEPS steps */
        int val = step * (RAND_MAX / (STEPS - 1));

        /* ----------  alternative A ---------- */

        gettimeofday(&t0, NULL);

        o = 0;
        for (i = 0; i < SIZE; i += 1) {
            res[o] = col[i];
            o += ((col[i] < val) & (col[i] % 2 == 0));
        }

        gettimeofday(&t1, NULL);
        duration1 = MILLISECS(t1) - MILLISECS(t0);


        /* ---------- alternative B ---------- */

        gettimeofday(&t0, NULL);

        o = 0;
        for (i = 0; i < SIZE; i += 1) {
            if (col[i] < val) {
                res[o] = col[i];
                o += (col[i] % 2 == 0);
            }
        }

        gettimeofday(&t1, NULL);
        duration2 = MILLISECS(t1) - MILLISECS(t0);


        /* ---------- alternative C ---------- */

        gettimeofday(&t0, NULL);

        o = 0;
        for (i = 0; i < SIZE; i += 1) {
            if (col[i] % 2 == 0) {
                res[o] = col[i];
                o += (col[i] < val);
            }
        }

        gettimeofday(&t1, NULL);
        duration3 = MILLISECS(t1) - MILLISECS(t0);

        selectivity = ((float)o / SIZE) * 100.0;

        printf ("%2u\t%5.2f%%\t%4lums\t%4lums\t%4lums\n",
                step, selectivity, duration1, duration2, duration3);
    }

   return 0;
}
