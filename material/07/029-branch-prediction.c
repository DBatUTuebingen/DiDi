/* Demonstrate the effects of branch mispredictions for a selection
 * col < val implemented in a tight loop
 *
 * Compile via
 *
 *   cc -Wall 029-branch-prediction.c -o 029-branch-prediction
 */
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <assert.h>

#define MICROSECS(t) (1000000 * (t).tv_sec + (t).tv_usec)

#define SIZE (32 * 1024 * 1024)

/* experiment: increase selectivity from 0% to 100% in STEPS steps */
#define STEPS 11

/* comparison of a, b (only used in qsort, Experiment (1)) */
int cmp (const void *a, const void *b)
{
    return *((int*) a) - *((int*) b);
}

int main()
{
    int *col;        /* column vector */
    int *res;        /* selection vector */

    int i, o;           /* indices into column + selection vectors */
    float selectivity;

    struct timeval t0, t1;
    unsigned long duration;

    /* allocate column + selection vector memory */
    col = calloc(SIZE, sizeof(int));
    assert(col);
    res = calloc(SIZE, sizeof(int));
    assert(res);

    /* initialize column with (pseudo) random values in interval 0...RAND_MAX */
    srand(42);
    for (int i = 0; i < SIZE; i += 1)
        col[i] = rand();

    /* Experiment (1) only:
     */
    // qsort(col, SIZE, sizeof(int), cmp);

    for (int step = 0; step < STEPS; step += 1) {

        /* val grows linearly 0...RAND_MAX in STEPS steps */
        int val = step * (RAND_MAX / (STEPS - 1));

        gettimeofday(&t0, NULL);

        o = 0;
        for (i = 0; i < SIZE; i += 1) {

            if (col[i] < val) {
                res[o] = i;
                o += 1;
            }

            /* Experiment (2) only:
             * a branch-less copy
             */
             // res[o] = i;
             // o += (col[i] < val);
        }

        gettimeofday(&t1, NULL);
        duration = MICROSECS(t1) - MICROSECS(t0);

        selectivity = ((float)o / SIZE) * 100.0;

        printf ("%2u (selectivity: %6.2f%%)\t%6luμs\n",
                step, selectivity, duration);
    }

    return 0;
}
