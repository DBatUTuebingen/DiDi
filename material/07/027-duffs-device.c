/*
 * Compile via
 *
 *   cc -Wall -O2 027-duffs-device.c -o 027-duffs-device
 */

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

/* Only if SIZE is divisble by 8, send() will do its job correctly */
#define SIZE 255

/* Eight-fold unrolled loop, copying 'count' ints from 'from' to 'to'.

   NB. Will work correctly only if 'count' is divisible by 8.
 */
void send(int *to, int *from, int count)
{
    int n = count / 8;
    do {
        *to++ = *from++;
        *to++ = *from++;
        *to++ = *from++;
        *to++ = *from++;
        *to++ = *from++;
        *to++ = *from++;
        *to++ = *from++;
        *to++ = *from++;
   } while (--n > 0);

   /* To correct send(), we would need the following epilogue loop:
   n = count % 8;
   while (n-- > 0) {
        *to++ = *from++;
   }
   */
}

/* Duff's device: an unrolled loop that can handle 'count's that
   are not divisible by 8 (due to Tom Duff, Lucasfilm, 1983).

   Note that the switch (...) jumps to a case branch in the
   *middle* of the do...while loop.
*/
void duff_send(int *to, int *from, int count)
{
    int n = (count + 7) / 8;
    switch (count % 8) {
    case 0: do { *to++ = *from++;
    case 7:      *to++ = *from++;
    case 6:      *to++ = *from++;
    case 5:      *to++ = *from++;
    case 4:      *to++ = *from++;
    case 3:      *to++ = *from++;
    case 2:      *to++ = *from++;
    case 1:      *to++ = *from++;
            } while (--n > 0);
    }
}

int main()
{
  int *from, *to;

  /* allocate and zero source and target memory regions */
  from = calloc(SIZE, sizeof(int));
  to   = calloc(SIZE, sizeof(int));
  assert(from);
  assert(to);

  /* initialize source: from[0...SIZE-1] = 42 */
  for (int i = 0; i < SIZE; i += 1)
      from[i] = 42;

  /* copy source to target */
  send(to, from, SIZE);
  printf("after send():      to[%d] = %d\n", SIZE-1, to[SIZE-1]);

  duff_send(to, from, SIZE);
  printf("after duff_send(): to[%d] = %d\n", SIZE-1, to[SIZE-1]);
}
