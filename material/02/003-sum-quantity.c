/* compile this file via
   cc -Wall -O2 -g -o 003-sum-quantity 003-sum-quantity.c
*/

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* sum the Nth column in each CSV row */
#define N 5

/* field delimiter */
#define DELIM '|'

int main(int argc, char *argv[])
{
  FILE    *file;

  char    *line;
  size_t  linecap;

  char    *delim;
  int     column;

  int     sum = 0;

  file = fopen(argv[1], "r");
  if (!file) {
    perror("cannot read input");
    exit(EXIT_FAILURE);
  }

  line = NULL;
  while (getline(&line, &linecap, file) > 0) {
    delim = line;

    for (column = 1; column < N; column++) {
      delim = strchr(delim, DELIM);
      if (!delim) {
        fprintf(stderr, "malformed input line: %s", line);
        exit(EXIT_FAILURE);
      }
      delim++;
    }

    sum = sum + atoi(delim);
  }

  printf("%d\n", sum);

  return 0;
}
