/* compile this file via
   cc -Wall -O2 -g -o 004-sum-quantity-mmap 004-sum-quantity-mmap.c
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

/* sum the Nth field in each CSV row */
#define N 5

/* field delimiter */
#define DELIM '|'

/* use strchr() to search for newline '\n'? */
#define USE_STRCHR 0

int main(int argc, char *argv[])
{
  int     file;
  size_t  size;

  char    *data;
  char    *end;
  int     column;

  int     sum = 0;

  file = open(argv[1], O_RDONLY);
  if (file < 0) {
    perror("cannot read input");
    exit(EXIT_FAILURE);
  }

  lseek(file, 0, SEEK_END);
  size = lseek(file, 0, SEEK_CUR);
  data = (char*)mmap(NULL, size, PROT_READ, MAP_SHARED, file, 0);
  if (data == MAP_FAILED) {
    perror("cannot map input into memory");
    exit(EXIT_FAILURE);
  }

  end = data + size;
  column = 1;

  while (data < end) {
    switch (*data) {
      case DELIM: column++; break;
      case '\n':  { fprintf(stderr, "malformed input line\n");
                    exit(EXIT_FAILURE);
                  }
    }
    data++;

    if (column < N)
      continue;

    sum = sum + atoi(data);

    column = 1;

#if USE_STRCHR
    data = strchr(data, '\n') + 1;
#else
    while (*data++ != '\n');
#endif
  }

  printf("%d\n", sum);

  return 0;
}
