/* compile this file via
   cc -Wall -O2 -g -o 006-sum-quantity-mmap-block 006-sum-quantity-mmap-block.c
*/

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

/* sum the Nth field in each CSV row */
#define N 5

/* field delimiter */
#define DELIM '|'

/* see https://graphics.stanford.edu/~seander/bithacks.html#ValueInWord */
#define HAS_ZERO(x) (((x) - 0x0101010101010101ULL) & ~(x) & 0x8080808080808080ULL)
#define HAS_NL(x)   (HAS_ZERO(x ^ 0x0a0a0a0a0a0a0a0aULL))

int main(int argc, char *argv[])
{
  int     file;
  size_t  size;

  char    *data;
  char    *end;
  int     column;

  uint64_t *block;
  uint64_t nl;

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

    block = (uint64_t*)data;
    while (!(nl = HAS_NL(*block)))
      block++;  /* advance by one 8-byte-block */

    data = (char*)block;
    /* skip over first \n found (arm64: reversed byte order) */
    if (nl & 0x0000000000000080ULL) { data += 1; continue; }
    if (nl & 0x0000000000008000ULL) { data += 2; continue; }
    if (nl & 0x0000000000800000ULL) { data += 3; continue; }
    if (nl & 0x0000000080000000ULL) { data += 4; continue; }
    if (nl & 0x0000008000000000ULL) { data += 5; continue; }
    if (nl & 0x0000800000000000ULL) { data += 6; continue; }
    if (nl & 0x0080000000000000ULL) { data += 7; continue; }
    data += 8;
  }

  printf("%d\n", sum);

  return 0;
}
