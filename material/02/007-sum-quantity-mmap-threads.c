/* compile this file via
   cc -Wall -O2 -g -o 007-sum-quantity-mmap-threads 007-sum-quantity-mmap-threads.c
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include <pthread.h>

/* sum the Nth field in each CSV row */
#define N 5

/* field delimiter */
#define DELIM '|'

/* thread-local data + arguments + and return value */
struct chunk {
  pthread_t thread;
  char      *data;
  char      *end;
  int       sum;
};

typedef struct chunk chunk_t;

/* Code for a thread (sums a chunk) */
void *sum_chunk(void *arg)
{
  chunk_t *chunk = (chunk_t*) arg;

  char *data = chunk->data;
  char *end  = chunk->end;
  int  column;

  int sum = 0;

  column = 1;

  while (data < end) {
    switch (*data) {
      case DELIM: column++; break;
      case '\n':  return "malformed input line";
    }
    data++;

    if (column < N)
      continue;

    sum = sum + atoi(data);

    column = 1;

    data = strchr(data, '\n') + 1;
  }

  chunk->sum = sum;
  return NULL;
}

int main(int argc, char *argv[])
{
  int     file;
  size_t  size;

  char    *data;
  char    *end;

  int     T;         /* number of threads */
  int     nchunks;
  chunk_t *chunks;
  size_t  chunk_size;

  int     sum = 0;

  /* memory-map input file */
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

  /* partition the data into equally-sized chunks */
  T = sysconf(_SC_NPROCESSORS_ONLN);

  chunks = malloc(T * sizeof(chunk_t));
  chunk_size = size / T;

  for (nchunks = 1; nchunks < T && data + chunk_size < end; nchunks++) {
    chunks[nchunks].data = data;
    /* chunk end is at first \n just after chunk_size */
    chunks[nchunks].end  = strchr(data + chunk_size, '\n');
    data = chunks[nchunks].end + 1;
  }
  /* chunk #0 holds remaining data */
  chunks[0].data = data;
  chunks[0].end  = end;

  /* create threads */
  for (int i = 1; i < nchunks; i++)
    if (pthread_create(&chunks[i].thread, NULL, sum_chunk, &chunks[i])) {
      perror("cannot create thread");
      exit(EXIT_FAILURE);
  }
  sum_chunk(&chunks[0]); /* main thread (us) computes chunk #0 */

  /* wait for threads to complete, aggregate partial sums */

  sum = chunks[0].sum;
  for (int i = 1; i < nchunks; i++) {
    void *ret;
    if (pthread_join(chunks[i].thread, &ret) != 0) {
      perror("cannot join thread");
      exit(EXIT_FAILURE);
    }
    if ((char*)ret) {
      fprintf(stderr, "%s\n", (char*)ret);
      exit(EXIT_FAILURE);
    }
    sum = sum + chunks[i].sum;
  }

  printf("%d\n", sum);

  return 0;
}
