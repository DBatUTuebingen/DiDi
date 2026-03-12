/* Explains the bit-twiddling technique (block-wise search for '\n')
   used in 006-sum-quantity-mmap-block.c
*/

/* compile this file via
   cc -Wall -o 005-bit-twiddling 005-bit-twiddling.c

   run via
   ./005-bit-twiddling 0x0a
   ./005-bit-twiddling 0x20
   ./005-bit-twiddling 0xc4
*/

#include <stdio.h>

/* print bit representation of c (with message s) */
void bitprint(char *s, char c)
{
  printf("%60s = ", s);
  for (int i = 7; i >= 0; i--)
    putchar('0' + ((c >> i) & 1));
  putchar('\n');
}

int main(int argc, char *argv[])
{
  char c;
  sscanf(argv[1], "%x", (unsigned int*)&c);

  bitprint("input c",c);
  bitprint("m = c masked with \\n", c ^ 0x0a);

  bitprint("high bit: is m = 0x00 or m > 0x80?", (c ^ 0x0a) - 0x01);
  bitprint("high bit: is m < 0x80?", ~(c ^ 0x0a) & 0x80);
  bitprint("high bit: (is m = 0x00 or m > 0x80) and (is m < 0x80)?", ((c ^ 0x0a) - 0x01) & (~(c ^ 0x0a) & 0x80));
}
