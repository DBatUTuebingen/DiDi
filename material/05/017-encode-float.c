/* Prepare values for lookup/insertion into Adaptive Radix Trees (ART):
 *
 * Map IEEE 754 floating point values to 32-bit sequences in big endian
 * (most significant byte comes first) whose lexicographic order properly
 * reflects floating point sort order.
 *
 * See http://stereopsis.com/radix.html
 *
 * Compile via cc -Wall -o 017-encode_float 017-encode-float.c
 */

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <assert.h>

#define BOOL(b) ((b)? "true" : "false")   /* 0: "false", otherwise: "true" */

/* Print the 32 0/1 bits of x */
void printbits(uint32_t x)
{
  for (int n = 31; n >= 0; n--)
    putchar((x & (1 << n)) ? '1' : '0');
  putchar('\n');
}

/* Turn float f into a 32-bit sequence (in big endian order) whose
 * lexicographic order reflects floating point sort order:
 *
 * 1. Flip the sign bit.
 * 2. If the sign bit was orignally set, now flip ALL bits.
 *
 * (| and ^ are bit-wise or and xor)
 */
uint32_t encode_float(float f)
{
  uint32_t x = *(uint32_t*)&f;  /* x interprets float f as a 32-bit sequence */

  uint32_t mask = -(int32_t)((x >> 31) | 0x80000000);
  return htonl(x ^ mask);       /* htonl(): convert to big endian */
}


int main()
{
  assert(sizeof(float) == 4);  /* does float use a 32-bit (IEEE754) encoding? */

  float pi  =  3.1415;
  float _pi =  -pi;
  float e   =  2.718;
  float _e  =  -e;

  /* encode floats as 32-bit sequences */
  uint32_t pi_enc   = encode_float(pi);
  uint32_t _pi_enc  = encode_float(_pi);
  uint32_t e_enc    = encode_float(e);
  uint32_t _e_enc   = encode_float(_e);

  /* show bit sequences before/after encoding */
  printf("π:\t\t");
  printbits(*(uint32_t*)&pi);  /* pi is stored in little endian */
  printf("π encoded:\t");
  printbits(ntohl(pi_enc));    /* use ntohl() such that both bit sequences are  */
                                  /* little endian for comparison (sign bit first) */

  printf("-π:\t\t");
  printbits(*(uint32_t*)&_pi);
  printf("-π encoded:\t");
  printbits(ntohl(_pi_enc));

  /* check that lexicographic bit sequence order reflects float sort order */
  printf("%s\n", BOOL(memcmp(&e_enc,   &pi_enc, 4) < 0));    /* is   e < pi? */
  printf("%s\n", BOOL(memcmp(&_pi_enc, &pi_enc, 4) < 0));    /* is -pi < pi? */
  printf("%s\n", BOOL(memcmp(&_pi_enc, &_e_enc, 4) < 0));    /* is -pi < -e? */

  return 0;
}
