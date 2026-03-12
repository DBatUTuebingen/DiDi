/* The SIMD instruction sets of modern CPUs can perform multiple
 * equality comparisons in parallel: store n values of m bits in
 * an n×m bit SIMD register, then compare the SIMD registers.
 *
 * Below:
 * - SIMD instruction set for ARM Neon (as found in the Apple MacBooks)
 * - n = 4, m = 32: use 128-bit SIMD registers
 *
 * Compile via cc -Wall -o 018-simd-compare 018-simd-compare.c
 */

#include <stdint.h>
#include <arm_neon.h>
#include <stdio.h>

int main()
{
  /* key byte to look for in key array */
  int32_t byte = 3;

  /* sample key array (as found in a Node4, for example) */
  int32_t __attribute__ ((aligned(16))) keys[4]  = { 0, 2, 3, 255 };
  /* multiplex key byte n times */
  int32_t __attribute__ ((aligned(16))) bytes[4] = { byte, byte, byte, byte };
  int32_t __attribute__ ((aligned(16))) result[4];

  /* Load vectors keys + multiplexed key byte into SIMD registers */
  int32x4_t ks = vld1q_s32(keys);
  int32x4_t bs = vld1q_s32(bytes);

  /* Perform four 32-bit equality comparisons in parallel */
  uint32x4_t eq = vceqq_s32(ks, bs);

  /* Store the result back to memory */
  vst1q_u32((uint32_t*)result, eq);

  for (int i = 0; i < 4; i++) {
    printf("Equality at index %d: %#x\n", i, result[i]);
  }

  return 0;
}

/* Intel-based SSE2 variant of the code below.  With thanks to GitHub Copilot. */

#if 0

#include <stdint.h>
#include <emmintrin.h>
#include <stdio.h>

int main()
{
  /* key byte to look for in key array */
  int32_t byte = 3;

  /* sample key array (as found in a Node4, for example) */
  int32_t __attribute__ ((aligned(16))) keys[4]  = { 0, 2, 3, 255 };
  /* multiplex key byte n times */
  int32_t __attribute__ ((aligned(16))) bytes[4] = { byte, byte, byte, byte };
  int32_t __attribute__ ((aligned(16))) result[4];

  /* Load vectors keys + multipled key byte into SIMD registers */
  __m128i ks = _mm_load_si128((__m128i*)keys);
  __m128i bs = _mm_load_si128((__m128i*)bytes);

  /* Perform four 32-bit equality comparisons in parallel */
  __m128i eq = _mm_cmpeq_epi32(ks, bs);

  /* Store the result back to memory */
  _mm_store_si128((__m128i*)result, eq);

  for (int i = 0; i < 4; i++) {
    printf("Equality at index %d: %#x\n", i, result[i]);
  }

  return 0;
}

#endif
