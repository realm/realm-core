#ifndef TIGHTDB_NMMINTRIN_H
#define TIGHTDB_NMMINTRIN_H

#ifndef _MSC_VER
static inline __m128i __attribute__((always_inline)) _mm_cmpgt_epi64(__m128i xmm1, __m128i xmm2)
{
    __asm__("pcmpgtq %1, %0" : "+x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

static inline __m128i __attribute__((always_inline)) _mm_cmpeq_epi64(__m128i xmm1, __m128i xmm2)
{
    __asm__("pcmpeqq %1, %0" : "+x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

static inline __m128i __attribute__((always_inline)) _mm_min_epi8(__m128i xmm1, __m128i xmm2)
{
    __asm__("pminsb %1, %0" : "+x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

static inline __m128i __attribute__((always_inline)) _mm_max_epi8(__m128i xmm1, __m128i xmm2)
{
    __asm__("pmaxsb %1, %0" : "+x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

static inline __m128i __attribute__((always_inline)) _mm_max_epi32(__m128i xmm1, __m128i xmm2)
{
    __asm__("pmaxsd %1, %0" : "+x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

static inline __m128i __attribute__((always_inline)) _mm_min_epi32(__m128i xmm1, __m128i xmm2)
{
    __asm__("pminsd %1, %0" : "+x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

#pragma GCC diagnostic ignored "-Wuninitialized"
static inline __m128i __attribute__((always_inline)) _mm_cvtepi8_epi16(__m128i xmm2)
{
    __m128i xmm1;
    __asm__("pmovsxbw %1, %0" : "+x" (xmm1) : "xm" (xmm2));
    return xmm1;
}
static inline __m128i __attribute__((always_inline)) _mm_cvtepi16_epi32(__m128i xmm2)
{
    __m128i xmm1;
    __asm__("pmovsxwd %1, %0" : "+x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

static inline __m128i __attribute__((always_inline)) _mm_cvtepi32_epi64(__m128i xmm2)
{
    __m128i xmm1;
    __asm__("pmovsxdq %1, %0" : "+x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

#endif
#endif