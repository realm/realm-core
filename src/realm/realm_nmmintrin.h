/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_NMMINTRIN_H
#define REALM_NMMINTRIN_H

/*
    We must support runtime detection of CPU support of SSE when distributing Realm as a closed source library.

    This is a problem on gcc and llvm: To use SSE intrinsics we need to pass -msse on the command line (to get offered
    __builtin_ accessors used by intrinsics functions). However, the -msse flag allows gcc to emit SSE instructions
    in its code generation/optimization. This is unwanted because the binary would crash on non-SSE CPUs.

    Since there exists no flag in gcc that enables intrinsics but probits SSE in code generation, we define our
    own intrinsics to be assembled by the back end assembler and omit passing -msse to gcc.
*/

#if !defined(_MSC_VER) && defined(REALM_COMPILER_SSE)

#include <emmintrin.h> // SSE2 (using __m128i)

namespace realm {

// Instructions introduced by SSE 3 and 4.2
static inline __m128i _mm_cmpgt_epi64(__m128i xmm1, __m128i xmm2)
{
    __asm__("pcmpgtq %1, %0" : "+x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

static inline __m128i _mm_cmpeq_epi64(__m128i xmm1, __m128i xmm2)
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

static inline __m128i __attribute__((always_inline)) _mm_cvtepi8_epi16(__m128i xmm2)
{
    __m128i xmm1;
    __asm__("pmovsxbw %1, %0" : "=x" (xmm1) : "xm" (xmm2) : "xmm1");
    return xmm1;
}
static inline __m128i __attribute__((always_inline)) _mm_cvtepi16_epi32(__m128i xmm2)
{
    __m128i xmm1;
    asm("pmovsxwd %1, %0" : "=x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

static inline __m128i __attribute__((always_inline)) _mm_cvtepi32_epi64(__m128i xmm2)
{
    __m128i xmm1;
    __asm__("pmovsxdq %1, %0" : "=x" (xmm1) : "xm" (xmm2));
    return xmm1;
}

} // namespace realm

#endif // !defined(_MSC_VER) && defined(REALM_COMPILER_SSE)
#endif
