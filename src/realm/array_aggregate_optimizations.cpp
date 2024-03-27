/*************************************************************************
 *
 * Copyright 2023 Realm Inc.
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

#include <realm/array.hpp>
#include <realm/array_with_find.hpp>

using namespace realm;

int64_t Array::sum(size_t start, size_t end) const
{
    REALM_TEMPEX(return sum, m_width, (start, end));
}

template <size_t w>
int64_t Array::sum(size_t start, size_t end) const
{
    if (end == size_t(-1))
        end = m_size;

    REALM_ASSERT_EX(end <= m_size && start <= end, start, end, m_size);

    if (start == end)
        return 0;

    int64_t s = 0;

    // Sum manually until 128 bit aligned
    for (; (start < end) && (((size_t(m_data) & 0xf) * 8 + start * w) % 128 != 0); start++) {
        s += get<w>(start);
    }

    if (w == 1 || w == 2 || w == 4) {
        // Sum of bitwidths less than a byte (which are always positive)
        // uses a divide and conquer algorithm that is a variation of popolation count:
        // http://graphics.stanford.edu/~seander/bithacks.html#CountBitsSetParallel

        // static values needed for fast sums
        const uint64_t m2 = 0x3333333333333333ULL;
        const uint64_t m4 = 0x0f0f0f0f0f0f0f0fULL;
        const uint64_t h01 = 0x0101010101010101ULL;

        int64_t* data = reinterpret_cast<int64_t*>(m_data + start * w / 8);
        size_t chunks = (end - start) * w / 8 / sizeof(int64_t);

        for (size_t t = 0; t < chunks; t++) {
            if (w == 1) {
#if 0
#if defined(USE_SSE42) && defined(_MSC_VER) && defined(REALM_PTR_64)
                s += __popcnt64(data[t]);
#elif !defined(_MSC_VER) && defined(USE_SSE42) && defined(REALM_PTR_64)
                s += __builtin_popcountll(data[t]);
#else
                uint64_t a = data[t];
                const uint64_t m1  = 0x5555555555555555ULL;
                a -= (a >> 1) & m1;
                a = (a & m2) + ((a >> 2) & m2);
                a = (a + (a >> 4)) & m4;
                a = (a * h01) >> 56;
                s += a;
#endif
#endif
                s += fast_popcount64(data[t]);
            }
            else if (w == 2) {
                uint64_t a = data[t];
                a = (a & m2) + ((a >> 2) & m2);
                a = (a + (a >> 4)) & m4;
                a = (a * h01) >> 56;

                s += a;
            }
            else if (w == 4) {
                uint64_t a = data[t];
                a = (a & m4) + ((a >> 4) & m4);
                a = (a * h01) >> 56;
                s += a;
            }
        }
        start += sizeof(int64_t) * 8 / no0(w) * chunks;
    }

#ifdef REALM_COMPILER_SSE
    if (sseavx<42>()) {
        // 2000 items summed 500000 times, 8/16/32 bits, miliseconds:
        // Naive, templated get<>: 391 371 374
        // SSE:                     97 148 282

        if ((w == 8 || w == 16 || w == 32) && end - start > sizeof(__m128i) * 8 / no0(w)) {
            __m128i* data = reinterpret_cast<__m128i*>(m_data + start * w / 8);
            __m128i sum_result = {0};
            __m128i sum2;

            size_t chunks = (end - start) * w / 8 / sizeof(__m128i);

            for (size_t t = 0; t < chunks; t++) {
                if (w == 8) {
                    /*
                    // 469 ms AND disadvantage of handling max 64k elements before overflow
                    __m128i vl = _mm_cvtepi8_epi16(data[t]);
                    __m128i vh = data[t];
                    vh.m128i_i64[0] = vh.m128i_i64[1];
                    vh = _mm_cvtepi8_epi16(vh);
                    sum_result = _mm_add_epi16(sum_result, vl);
                    sum_result = _mm_add_epi16(sum_result, vh);
                    */

                    /*
                    // 424 ms
                    __m128i vl = _mm_unpacklo_epi8(data[t], _mm_set1_epi8(0));
                    __m128i vh = _mm_unpackhi_epi8(data[t], _mm_set1_epi8(0));
                    sum_result = _mm_add_epi32(sum_result, _mm_madd_epi16(vl, _mm_set1_epi16(1)));
                    sum_result = _mm_add_epi32(sum_result, _mm_madd_epi16(vh, _mm_set1_epi16(1)));
                    */

                    __m128i vl = _mm_cvtepi8_epi16(data[t]); // sign extend lower words 8->16
                    __m128i vh = data[t];
                    vh = _mm_srli_si128(vh, 8); // v >>= 64
                    vh = _mm_cvtepi8_epi16(vh); // sign extend lower words 8->16
                    __m128i sum1 = _mm_add_epi16(vl, vh);
                    __m128i sumH = _mm_cvtepi16_epi32(sum1);
                    __m128i sumL = _mm_srli_si128(sum1, 8); // v >>= 64
                    sumL = _mm_cvtepi16_epi32(sumL);
                    sum_result = _mm_add_epi32(sum_result, sumL);
                    sum_result = _mm_add_epi32(sum_result, sumH);
                }
                else if (w == 16) {
                    // todo, can overflow for array size > 2^32
                    __m128i vl = _mm_cvtepi16_epi32(data[t]); // sign extend lower words 16->32
                    __m128i vh = data[t];
                    vh = _mm_srli_si128(vh, 8);  // v >>= 64
                    vh = _mm_cvtepi16_epi32(vh); // sign extend lower words 16->32
                    sum_result = _mm_add_epi32(sum_result, vl);
                    sum_result = _mm_add_epi32(sum_result, vh);
                }
                else if (w == 32) {
                    __m128i v = data[t];
                    __m128i v0 = _mm_cvtepi32_epi64(v); // sign extend lower dwords 32->64
                    v = _mm_srli_si128(v, 8);           // v >>= 64
                    __m128i v1 = _mm_cvtepi32_epi64(v); // sign extend lower dwords 32->64
                    sum_result = _mm_add_epi64(sum_result, v0);
                    sum_result = _mm_add_epi64(sum_result, v1);

                    /*
                    __m128i m = _mm_set1_epi32(0xc000);             // test if overflow could happen (still need
                    underflow test).
                    __m128i mm = _mm_and_si128(data[t], m);
                    zz = _mm_or_si128(mm, zz);
                    sum_result = _mm_add_epi32(sum_result, data[t]);
                    */
                }
            }
            start += sizeof(__m128i) * 8 / no0(w) * chunks;

            // prevent taking address of 'state' to make the compiler keep it in SSE register in above loop
            // (vc2010/gcc4.6)
            sum2 = sum_result;

            // Avoid aliasing bug where sum2 might not yet be initialized when accessed by get_universal
            char sum3[sizeof sum2];
            memcpy(&sum3, &sum2, sizeof sum2);

            // Sum elements of sum
            for (size_t t = 0; t < sizeof(__m128i) * 8 / ((w == 8 || w == 16) ? 32 : 64); ++t) {
                int64_t v = get_universal < (w == 8 || w == 16) ? 32 : 64 > (reinterpret_cast<char*>(&sum3), t);
                s += v;
            }
        }
    }
#endif

    // Sum remaining elements
    for (; start < end; ++start)
        s += get<w>(start);

    return s;
}

size_t Array::count(int64_t value) const noexcept
{
    // This is not used anywhere in the code, I believe we can delete this
    // since the query logic does not use this
    const uint64_t* next = reinterpret_cast<uint64_t*>(m_data);
    size_t value_count = 0;
    const size_t end = m_size;
    size_t i = 0;

    // static values needed for fast population count
    const uint64_t m1 = 0x5555555555555555ULL;
    const uint64_t m2 = 0x3333333333333333ULL;
    const uint64_t m4 = 0x0f0f0f0f0f0f0f0fULL;
    const uint64_t h01 = 0x0101010101010101ULL;

    if (m_width == 0) {
        if (value == 0)
            return m_size;
        return 0;
    }
    if (m_width == 1) {
        if (uint64_t(value) > 1)
            return 0;

        const size_t chunkvals = 64;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            if (value == 0)
                a = ~a; // reverse

            a -= (a >> 1) & m1;
            a = (a & m2) + ((a >> 2) & m2);
            a = (a + (a >> 4)) & m4;
            a = (a * h01) >> 56;

            // Could use intrinsic instead:
            // a = __builtin_popcountll(a); // gcc intrinsic

            value_count += to_size_t(a);
        }
    }
    else if (m_width == 2) {
        if (uint64_t(value) > 3)
            return 0;

        const uint64_t v = ~0ULL / 0x3 * value;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0x3 * 0x1;

        const size_t chunkvals = 32;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;             // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a &= m1;            // isolate single bit in each segment
            a ^= m1;            // reverse isolated bits
            // if (!a) continue;

            // Population count
            a = (a & m2) + ((a >> 2) & m2);
            a = (a + (a >> 4)) & m4;
            a = (a * h01) >> 56;

            value_count += to_size_t(a);
        }
    }
    else if (m_width == 4) {
        if (uint64_t(value) > 15)
            return 0;

        const uint64_t v = ~0ULL / 0xF * value;
        const uint64_t m = ~0ULL / 0xF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0xF * 0x7;
        const uint64_t c2 = ~0ULL / 0xF * 0x3;

        const size_t chunkvals = 16;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;             // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a |= (a >> 2) & c2;
            a &= m; // isolate single bit in each segment
            a ^= m; // reverse isolated bits

            // Population count
            a = (a + (a >> 4)) & m4;
            a = (a * h01) >> 56;

            value_count += to_size_t(a);
        }
    }
    else if (m_width == 8) {
        if (value > 0x7FLL || value < -0x80LL)
            return 0; // by casting?

        const uint64_t v = ~0ULL / 0xFF * value;
        const uint64_t m = ~0ULL / 0xFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0xFF * 0x7F;
        const uint64_t c2 = ~0ULL / 0xFF * 0x3F;
        const uint64_t c3 = ~0ULL / 0xFF * 0x0F;

        const size_t chunkvals = 8;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;             // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a |= (a >> 2) & c2;
            a |= (a >> 4) & c3;
            a &= m; // isolate single bit in each segment
            a ^= m; // reverse isolated bits

            // Population count
            a = (a * h01) >> 56;

            value_count += to_size_t(a);
        }
    }
    else if (m_width == 16) {
        if (value > 0x7FFFLL || value < -0x8000LL)
            return 0; // by casting?

        const uint64_t v = ~0ULL / 0xFFFF * value;
        const uint64_t m = ~0ULL / 0xFFFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0xFFFF * 0x7FFF;
        const uint64_t c2 = ~0ULL / 0xFFFF * 0x3FFF;
        const uint64_t c3 = ~0ULL / 0xFFFF * 0x0FFF;
        const uint64_t c4 = ~0ULL / 0xFFFF * 0x00FF;

        const size_t chunkvals = 4;
        for (; i + chunkvals <= end; i += chunkvals) {
            uint64_t a = next[i / chunkvals];
            a ^= v;             // zero matching bit segments
            a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
            a |= (a >> 2) & c2;
            a |= (a >> 4) & c3;
            a |= (a >> 8) & c4;
            a &= m; // isolate single bit in each segment
            a ^= m; // reverse isolated bits

            // Population count
            a = (a * h01) >> 56;

            value_count += to_size_t(a);
        }
    }
    else if (m_width == 32) {
        int32_t v = int32_t(value);
        const int32_t* d = reinterpret_cast<int32_t*>(m_data);
        for (; i < end; ++i) {
            if (d[i] == v)
                ++value_count;
        }
        return value_count;
    }
    else if (m_width == 64) {
        const int64_t* d = reinterpret_cast<int64_t*>(m_data);
        for (; i < end; ++i) {
            if (d[i] == value)
                ++value_count;
        }
        return value_count;
    }

    // Check remaining elements
    for (; i < end; ++i)
        if (value == get(i))
            ++value_count;

    return value_count;
}
