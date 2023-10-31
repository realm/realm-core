/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
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

/*
Searching: The main finding function is:
    template <class cond, size_t bitwidth>
    void find(int64_t value, size_t start, size_t end, size_t baseindex, QueryState *state) const

    cond:       One of Equal, NotEqual, Greater, etc. classes

    find() will call QueryStateBase::match() for each search result. If match()
    returns false, find() will exit, otherwise it will keep searching remaining
    items in array.
*/

#ifndef REALM_ARRAY_WITH_FIND_HPP
#define REALM_ARRAY_WITH_FIND_HPP

#include <realm/array.hpp>
#include <realm/query_conditions.hpp>

/*
    MMX: mmintrin.h
    SSE: xmmintrin.h
    SSE2: emmintrin.h
    SSE3: pmmintrin.h
    SSSE3: tmmintrin.h
    SSE4A: ammintrin.h
    SSE4.1: smmintrin.h
    SSE4.2: nmmintrin.h
*/
#ifdef REALM_COMPILER_SSE
#include <emmintrin.h>             // SSE2
#include <realm/realm_nmmintrin.h> // SSE42
#endif

namespace realm {

namespace {
template <class T>
inline constexpr T no0(T v)
{
    return v == 0 ? 1 : v;
}

template <size_t width>
inline constexpr uint64_t lower_bits()
{
    if (width == 1)
        return 0xFFFFFFFFFFFFFFFFULL;
    else if (width == 2)
        return 0x5555555555555555ULL;
    else if (width == 4)
        return 0x1111111111111111ULL;
    else if (width == 8)
        return 0x0101010101010101ULL;
    else if (width == 16)
        return 0x0001000100010001ULL;
    else if (width == 32)
        return 0x0000000100000001ULL;
    else if (width == 64)
        return 0x0000000000000001ULL;
    else {
        return uint64_t(-1);
    }
}
} // namespace

class ArrayWithFind {
public:
    ArrayWithFind(const Array& array) noexcept
        : m_array(array)
    {
    }

    // Main finding function - used for find_first, find_all, sum, max, min, etc.
    bool find(int cond, int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const;

    template <class cond>
    bool find(int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const;

    void find_all(IntegerColumn* result, int64_t value, size_t col_offset = 0, size_t begin = 0,
                  size_t end = size_t(-1)) const;

    // Non-SSE find for the four functions Equal/NotEqual/Less/Greater
    template <class cond, size_t bitwidth>
    bool compare(int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const;

    // Non-SSE find for Equal/NotEqual
    template <bool eq, size_t width>
    inline bool compare_equality(int64_t value, size_t start, size_t end, size_t baseindex,
                                 QueryStateBase* state) const;

    // Non-SSE find for Less/Greater
    template <bool gt, size_t bitwidth>
    bool compare_relation(int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const;

    template <class cond, size_t foreign_width, size_t width>
    bool compare_leafs_4(const Array* foreign, size_t start, size_t end, size_t baseindex,
                         QueryStateBase* state) const;

    template <class cond>
    bool compare_leafs(const Array* foreign, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const;

    template <class cond, size_t width>
    bool compare_leafs(const Array* foreign, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const;

// SSE find for the four functions Equal/NotEqual/Less/Greater
#ifdef REALM_COMPILER_SSE
    template <class cond, size_t width>
    bool find_sse(int64_t value, __m128i* data, size_t items, QueryStateBase* state, size_t baseindex) const;

    template <class cond, size_t width>
    REALM_FORCEINLINE bool find_sse_intern(__m128i* action_data, __m128i* data, size_t items, QueryStateBase* state,
                                           size_t baseindex) const;

#endif

    template <size_t width>
    inline bool test_zero(uint64_t value) const; // Tests value for 0-elements

    template <bool eq, size_t width>
    size_t find_zero(uint64_t v) const; // Finds position of 0/non-zero element

    template <size_t width, bool zero>
    uint64_t cascade(uint64_t a) const; // Sets lowermost bits of zero or non-zero elements

    template <bool gt, size_t width>
    int64_t
    find_gtlt_magic(int64_t v) const; // Compute magic constant needed for searching for value 'v' using bit hacks

    size_t first_set_bit(uint32_t v) const;
    size_t first_set_bit64(int64_t v) const;

    // Find value greater/less in 64-bit chunk - only works for positive values
    template <bool gt, size_t width>
    bool find_gtlt_fast(uint64_t chunk, uint64_t magic, QueryStateBase* state, size_t baseindex) const;

    // Find value greater/less in 64-bit chunk - no constraints
    template <bool gt, size_t width>
    bool find_gtlt(int64_t v, uint64_t chunk, QueryStateBase* state, size_t baseindex) const;
    // Optimized implementation for release mode
    template <class cond, size_t bitwidth>
    bool find_optimized(int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const;

private:
    const Array& m_array;

    template <size_t bitwidth>
    bool find_all_will_match(size_t start, size_t end, size_t baseindex, QueryStateBase* state) const;
};
//*************************************************************************************
// Finding code                                                                       *
//*************************************************************************************

template <size_t width, bool zero>
uint64_t ArrayWithFind::cascade(uint64_t a) const
{
    // Takes a chunk of values as argument and sets the least significant bit for each
    // element which is zero or non-zero, depending on the template parameter.
    // Example for zero=true:
    // width == 4 and a = 0x5fd07a107610f610
    // will return:       0x0001000100010001

    // static values needed for fast population count
    const uint64_t m1 = 0x5555555555555555ULL;

    if (width == 1) {
        return zero ? ~a : a;
    }
    else if (width == 2) {
        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0x3 * 0x1;

        a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
        a &= m1;            // isolate single bit in each segment
        if (zero)
            a ^= m1; // reverse isolated bits if checking for zeroed segments

        return a;
    }
    else if (width == 4) {
        const uint64_t m = ~0ULL / 0xF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0xF * 0x7;
        const uint64_t c2 = ~0ULL / 0xF * 0x3;

        a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
        a |= (a >> 2) & c2;
        a &= m; // isolate single bit in each segment
        if (zero)
            a ^= m; // reverse isolated bits if checking for zeroed segments

        return a;
    }
    else if (width == 8) {
        const uint64_t m = ~0ULL / 0xFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0xFF * 0x7F;
        const uint64_t c2 = ~0ULL / 0xFF * 0x3F;
        const uint64_t c3 = ~0ULL / 0xFF * 0x0F;

        a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
        a |= (a >> 2) & c2;
        a |= (a >> 4) & c3;
        a &= m; // isolate single bit in each segment
        if (zero)
            a ^= m; // reverse isolated bits if checking for zeroed segments

        return a;
    }
    else if (width == 16) {
        const uint64_t m = ~0ULL / 0xFFFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0xFFFF * 0x7FFF;
        const uint64_t c2 = ~0ULL / 0xFFFF * 0x3FFF;
        const uint64_t c3 = ~0ULL / 0xFFFF * 0x0FFF;
        const uint64_t c4 = ~0ULL / 0xFFFF * 0x00FF;

        a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
        a |= (a >> 2) & c2;
        a |= (a >> 4) & c3;
        a |= (a >> 8) & c4;
        a &= m; // isolate single bit in each segment
        if (zero)
            a ^= m; // reverse isolated bits if checking for zeroed segments

        return a;
    }

    else if (width == 32) {
        const uint64_t m = ~0ULL / 0xFFFFFFFF * 0x1;

        // Masks to avoid spillover between segments in cascades
        const uint64_t c1 = ~0ULL / 0xFFFFFFFF * 0x7FFFFFFF;
        const uint64_t c2 = ~0ULL / 0xFFFFFFFF * 0x3FFFFFFF;
        const uint64_t c3 = ~0ULL / 0xFFFFFFFF * 0x0FFFFFFF;
        const uint64_t c4 = ~0ULL / 0xFFFFFFFF * 0x00FFFFFF;
        const uint64_t c5 = ~0ULL / 0xFFFFFFFF * 0x0000FFFF;

        a |= (a >> 1) & c1; // cascade ones in non-zeroed segments
        a |= (a >> 2) & c2;
        a |= (a >> 4) & c3;
        a |= (a >> 8) & c4;
        a |= (a >> 16) & c5;
        a &= m; // isolate single bit in each segment
        if (zero)
            a ^= m; // reverse isolated bits if checking for zeroed segments

        return a;
    }
    else if (width == 64) {
        return (a == 0) == zero;
    }
    else {
        REALM_ASSERT_DEBUG(false);
        return uint64_t(-1);
    }
}

template <size_t bitwidth>
REALM_NOINLINE bool ArrayWithFind::find_all_will_match(size_t start2, size_t end, size_t baseindex,
                                                       QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(state->match_count() < state->limit());
    size_t process = state->limit() - state->match_count();
    size_t end2 = end - start2 > process ? start2 + process : end;
    for (; start2 < end2; start2++)
        if (!state->match(start2 + baseindex))
            return false;
    return true;
}

// This is the main finding function for Array. Other finding functions are just
// wrappers around this one. Search for 'value' using condition cond (Equal,
// NotEqual, Less, etc) and call QueryStateBase::match() for each match. Break and
// return if QueryStateBase::match() returns false or 'end' is reached.
template <class cond, size_t bitwidth>
bool ArrayWithFind::find_optimized(int64_t value, size_t start, size_t end, size_t baseindex,
                                   QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(start <= m_array.m_size && (end <= m_array.m_size || end == size_t(-1)) && start <= end);

    size_t start2 = start;
    cond c;

    if (end == npos)
        end = m_array.m_size;

    if (!(m_array.m_size > start2 && start2 < end))
        return true;

    constexpr int64_t lbound = Array::lbound_for_width(bitwidth);
    constexpr int64_t ubound = Array::ubound_for_width(bitwidth);

    // Return immediately if no items in array can match (such as if cond == Greater && value == 100 &&
    // m_ubound == 15)
    if (!c.can_match(value, lbound, ubound))
        return true;

    // optimization if all items are guaranteed to match (such as cond == NotEqual && value == 100 && m_ubound == 15)
    if (c.will_match(value, lbound, ubound)) {
        return find_all_will_match<bitwidth>(start2, end, baseindex, state);
    }

    // finder cannot handle this bitwidth
    REALM_ASSERT_3(m_array.m_width, !=, 0);

#if defined(REALM_COMPILER_SSE)
    // Only use SSE if payload is at least one SSE chunk (128 bits) in size. Also note taht SSE doesn't support
    // Less-than comparison for 64-bit values.
    if ((!(std::is_same<cond, Less>::value && m_array.m_width == 64)) && end - start2 >= sizeof(__m128i) &&
        m_array.m_width >= 8 &&
        (sseavx<42>() || (sseavx<30>() && std::is_same<cond, Equal>::value && m_array.m_width < 64))) {

        // find_sse() must start2 at 16-byte boundary, so search area before that using compare_equality()
        __m128i* const a =
            reinterpret_cast<__m128i*>(round_up(m_array.m_data + start2 * bitwidth / 8, sizeof(__m128i)));
        __m128i* const b =
            reinterpret_cast<__m128i*>(round_down(m_array.m_data + end * bitwidth / 8, sizeof(__m128i)));

        if (!compare<cond, bitwidth>(value, start2, (reinterpret_cast<char*>(a) - m_array.m_data) * 8 / no0(bitwidth),
                                     baseindex, state))
            return false;

        // Search aligned area with SSE
        if (b > a) {
            if (sseavx<42>()) {
                if (!find_sse<cond, bitwidth>(
                        value, a, b - a, state,
                        baseindex + ((reinterpret_cast<char*>(a) - m_array.m_data) * 8 / no0(bitwidth))))
                    return false;
            }
            else if (sseavx<30>()) {

                if (!find_sse<Equal, bitwidth>(
                        value, a, b - a, state,
                        baseindex + ((reinterpret_cast<char*>(a) - m_array.m_data) * 8 / no0(bitwidth))))
                    return false;
            }
        }

        // Search remainder with compare_equality()
        if (!compare<cond, bitwidth>(value, (reinterpret_cast<char*>(b) - m_array.m_data) * 8 / no0(bitwidth), end,
                                     baseindex, state))
            return false;

        return true;
    }
    else {
        return compare<cond, bitwidth>(value, start2, end, baseindex, state);
    }
#else
    return compare<cond, bitwidth>(value, start2, end, baseindex, state);
#endif
}

// Tests if any chunk in 'value' is 0
template <size_t width>
inline bool ArrayWithFind::test_zero(uint64_t value) const
{
    uint64_t hasZeroByte;
    constexpr uint64_t lower = lower_bits<width>();
    constexpr uint64_t upper = lower_bits<width>() * 1ULL << (width == 0 ? 0 : (width - 1ULL));
    hasZeroByte = (value - lower) & ~value & upper;
    return hasZeroByte != 0;
}

// Finds first zero (if eq == true) or non-zero (if eq == false) element in v and returns its position.
// IMPORTANT: This function assumes that at least 1 item matches (test this with test_zero() or other means first)!
template <bool eq, size_t width>
size_t ArrayWithFind::find_zero(uint64_t v) const
{
    size_t start = 0;
    uint64_t hasZeroByte;
    // Warning free way of computing (1ULL << width) - 1
    uint64_t mask = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL));

    if (eq == (((v >> (width * start)) & mask) == 0)) {
        return 0;
    }

    // Bisection optimization, speeds up small bitwidths with high match frequency. More partions than 2 do NOT pay
    // off because the work done by test_zero() is wasted for the cases where the value exists in first half, but
    // useful if it exists in last half. Sweet spot turns out to be the widths and partitions below.
    if (width <= 8) {
        hasZeroByte = test_zero<width>(v | 0xffffffff00000000ULL);
        if (eq ? !hasZeroByte : (v & 0x00000000ffffffffULL) == 0) {
            // 00?? -> increasing
            start += 64 / no0(width) / 2;
            if (width <= 4) {
                hasZeroByte = test_zero<width>(v | 0xffff000000000000ULL);
                if (eq ? !hasZeroByte : (v & 0x0000ffffffffffffULL) == 0) {
                    // 000?
                    start += 64 / no0(width) / 4;
                }
            }
        }
        else {
            if (width <= 4) {
                // ??00
                hasZeroByte = test_zero<width>(v | 0xffffffffffff0000ULL);
                if (eq ? !hasZeroByte : (v & 0x000000000000ffffULL) == 0) {
                    // 0?00
                    start += 64 / no0(width) / 4;
                }
            }
        }
    }

    while (eq == (((v >> (width * start)) & mask) != 0)) {
        // You must only call find_zero() if you are sure that at least 1 item matches
        REALM_ASSERT_3(start, <=, 8 * sizeof(v));
        start++;
    }

    return start;
}

// Generate a magic constant used for later bithacks
template <bool gt, size_t width>
int64_t ArrayWithFind::find_gtlt_magic(int64_t v) const
{
    uint64_t mask1 =
        (width == 64
             ? ~0ULL
             : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
    uint64_t mask2 = mask1 >> 1;
    uint64_t magic = gt ? (~0ULL / no0(mask1) * (mask2 - v)) : (~0ULL / no0(mask1) * v);
    return magic;
}

template <bool gt, size_t width>
bool ArrayWithFind::find_gtlt_fast(uint64_t chunk, uint64_t magic, QueryStateBase* state, size_t baseindex) const
{
    // Tests if a a chunk of values contains values that are greater (if gt == true) or less (if gt == false) than v.
    // Fast, but limited to work when all values in the chunk are positive.

    uint64_t mask1 =
        (width == 64
             ? ~0ULL
             : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
    uint64_t mask2 = mask1 >> 1;
    uint64_t m = gt ? (((chunk + magic) | chunk) & ~0ULL / no0(mask1) * (mask2 + 1))
                    : ((chunk - magic) & ~chunk & ~0ULL / no0(mask1) * (mask2 + 1));
    size_t p = 0;
    while (m) {
        size_t t = first_set_bit64(m) / no0(width);
        p += t;
        if (!state->match(p + baseindex, Mixed{int64_t((chunk >> (p * width)) & mask1)}))
            return false;

        if ((t + 1) * width == 64)
            m = 0;
        else
            m >>= (t + 1) * width;
        p++;
    }

    return true;
}

// clang-format off
template <bool gt, size_t width>
bool ArrayWithFind::find_gtlt(int64_t v, uint64_t chunk, QueryStateBase* state, size_t baseindex) const
{
    // Find items in 'chunk' that are greater (if gt == true) or smaller (if gt == false) than 'v'. Fixme, __forceinline can make it crash in vS2010 - find out why
    if constexpr (width == 1) {
        for (size_t i = 0; i < 64; ++i) {
            int64_t v2 = static_cast<int64_t>(chunk & 0x1);
            if (gt ? v2 > v : v2 < v) {
                if (!state->match(i + baseindex, v2)) {
                    return false;
                }
            }
            chunk >>= 1;
        }
    }
    else if constexpr (width == 2) {
        for (size_t i = 0; i < 32; ++i) {
            int64_t v2 = static_cast<int64_t>(chunk & 0x3);
            if (gt ? v2 > v : v2 < v) {
                if (!state->match(i + baseindex, v2)) {
                    return false;
                }
            }
            chunk >>= 2;
        }
    }
    else if constexpr (width == 4) {
        for (size_t i = 0; i < 16; ++i) {
            int64_t v2 = static_cast<int64_t>(chunk & 0xf);
            if (gt ? v2 > v : v2 < v) {
                if (!state->match(i + baseindex, v2)) {
                    return false;
                }
            }
            chunk >>= 4;
        }
    }
    else if constexpr (width == 8) {
        for (size_t i = 0; i < 8; ++i) {
            int64_t v2 = static_cast<int64_t>(static_cast<int8_t>(chunk & 0xff));
            if (gt ? v2 > v : v2 < v) {
                if (!state->match(i + baseindex, v2)) {
                    return false;
                }
            }
            chunk >>= 8;
        }
    }
    else if constexpr (width == 16) {
        for (size_t i = 0; i < 4; ++i) {
            int64_t v2 = static_cast<int64_t>(static_cast<int16_t>(chunk & 0xffff));
            if (gt ? v2 > v : v2 < v) {
                if (!state->match(i + baseindex, v2)) {
                    return false;
                }
            }
            chunk >>= 16;
        }
    }
    else if constexpr (width == 32) {
        for (size_t i = 0; i < 2; ++i) {
            int64_t v2 = static_cast<int64_t>(static_cast<int32_t>(chunk & 0xffffffff));
            if (gt ? v2 > v : v2 < v) {
                if (!state->match(i + baseindex, v2)) {
                    return false;
                }
            }
            chunk >>= 32;
        }
    }
    else if constexpr (width == 64) {
        int64_t v2 = static_cast<int64_t>(chunk);
        if (gt ? v2 > v : v2 < v) {
            return state->match(baseindex, v2);
        }
    }

    static_cast<void>(state);
    return true;
}
// clang-format on

/// Find items in this Array that are equal (eq == true) or different (eq = false) from 'value'
template <bool eq, size_t width>
inline bool ArrayWithFind::compare_equality(int64_t value, size_t start, size_t end, size_t baseindex,
                                            QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(start <= m_array.m_size && (end <= m_array.m_size || end == size_t(-1)) && start <= end);

    size_t ee = round_up(start, 64 / no0(width));
    ee = ee > end ? end : ee;
    for (; start < ee; ++start)
        if (eq ? (m_array.get<width>(start) == value) : (m_array.get<width>(start) != value)) {
            if (!state->match(start + baseindex))
                return false;
        }

    if (start >= end)
        return true;

    if (width != 32 && width != 64) {
        const int64_t* p = reinterpret_cast<const int64_t*>(m_array.m_data + (start * width / 8));
        const int64_t* const e = reinterpret_cast<int64_t*>(m_array.m_data + (end * width / 8)) - 1;
        const uint64_t mask =
            (width == 64
                 ? ~0ULL
                 : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
        const uint64_t valuemask =
            ~0ULL / no0(mask) * (value & mask); // the "== ? :" is to avoid division by 0 compiler error

        while (p < e) {
            uint64_t chunk = *p;
            uint64_t v2 = chunk ^ valuemask;
            start = (p - reinterpret_cast<int64_t*>(m_array.m_data)) * 8 * 8 / no0(width);
            size_t a = 0;

            while (eq ? test_zero<width>(v2) : v2) {
                size_t t = find_zero<eq, width>(v2);
                a += t;

                if (a >= 64 / no0(width))
                    break;

                if (!state->match(a + start + baseindex)) {
                    return false;
                }
                auto shift = (t + 1) * width;
                if (shift < 64)
                    v2 >>= shift;
                else
                    v2 = 0;
                a += 1;
            }

            ++p;
        }

        // Loop ended because we are near end or end of array. No need to optimize search in remainder in this case
        // because end of array means that
        // lots of search work has taken place prior to ending here. So time spent searching remainder is relatively
        // tiny
        start = (p - reinterpret_cast<int64_t*>(m_array.m_data)) * 8 * 8 / no0(width);
    }

    while (start < end) {
        if (eq ? m_array.get<width>(start) == value : m_array.get<width>(start) != value) {
            if (!state->match(start + baseindex)) {
                return false;
            }
        }
        ++start;
    }

    return true;
}

// There exists a couple of find() functions that take more or less template arguments. Always call the one that
// takes as most as possible to get best performance.

template <class cond>
bool ArrayWithFind::find(int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const
{
    REALM_TEMPEX2(return find_optimized, cond, m_array.m_width, (value, start, end, baseindex, state));
}

#ifdef REALM_COMPILER_SSE
// 'items' is the number of 16-byte SSE chunks. Returns index of packed element relative to first integer of first
// chunk
template <class cond, size_t width>
bool ArrayWithFind::find_sse(int64_t value, __m128i* data, size_t items, QueryStateBase* state,
                             size_t baseindex) const
{
    __m128i search = {0};

    if (width == 8)
        search = _mm_set1_epi8(static_cast<char>(value));
    else if (width == 16)
        search = _mm_set1_epi16(static_cast<short int>(value));
    else if (width == 32)
        search = _mm_set1_epi32(static_cast<int>(value));
    else if (width == 64) {
        if (std::is_same<cond, Less>::value)
            REALM_ASSERT(false);
        else
            search = _mm_set_epi64x(value, value);
    }

    return find_sse_intern<cond, width>(data, &search, items, state, baseindex);
}

// Compares packed action_data with packed data (equal, less, etc) and performs aggregate action (max, min, sum,
// find_all, etc) on value inside action_data for first match, if any
template <class cond, size_t width>
REALM_FORCEINLINE bool ArrayWithFind::find_sse_intern(__m128i* action_data, __m128i* data, size_t items,
                                                      QueryStateBase* state, size_t baseindex) const
{
    size_t i = 0;
    __m128i compare_result = {0};
    unsigned int resmask;

    // Search loop. Unrolling it has been tested to NOT increase performance (apparently mem bound)
    for (i = 0; i < items; ++i) {
        // equal / not-equal
        if (std::is_same<cond, Equal>::value || std::is_same<cond, NotEqual>::value) {
            if (width == 8)
                compare_result = _mm_cmpeq_epi8(action_data[i], *data);
            if (width == 16)
                compare_result = _mm_cmpeq_epi16(action_data[i], *data);
            if (width == 32)
                compare_result = _mm_cmpeq_epi32(action_data[i], *data);
            if (width == 64) {
                compare_result = _mm_cmpeq_epi64(action_data[i], *data); // SSE 4.2 only
            }
        }

        // greater
        else if (std::is_same<cond, Greater>::value) {
            if (width == 8)
                compare_result = _mm_cmpgt_epi8(action_data[i], *data);
            if (width == 16)
                compare_result = _mm_cmpgt_epi16(action_data[i], *data);
            if (width == 32)
                compare_result = _mm_cmpgt_epi32(action_data[i], *data);
            if (width == 64)
                compare_result = _mm_cmpgt_epi64(action_data[i], *data);
        }
        // less
        else if (std::is_same<cond, Less>::value) {
            if (width == 8)
                compare_result = _mm_cmplt_epi8(action_data[i], *data);
            else if (width == 16)
                compare_result = _mm_cmplt_epi16(action_data[i], *data);
            else if (width == 32)
                compare_result = _mm_cmplt_epi32(action_data[i], *data);
            else
                REALM_ASSERT(false);
        }

        resmask = _mm_movemask_epi8(compare_result);

        if (std::is_same<cond, NotEqual>::value)
            resmask = ~resmask & 0x0000ffff;

        size_t s = i * sizeof(__m128i) * 8 / no0(width);

        while (resmask != 0) {
            size_t idx = first_set_bit(resmask) * 8 / no0(width);
            s += idx;
            if (!state->match(s + baseindex))
                return false;
            resmask >>= (idx + 1) * no0(width) / 8;
            ++s;
        }
    }

    return true;
}
#endif // REALM_COMPILER_SSE

template <class cond>
bool ArrayWithFind::compare_leafs(const Array* foreign, size_t start, size_t end, size_t baseindex,
                                  QueryStateBase* state) const
{
    cond c;
    REALM_ASSERT_3(start, <=, end);
    if (start == end)
        return true;


    int64_t v;

    // We can compare first element without checking for out-of-range
    v = m_array.get(start);
    if (c(v, foreign->get(start))) {
        if (!state->match(start + baseindex, v))
            return false;
    }

    start++;

    if (start + 3 < end) {
        v = m_array.get(start);
        if (c(v, foreign->get(start)))
            if (!state->match(start + baseindex, v))
                return false;

        v = m_array.get(start + 1);
        if (c(v, foreign->get(start + 1)))
            if (!state->match(start + 1 + baseindex, v))
                return false;

        v = m_array.get(start + 2);
        if (c(v, foreign->get(start + 2)))
            if (!state->match(start + 2 + baseindex, v))
                return false;

        start += 3;
    }
    else if (start == end) {
        return true;
    }

    bool r;
    REALM_TEMPEX2(r = compare_leafs, cond, m_array.m_width, (foreign, start, end, baseindex, state))
    return r;
}


template <class cond, size_t width>
bool ArrayWithFind::compare_leafs(const Array* foreign, size_t start, size_t end, size_t baseindex,
                                  QueryStateBase* state) const
{
    size_t fw = foreign->m_width;
    bool r;
    REALM_TEMPEX3(r = compare_leafs_4, cond, width, fw, (foreign, start, end, baseindex, state))
    return r;
}


template <class cond, size_t width, size_t foreign_width>
bool ArrayWithFind::compare_leafs_4(const Array* foreign, size_t start, size_t end, size_t baseindex,
                                    QueryStateBase* state) const
{
    cond c;
    char* foreign_m_data = foreign->m_data;

    if (width == 0 && foreign_width == 0) {
        if (c(0, 0)) {
            while (start < end) {
                if (!state->match(start + baseindex, 0))
                    return false;
                start++;
            }
        }
        else {
            return true;
        }
    }


#if defined(REALM_COMPILER_SSE)
    if (sseavx<42>() && width == foreign_width && (width == 8 || width == 16 || width == 32)) {
        // We can only use SSE if both bitwidths are equal and above 8 bits and all values are signed
        // and the two arrays are aligned the same way
        if ((reinterpret_cast<size_t>(m_array.m_data) & 0xf) == (reinterpret_cast<size_t>(foreign_m_data) & 0xf)) {
            while (start < end &&
                   (((reinterpret_cast<size_t>(m_array.m_data) & 0xf) * 8 + start * width) % (128) != 0)) {
                int64_t v = m_array.get_universal<width>(m_array.m_data, start);
                int64_t fv = m_array.get_universal<foreign_width>(foreign_m_data, start);
                if (c(v, fv)) {
                    if (!state->match(start + baseindex, v))
                        return false;
                }
                start++;
            }
            if (start == end)
                return true;


            size_t sse_items = (end - start) * width / 128;
            size_t sse_end = start + sse_items * 128 / no0(width);

            while (start < sse_end) {
                __m128i* a = reinterpret_cast<__m128i*>(m_array.m_data + start * width / 8);
                __m128i* b = reinterpret_cast<__m128i*>(foreign_m_data + start * width / 8);

                bool continue_search = find_sse_intern<cond, width>(a, b, 1, state, baseindex + start);

                if (!continue_search)
                    return false;

                start += 128 / no0(width);
            }
        }
    }
#endif

    while (start < end) {
        int64_t v = m_array.get_universal<width>(m_array.m_data, start);
        int64_t fv = m_array.get_universal<foreign_width>(foreign_m_data, start);

        if (c(v, fv)) {
            if (!state->match(start + baseindex, v))
                return false;
        }

        start++;
    }

    return true;
}


template <class cond, size_t bitwidth>
bool ArrayWithFind::compare(int64_t value, size_t start, size_t end, size_t baseindex, QueryStateBase* state) const
{
    bool ret = false;

    if (std::is_same<cond, Equal>::value)
        ret = compare_equality<true, bitwidth>(value, start, end, baseindex, state);
    else if (std::is_same<cond, NotEqual>::value)
        ret = compare_equality<false, bitwidth>(value, start, end, baseindex, state);
    else if (std::is_same<cond, Greater>::value)
        ret = compare_relation<true, bitwidth>(value, start, end, baseindex, state);
    else if (std::is_same<cond, Less>::value)
        ret = compare_relation<false, bitwidth>(value, start, end, baseindex, state);
    else
        REALM_ASSERT_DEBUG(false);

    return ret;
}

template <bool gt, size_t bitwidth>
bool ArrayWithFind::compare_relation(int64_t value, size_t start, size_t end, size_t baseindex,
                                     QueryStateBase* state) const
{
    REALM_ASSERT(start <= m_array.m_size && (end <= m_array.m_size || end == size_t(-1)) && start <= end);
    uint64_t mask = (bitwidth == 64 ? ~0ULL
                                    : ((1ULL << (bitwidth == 64 ? 0 : bitwidth)) -
                                       1ULL)); // Warning free way of computing (1ULL << width) - 1

    size_t ee = round_up(start, 64 / no0(bitwidth));
    ee = ee > end ? end : ee;
    for (; start < ee; start++) {
        if (gt ? (m_array.get<bitwidth>(start) > value) : (m_array.get<bitwidth>(start) < value)) {
            if (!state->match(start + baseindex, m_array.get<bitwidth>(start)))
                return false;
        }
    }

    if (start >= end)
        return true; // none found, continue (return true) regardless what QueryStateBase->match() would have returned
                     // on match

    const int64_t* p = reinterpret_cast<const int64_t*>(m_array.m_data + (start * bitwidth / 8));
    const int64_t* const e = reinterpret_cast<int64_t*>(m_array.m_data + (end * bitwidth / 8)) - 1;

    // Matches are rare enough to setup fast linear search for remaining items. We use
    // bit hacks from http://graphics.stanford.edu/~seander/bithacks.html#HasLessInWord

    if (bitwidth == 1 || bitwidth == 2 || bitwidth == 4 || bitwidth == 8 || bitwidth == 16) {
        uint64_t magic = find_gtlt_magic<gt, bitwidth>(value);

        // Bit hacks only work if searched item has its most significant bit clear for 'greater than' or
        // 'item <= 1 << bitwidth' for 'less than'
        if (value != int64_t((magic & mask)) && value >= 0 && bitwidth >= 2 &&
            value <= static_cast<int64_t>((mask >> 1) - (gt ? 1 : 0))) {
            // 15 ms
            while (p < e) {
                uint64_t upper = lower_bits<bitwidth>() << (no0(bitwidth) - 1);

                const int64_t v = *p;
                size_t idx;

                // Bit hacks only works if all items in chunk have their most significant bit clear. Test this:
                upper = upper & v;

                if (!upper) {
                    idx = find_gtlt_fast<gt, bitwidth>(
                        v, magic, state,
                        (p - reinterpret_cast<int64_t*>(m_array.m_data)) * 8 * 8 / no0(bitwidth) + baseindex);
                }
                else
                    idx = find_gtlt<gt, bitwidth>(
                        value, v, state,
                        (p - reinterpret_cast<int64_t*>(m_array.m_data)) * 8 * 8 / no0(bitwidth) + baseindex);

                if (!idx)
                    return false;
                ++p;
            }
        }
        else {
            // 24 ms
            while (p < e) {
                int64_t v = *p;
                if (!find_gtlt<gt, bitwidth>(
                        value, v, state,
                        (p - reinterpret_cast<int64_t*>(m_array.m_data)) * 8 * 8 / no0(bitwidth) + baseindex))
                    return false;
                ++p;
            }
        }
        start = (p - reinterpret_cast<int64_t*>(m_array.m_data)) * 8 * 8 / no0(bitwidth);
    }

    // matchcount logic in SIMD no longer pays off for 32/64 bit ints because we have just 4/2 elements

    // Test unaligned end and/or values of width > 16 manually
    while (start < end) {
        if (gt ? m_array.get<bitwidth>(start) > value : m_array.get<bitwidth>(start) < value) {
            if (!state->match(start + baseindex))
                return false;
        }
        ++start;
    }
    return true;
}

//*************************************************************************************
// Finding code ends                                                                  *
//*************************************************************************************

} // namespace realm

#endif /* REALM_ARRAY_WITH_FIND_HPP */
