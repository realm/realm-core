/*************************************************************************
 *
 * Copyright 2024 Realm Inc.
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

#ifndef REALM_ARRAY_PACKED_HPP
#define REALM_ARRAY_PACKED_HPP

#include <realm/array_encode.hpp>
#include <realm/node_header.hpp>
#include <realm/array.hpp>

namespace realm {

//
// Compress array in Packed format
// Decompress array in WTypeBits formats
//
class Array;
class ArrayPacked {
public:
    // encoding/decoding
    void init_array(char*, uint8_t, size_t, size_t) const;
    void copy_data(const Array&, Array&) const;
    // get or set
    int64_t get(const Array&, size_t) const;
    int64_t get(const char*, size_t, size_t, size_t, size_t) const;
    void get_chunk(const Array&, size_t, int64_t res[8]) const;
    void set_direct(const Array&, size_t, int64_t) const;

    template <typename Cond>
    bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

private:
    int64_t do_get(uint64_t*, size_t, size_t, size_t, size_t) const;

    template <typename Cond>
    bool compare_equality(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

    template <typename Cond>
    size_t find_zero(size_t width, uint64_t v) const;

    inline bool test_zero(size_t width, uint64_t value) const;
};

namespace impl {
// template <class T>
// inline constexpr T no0(T v)
//{
//     return v == 0 ? 1 : v;
// }

// inline uint64_t lower_bits(size_t width)
//{
// WE need 64 if elses....
//     if (width == 1)
//         return 0xFFFFFFFFFFFFFFFFULL;
//     else if (width == 2)
//         return 0x5555555555555555ULL;
//     else if (width == 4)
//         return 0x1111111111111111ULL;
//     else if (width == 8)
//         return 0x0101010101010101ULL;
//     else if (width == 16)
//         return 0x0001000100010001ULL;
//     else if (width == 32)
//         return 0x0000000100000001ULL;
//     else if (width == 64)
//         return 0x0000000000000001ULL;
//     else {
//         return uint64_t(-1);
//     }
// }

inline bool find_all_match(size_t start, size_t end, size_t baseindex, QueryStateBase* state)
{
    REALM_ASSERT_DEBUG(state->match_count() < state->limit());
    const auto process = state->limit() - state->match_count();
    const auto end2 = end - start > process ? start + process : end;
    for (; start < end2; start++)
        if (!state->match(start + baseindex))
            return false;
    return true;
}
} // namespace impl


// inline bool ArrayPacked::test_zero(size_t width, uint64_t value) const
//{
//     uint64_t hasZeroByte;
//     uint64_t lower = impl::lower_bits(width);
//     uint64_t upper = impl::lower_bits(width) * 1ULL << (width == 0 ? 0 : (width - 1ULL));
//     hasZeroByte = (value - lower) & ~value & upper;
//     return hasZeroByte != 0;
// }

//// Finds first zero (if eq == true) or non-zero (if eq == false) element in v and returns its position.
//// IMPORTANT: This function assumes that at least 1 item matches (test this with test_zero() or other means first)!
// template <typename Cond>
// size_t ArrayPacked::find_zero(size_t width, uint64_t v) const
//{
//     constexpr auto eq = std::is_same_v<Cond, Equal>;
//     size_t start = 0;
//     uint64_t hasZeroByte;
//     // Warning free way of computing (1ULL << width) - 1
//     uint64_t mask = (width == 64 ? ~0ULL : ((1ULL << (width == 64 ? 0 : width)) - 1ULL));
//
//     if (eq == (((v >> (width * start)) & mask) == 0)) {
//         return 0;
//     }
//
//     // Bisection optimization, speeds up small bitwidths with high match frequency. More partions than 2 do NOT pay
//     // off because the work done by test_zero() is wasted for the cases where the value exists in first half, but
//     // useful if it exists in last half. Sweet spot turns out to be the widths and partitions below.
//     if (width <= 8) {
//         hasZeroByte = test_zero(width, v | 0xffffffff00000000ULL);
//         if (eq ? !hasZeroByte : (v & 0x00000000ffffffffULL) == 0) {
//             // 00?? -> increasing
//             start += 64 / impl::no0(width) / 2;
//             if (width <= 4) {
//                 hasZeroByte = test_zero(width, v | 0xffff000000000000ULL);
//                 if (eq ? !hasZeroByte : (v & 0x0000ffffffffffffULL) == 0) {
//                     // 000?
//                     start += 64 / impl::no0(width) / 4;
//                 }
//             }
//         }
//         else {
//             if (width <= 4) {
//                 // ??00
//                 hasZeroByte = test_zero(width, v | 0xffffffffffff0000ULL);
//                 if (eq ? !hasZeroByte : (v & 0x000000000000ffffULL) == 0) {
//                     // 0?00
//                     start += 64 / impl::no0(width) / 4;
//                 }
//             }
//         }
//     }
//
//     while (eq == (((v >> (width * start)) & mask) != 0)) {
//         // You must only call find_zero() if you are sure that at least 1 item matches
//         REALM_ASSERT_3(start, <=, 8 * sizeof(v));
//         start++;
//     }
//
//     return start;
// }
//
// template<typename Cond>
// bool ArrayPacked::compare_equality(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
// QueryStateBase* state) const
//{
//     REALM_ASSERT_DEBUG(start <= arr.m_size && (end <= arr.m_size || end == size_t(-1)) && start <= end);
//     const size_t width = arr.m_width;
//     constexpr auto eq = std::is_same_v<Equal, Cond>;
//
//     auto v = 64 / impl::no0(width);
//     size_t ee = round_up(start, v);
//     ee = ee > end ? end : ee;
//     for (; start < ee; ++start) {
//         auto v = arr.get(start);
//         if (eq ? (v == value) : (v != value)) {
//             if (!state->match(start + baseindex))
//                 return false;
//         }
//     }
//
//     if (start >= end)
//         return true;
//
//     if (width != 32 && width != 64) {
//         const int64_t* p = reinterpret_cast<const int64_t*>(arr.m_data + (start * width / 8));
//         const int64_t* const e = reinterpret_cast<int64_t*>(arr.m_data + (end * width / 8)) - 1;
//         const uint64_t mask =
//         (width == 64
//          ? ~0ULL
//          : ((1ULL << (width == 64 ? 0 : width)) - 1ULL)); // Warning free way of computing (1ULL << width) - 1
//         const uint64_t valuemask =
//         ~0ULL / impl::no0(mask) * (value & mask); // the "== ? :" is to avoid division by 0 compiler error
//
//         while (p < e) {
//             uint64_t chunk = *p;
//             uint64_t v2 = chunk ^ valuemask;
//             start = (p - reinterpret_cast<int64_t*>(arr.m_data)) * 8 * 8 / impl::no0(width);
//             size_t a = 0;
//
//             while (eq ? test_zero(width, v2) : v2) {
//                 size_t t = find_zero<Cond>(width, v2);
//                 a += t;
//
//                 if (a >= 64 / impl::no0(width))
//                     break;
//
//                 if (!state->match(a + start + baseindex)) {
//                     return false;
//                 }
//                 auto shift = (t + 1) * width;
//                 if (shift < 64)
//                     v2 >>= shift;
//                 else
//                     v2 = 0;
//                 a += 1;
//             }
//
//             ++p;
//         }
//
//         // Loop ended because we are near end or end of array. No need to optimize search in remainder in this case
//         // because end of array means that
//         // lots of search work has taken place prior to ending here. So time spent searching remainder is
//         relatively
//         // tiny
//         start = (p - reinterpret_cast<int64_t*>(arr.m_data)) * 8 * 8 / impl::no0(width);
//     }
//
//     while (start < end) {
//         if (eq ? arr.get(start) == value : arr.get(start) != value) {
//             if (!state->match(start + baseindex)) {
//                 return false;
//             }
//         }
//         ++start;
//     }
//
//     return true;
// }


template <typename Cond>
bool ArrayPacked::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                           QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(start <= arr.m_size && (end <= arr.m_size || end == size_t(-1)) && start <= end);
    size_t start2 = start;
    Cond c;

    if (end == npos)
        end = arr.m_size;

    if (!(arr.m_size > start2 && start2 < end))
        return true;

    const auto lbound = arr.m_lbound;
    const auto ubound = arr.m_ubound;

    // Return immediately if no items in array can match (such as if cond == Greater && value == 100 &&
    // m_ubound == 15)
    if (!c.can_match(value, lbound, ubound))
        return true;

    // optimization if all items are guaranteed to match (such as cond == NotEqual && value == 100 && m_ubound == 15)
    if (c.will_match(value, lbound, ubound)) {
        return impl::find_all_match(start2, end, baseindex, state);
    }

    // finder cannot handle this bitwidth
    REALM_ASSERT_3(arr.m_width, !=, 0);

    auto cmp = [](int64_t v, int64_t value) {
        if constexpr (std::is_same_v<Cond, Equal>)
            return v == value;
        if constexpr (std::is_same_v<Cond, NotEqual>)
            return v != value;
        if constexpr (std::is_same_v<Cond, Greater>)
            return v > value;
        if constexpr (std::is_same_v<Cond, Less>)
            return v < value;
    };

    // 20x slower;
    for (; start2 < end; ++start2) {
        if (cmp(arr.get(start2), value) && !state->match(start2 + baseindex))
            return false;
    }
    return true;

    // not finding , NEED to understnad if it needs to be aligned
    // this is just for test. call compare equality
    // return compare_equality<Cond>(arr, value, start2, end, baseindex, state);
}


} // namespace realm

#endif // REALM_ARRAY_PACKED_HPP
