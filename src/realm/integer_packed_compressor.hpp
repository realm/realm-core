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

#ifndef PACKED_COMPRESSOR_HPP
#define PACKED_COMPRESSOR_HPP

#include <realm/array.hpp>
#include <realm/array_direct.hpp>

#include <cstdint>
#include <stddef.h>

namespace realm {

//
// Compress array in Packed format
// Decompress array in WTypeBits formats
//
class PackedCompressor {
public:
    // encoding/decoding
    void init_array(char*, uint8_t, size_t, size_t) const;
    void copy_data(const Array&, Array&) const;
    // get or set
    inline int64_t get(bf_iterator& it, size_t, uint64_t) const;
    inline void get_chunk(bf_iterator& it, size_t, uint64_t, int64_t res[8]) const;
    inline void set_direct(bf_iterator& it, size_t, int64_t) const;

    template <typename Cond>
    inline bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

private:
    bool find_all_match(size_t start, size_t end, size_t baseindex, QueryStateBase* state) const;

    template <typename Cond>
    inline bool find_parallel(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

    template <typename Cond>
    inline bool find_linear(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

    inline bool run_parallel_scan(size_t width, size_t range) const;
};

inline int64_t PackedCompressor::get(bf_iterator& it, size_t ndx, uint64_t mask) const
{
    it.move(ndx);
    return sign_extend_field_by_mask(mask, *it);
}

template <typename Cond>
inline bool PackedCompressor::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                                       QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(start <= arr.m_size && (end <= arr.m_size || end == size_t(-1)) && start <= end);
    Cond c;

    if (end == npos)
        end = arr.m_size;

    if (!(arr.m_size > start && start < end))
        return true;

    const auto lbound = arr.m_lbound;
    const auto ubound = arr.m_ubound;

    if (!c.can_match(value, lbound, ubound))
        return true;

    if (c.will_match(value, lbound, ubound)) {
        return find_all_match(start, end, baseindex, state);
    }

    REALM_ASSERT_DEBUG(arr.m_width != 0);

    if (!run_parallel_scan(arr.m_width, end - start))
        return find_linear<Cond>(arr, value, start, end, baseindex, state);

    return find_parallel<Cond>(arr, value, start, end, baseindex, state);
}

inline void PackedCompressor::set_direct(bf_iterator& it, size_t ndx, int64_t value) const
{
    it.move(ndx);
    it.set_value(value);
}

inline void PackedCompressor::get_chunk(bf_iterator& it, size_t ndx, uint64_t mask, int64_t res[8]) const
{
    auto sz = 8;
    std::memset(res, 0, sizeof(int64_t) * sz);
    auto supposed_end = ndx + sz;
    size_t i = ndx;
    size_t index = 0;
    // this can be done better, in one go, retrieve both!!!
    for (; i < supposed_end; ++i) {
        res[index++] = get(it, i, mask);
    }
    for (; index < 8; ++index) {
        res[index++] = get(it, i++, mask);
    }
}

template <typename Cond>
inline bool PackedCompressor::find_parallel(const Array& arr, int64_t value, size_t start, size_t end,
                                            size_t baseindex, QueryStateBase* state) const
{
    //
    // Main idea around find parallel (applicable to flex arrays too).
    // Try to find the starting point where the condition can be met, comparing as many values as a single 64bit can
    // contain in parallel. Once we have found the starting point, keep matching values as much as we can between
    // start and end.
    //
    // EG: we store the value 6, with width 4bits (0110), 6 is 4 bits because, 110 (6) + sign bit 0.
    // Inside 64bits we can fit max 16 times 6. If we go from index 0 to 15 throughout the same 64 bits, we need to
    // apply a mask and a shift bits every time, then compare the values.
    // This is not the cheapest thing to do. Instead we can compare all values contained within 64 bits in one go and
    // see if there is a match with what we are looking for. Reducing the number of comparison by ~logk(N) where K is
    // the width of each single value within a 64 bit word and N is the total number of values stored in the array.

    // apparently the compiler is not able to deduce the type of a global function after moving stuff in the header
    // (no so sure why)
    static auto vector_compare = [](uint64_t MSBs, uint64_t a, uint64_t b) {
        if constexpr (std::is_same_v<Cond, Equal>)
            return find_all_fields_EQ(MSBs, a, b);
        if constexpr (std::is_same_v<Cond, NotEqual>)
            return find_all_fields_NE(MSBs, a, b);
        if constexpr (std::is_same_v<Cond, Greater>)
            return find_all_fields_signed_GT(MSBs, a, b);
        if constexpr (std::is_same_v<Cond, Less>)
            return find_all_fields_signed_LT(MSBs, a, b);
        REALM_UNREACHABLE();
    };

    const auto data = (const uint64_t*)arr.m_data;
    const auto width = arr.m_width;
    const auto MSBs = arr.integer_compressor().msb();
    const auto search_vector = populate(arr.m_width, value);
    while (start < end) {
        start = parallel_subword_find(vector_compare, data, 0, width, MSBs, search_vector, start, end);
        if (start < end)
            if (!state->match(start + baseindex))
                return false;
        ++start;
    }
    return true;
}

template <typename Cond>
inline bool PackedCompressor::find_linear(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                                          QueryStateBase* state) const
{
    auto compare = [](int64_t a, int64_t b) {
        if constexpr (std::is_same_v<Cond, Equal>)
            return a == b;
        if constexpr (std::is_same_v<Cond, NotEqual>)
            return a != b;
        if constexpr (std::is_same_v<Cond, Greater>)
            return a > b;
        if constexpr (std::is_same_v<Cond, Less>)
            return a < b;
    };
    const auto mask = arr.integer_compressor().width_mask();
    auto& data_it = arr.integer_compressor().data_iterator();
    data_it.move(start);
    while (start < end) {
        const auto sv = sign_extend_field_by_mask(mask, *data_it);
        if (compare(sv, value) && !state->match(start + baseindex))
            return false;
        ++start;
        ++data_it;
    }
    return true;
}

inline bool PackedCompressor::run_parallel_scan(size_t width, size_t range) const
{
    return width < 32 && range >= 32;
}

} // namespace realm

#endif // PACKED_COMPRESSOR_HPP
