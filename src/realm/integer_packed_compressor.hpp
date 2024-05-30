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
    static void init_header(char*, uint8_t, uint8_t, size_t);
    static void copy_data(const Array&, Array&);
    // get or set
    static int64_t get(const IntegerCompressor&, size_t);
    static std::vector<int64_t> get_all(const IntegerCompressor& c, size_t b, size_t e);
    static void get_chunk(const IntegerCompressor&, size_t, int64_t res[8]);
    static void set_direct(const IntegerCompressor&, size_t, int64_t);

    template <typename Cond>
    static bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*);

private:
    static bool find_all_match(size_t start, size_t end, size_t baseindex, QueryStateBase* state);

    template <typename VectorCond>
    static bool find_parallel(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*);

    template <typename Cond>
    static bool find_linear(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*);

    template <typename Cond>
    static bool run_parallel_scan(size_t, size_t);
};

inline int64_t PackedCompressor::get(const IntegerCompressor& c, size_t ndx)
{
    BfIterator it{c.data(), 0, c.v_width(), c.v_width(), ndx};
    return sign_extend_field_by_mask(c.v_mask(), *it);
}

inline std::vector<int64_t> PackedCompressor::get_all(const IntegerCompressor& c, size_t b, size_t e)
{
    const auto range = (e - b);
    const auto v_w = c.v_width();
    const auto data = c.data();
    const auto sign_mask = c.v_mask();
    const auto starting_bit = b * v_w;
    const auto total_bits = starting_bit + (v_w * range);
    const auto mask = 0xFFFFFFFFFFFFFFFFULL >> (64 - v_w);
    const auto bit_per_it = num_bits_for_width(v_w);
    const auto values_per_word = num_fields_for_width(v_w);

    std::vector<int64_t> res;
    res.reserve(range);

    UnalignedWordIter unaligned_data_iterator(data, starting_bit);
    auto cnt_bits = starting_bit;
    while (cnt_bits + bit_per_it < total_bits) {
        auto word = unaligned_data_iterator.consume(bit_per_it);
        for (int i = 0; i < values_per_word; ++i) {
            res.push_back(sign_extend_field_by_mask(sign_mask, word & mask));
            word >>= v_w;
        }
        cnt_bits += bit_per_it;
    }
    if (cnt_bits < total_bits) {
        auto last_word = unaligned_data_iterator.consume(static_cast<unsigned>(total_bits - cnt_bits));
        while (cnt_bits < total_bits) {
            res.push_back(sign_extend_field_by_mask(sign_mask, last_word & mask));
            cnt_bits += v_w;
            last_word >>= v_w;
        }
    }
    return res;
}

inline void PackedCompressor::set_direct(const IntegerCompressor& c, size_t ndx, int64_t value)
{
    BfIterator it{c.data(), 0, c.v_width(), c.v_width(), ndx};
    it.set_value(value);
}

inline void PackedCompressor::get_chunk(const IntegerCompressor& c, size_t ndx, int64_t res[8])
{
    auto sz = 8;
    std::memset(res, 0, sizeof(int64_t) * sz);
    auto supposed_end = ndx + sz;
    size_t i = ndx;
    size_t index = 0;
    // this can be done better, in one go, retrieve both!!!
    for (; i < supposed_end; ++i) {
        res[index++] = get(c, i);
    }
    for (; index < 8; ++index) {
        res[index++] = get(c, i++);
    }
}


template <typename Cond>
inline bool PackedCompressor::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                                       QueryStateBase* state)
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

    if (!run_parallel_scan<Cond>(arr.m_width, end - start))
        return find_linear<Cond>(arr, value, start, end, baseindex, state);

    return find_parallel<Cond>(arr, value, start, end, baseindex, state);
}

template <typename VectorCond>
inline bool PackedCompressor::find_parallel(const Array& arr, int64_t value, size_t start, size_t end,
                                            size_t baseindex, QueryStateBase* state)
{
    //
    // Main idea around find parallel (applicable to flex arrays too).
    // Try to find the starting point where the condition can be met, comparing as many values as a single 64bit can
    // contain in parallel. Once we have found the starting point, keep matching values as much as we can between
    // start and end.
    //
    // EG: let's store 6, it gets stored in 4 bits (0110). 6 is 4 bits because 110 (6) + sign bit 0.
    // Inside 64bits we can fit max 16 times 6. If we go from index 0 to 15 throughout the same 64 bits, we need to
    // apply a mask and a shift bits every time, then compare the extracted values.
    // This is not the cheapest thing to do. Instead we can compare all values contained within 64 bits in one go, and
    // see if there is a match with what we are looking for. Reducing the number of comparison by ~logk(N) where K is
    // the width of each single value within a 64 bit word and N is the total number of values stored in the array.

    const auto data = (const uint64_t*)arr.m_data;
    const auto width = arr.m_width;
    const auto MSBs = arr.integer_compressor().msb();
    const auto search_vector = populate(arr.m_width, value);
    while (start < end) {
        start = parallel_subword_find(find_all_fields<VectorCond>, data, 0, width, MSBs, search_vector, start, end);
        if (start < end && !state->match(start + baseindex))
            return false;
        ++start;
    }
    return true;
}

template <typename Cond>
inline bool PackedCompressor::find_linear(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                                          QueryStateBase* state)
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
    const auto& c = arr.integer_compressor();
    BfIterator it{c.data(), 0, c.v_width(), c.v_width(), start};
    for (; start < end; ++start) {
        it.move(start);
        const auto sv = sign_extend_field_by_mask(c.v_mask(), *it);
        if (compare(sv, value) && !state->match(start + baseindex))
            return false;
    }
    return true;
}

template <typename Cond>
inline bool PackedCompressor::run_parallel_scan(size_t width, size_t range)
{
    if constexpr (std::is_same_v<Cond, NotEqual>) {
        // we seem to be particularly slow doing parallel scan in packed for NotEqual.
        // we are much better with a linear scan. TODO: investigate this.
        return false;
    }
    if constexpr (std::is_same_v<Cond, Equal>) {
        return width < 32 && range >= 20;
    }
    // > and < need a different heuristic
    return width <= 20 && range >= 20;
}

} // namespace realm

#endif // PACKED_COMPRESSOR_HPP
