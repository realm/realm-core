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

#ifndef FLEX_COMPRESSOR_HPP
#define FLEX_COMPRESSOR_HPP

#include <realm/array.hpp>

#include <cstdint>
#include <stddef.h>
#include <vector>


namespace realm {

struct WordTypeValue {};
struct WordTypeIndex {};

//
// Compress array in Flex format
// Decompress array in WTypeBits formats
//
class FlexCompressor {
public:
    // encoding/decoding
    void init_array(char*, uint8_t, uint8_t, uint8_t, size_t, size_t) const;
    void copy_data(const Array&, const std::vector<int64_t>&, const std::vector<size_t>&) const;
    // getters/setters
    inline int64_t get(const IntegerCompressor&, size_t) const;
    inline std::vector<int64_t> get_all(const IntegerCompressor&, size_t, size_t) const;
    inline void get_chunk(const IntegerCompressor&, size_t, int64_t[8]) const;
    inline void set_direct(const IntegerCompressor&, size_t, int64_t) const;

    template <typename Cond>
    inline bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

private:
    bool find_all_match(size_t, size_t, size_t, QueryStateBase*) const;

    template <typename Cond>
    inline bool find_linear(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

    template <typename CondVal, typename CondIndex>
    inline bool find_parallel(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

    template <typename LinearCond, typename ParallelCond1, typename ParallelCond2>
    inline bool do_find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

    template <typename Cond>
    inline bool run_parallel_subscan(size_t, size_t, size_t) const;
};

inline int64_t FlexCompressor::get(const IntegerCompressor& c, size_t ndx) const
{
    const auto offset = c.v_width() * c.v_size();
    const auto ndx_w = c.ndx_width();
    const auto v_w = c.v_width();
    const auto data = c.data();
    bf_iterator ndx_iterator{data, offset, ndx_w, ndx_w, ndx};
    bf_iterator data_iterator{data, 0, v_w, v_w, static_cast<size_t>(*ndx_iterator)};
    return sign_extend_field_by_mask(c.v_mask(), *data_iterator);
}

inline std::vector<int64_t> FlexCompressor::get_all(const IntegerCompressor& c, size_t b, size_t e) const
{
    const auto offset = c.v_width() * c.v_size();
    const auto ndx_w = c.ndx_width();
    const auto v_w = c.v_width();
    const auto data = c.data();
    const auto sign_mask = c.v_mask();
    const auto range = (e - b);
    const auto starting_bit = offset + b * ndx_w;
    const auto total_bits = starting_bit + ndx_w * range;
    const auto bit_per_it = num_bits_for_width(ndx_w);
    const auto ndx_mask = 0xFFFFFFFFFFFFFFFFULL >> (64 - ndx_w);
    const auto values_per_word = num_fields_for_width(ndx_w);

    // this is very important, x4 faster pre-allocating the array
    std::vector<int64_t> res;
    res.reserve(range);

    unaligned_word_iter unaligned_ndx_iterator(data, starting_bit);
    bf_iterator data_iterator{data, 0, v_w, v_w, 0};
    auto cnt_bits = starting_bit;
    while (cnt_bits + bit_per_it < total_bits) {
        auto word = unaligned_ndx_iterator.get(bit_per_it);
        for (int i = 0; i < values_per_word; ++i) {
            const auto index = word & ndx_mask;
            data_iterator.move(static_cast<size_t>(index));
            const auto sv = sign_extend_field_by_mask(sign_mask, *data_iterator);
            res.push_back(sv);
            word >>= ndx_w;
        }
        cnt_bits += bit_per_it;
        unaligned_ndx_iterator.bump(bit_per_it);
    }
    if (cnt_bits < total_bits) {
        auto last_word = unaligned_ndx_iterator.get(total_bits - cnt_bits);
        while (cnt_bits < total_bits) {
            const auto index = last_word & ndx_mask;
            data_iterator.move(static_cast<size_t>(index));
            const auto sv = sign_extend_field_by_mask(sign_mask, *data_iterator);
            res.push_back(sv);
            cnt_bits += ndx_w;
            last_word >>= ndx_w;
        }
    }
    return res;
}

inline void FlexCompressor::get_chunk(const IntegerCompressor& c, size_t ndx, int64_t res[8]) const
{
    auto sz = 8;
    std::memset(res, 0, sizeof(int64_t) * sz);
    auto supposed_end = ndx + sz;
    size_t i = ndx;
    size_t index = 0;
    for (; i < supposed_end; ++i) {
        res[index++] = get(c, i);
    }
    for (; index < 8; ++index) {
        res[index++] = get(c, i++);
    }
}

void FlexCompressor::set_direct(const IntegerCompressor& c, size_t ndx, int64_t value) const
{
    const auto offset = c.v_width() * c.v_size();
    const auto ndx_w = c.ndx_width();
    const auto v_w = c.v_width();
    const auto data = c.data();
    bf_iterator ndx_iterator{data, offset, ndx_w, ndx_w, ndx};
    bf_iterator data_iterator{data, 0, v_w, v_w, static_cast<size_t>(*ndx_iterator)};
    data_iterator.set_value(value);
}

template <typename Cond>
inline bool FlexCompressor::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
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

    if constexpr (std::is_same_v<Equal, Cond>) {
        return do_find_all<Equal, Equal, Equal>(arr, value, start, end, baseindex, state);
    }
    else if constexpr (std::is_same_v<NotEqual, Cond>) {
        return do_find_all<NotEqual, NotEqual, LessEqual>(arr, value, start, end, baseindex, state);
    }
    else if constexpr (std::is_same_v<Less, Cond>) {
        return do_find_all<Less, GreaterEqual, Less>(arr, value, start, end, baseindex, state);
    }
    else if constexpr (std::is_same_v<Greater, Cond>) {
        return do_find_all<Greater, Greater, GreaterEqual>(arr, value, start, end, baseindex, state);
    }
    return true;
}

template <typename LinearCond, typename ParallelCond1, typename ParallelCond2>
inline bool FlexCompressor::do_find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                                        QueryStateBase* state) const
{
    const auto v_width = arr.m_width;
    const auto v_range = arr.integer_compressor().v_size();
    const auto ndx_range = end - start;
    if (!run_parallel_subscan<LinearCond>(v_width, v_range, ndx_range))
        return find_linear<LinearCond>(arr, value, start, end, baseindex, state);
    return find_parallel<ParallelCond1, ParallelCond2>(arr, value, start, end, baseindex, state);
}

template <typename Cond>
inline bool FlexCompressor::find_linear(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                                        QueryStateBase* state) const
{
    const auto cmp = [](int64_t item, int64_t key) {
        if constexpr (std::is_same_v<Cond, Equal>)
            return item == key;
        if constexpr (std::is_same_v<Cond, NotEqual>)
            return item != key;
        if constexpr (std::is_same_v<Cond, Less>)
            return item < key;
        if constexpr (std::is_same_v<Cond, Greater>)
            return item > key;
        REALM_UNREACHABLE();
    };

    const auto& c = arr.integer_compressor();
    const auto offset = c.v_width() * c.v_size();
    const auto ndx_w = c.ndx_width();
    const auto v_w = c.v_width();
    const auto data = c.data();
    const auto mask = c.v_mask();
    bf_iterator ndx_iterator{data, offset, ndx_w, ndx_w, start};
    bf_iterator data_iterator{data, 0, v_w, v_w, static_cast<size_t>(*ndx_iterator)};
    while (start < end) {
        const auto sv = sign_extend_field_by_mask(mask, *data_iterator);
        if (cmp(sv, value) && !state->match(start + baseindex))
            return false;
        ndx_iterator.move(++start);
        data_iterator.move(static_cast<size_t>(*ndx_iterator));
    }
    return true;
}

template <typename Cond, typename Type = WordTypeValue>
inline uint64_t vector_compare(uint64_t MSBs, uint64_t a, uint64_t b)
{
    if constexpr (std::is_same_v<Cond, Equal>)
        return find_all_fields_EQ(MSBs, a, b);
    if constexpr (std::is_same_v<Cond, NotEqual>)
        return find_all_fields_NE(MSBs, a, b);

    if constexpr (std::is_same_v<Cond, Greater>) {
        if (std::is_same_v<Type, WordTypeValue>)
            return find_all_fields_signed_GT(MSBs, a, b);
        if (std::is_same_v<Type, WordTypeIndex>)
            return find_all_fields_unsigned_GT(MSBs, a, b);
        REALM_UNREACHABLE();
    }
    if constexpr (std::is_same_v<Cond, GreaterEqual>) {
        if constexpr (std::is_same_v<Type, WordTypeValue>)
            return find_all_fields_signed_GE(MSBs, a, b);
        if constexpr (std::is_same_v<Type, WordTypeIndex>)
            return find_all_fields_unsigned_GE(MSBs, a, b);
        REALM_UNREACHABLE();
    }
    if constexpr (std::is_same_v<Cond, Less>) {
        if constexpr (std::is_same_v<Type, WordTypeValue>)
            return find_all_fields_signed_LT(MSBs, a, b);
        if constexpr (std::is_same_v<Type, WordTypeIndex>)
            return find_all_fields_unsigned_LT(MSBs, a, b);
        REALM_UNREACHABLE();
    }
    if constexpr (std::is_same_v<Cond, LessEqual>) {
        if constexpr (std::is_same_v<Type, WordTypeValue>)
            return find_all_fields_signed_LT(MSBs, a, b);
        if constexpr (std::is_same_v<Type, WordTypeIndex>)
            return find_all_fields_unsigned_LE(MSBs, a, b);
        REALM_UNREACHABLE();
    }
}

template <typename CondVal, typename CondIndex>
inline bool FlexCompressor::find_parallel(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                                          QueryStateBase* state) const
{
    const auto& compressor = arr.integer_compressor();
    const auto v_width = compressor.v_width();
    const auto v_size = compressor.v_size();
    const auto ndx_width = compressor.ndx_width();
    const auto offset = v_size * v_width;
    uint64_t* data = (uint64_t*)arr.m_data;

    auto MSBs = compressor.msb();
    auto search_vector = populate(v_width, value);
    auto v_start = parallel_subword_find(vector_compare<CondVal>, data, 0, v_width, MSBs, search_vector, 0, v_size);
    if (v_start == v_size)
        return true;

    MSBs = compressor.ndx_msb();
    search_vector = populate(ndx_width, v_start);
    while (start < end) {
        start = parallel_subword_find(vector_compare<CondIndex, WordTypeIndex>, data, offset, ndx_width, MSBs,
                                      search_vector, start, end);
        if (start < end)
            if (!state->match(start + baseindex))
                return false;

        ++start;
    }
    return true;
}

template <typename Cond>
inline bool FlexCompressor::run_parallel_subscan(size_t v_width, size_t v_range, size_t ndx_range) const
{
    if constexpr (std::is_same_v<Cond, Equal> || std::is_same_v<Cond, NotEqual>) {
        return v_width < 32 && v_range >= 20 && ndx_range >= 20;
    }
    // > and < need looks slower in parallel scan for large values
    return v_width <= 16 && v_range >= 20 && ndx_range >= 20;
}

} // namespace realm
#endif // FLEX_COMPRESSOR_HPP
