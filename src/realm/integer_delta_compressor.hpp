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

#ifndef DELTA_COMPRESSOR_HPP
#define DELTA_COMPRESSOR_HPP

#include <realm/array.hpp>

#include <cstdint>
#include <stddef.h>
#include <vector>

namespace realm {

//
// Compress array in Delta format
// Decompress array in WTypeBits formats
//
class DeltaCompressor {
public:
    // encoding/decoding
    static void init_header(char*, uint8_t, uint8_t, uint8_t, size_t, size_t);
    static void copy_data(const Array&, Array&, const std::vector<int64_t>&);
    // getters/setters
    static int64_t get(const IntegerCompressor&, size_t);
    static std::vector<int64_t> get_all(const IntegerCompressor&, size_t, size_t);
    static void get_chunk(const IntegerCompressor&, size_t, int64_t[8]);
    static void set_direct(const IntegerCompressor&, size_t, int64_t);

    template <typename Cond>
    static bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*);
};

inline int64_t DeltaCompressor::get(const IntegerCompressor& c, size_t ndx)
{
    const auto offset = c.v_width() * c.v_size();
    const auto ndx_w = c.ndx_width();
    const auto v_w = c.v_width();
    const auto data = c.data();
    BfIterator delta_iterator{data, offset, ndx_w, ndx_w, ndx};
    BfIterator data_iterator{data, 0, v_w, v_w, 0};
    auto delta = *delta_iterator;
    if (delta == 0) {
        return sign_extend_field_by_mask(c.v_mask(), *data_iterator);
    }
    else {
        ++data_iterator;
        return sign_extend_field_by_mask(c.v_mask(), *data_iterator) + delta - 1;
    }
}

inline std::vector<int64_t> DeltaCompressor::get_all(const IntegerCompressor& c, size_t b, size_t e)
{
    const auto offset = c.v_width() * c.v_size();
    const auto ndx_w = c.ndx_width();
    const auto v_w = c.v_width();
    const auto data = c.data();
    const auto sign_mask = c.v_mask();
    const auto range = (e - b);
    const auto starting_bit = offset + b * ndx_w;
    const auto bit_per_it = num_bits_for_width(ndx_w);
    const auto ndx_mask = 0xFFFFFFFFFFFFFFFFULL >> (64 - ndx_w);
    const auto values_per_word = num_fields_for_width(ndx_w);

    // this is very important, x4 faster pre-allocating the array
    std::vector<int64_t> res;
    res.reserve(range);

    UnalignedWordIter unaligned_ndx_iterator(data, starting_bit);
    BfIterator data_iterator{data, 0, v_w, v_w, 0};
    auto remaining_bits = ndx_w * range;
    while (remaining_bits >= bit_per_it) {
        auto word = unaligned_ndx_iterator.consume(bit_per_it);
        for (int i = 0; i < values_per_word; ++i) {
            const auto index = word & ndx_mask;
            data_iterator.move(static_cast<size_t>(index));
            const auto sv = sign_extend_field_by_mask(sign_mask, *data_iterator);
            res.push_back(sv);
            word >>= ndx_w;
        }
        remaining_bits -= bit_per_it;
    }
    if (remaining_bits) {
        auto last_word = unaligned_ndx_iterator.consume(remaining_bits);
        while (remaining_bits) {
            const auto index = last_word & ndx_mask;
            data_iterator.move(static_cast<size_t>(index));
            const auto sv = sign_extend_field_by_mask(sign_mask, *data_iterator);
            res.push_back(sv);
            remaining_bits -= ndx_w;
            last_word >>= ndx_w;
        }
    }
    return res;
}

inline void DeltaCompressor::get_chunk(const IntegerCompressor& c, size_t ndx, int64_t res[8])
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

inline void DeltaCompressor::set_direct(const IntegerCompressor& c, size_t ndx, int64_t value)
{
    const auto offset = c.v_width() * c.v_size();
    const auto ndx_w = c.ndx_width();
    const auto v_w = c.v_width();
    const auto data = c.data();
    BfIterator ndx_iterator{data, offset, ndx_w, ndx_w, ndx};
    BfIterator data_iterator{data, 0, v_w, v_w, static_cast<size_t>(*ndx_iterator)};
    data_iterator.set_value(value);
}

template <typename Cond>
inline bool DeltaCompressor::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                                      QueryStateBase* state)
{
    REALM_ASSERT_DEBUG(start <= arr.m_size && (end <= arr.m_size || end == size_t(-1)) && start <= end);
    Cond c;

    const auto& compressor = arr.integer_compressor();
    const auto v_width = arr.m_width;
    const auto mask = compressor.v_mask();
    uint64_t* data = (uint64_t*)arr.m_data;

    int64_t v;
    BfIterator data_iterator{data, 0, v_width, v_width, 0};
    auto largest_val = int64_t(*data_iterator);
    if (value == largest_val) {
        v = 0;
    }
    else {
        ++data_iterator;
        v = value - sign_extend_field_by_mask(mask, *data_iterator) + 1;
    }

    // Search the deltas
    const auto ndx_range = end - start;
    const auto ndx_width = compressor.ndx_width();
    const auto v_offset = 2 * size_t(v_width);
    if (ndx_range >= 20 && ndx_width <= 16) {
        auto search_vector = populate(ndx_width, v);
        while (start < end) {
            start = parallel_subword_find(find_all_fields_unsigned<Cond>, data, v_offset, ndx_width,
                                          compressor.ndx_msb(), search_vector, start, end);
            if (start < end) {
                if (!state->match(start + baseindex))
                    return false;
            }
            ++start;
        }
    }
    else {
        BfIterator ndx_iterator{data, v_offset, ndx_width, ndx_width, start};
        while (start < end) {
            if (c(int64_t(*ndx_iterator), v)) {
                if (!state->match(start + baseindex))
                    return false;
            }
            ndx_iterator.move(++start);
        }
    }
    return true;
}

} // namespace realm
#endif // DELTA_COMPRESSOR_HPP
