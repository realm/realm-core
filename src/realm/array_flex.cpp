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

#include <realm/array_flex.hpp>
#include <realm/node_header.hpp>
#include <realm/array_direct.hpp>
#include <realm/array.hpp>

#include <vector>
#include <algorithm>

#ifdef REALM_DEBUG
#include <iostream>
#include <sstream>
#endif

using namespace realm;

template bool ArrayFlex::find_all<Equal>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
template bool ArrayFlex::find_all<NotEqual>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
template bool ArrayFlex::find_all<Greater>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
template bool ArrayFlex::find_all<Less>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

void ArrayFlex::init_array(char* h, uint8_t flags, size_t v_width, size_t ndx_width, size_t v_size,
                           size_t ndx_size) const
{
    using Encoding = NodeHeader::Encoding;
    NodeHeader::init_header(h, Encoding::Flex, flags, v_width, ndx_width, v_size, ndx_size);
}

void ArrayFlex::copy_data(const Array& arr, const std::vector<int64_t>& values,
                          const std::vector<size_t>& indices) const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.m_encoder.get_encoding() == Encoding::Flex);

    const auto& encoder = arr.get_encoder();
    const auto v_width = encoder.m_v_width;
    const auto ndx_width = encoder.m_ndx_width;
    const auto v_size = values.size();
    const auto data = (uint64_t*)arr.m_data;
    const auto offset = static_cast<size_t>(v_size * v_width);
    bf_iterator it_value{data, 0, v_width, v_width, 0};
    bf_iterator it_index{data, offset, ndx_width, ndx_width, 0};
    for (size_t i = 0; i < v_size; ++i) {
        it_value.set_value(values[i]);
        REALM_ASSERT_DEBUG(sign_extend_value(v_width, it_value.get_value()) == values[i]);
        ++it_value;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
        REALM_ASSERT_DEBUG(values[indices[i]] ==
                           sign_extend_value(v_width, read_bitfield(data, indices[i] * v_width, v_width)));
        it_index.set_value(indices[i]);
        REALM_ASSERT_DEBUG(indices[i] == it_index.get_value());
        REALM_ASSERT_DEBUG(values[indices[i]] ==
                           sign_extend_value(v_width, read_bitfield(data, indices[i] * v_width, v_width)));
        ++it_index;
    }
}

void ArrayFlex::set_direct(const Array& arr, size_t ndx, int64_t value) const
{
    const auto v_width = arr.m_encoder.m_v_width;
    const auto v_size = arr.m_encoder.m_v_size;
    const auto ndx_width = arr.m_encoder.m_ndx_width;
    const auto ndx_size = arr.m_encoder.m_ndx_size;
    REALM_ASSERT_DEBUG(ndx < ndx_size);
    auto data = (uint64_t*)arr.m_data;
    uint64_t offset = v_size * v_width;
    bf_iterator it_index{data, static_cast<size_t>(offset + (ndx * ndx_width)), ndx_width, ndx_size, 0};
    bf_iterator it_value{data, static_cast<size_t>(it_index.get_value() * v_width), v_width, v_width, 0};
    it_value.set_value(value);
}

int64_t ArrayFlex::get(const Array& arr, size_t ndx) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.is_encoded());
    const auto& encoder = arr.m_encoder;
    const auto v_width = encoder.m_v_width;
    const auto v_size = encoder.m_v_size;
    const auto ndx_width = encoder.m_ndx_width;
    const auto ndx_size = encoder.m_ndx_size;
    const auto mask = encoder.width_mask();
    return get(arr.m_data, ndx, v_width, ndx_width, v_size, ndx_size, mask);
}

int64_t ArrayFlex::get(const char* data, size_t ndx, size_t v_width, size_t v_size, size_t ndx_width, size_t ndx_size,
                       size_t mask) const
{
    return do_get((uint64_t*)data, ndx, v_width, v_size, ndx_width, ndx_size, mask);
}

int64_t ArrayFlex::do_get(uint64_t* data, size_t ndx, size_t v_width, size_t ndx_width, size_t v_size,
                          size_t ndx_size, size_t mask) const
{
    if (ndx >= ndx_size)
        return realm::not_found;
    const uint64_t offset = v_size * v_width;
    const bf_iterator it_index{data, static_cast<size_t>(offset + (ndx * ndx_width)), ndx_width, ndx_width, 0};
    const bf_iterator it_value{data, static_cast<size_t>(v_width * it_index.get_value()), v_width, v_width, 0};
    return sign_extend_field_by_mask(mask, it_value.get_value());
}

void ArrayFlex::get_chunk(const Array& arr, size_t ndx, int64_t res[8]) const
{
    REALM_ASSERT_DEBUG(ndx < arr.m_size);
    auto sz = 8;
    std::memset(res, 0, sizeof(int64_t) * sz);
    auto supposed_end = ndx + sz;
    size_t i = ndx;
    size_t index = 0;
    for (; i < supposed_end; ++i) {
        res[index++] = get(arr, i);
    }
    for (; index < 8; ++index) {
        res[index++] = get(arr, i++);
    }
}

template <typename Cond>
bool ArrayFlex::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
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

    REALM_ASSERT_3(arr.m_width, !=, 0);

    if constexpr (std::is_same_v<Equal, Cond>) {
        find_eq(arr, value, start, end, baseindex, state);
    }
    else if constexpr (std::is_same_v<NotEqual, Cond>) {
        find_neq(arr, value, start, end, baseindex, state);
    }
    else if constexpr (std::is_same_v<Less, Cond>) {
        find_lt(arr, value, start, end, baseindex, state);
    }
    else if constexpr (std::is_same_v<Greater, Cond>) {
        find_gt(arr, value, start, end, baseindex, state);
    }

    return true;
}

// this is not nice code!! just for testing
template <typename Cond, bool v = true>
inline uint64_t bitwidth_cmp(uint64_t MSBs, uint64_t a, uint64_t b)
{
    if constexpr (std::is_same_v<Cond, Equal>)
        return find_all_fields_EQ(MSBs, a, b);
    else if constexpr (std::is_same_v<Cond, NotEqual>)
        return find_all_fields_NE(MSBs, a, b);
    else if constexpr (std::is_same_v<Cond, GreaterEqual>) {
        if constexpr (v == true)
            return find_all_fields_signed_GE(MSBs, a, b);
        if constexpr (v == false)
            return find_all_fields_unsigned_GE(MSBs, a, b);
        REALM_UNREACHABLE();
    }

    else if constexpr (std::is_same_v<Cond, Greater>)
        return find_all_fields_signed_GT(MSBs, a, b);
    else if constexpr (std::is_same_v<Cond, Less>)
        return find_all_fields_unsigned_LT(MSBs, a, b);
}

template <bool v>
inline uint64_t get_MSBs(const Array& arr)
{
    if constexpr (v == false)
        return arr.get_encoder().m_ndx_MSBs;
    return arr.get_encoder().m_MSBs;
}

template <bool v>
inline size_t get_field_count(const Array& arr)
{
    if constexpr (v == false)
        return arr.get_encoder().m_ndx_field_count;
    return arr.get_encoder().m_field_count;
}

template <bool v>
inline int8_t get_bit_count_per_iteration(const Array& arr)
{
    if constexpr (v == false)
        return arr.get_encoder().m_ndx_bit_count_pr_iteration;
    return arr.get_encoder().m_bit_count_pr_iteration;
}

inline uint64_t last_word_mask(size_t total_bit_count_left)
{
    // generates a mask of 1s shifting by 64-<bit count left> 0xFFFFULL.
    // Useful for extracting the last number of bits from a vector of matching positions.
    return 0xFFFFFFFFFFFFFFFFULL >> (64 - total_bit_count_left);
}

template <typename Cond, bool v>
inline size_t ArrayFlex::parallel_subword_find(const Array& arr, size_t offset, uint_least8_t width, size_t start,
                                               size_t end, uint64_t search_vector, int64_t total_bit_count_left) const
{
    // a bit hacky needs to be redesign... true means that we are parallel searching for values,
    // false that we are paralleling searching for indices.
    auto MSBs = get_MSBs<v>(arr);
    auto field_count = get_field_count<v>(arr);
    auto bit_count_pr_iteration = get_bit_count_per_iteration<v>(arr);

    REALM_ASSERT_DEBUG(total_bit_count_left >= 0);

    unaligned_word_iter it((uint64_t*)(arr.m_data), offset + start * width);
    uint64_t vector = 0;
    while (total_bit_count_left >= bit_count_pr_iteration) {
        const auto word = it.get(bit_count_pr_iteration);
        vector = bitwidth_cmp<Cond, v>(MSBs, word, search_vector);
        if (vector) {
            int sub_word_index = first_field_marked(width, vector);
            return start + sub_word_index;
        }
        total_bit_count_left -= bit_count_pr_iteration;
        start += field_count;
        it.bump(bit_count_pr_iteration);
    }
    if (total_bit_count_left) {                         // final subword, may be partial
        const auto word = it.get(total_bit_count_left); // <-- limit lookahead to avoid touching memory beyond array
        vector = bitwidth_cmp<Cond, v>(MSBs, word, search_vector);
        vector &= last_word_mask(total_bit_count_left);
        if (vector) {
            int sub_word_index = first_field_marked(width, vector);
            return start + sub_word_index;
        }
    }
    return end;
}

bool ArrayFlex::find_eq(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                        QueryStateBase* state) const
{
    using namespace std::chrono;
    const auto& encoder = arr.m_encoder;
    const auto v_width = encoder.m_v_width;
    const auto v_size = encoder.m_v_size;
    const auto ndx_width = encoder.m_ndx_width;
    const auto offset = v_size * v_width;
    const auto search_vector_val = populate(v_width, value);
    const auto total_bit_count_left_val = static_cast<int64_t>(v_size) * v_width;


    // auto t1 = std::chrono::high_resolution_clock::now();

    // this is a search among all the values, if we have many values with a large size, it is going to be more
    // expensive that what it is needed, if [start, end] is a narrow range..
    const auto v_start =
        parallel_subword_find<Equal>(arr, 0, v_width, 0, v_size, search_vector_val, total_bit_count_left_val);
    if (v_start == v_size)
        return true;

    //    auto t2 = std::chrono::high_resolution_clock::now();
    //    std::cout << "Time first parallel find: " <<  std::chrono::duration_cast<nanoseconds>(t2 - t1).count() << "
    //    ns" << std::endl;

    // t1 = std::chrono::high_resolution_clock::now();
    const auto search_vector_ndx = populate(ndx_width, v_start);
    const auto total_bit_count_left_ndx = static_cast<int64_t>(end - start) * ndx_width;
    while (start < end) {
        start = parallel_subword_find<Equal, false>(arr, offset, ndx_width, start, end, search_vector_ndx,
                                                    total_bit_count_left_ndx);
        if (start < end)
            if (!state->match(start + baseindex))
                return false;
        start += 1;
    }
    //    t2 = std::chrono::high_resolution_clock::now();
    //    std::cout << "Time second parallel find: " <<  std::chrono::duration_cast<nanoseconds>(t2 - t1).count() << "
    //    ns" << std::endl;
    return true;
}

bool ArrayFlex::find_neq(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                         QueryStateBase* state) const
{
    const auto& encoder = arr.m_encoder;
    const auto v_width = encoder.m_v_width;
    const auto v_size = encoder.m_v_size;
    const auto ndx_width = encoder.m_ndx_width;
    const auto offset = v_size * v_width;
    const auto search_vector_val = populate(v_width, value);
    const auto total_bit_count_left_val = static_cast<signed>(v_size * v_width);

    const auto v_start =
        parallel_subword_find<Equal>(arr, 0, v_width, 0, v_size, search_vector_val, total_bit_count_left_val);
    if (v_start == v_size)
        return true;

    const auto search_vector_ndx = populate(ndx_width, v_start);
    const auto total_bit_count_left_ndx = static_cast<signed>(end - start) * ndx_width;
    while (start < end) {
        start = parallel_subword_find<NotEqual, false>(arr, offset, ndx_width, start, end, search_vector_ndx,
                                                       total_bit_count_left_ndx);
        if (start < end)
            if (!state->match(start + baseindex))
                return false;
        start += 1;
    }
    return true;
}

bool ArrayFlex::find_lt(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                        QueryStateBase* state) const
{
    const auto& encoder = arr.m_encoder;
    const auto v_width = encoder.m_v_width;
    const auto v_size = encoder.m_v_size;
    const auto ndx_width = encoder.m_ndx_width;
    const auto offset = v_size * v_width;
    const auto search_vector_val = populate(v_width, value);
    const auto total_bit_count_left_val = static_cast<signed>(v_size * v_width);

    auto v_start =
        parallel_subword_find<GreaterEqual>(arr, 0, v_width, 0, v_size, search_vector_val, total_bit_count_left_val);
    if (v_start == v_size)
        return true;

    const auto search_vector_ndx = populate(ndx_width, v_start);
    const auto total_bit_count_left_ndx = static_cast<signed>(end - start) * ndx_width;
    while (start < end) {
        start = parallel_subword_find<Less, false>(arr, offset, ndx_width, start, end, search_vector_ndx,
                                                   total_bit_count_left_ndx);
        if (start < end)
            if (!state->match(start + baseindex))
                return false;
        start += 1;
    }
    return true;
}

bool ArrayFlex::find_gt(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                        QueryStateBase* state) const
{
    const auto& encoder = arr.m_encoder;
    const auto v_width = encoder.m_v_width;
    const auto v_size = encoder.m_v_size;
    const auto ndx_width = encoder.m_ndx_width;
    const auto offset = v_size * v_width;
    const auto search_vector_val = populate(v_width, value);
    const auto total_bit_count_left_val = static_cast<signed>(v_size * v_width);

    auto v_start =
        parallel_subword_find<Greater>(arr, 0, v_width, 0, v_size, search_vector_val, total_bit_count_left_val);
    if (v_start == v_size)
        return true;

    const auto search_vector_ndx = populate(ndx_width, v_start);
    const auto total_bit_count_left_ndx = static_cast<signed>(end - start) * ndx_width;
    while (start < end) {
        start = parallel_subword_find<GreaterEqual, false>(arr, offset, ndx_width, start, end, search_vector_ndx,
                                                           total_bit_count_left_ndx);
        if (start < end)
            if (!state->match(start + baseindex))
                return false;
        start += 1;
    }
    return true;
}

int64_t ArrayFlex::sum(const Array& arr, size_t start, size_t end) const
{
    const auto& encoder = arr.m_encoder;
    const auto data = (uint64_t*)arr.m_data;
    const auto v_width = encoder.m_v_width;
    const auto v_size = encoder.m_v_size;
    const auto ndx_width = encoder.m_ndx_width;
    const auto ndx_size = encoder.m_ndx_size;
    const auto mask = encoder.width_mask();

    REALM_ASSERT_DEBUG(start < ndx_size && end < ndx_size);

    const auto offset = v_size * v_width;
    int64_t acc = 0;

    bf_iterator it_index{data, static_cast<size_t>(offset), ndx_width, ndx_width, start};
    for (; start < end; ++start, ++it_index) {
        const auto v = read_bitfield(data, it_index.get_value() * v_width, v_width);
        acc += sign_extend_field_by_mask(mask, v);
    }
    return acc;
}

bool ArrayFlex::find_all_match(size_t start, size_t end, size_t baseindex, QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(state->match_count() < state->limit());
    const auto process = state->limit() - state->match_count();
    const auto end2 = end - start > process ? start + process : end;
    for (; start < end2; start++)
        if (!state->match(start + baseindex))
            return false;
    return true;
}
