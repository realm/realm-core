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

#include <realm/array_packed.hpp>
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

template bool ArrayPacked::find_all<Equal>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
template bool ArrayPacked::find_all<NotEqual>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
template bool ArrayPacked::find_all<Greater>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
template bool ArrayPacked::find_all<Less>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;


void ArrayPacked::init_array(char* h, uint8_t flags, size_t v_width, size_t v_size) const
{
    using Encoding = NodeHeader::Encoding;
    NodeHeader::init_header((char*)h, Encoding::Packed, flags, v_width, v_size);
}

void ArrayPacked::copy_data(const Array& origin, Array& arr) const
{
    // this can be boosted a little bit, with and size should be known at this stage.
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.m_encoder.get_encoding() == Encoding::Packed);
    // we don't need to access the header, init from mem must have been called
    const auto v_width = arr.m_width;
    const auto v_size = arr.m_size;
    auto data = (uint64_t*)arr.m_data;
    bf_iterator it_value{data, 0, v_width, v_width, 0};
    for (size_t i = 0; i < v_size; ++i) {
        it_value.set_value(origin.get(i));
        REALM_ASSERT_DEBUG(sign_extend_value(v_width, it_value.get_value()) == origin.get(i));
        ++it_value;
    }
}

void ArrayPacked::set_direct(const Array& arr, size_t ndx, int64_t value) const
{
    REALM_ASSERT_DEBUG(arr.is_encoded());
    const auto v_width = arr.m_encoder.m_v_width;
    const auto v_size = arr.m_encoder.m_v_size;
    REALM_ASSERT_DEBUG(ndx < v_size);
    auto data = (uint64_t*)arr.m_data;
    bf_iterator it_value{data, static_cast<size_t>(ndx * v_width), v_width, v_width, 0};
    it_value.set_value(value);
}

int64_t ArrayPacked::get(const Array& arr, size_t ndx) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.is_encoded());
    const auto w = arr.m_encoder.m_v_width;
    const auto sz = arr.m_encoder.m_v_size;
    return do_get((uint64_t*)arr.m_data, ndx, w, sz, arr.get_encoder().width_mask());
}

int64_t ArrayPacked::get(const char* data, size_t ndx, size_t width, size_t sz, uint64_t mask) const
{
    return do_get((uint64_t*)data, ndx, width, sz, mask);
}

int64_t ArrayPacked::do_get(uint64_t* data, size_t ndx, size_t v_width, size_t v_size, uint64_t mask) const
{
    if (ndx >= v_size)
        return realm::not_found;
    bf_iterator it{data, 0, v_width, v_width, ndx};
    const auto value = it.get_value();
    return sign_extend_field_by_mask(mask, value);
}

void ArrayPacked::get_chunk(const Array& arr, size_t ndx, int64_t res[8]) const
{
    const auto v_size = arr.m_size;
    REALM_ASSERT_DEBUG(ndx < v_size);
    auto sz = 8;
    std::memset(res, 0, sizeof(int64_t) * sz);
    auto supposed_end = ndx + sz;
    size_t i = ndx;
    size_t index = 0;
    // this can be done better, in one go, retrieve both!!!
    for (; i < supposed_end; ++i) {
        res[index++] = get(arr, i);
    }
    for (; index < 8; ++index) {
        res[index++] = get(arr, i++);
    }
}

template <typename Cond>
uint64_t vector_compare(uint64_t MSBs, uint64_t a, uint64_t b)
{
    if constexpr (std::is_same_v<Cond, Equal>)
        return find_all_fields_EQ(MSBs, a, b);
    if constexpr (std::is_same_v<Cond, NotEqual>)
        return find_all_fields_NE(MSBs, a, b);
    if constexpr (std::is_same_v<Cond, Greater>)
        return find_all_fields_signed_GT(MSBs, a, b);
    if constexpr (std::is_same_v<Cond, Less>)
        return find_all_fields_signed_LT(MSBs, a, b);
};

template <typename Cond>
size_t parallel_subword_find(Cond cond, const uint64_t* data, size_t offset, size_t width, uint64_t MSBs,
                             uint64_t search_vector, size_t start, size_t end);

template <typename Cond>
bool ArrayPacked::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
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


    // NOTE: this is one of the most important functions in the whole codebase, since it determines how fast the
    // queries run.
    //
    // Main idea around find.
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

    // in packed format a parallel subword find pays off also for width >= 32

    const auto MSBs = populate(arr.m_width, arr.get_encoder().width_mask());
    const auto search_vector = populate(arr.m_width, value);
    while (start < end) {
        start = parallel_subword_find(vector_compare<Cond>, (const uint64_t*)arr.m_data, 0, arr.m_width, MSBs,
                                      search_vector, start, end);
        if (start < end)
            if (!state->match(start + baseindex))
                return false;

        ++start;
    }
    return true;
}

template <typename VectorCompare>
size_t parallel_subword_find(VectorCompare vector_compare, const uint64_t* data, size_t offset, size_t width,
                             uint64_t MSBs, uint64_t search_vector, size_t start, size_t end)
{
    const auto field_count = num_fields_for_width(width);
    const auto bit_count_pr_iteration = num_bits_for_width(width);
    // use signed to make it easier to ascertain correctness of loop condition below
    signed total_bit_count_left = static_cast<signed>(end - start) * static_cast<signed>(width);
    REALM_ASSERT(total_bit_count_left >= 0);
    unaligned_word_iter it(data, offset + start * width);
    uint64_t found_vector = 0;
    while (total_bit_count_left >= bit_count_pr_iteration) {
        const auto word = it.get(bit_count_pr_iteration);
        found_vector = vector_compare(MSBs, word, search_vector);
        if (found_vector) {
            int sub_word_index = first_field_marked(width, found_vector);
            return start + sub_word_index;
        }
        total_bit_count_left -= bit_count_pr_iteration;
        start += field_count;
        it.bump(bit_count_pr_iteration);
    }
    if (total_bit_count_left) {                         // final subword, may be partial
        const auto word = it.get(total_bit_count_left); // <-- limit lookahead to avoid touching memory beyond array
        found_vector = vector_compare(MSBs, word, search_vector);
        auto last_word_mask = 0xFFFFFFFFFFFFFFFFULL >> (64 - total_bit_count_left);
        found_vector &= last_word_mask;
        if (found_vector) {
            int sub_word_index = first_field_marked(width, found_vector);
            return start + sub_word_index;
        }
    }
    return end;
}

bool ArrayPacked::find_all_match(size_t start, size_t end, size_t baseindex, QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(state->match_count() < state->limit());
    const auto process = state->limit() - state->match_count();
    const auto end2 = end - start > process ? start + process : end;
    for (; start < end2; start++)
        if (!state->match(start + baseindex))
            return false;
    return true;
}

int64_t ArrayPacked::sum(const Array& arr, size_t start, size_t end) const
{
    const auto mask = arr.get_encoder().width_mask();
    int64_t acc = 0;
    bf_iterator it((uint64_t*)arr.m_data, 0, arr.m_width, arr.m_width, start);
    for (; start < end; ++start, ++it)
        acc += sign_extend_field_by_mask(mask, it.get_value());
    return acc;
}
