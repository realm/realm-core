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

NodeHeader::Encoding ArrayFlex::get_encoding() const
{
    return NodeHeader::Encoding::Flex;
}

void ArrayFlex::init_array(char* h, uint8_t flags, size_t v_width, size_t ndx_width, size_t v_size,
                           size_t ndx_size) const
{
    using Encoding = NodeHeader::Encoding;
    NodeHeader::init_header(h, 'B', Encoding::Flex, flags, v_width, ndx_width, v_size, ndx_size);
}

void ArrayFlex::copy_data(const Array& arr, const std::vector<int64_t>& values,
                          const std::vector<size_t>& indices) const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.m_kind == 'B');
    REALM_ASSERT_DEBUG(arr.m_encoding == Encoding::Flex);

    auto h = arr.get_header();
    auto v_width = NodeHeader::get_elementA_size<Encoding::Flex>(h);
    auto ndx_width = NodeHeader::get_elementB_size<Encoding::Flex>(h);
    auto v_size = values.size();
    auto data = (uint64_t*)arr.m_data;
    auto offset = static_cast<size_t>(v_size * v_width);
    bf_iterator it_value{data, 0, v_width, v_width, 0};
    bf_iterator it_index{data, offset, ndx_width, ndx_width, 0};
    for (size_t i = 0; i < values.size(); ++i) {
        it_value.set_value(values[i]);
        REALM_ASSERT_DEBUG(sign_extend_field(v_width, it_value.get_value()) == values[i]);
        ++it_value;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
        REALM_ASSERT_DEBUG(values[indices[i]] ==
                           sign_extend_field(v_width, read_bitfield(data, indices[i] * v_width, v_width)));
        it_index.set_value(indices[i]);
        REALM_ASSERT_DEBUG(indices[i] == it_index.get_value());
        REALM_ASSERT_DEBUG(values[indices[i]] ==
                           sign_extend_field(v_width, read_bitfield(data, indices[i] * v_width, v_width)));
        ++it_index;
    }
}

void ArrayFlex::set_direct(const char* h, size_t ndx, int64_t value) const
{
    size_t v_width, ndx_width, v_size, ndx_size;
    get_encode_info(h, v_width, ndx_width, v_size, ndx_size);
    REALM_ASSERT_DEBUG(ndx < ndx_size);
    auto data = (uint64_t*)NodeHeader::get_data_from_header(h);
    uint64_t offset = v_size * v_width;
    bf_iterator it_index{data, static_cast<size_t>(offset + (ndx * ndx_width)), ndx_width, ndx_size, 0};
    bf_iterator it_value{data, static_cast<size_t>(it_index.get_value() * v_width), v_width, v_width, 0};
    it_value.set_value(value);
}

int64_t ArrayFlex::get(const char* h, size_t ndx)
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(h) == 'B');
    REALM_ASSERT_DEBUG(NodeHeader::get_encoding(h) == NodeHeader::Encoding::Flex);
    const auto v_size = NodeHeader::get_arrayA_num_elements<Encoding::Flex>(h);
    const auto ndx_size = NodeHeader::get_arrayB_num_elements<Encoding::Flex>(h);
    const auto v_width = NodeHeader::get_elementA_size<Encoding::Flex>(h);
    const auto ndx_width = NodeHeader::get_elementB_size<Encoding::Flex>(h);
    if (ndx >= ndx_size)
        return realm::not_found;
    const auto data = (uint64_t*)NodeHeader::get_data_from_header(h);
    const uint64_t offset = v_size * v_width;
    const bf_iterator it_index{data, static_cast<size_t>(offset + (ndx * ndx_width)), ndx_width, ndx_width, 0};
    const bf_iterator it_value{data, static_cast<size_t>(v_width * it_index.get_value()), v_width, v_width, 0};
    auto v = sign_extend_field(v_width, it_value.get_value());
    return v;
}

void ArrayFlex::get_chunk(const char* h, size_t ndx, int64_t res[8]) const
{
    size_t v_width, ndx_width, v_size, ndx_size;
    get_encode_info(h, v_width, ndx_width, v_size, ndx_size);
    REALM_ASSERT_DEBUG(ndx < ndx_width);
    auto sz = 8;
    std::memset(res, 0, sizeof(int64_t) * sz);
    auto supposed_end = ndx + sz;
    size_t i = ndx;
    size_t index = 0;
    for (; i < supposed_end; ++i) {
        res[index++] = get(h, i);
    }
    for (; index < 8; ++index) {
        res[index++] = get(h, i++);
    }
}

bool inline ArrayFlex::get_encode_info(const char* h, size_t& v_width, size_t& ndx_width, size_t& v_size,
                                       size_t& ndx_size)
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(NodeHeader::get_encoding(h) == Encoding::Flex);
    v_size = NodeHeader::get_arrayA_num_elements<Encoding::Flex>(h);
    ndx_size = NodeHeader::get_arrayB_num_elements<Encoding::Flex>(h);
    v_width = NodeHeader::get_elementA_size<Encoding::Flex>(h);
    ndx_width = NodeHeader::get_elementB_size<Encoding::Flex>(h);
    return true;
}

std::vector<int64_t> ArrayFlex::fetch_signed_values_from_encoded_array(const Array& arr) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.m_encoding == NodeHeader::Encoding::Flex);
    const auto h = arr.get_header();
    std::size_t v_size, ndx_size, v_width, ndx_width;
    get_encode_info(h, v_width, ndx_width, v_size, ndx_size);
    std::vector<int64_t> values;
    values.reserve(ndx_size);
    auto data = (uint64_t*)arr.m_data;
    const auto offset = v_size * v_width;
    bf_iterator index_iterator{data, offset, ndx_width, ndx_width, 0};
    for (size_t i = 0; i < ndx_size; ++i) {
        const auto index = index_iterator.get_value();
        bf_iterator it_value{data, static_cast<size_t>(index * v_width), v_width, v_width, 0};
        const auto value = it_value.get_value();
        const auto ivalue = sign_extend_field(v_width, value);
        values.push_back(ivalue);
        ++index_iterator;
    }
    return values;
}

int64_t ArrayFlex::sum(const Array& arr, size_t start, size_t end) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    get_encode_info(arr.get_header(), v_width, ndx_width, v_size, ndx_size);
    REALM_ASSERT_DEBUG(ndx_size >= start && ndx_size <= end);
    const auto data = (uint64_t*)arr.m_data;
    ;
    const auto offset = v_size * v_width + (start * ndx_width);
    bf_iterator index_iterator{data, offset, ndx_width, ndx_width, 0};
    int64_t total_sum = 0;
    for (size_t i = start; i < end; ++i) {
        const auto offset = static_cast<size_t>(index_iterator.get_value() * v_width);
        total_sum += *(data + offset);
        ++index_iterator;
    }
    return total_sum;
}


template <typename F>
size_t ArrayFlex::find_first(const Array& arr, int64_t key, size_t start, size_t end, F cmp)
{
    REALM_ASSERT_DEBUG(arr.is_attached());

    size_t v_width, v_size, ndx_width, ndx_size;
    const auto h = arr.get_header();
    get_encode_info(h, v_width, v_size, ndx_width, ndx_size);

    if (ndx_size <= 15) {
        const auto ndx_offset = v_size * v_width;
        for (size_t i = 0; i < ndx_size; ++i) {
            auto data = (uint64_t*)arr.m_data;
            const auto ndx = static_cast<size_t>(realm::read_bitfield(data, ndx_offset + (i * ndx_width), ndx_width));
            const auto unsigned_val = realm::read_bitfield(data, v_width * ndx, v_width);
            const auto v = sign_extend_field(v_width, unsigned_val);
            if (cmp(v, key))
                return ndx;
        }
    }
    else {
        size_t lo = start;
        size_t hi = std::min(ndx_size, end);
        const auto ndx_offset = v_size * v_width;
        while (lo <= hi) {
            size_t mid = lo + (hi - lo) / 2;
            auto data = (uint64_t*)arr.m_data;
            const auto ndx =
                static_cast<size_t>(realm::read_bitfield(data, ndx_offset + (mid * ndx_width), ndx_width));
            const auto unsigned_val = realm::read_bitfield(data, v_width * ndx, v_width);
            const auto v = sign_extend_field(v_width, unsigned_val);
            if (cmp(v, key))
                return ndx;
            else if (key < v)
                hi = mid - 1;
            else
                lo = mid + 1;
        }
    }
    return realm::not_found;
}