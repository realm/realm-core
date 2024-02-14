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
    REALM_ASSERT_DEBUG(arr.m_encoder.get_kind() == 'B');
    REALM_ASSERT_DEBUG(arr.m_encoder.get_encoding() == Encoding::Flex);

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
    const auto v_width = arr.m_encoder.m_v_width;
    const auto v_size = arr.m_encoder.m_v_size;
    const auto ndx_width = arr.m_encoder.m_ndx_width;
    const auto ndx_size = arr.m_encoder.m_ndx_size;
    return do_get((uint64_t*)arr.m_data, ndx, v_width, ndx_width, v_size, ndx_size);
}

int64_t ArrayFlex::get(const char* data, size_t ndx, size_t v_width, size_t v_size, size_t ndx_width,
                       size_t ndx_size) const
{
    return do_get((uint64_t*)data, ndx, v_width, v_size, ndx_width, ndx_size);
    //    using Encoding = NodeHeader::Encoding;
    //    REALM_ASSERT_DEBUG(NodeHeader::get_kind(h) == 'B');
    //    REALM_ASSERT_DEBUG(NodeHeader::get_encoding(h) == Encoding::Flex);
    //    //size_t v_size, ndx_size, v_width, ndx_width;
    //    //get_encode_info(h, v_width, ndx_width, v_size, ndx_size);
    //    const auto data = (uint64_t*)(NodeHeader::get_data_from_header(h));
    //    return do_get(data, ndx, v_width, ndx_width, v_size, ndx_size);
}

int64_t ArrayFlex::do_get(uint64_t* data, size_t ndx, size_t v_width, size_t ndx_width, size_t v_size,
                          size_t ndx_size)
{
    if (ndx >= ndx_size)
        return realm::not_found;
    const uint64_t offset = v_size * v_width;
    const bf_iterator it_index{data, static_cast<size_t>(offset + (ndx * ndx_width)), ndx_width, ndx_width, 0};
    const bf_iterator it_value{data, static_cast<size_t>(v_width * it_index.get_value()), v_width, v_width, 0};
    auto v = sign_extend_field(v_width, it_value.get_value());
    return v;
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

std::vector<int64_t> ArrayFlex::fetch_all_values(const Array& arr) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.m_encoder.get_encoding() == NodeHeader::Encoding::Flex);
    std::vector<int64_t> values;
    values.reserve(arr.m_size);
    for (size_t i = 0; i < arr.m_size; ++i)
        values.push_back(get(arr, i));
    return values;
}
