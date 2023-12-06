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

bool ArrayFlex::encode(const Array& origin, Array& encoded) const
{
    REALM_ASSERT(origin.is_attached());
    const auto sz = origin.size();
    std::vector<int64_t> values;
    std::vector<size_t> indices;
    if (!is_encoded(origin) && try_encode(origin, encoded, values, indices)) {
        REALM_ASSERT(!values.empty());
        REALM_ASSERT(!indices.empty());
        REALM_ASSERT(indices.size() == sz);
        copy_into_encoded_array(encoded, values, indices);
        return true;
    }
    return false;
}

bool ArrayFlex::decode(Array& arr)
{
    REALM_ASSERT(arr.is_attached());
    size_t value_width, index_width, value_size, index_size;
    if (get_encode_info(arr.get_header(), value_width, index_width, value_size, index_size)) {
        auto values = fetch_values_from_encoded_array(arr, value_width, index_width, value_size, index_size);
        REALM_ASSERT(values.size() == index_size);
        restore_array(arr, values);
        return true;
    }
    return false;
}

bool ArrayFlex::is_encoded(const Array& arr) const
{
    // We are calling this when the header is not yet initiliased!
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT(arr.is_attached());
    auto header = arr.get_header();
    auto kind = Node::get_kind(header);
    return kind == 'B' && Node::get_encoding(header) == Encoding::Flex;
}

size_t ArrayFlex::size(const Array& arr) const
{
    REALM_ASSERT(arr.is_attached());
    using Encoding = NodeHeader::Encoding;
    auto header = arr.get_header();
    REALM_ASSERT(NodeHeader::get_kind(header) == 'B' && NodeHeader::get_encoding(header) == Encoding::Flex);
    return NodeHeader::get_arrayB_num_elements<NodeHeader::Encoding::Flex>(header);
}

int64_t ArrayFlex::get(const Array& arr, size_t ndx) const
{
    REALM_ASSERT(arr.is_attached());
    size_t value_width, index_width, value_size, index_size;
    if (get_encode_info(arr.get_header(), value_width, index_width, value_size, index_size)) {

        if (ndx >= index_size)
            return realm::not_found;

        auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        const auto offset = (value_size * value_width) + (ndx * index_width);
        const auto index = read_bitfield(data, offset, index_width);
        bf_iterator it_value{data, 0, value_width, value_width, index};
        const auto v = it_value.get_value();
        const auto sign_v = sign_extend_field(value_width, v);
        return sign_v;
    }
    REALM_UNREACHABLE();
}

bool ArrayFlex::try_encode(const Array& origin, Array& encoded, std::vector<int64_t>& values,
                           std::vector<size_t>& indices) const
{
    const auto sz = origin.size();

    if (sz <= 1)
        return false;

    // put data in flex format, essentially arrays of values + arrays of indices
    arrange_data_in_flex_format(origin, values, indices);

    // check if makes sense to move forward and replace the current array's data with an encoded version of it
    int value_bit_width = 0;
    int index_bit_width = 0;
    if (check_gain(origin, values, indices, value_bit_width, index_bit_width)) {
#if REALM_DEBUG
        for (size_t i = 0; i < sz; ++i)
            REALM_ASSERT(origin.get(i) == values[indices[i]]);
#endif
        setup_array_in_flex_format(origin, encoded, values, indices, value_bit_width, index_bit_width);
        return true;
    }
    return false;
}

void ArrayFlex::copy_into_encoded_array(Array& arr, std::vector<int64_t>& values, std::vector<size_t>& indices) const
{
    REALM_ASSERT(arr.is_attached());
    using Encoding = NodeHeader::Encoding;
    auto header = arr.get_header();
    auto value_width = NodeHeader::get_elementA_size<Encoding::Flex>(header);
    auto index_width = NodeHeader::get_elementB_size<Encoding::Flex>(header);
    auto value_size = values.size();
    // fill data
    auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
    uint64_t offset = value_size * value_width;
    bf_iterator it_value{data, 0, value_width, value_width, 0};
    bf_iterator it_index{data, offset, index_width, index_width, 0};
    for (size_t i = 0; i < values.size(); ++i) {
        it_value.set_value(values[i]);
        auto v2 = it_value.get_value();
        auto v = sign_extend_field(value_width, v2);
        REALM_ASSERT_3(v, ==, values[i]);
        ++it_value;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
        auto v = sign_extend_field(value_width, read_bitfield(data, indices[i] * value_width, value_width));
        REALM_ASSERT(values[indices[i]] == v);
        it_index.set_value(indices[i]);
        REALM_ASSERT(indices[i] == it_index.get_value());
        v = sign_extend_field(value_width, read_bitfield(data, indices[i] * value_width, value_width));
        REALM_ASSERT(values[indices[i]] == v);
        ++it_index;
    }
    REALM_ASSERT(arr.get_kind(header) == 'B');
    REALM_ASSERT(arr.get_encoding(header) == Encoding::Flex);
}

void ArrayFlex::arrange_data_in_flex_format(const Array& arr, std::vector<int64_t>& values,
                                            std::vector<size_t>& indices) const
{
    // Implements the main logic for supporting the encondig Flex protocol.
    // Flex enconding works keeping 2 arrays, one for storing the values and, the other one for storing the indices of
    // these values in the original array. All the values and indices take the same amount of memory space,
    // essentially the max(value) and max(index) bid width is used to determine how much space each value and index
    // take in the array. The 2 arrays are allocated continuosly in one chunk of memory, first comes the array of
    // values and later  the array of indices.
    //
    //   || node header || ..... values ..... || ..... indices ..... ||
    //
    // The encoding algorithm runs in O(n lg n).

    const auto sz = arr.size();

    if (sz <= 1)
        return;

    values.reserve(sz);
    indices.reserve(sz);

    for (size_t i = 0; i < sz; ++i) {
        auto item = arr.get(i);
        values.push_back(item);
        REALM_ASSERT_3(values.back(), ==, item);
    }

    std::sort(values.begin(), values.end());
    auto last = std::unique(values.begin(), values.end());
    values.erase(last, values.end());

    for (size_t i = 0; i < arr.size(); ++i) {
        auto pos = std::lower_bound(values.begin(), values.end(), arr.get(i));
        indices.push_back(std::distance(values.begin(), pos));
        REALM_ASSERT(values[indices[i]] == arr.get(i));
    }

    for (size_t i = 0; i < sz; ++i) {
        auto old_value = arr.get(i);
        auto new_value = values[indices[i]];
        REALM_ASSERT_3(new_value, ==, old_value);
    }
}

bool ArrayFlex::check_gain(const Array& arr, std::vector<int64_t>& values, std::vector<size_t>& indices,
                           int& value_bit_width, int& index_bit_width) const
{
    using Encoding = NodeHeader::Encoding;
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    const auto index = *std::max_element(indices.begin(), indices.end());
    value_bit_width = std::max(Node::signed_to_num_bits(*min_value), Node::signed_to_num_bits(*max_value));
    index_bit_width = index == 0 ? 1 : Node::unsigned_to_num_bits(index);
    REALM_ASSERT(value_bit_width > 0);
    REALM_ASSERT(index_bit_width > 0);
    // we should consider Encoding::Packed as well here.
    const auto uncompressed_size = arr.get_byte_size();
    auto byte_size =
        NodeHeader::calc_size<Encoding::Flex>(values.size(), indices.size(), value_bit_width, index_bit_width);
    return byte_size < uncompressed_size;
}

void ArrayFlex::setup_array_in_flex_format(const Array& origin, Array& arr, std::vector<int64_t>& values,
                                           std::vector<size_t>& indices, int value_bit_width,
                                           int index_bit_width) const
{
    using Encoding = NodeHeader::Encoding;

    // I'm assuming that flags are taken from the owning Array.
    uint8_t flags = NodeHeader::get_flags(origin.get_header());

    auto byte_size =
        NodeHeader::calc_size<Encoding::Flex>(values.size(), indices.size(), value_bit_width, index_bit_width);


    Allocator& allocator = arr.get_alloc();
    auto mem = allocator.alloc(byte_size);
    auto header = mem.get_addr();
    NodeHeader::init_header(header, 'B', Encoding::Flex, flags, value_bit_width, index_bit_width, values.size(),
                            indices.size());
    REALM_ASSERT(NodeHeader::get_kind(header) == 'B');
    REALM_ASSERT(NodeHeader::get_encoding(header) == Encoding::Flex);
    NodeHeader::set_capacity_in_header(byte_size, (char*)header);
    arr.init_from_mem(mem);
    REALM_ASSERT(arr.m_ref == mem.get_ref());
    REALM_ASSERT(arr.get_kind(header) == 'B');
    REALM_ASSERT(arr.get_encoding(header) == Encoding::Flex);
}

bool inline ArrayFlex::get_encode_info(const char* header, size_t& value_width, size_t& index_width,
                                       size_t& value_size, size_t& index_size)
{
    using Encoding = NodeHeader::Encoding;
    auto h = header;
    if (NodeHeader::get_kind(h) == 'B' && NodeHeader::get_encoding(h) == Encoding::Flex) {
        value_size = NodeHeader::get_arrayA_num_elements<Encoding::Flex>(h);
        index_size = NodeHeader::get_arrayB_num_elements<Encoding::Flex>(h);
        value_width = NodeHeader::get_elementA_size<Encoding::Flex>(h);
        index_width = NodeHeader::get_elementB_size<Encoding::Flex>(h);
        return true;
    }
    return false;
}

std::vector<int64_t> ArrayFlex::fetch_values_from_encoded_array(const Array& arr, size_t value_width,
                                                                size_t index_width, size_t value_size,
                                                                size_t index_size) const
{
    std::vector<int64_t> values;
    values.reserve(index_size);
    auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
    const auto offset = value_size * value_width;
    bf_iterator index_iterator{data, offset, index_width, index_width, 0};
    for (size_t i = 0; i < index_size; ++i) {
        const auto index = index_iterator.get_value();
        bf_iterator it_value{data, index * value_width, value_width, value_width, 0};
        const auto value = it_value.get_value();
        const auto ivalue = sign_extend_field(value_width, value);
        values.push_back(ivalue);
        ++index_iterator;
    }
    return values;
}

void ArrayFlex::restore_array(Array& arr, const std::vector<int64_t>& values) const
{
    // do the reverse of compressing the array
    REALM_ASSERT(arr.is_attached());
    using Encoding = NodeHeader::Encoding;
    const auto flags = NodeHeader::get_flags(arr.get_header());
    const auto size = values.size();
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    auto width_a = NodeHeader::signed_to_num_bits(*min_value);
    auto width_b = NodeHeader::signed_to_num_bits(*max_value);
    auto width = std::max(width_a, width_b);
    auto byte_size = NodeHeader::calc_size<Encoding::WTypBits>(size, width);
    REALM_ASSERT(byte_size % 8 == 0); // 8 bytes aligned value
    Allocator& allocator = arr.get_alloc();
    arr.destroy();
    auto mem = allocator.alloc(byte_size);
    auto header = mem.get_addr();
    NodeHeader::init_header(header, 'A', Encoding::WTypBits, flags, 0, 0);
    NodeHeader::set_capacity_in_header(byte_size, (char*)header);
    arr.init_from_mem(mem);
    arr.update_parent();
    size_t i = 0;
    for (const auto& v : values)
        arr.insert(i++, v);
    size_t w = arr.get_width();
    REALM_ASSERT(w == 0 || w == 1 || w == 2 || w == 4 || w == 8 || w == 16 || w == 32 || w == 64);
    REALM_ASSERT(arr.size() == values.size());
}

size_t ArrayFlex::find_first(const Array& arr, int64_t value) const
{
    REALM_ASSERT(arr.is_attached());
    size_t value_width, index_width, value_size, index_size;
    if (get_encode_info(arr.get_header(), value_width, index_width, value_size, index_size)) {
        auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        const auto offset = value_size * value_width;
        bf_iterator index_iterator{data, offset, index_width, index_width, 0};
        for (size_t i = 0; i < index_size; ++i) {
            const auto index = index_iterator.get_value();
            bf_iterator it_value{data, index * value_width, value_width, value_width, 0};
            const auto arr_value = it_value.get_value();
            const auto ivalue = sign_extend_field(value_width, arr_value);
            if (ivalue == value)
                return i;
            ++index_iterator;
        }
    }
    return realm::not_found;
}

int64_t ArrayFlex::get(const char* header, size_t ndx)
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT(NodeHeader::get_kind(header) == 'B');
    REALM_ASSERT(NodeHeader::get_encoding(header) == Encoding::Flex);
    size_t value_width, index_width, value_size, index_size;
    if (get_encode_info(header, value_width, index_width, value_size, index_size)) {
        if (ndx >= index_size)
            return realm::not_found;
        auto data = (uint64_t*)NodeHeader::get_data_from_header(header);
        const auto offset = value_size * value_width;
        bf_iterator index_iterator{data, offset + (ndx * index_width), index_width, index_width, 0};
        auto index_value = index_iterator.get_value();
        bf_iterator it_value{data, index_value * value_width, value_width, value_width, 0};
        const auto arr_value = it_value.get_value();
        const auto ivalue = sign_extend_field(value_width, arr_value);
        return ivalue;
    }
    return realm::not_found;
}
