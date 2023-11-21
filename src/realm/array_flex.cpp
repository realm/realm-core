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

ArrayFlex::ArrayFlex(Array& array)
    : m_array(array)
{
}

bool ArrayFlex::encode() const
{
    REALM_ASSERT(m_array.is_attached());
    const auto sz = m_array.size();
    std::vector<int64_t> values;
    std::vector<size_t> indices;
    if (!is_encoded() && try_encode(values, indices)) {
        REALM_ASSERT(!values.empty());
        REALM_ASSERT(!indices.empty());
        REALM_ASSERT(indices.size() == sz);
        copy_into_encoded_array(values, indices);
        return true;
    }
    return false;
}

bool ArrayFlex::decode() const
{
    REALM_ASSERT(m_array.is_attached());
    size_t value_width, index_width, value_size, index_size;
    if (get_encode_info(value_width, index_width, value_size, index_size)) {

        std::vector<int64_t> values;
        values.reserve(index_size);
        auto data = (uint64_t*)m_array.get_data_from_header(m_array.get_header());
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
        // TODO understand if it is possible to do better than this.
        m_array.destroy(); // free the memory containing the compressed representation for this array.
        m_array.create(NodeHeader::Type::type_Normal); // recreate the array from scratch as type A
        size_t i = 0;
        for (const auto& v : values)
            m_array.insert(i++, v);
        const auto sz = m_array.size();
        REALM_ASSERT(sz == values.size());
        return true;
    }
    return false;
}

bool ArrayFlex::is_encoded() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT(m_array.is_attached());
    auto header = (uint64_t*)m_array.get_header();
    return Node::get_kind(header) == 'B' && Node::get_encoding((uint64_t*)header) == Encoding::Flex;
}

size_t ArrayFlex::size() const
{
    size_t value_width, index_width, value_size, index_size;
    if (get_encode_info(value_width, index_width, value_size, index_size)) {
        return index_size;
    }
    // calling array flex size for a uncompressed array is an error.
    REALM_UNREACHABLE();
}

int64_t ArrayFlex::get(size_t ndx) const
{
    REALM_ASSERT(m_array.is_attached());
    size_t value_width, index_width, value_size, index_size;
    if (get_encode_info(value_width, index_width, value_size, index_size)) {

        if (ndx >= index_size)
            return realm::not_found;

        auto data = (uint64_t*)m_array.get_data_from_header(m_array.get_header());
        const auto offset = (value_size * value_width) + (ndx * index_width);
        const auto index = read_bitfield(data, offset, index_width);
        bf_iterator it_value{data, 0, value_width, value_width, index};
        const auto v = it_value.get_value();
        const auto sign_v = sign_extend_field(value_width, v);
        return sign_v;
    }
    REALM_UNREACHABLE();
}

bool ArrayFlex::try_encode(std::vector<int64_t>& values, std::vector<size_t>& indices) const
{
    const auto sz = m_array.size();

    if (sz <= 1)
        return false;

    // encode the array in values and indices in order to verify if we can gain
    // something from this different format
    do_encode_array(values, indices);

    // check if makes sense to move forward and replace the current array's data with an encoded version of it
    int value_bit_width = 0;
    int index_bit_width = 0;
    if (check_gain(values, indices, value_bit_width, index_bit_width)) {
        // release array's memory and re-init the same array with a B header and flex encoding
        setup_header_in_flex_format(values, indices, value_bit_width, index_bit_width);
        return true;
    }
    return false;
}

void ArrayFlex::copy_into_encoded_array(std::vector<int64_t>& values, std::vector<size_t>& indices) const
{
    REALM_ASSERT(m_array.is_attached());
    using Encoding = NodeHeader::Encoding;
    auto header = (uint64_t*)m_array.get_header();
    auto value_width = m_array.get_elementA_size<Encoding::Flex>(header);
    auto index_width = m_array.get_elementB_size<Encoding::Flex>(header);
    auto value_size = values.size();
    // fill data
    auto data = (uint64_t*)NodeHeader::get_data_from_header(m_array.get_header());
    uint64_t offset = value_size * value_width;
    bf_iterator it_value{data, 0, value_width, value_width, 0};
    bf_iterator it_index{data, offset, index_width, index_width, 0};
    for (size_t i = 0; i < values.size(); ++i) {
        it_value.set_value(values[i]);
        auto v = sign_extend_field(value_width, it_value.get_value());
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
}

void ArrayFlex::do_encode_array(std::vector<int64_t>& values, std::vector<size_t>& indices) const
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

    const auto sz = m_array.size();

    if (sz <= 1)
        return;

    values.reserve(sz);
    indices.reserve(sz);

    for (size_t i = 0; i < sz; ++i) {
        auto item = m_array.get(i);
        values.push_back(item);
        REALM_ASSERT_3(values.back(), ==, item);
    }

    std::sort(values.begin(), values.end());
    auto last = std::unique(values.begin(), values.end());
    values.erase(last, values.end());

    for (size_t i = 0; i < m_array.size(); ++i) {
        auto pos = std::lower_bound(values.begin(), values.end(), m_array.get(i));
        indices.push_back(std::distance(values.begin(), pos));
        REALM_ASSERT(values[indices[i]] == m_array.get(i));
    }

    for (size_t i = 0; i < sz; ++i) {
        auto old_value = m_array.get(i);
        auto new_value = values[indices[i]];
        REALM_ASSERT_3(new_value, ==, old_value);
    }
}

bool ArrayFlex::check_gain(std::vector<int64_t>& values, std::vector<size_t>& indices, int& value_bit_width,
                           int& index_bit_width) const
{
    using Encoding = NodeHeader::Encoding;
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    const auto index = *std::max_element(indices.begin(), indices.end());
    value_bit_width = std::max(Node::signed_to_num_bits(*min_value), Node::signed_to_num_bits(*max_value));
    index_bit_width = index == 0 ? 1 : Node::unsigned_to_num_bits(index);
    REALM_ASSERT(value_bit_width > 0);
    REALM_ASSERT(index_bit_width > 0);

    // we should remember to reconsider this. The old type of array has value sizes aligned to the power of two,
    // so uncompressed size is not actually the size of the old type. It is the size we could get by using
    // Encoding::Packed instead of LocalDir... which is something we should do.
    const auto uncompressed_size = value_bit_width * indices.size();
    // we need to round compressed size in order to make % 8 compliant, since memory is aligned in this way
    auto byte_size =
        m_array.calc_size<Encoding::Flex>(values.size(), indices.size(), value_bit_width, index_bit_width);
    return byte_size < uncompressed_size;
}

void ArrayFlex::setup_header_in_flex_format(std::vector<int64_t>& values, std::vector<size_t>& indices,
                                            int value_bit_width, int index_bit_width) const
{
    using Encoding = NodeHeader::Encoding;
    // at this stage we know we need to compress the array.
    // we need to round compressed size in order to make % 8 compliant, since memory is aligned in this way
    auto byte_size =
        m_array.calc_size<Encoding::Flex>(values.size(), indices.size(), value_bit_width, index_bit_width);

    // I'm assuming that flags are taken from the owning Array.
    uint8_t flags = NodeHeader::get_flags((uint64_t*)m_array.get_header());
    // TODO: check if this can be done better. (e.g use realloc and not destroy)
    m_array.destroy();
    // TODO: this is temporary because it means that we are not really gaining any benefit from compressing and array
    // that is less than 128bytes
    MemRef mem = m_array.get_alloc().alloc(std::max(byte_size, size_t{128}));
    auto header = (uint64_t*)mem.get_addr();
    NodeHeader::init_header(header, 'B', Encoding::Flex, flags, value_bit_width, index_bit_width, values.size(),
                            indices.size());
    // TODO: same as above, do we need to set capacity to 128 or actual needed size?
    const auto capacity = std::max(byte_size, size_t{128});
    NodeHeader::set_capacity_in_header(capacity, (char*)header);
    // NodeHeader::set_flags(header, flags);
    m_array.init_from_mem(mem);
    REALM_ASSERT(m_array.m_ref == mem.get_ref());
}

bool ArrayFlex::get_encode_info(size_t& value_width, size_t& index_width, size_t& value_size,
                                size_t& index_size) const
{
    REALM_ASSERT(m_array.is_attached());
    using Encoding = NodeHeader::Encoding;
    auto header = (uint64_t*)m_array.get_header();
    if (m_array.get_kind(header) == 'B' && m_array.get_encoding(header) == Encoding::Flex) {
        value_size = NodeHeader::get_arrayA_num_elements<Encoding::Flex>(header);
        index_size = NodeHeader::get_arrayB_num_elements<Encoding::Flex>(header);
        value_width = NodeHeader::get_elementA_size<Encoding::Flex>(header);
        index_width = NodeHeader::get_elementB_size<Encoding::Flex>(header);
        return true;
    }
    return false;
}

size_t ArrayFlex::byte_size() const
{
    REALM_ASSERT(m_array.is_attached());
    // auto ref = m_array.get_ref();
    auto num_bytes = m_array.get_byte_size_from_header(m_array.get_header());
    //    REALM_ASSERT_7(m_array.get_alloc().is_read_only(ref), ==, true, ||, num_bytes, <=,
    //                   m_array.get_capacity_from_header(m_array.get_header()));
    // this is somehow failing.. TODO: investigate this
    // REALM_ASSERT(m_array.get_alloc().is_read_only(ref) == true);
    // REALM_ASSERT(num_bytes <= m_array.get_capacity_from_header(m_array.get_header()));

    return num_bytes;
}
