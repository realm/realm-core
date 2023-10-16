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

#include <vector>
#include <algorithm>

using namespace realm;

ArrayFlex::ArrayFlex(Array& array)
    : ArrayEncode(array)
{
}

void ArrayFlex::init_array_encode(MemRef mem)
{
    auto src_header = mem.get_addr();
    auto src_data = get_data_from_header(src_header);

    auto value_size = NodeHeader::get_arrayA_num_elements<Encoding::Flex>((uint64_t*)src_header);
    auto index_size = NodeHeader::get_arrayB_num_elements<Encoding::Flex>((uint64_t*)src_header);
    auto value_width = NodeHeader::get_elementA_size<Encoding::Flex>((uint64_t*)src_header);
    auto index_width = NodeHeader::get_elementB_size<Encoding::Flex>((uint64_t*)src_header);

    // deep copy.
    const auto compressed_values_size = value_width * value_size;
    const auto compressed_indices_size = index_width * index_size;
    const auto compressed_size = compressed_values_size + compressed_indices_size;
    const size_t size = Array::header_size + compressed_size;
    create(Type::type_Normal, false, size);
    auto dst_header = Array::get_header();
    using Encoding = NodeHeader::Encoding;
    NodeHeader::set_kind((uint64_t*)dst_header,
                         static_cast<std::underlying_type_t<Encoding>>(NodeHeader::Encoding::Flex));
    NodeHeader::set_arrayA_num_elements<Encoding::Flex>((uint64_t*)dst_header, value_size);
    NodeHeader::set_arrayB_num_elements<Encoding::Flex>((uint64_t*)dst_header, index_size);
    NodeHeader::set_elementA_size<Encoding::Flex>((uint64_t*)dst_header, value_width);
    NodeHeader::set_elementB_size<Encoding::Flex>((uint64_t*)dst_header, index_width);
    auto dst_data = get_data_from_header(dst_header);
    const auto offset = value_size * value_width;
    bf_iterator src_it_value{(uint64_t*)src_data, 0, value_width, value_width, 0};
    bf_iterator dst_it_value{(uint64_t*)dst_data, 0, value_width, value_width, 0};

    bf_iterator src_it_index{(uint64_t*)src_data, offset, index_width, index_width, 0};
    bf_iterator dst_it_index{(uint64_t*)dst_data, offset, index_width, index_width, 0};

    for (size_t i = 0; i < value_size; ++i) {
        *dst_it_value = src_it_value.get_value();
        ++dst_it_value;
        ++src_it_value;
    }
    for (size_t i = 0; i < index_size; ++i) {
        *dst_it_index = src_it_index.get_value();
        ++dst_it_index;
        ++src_it_index;
    }
    m_size = index_size;
    REALM_ASSERT(Encoding{NodeHeader::get_kind((uint64_t*)dst_header)} == Encoding::Flex);
}

bool ArrayFlex::encode()
{
    std::vector<int64_t> values;
    std::vector<size_t> indices;
    if (!is_encoded() && try_encode(values, indices)) {
        REALM_ASSERT(!values.empty());
        REALM_ASSERT(!indices.empty());

        auto header = (uint64_t*)get_header();
        auto value_width = get_elementA_size<Encoding::Flex>(header);
        auto index_width = get_elementB_size<Encoding::Flex>(header);
        auto value_size = values.size();
        // fill data
        auto data = (uint64_t*)get_data_from_header(get_header());
        const auto offset = value_size * value_width;
        bf_iterator it_value{data, 0, value_width, value_width, 0};
        bf_iterator it_index{data, offset, index_width, index_width, 0};
        for (size_t i = 0; i < values.size(); ++i) {
            it_value.set_value(values[i]);
            REALM_ASSERT(it_value.get_value() == (uint64_t)values[i]);
            ++it_value;
        }
        for (size_t i = 0; i < indices.size(); ++i) {
            it_index.set_value(indices[i]);
            REALM_ASSERT(it_index.get_value() == indices[i]);
            ++it_index;
        }
        return true;
    }
    return false;
}

bool ArrayFlex::decode()
{
    size_t value_width, index_width, value_size, index_size;
    if (get_encode_info(value_width, index_width, value_size, index_size)) {
        // recreate the array
        m_array.create(NodeHeader::Type::type_Normal);
        auto data = (uint64_t*)get_data_from_header(get_header());
        const auto offset = value_size * value_width;
        bf_iterator index_iterator{data, offset, index_width, index_width, 0};
        for (size_t i = 0; i < index_size; ++i) {
            const auto index = (int)index_iterator.get_value();
            const auto value = read_bitfield(data, index * value_width, value_width);
            m_array.add(value);
            ++index_iterator;
        }
        // free encoded array
        destroy();
        return true;
    }
    return false;
}

bool ArrayFlex::is_encoded() const
{
    using Encoding = NodeHeader::Encoding;
    auto header = m_array.is_attached() ? m_array.get_header() : get_header();
    Encoding enconding{NodeHeader::get_kind((uint64_t*)header)};
    return enconding == Encoding::Flex;
}

size_t ArrayFlex::size() const
{
    size_t value_width, index_width, value_size, index_size;
    if (get_encode_info(value_width, index_width, value_size, index_size)) {
        return index_size;
    }
    return m_array.size();
}

int64_t ArrayFlex::get(size_t ndx) const
{
    size_t value_width, index_width, value_size, index_size;
    if (get_encode_info(value_width, index_width, value_size, index_size)) {

        if (ndx >= index_size)
            return realm::not_found;

        auto data = (uint64_t*)get_data_from_header(get_header());
        REALM_ASSERT(data == (uint64_t*)m_data);
        const auto offset = (value_size * value_width) + (ndx * index_width);
        const auto index = read_bitfield(data, offset, index_width);
        int64_t v = read_bitfield(data, index * value_width, value_width);
        return v;
    }
    return m_array.get(ndx);
}

bool ArrayFlex::try_encode(std::vector<int64_t>& values, std::vector<size_t>& indices)
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
    values.reserve(sz);
    indices.reserve(sz);

    for (size_t i = 0; i < sz; ++i) {
        auto item = m_array.get(i);
        values.push_back(item);
        indices.push_back(item);
    }

    std::sort(values.begin(), values.end());
    auto last = std::unique(values.begin(), values.end());
    values.erase(last, values.end());

    for (auto& v : indices) {
        auto pos = std::lower_bound(values.begin(), values.end(), v);
        v = std::distance(values.begin(), pos);
    }

    const auto value = *std::max_element(values.begin(), values.end());
    const auto index = *std::max_element(indices.begin(), indices.end());
    const auto value_bit_width = Array::bit_width(value);
    const auto index_bit_width = Array::bit_width(index);
    const auto compressed_values_size = value_bit_width * values.size();
    const auto compressed_indices_size = index_bit_width * indices.size();
    const auto compressed_size = compressed_values_size + compressed_indices_size;
    const auto uncompressed_size = value_bit_width * sz;

    // encode array only if there is some gain, for simplicity the header is not taken into consideration, since it is
    // constantly equal to 8 bytes.
    if (compressed_size < uncompressed_size) {
        // allocate new space for the encoded array
        const size_t size = Array::header_size + compressed_size;
        if (is_attached())
            destroy();
        auto mem = create_array(Type::type_Normal, false, size, 0, m_array.get_alloc());
        init_from_mem(mem);
        auto addr = (uint64_t*)get_header();
        using Encoding = NodeHeader::Encoding;
        NodeHeader::set_kind(addr, static_cast<std::underlying_type_t<Encoding>>(NodeHeader::Encoding::Flex));
        NodeHeader::set_arrayA_num_elements<Encoding::Flex>(addr, values.size());
        NodeHeader::set_arrayB_num_elements<Encoding::Flex>(addr, indices.size());
        NodeHeader::set_elementA_size<Encoding::Flex>(addr, value_bit_width);
        NodeHeader::set_elementB_size<Encoding::Flex>(addr, index_bit_width);
        // destroy original uncompressed array
        m_array.destroy();
        return true;
    }
    return false;
}

bool ArrayFlex::get_encode_info(size_t& value_width, size_t& index_width, size_t& value_size,
                                size_t& index_size) const
{
    using Encoding = NodeHeader::Encoding;
    if (is_encoded()) {
        const auto addr = (uint64_t*)Array::get_mem().get_addr();
        value_size = NodeHeader::get_arrayA_num_elements<Encoding::Flex>(addr);
        index_size = NodeHeader::get_arrayB_num_elements<Encoding::Flex>(addr);
        value_width = NodeHeader::get_elementA_size<Encoding::Flex>(addr);
        index_width = NodeHeader::get_elementB_size<Encoding::Flex>(addr);
        return true;
    }
    return false;
}