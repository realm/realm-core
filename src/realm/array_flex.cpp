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
    size_t v_width, ndx_width, v_size, ndx_size;
    if (get_encode_info(arr.get_header(), v_width, ndx_width, v_size, ndx_size)) {
        auto values = fetch_signed_values_from_encoded_array(arr, v_width, ndx_width, v_size, ndx_size);
        REALM_ASSERT(values.size() == ndx_size);
        restore_array(arr, values); // restore array sets capacity
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

void ArrayFlex::set_direct(const Array& arr, size_t ndx, int64_t value) const
{
    REALM_ASSERT(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    const auto header = arr.get_header();
    if (get_encode_info(header, v_width, ndx_width, v_size, ndx_size)) {
        REALM_ASSERT(ndx < ndx_size);
        auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        uint64_t offset = v_size * v_width;
        bf_iterator it_index{data, static_cast<size_t>(offset + (ndx * ndx_width)), ndx_width, ndx_size, 0};
        bf_iterator it_value{data, static_cast<size_t>(it_index.get_value() * v_width), v_width, v_width, 0};
        it_value.set_value(value);
    }
    REALM_UNREACHABLE();
}

int64_t ArrayFlex::get(const Array& arr, size_t ndx) const
{
    size_t v_width;
    auto v = get_unsigned(arr, ndx, v_width);
    const auto sign_v = sign_extend_field(v_width, v);
    return sign_v;
}

void ArrayFlex::get_chunk(const Array& arr, size_t ndx, int64_t res[8]) const
{
    REALM_ASSERT(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    const auto header = arr.get_header();
    if (get_encode_info(header, v_width, ndx_width, v_size, ndx_size)) {
        REALM_ASSERT(ndx < ndx_width);
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
    int v_width = 0;
    int ndx_width = 0;
    if (check_gain(origin, values, indices, v_width, ndx_width)) {
#if REALM_DEBUG
        for (size_t i = 0; i < sz; ++i)
            REALM_ASSERT(origin.get(i) == values[indices[i]]);
#endif
        setup_array_in_flex_format(origin, encoded, values, indices, v_width, ndx_width);
        return true;
    }
    return false;
}

void ArrayFlex::copy_into_encoded_array(Array& arr, std::vector<int64_t>& values, std::vector<size_t>& indices) const
{
    REALM_ASSERT(arr.is_attached());
    using Encoding = NodeHeader::Encoding;
    auto header = arr.get_header();
    auto v_width = NodeHeader::get_elementA_size<Encoding::Flex>(header);
    auto ndx_width = NodeHeader::get_elementB_size<Encoding::Flex>(header);
    auto v_size = values.size();
    // fill data
    auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
    auto offset = static_cast<size_t>(v_size * v_width);
    bf_iterator it_value{data, 0, v_width, v_width, 0};
    bf_iterator it_index{data, offset, ndx_width, ndx_width, 0};
    for (size_t i = 0; i < values.size(); ++i) {
        it_value.set_value(values[i]);
        auto v2 = it_value.get_value();
        auto v = sign_extend_field(v_width, v2);
        REALM_ASSERT_3(v, ==, values[i]);
        ++it_value;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
        auto v = sign_extend_field(v_width, read_bitfield(data, indices[i] * v_width, v_width));
        REALM_ASSERT(values[indices[i]] == v);
        it_index.set_value(indices[i]);
        REALM_ASSERT(indices[i] == it_index.get_value());
        v = sign_extend_field(v_width, read_bitfield(data, indices[i] * v_width, v_width));
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

bool ArrayFlex::check_gain(const Array& arr, std::vector<int64_t>& values, std::vector<size_t>& indices, int& v_width,
                           int& ndx_width) const
{
    using Encoding = NodeHeader::Encoding;
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    const auto index = *std::max_element(indices.begin(), indices.end());
    v_width = std::max(Node::signed_to_num_bits(*min_value), Node::signed_to_num_bits(*max_value));
    ndx_width = index == 0 ? 1 : Node::unsigned_to_num_bits(index);
    REALM_ASSERT(v_width > 0);
    REALM_ASSERT(ndx_width > 0);
    // we should consider Encoding::Packed as well here.
    const auto uncompressed_size = arr.get_byte_size();
    auto byte_size = NodeHeader::calc_size<Encoding::Flex>(values.size(), indices.size(), v_width, ndx_width);
    return byte_size < uncompressed_size;
}

void ArrayFlex::setup_array_in_flex_format(const Array& origin, Array& arr, std::vector<int64_t>& values,
                                           std::vector<size_t>& indices, int v_width, int ndx_width) const
{
    using Encoding = NodeHeader::Encoding;

    // I'm assuming that flags are taken from the owning Array.
    uint8_t flags = NodeHeader::get_flags(origin.get_header());

    auto byte_size = NodeHeader::calc_size<Encoding::Flex>(values.size(), indices.size(), v_width, ndx_width);

    Allocator& allocator = arr.get_alloc();
    auto mem = allocator.alloc(byte_size);
    auto header = mem.get_addr();
    NodeHeader::init_header(header, 'B', Encoding::Flex, flags, v_width, ndx_width, values.size(), indices.size());
    REALM_ASSERT(NodeHeader::get_kind(header) == 'B');
    REALM_ASSERT(NodeHeader::get_encoding(header) == Encoding::Flex);
    NodeHeader::set_capacity_in_header(byte_size, (char*)header);
    arr.init_from_mem(mem);
    REALM_ASSERT(arr.m_ref == mem.get_ref());
    REALM_ASSERT(arr.get_kind(header) == 'B');
    REALM_ASSERT(arr.get_encoding(header) == Encoding::Flex);
}

bool inline ArrayFlex::get_encode_info(const char* header, size_t& v_width, size_t& ndx_width, size_t& v_size,
                                       size_t& ndx_size)
{
    using Encoding = NodeHeader::Encoding;
    auto h = header;
    if (NodeHeader::get_kind(h) == 'B' && NodeHeader::get_encoding(h) == Encoding::Flex) {
        v_size = NodeHeader::get_arrayA_num_elements<Encoding::Flex>(h);
        ndx_size = NodeHeader::get_arrayB_num_elements<Encoding::Flex>(h);
        v_width = NodeHeader::get_elementA_size<Encoding::Flex>(h);
        ndx_width = NodeHeader::get_elementB_size<Encoding::Flex>(h);
        return true;
    }
    return false;
}

std::vector<int64_t> ArrayFlex::fetch_signed_values_from_encoded_array(const Array& arr, size_t v_width,
                                                                       size_t ndx_width, size_t v_size,
                                                                       size_t ndx_size, size_t ndx_begin) const
{
    std::vector<int64_t> values;
    values.reserve(ndx_size);
    auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
    const auto offset = v_size * v_width;
    bf_iterator index_iterator{data, offset, ndx_width, ndx_width, 0};
    for (size_t i = ndx_begin; i < ndx_size; ++i) {
        const auto index = index_iterator.get_value();
        bf_iterator it_value{data, static_cast<size_t>(index * v_width), v_width, v_width, 0};
        const auto value = it_value.get_value();
        const auto ivalue = sign_extend_field(v_width, value);
        values.push_back(ivalue);
        ++index_iterator;
    }
    return values;
}

std::vector<uint64_t> ArrayFlex::fetch_unsigned_values_from_encoded_array(const Array& arr, size_t v_width,
                                                                          size_t ndx_width, size_t v_size,
                                                                          size_t ndx_size, size_t ndx_begin) const
{
    std::vector<uint64_t> values;
    values.reserve(ndx_size);
    auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
    const auto offset = v_size * v_width;
    bf_iterator index_iterator{data, offset, ndx_width, ndx_width, 0};
    for (size_t i = ndx_begin; i < ndx_size; ++i) {
        const auto index = index_iterator.get_value();
        bf_iterator it_value{data, static_cast<size_t>(index * v_width), v_width, v_width, 0};
        const auto value = it_value.get_value();
        values.push_back(value);
        ++index_iterator;
    }
    return values;
}

std::vector<std::pair<int64_t, size_t>> ArrayFlex::fetch_values_and_indices(const Array& arr, size_t v_width,
                                                                            size_t ndx_width, size_t v_size,
                                                                            size_t ndx_size) const
{
    std::vector<std::pair<int64_t, size_t>> values_and_indices;
    values_and_indices.reserve(ndx_size);
    auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
    const auto offset = v_size * v_width;
    bf_iterator index_iterator{data, offset, ndx_width, ndx_width, 0};
    for (size_t i = 0; i < ndx_size; ++i) {
        const auto index = index_iterator.get_value();
        bf_iterator it_value{data, static_cast<size_t>(index * v_width), v_width, v_width, 0};
        const auto value = it_value.get_value();
        values_and_indices.push_back({value, i});
        ++index_iterator;
    }
    return values_and_indices;
}

void ArrayFlex::restore_array(Array& arr, const std::vector<int64_t>& values) const
{
    // do the reverse of compressing the array
    REALM_ASSERT(arr.is_attached());
    using Encoding = NodeHeader::Encoding;
    const auto flags = NodeHeader::get_flags(arr.get_header());
    const auto size = values.size();
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    const size_t width_min = NodeHeader::signed_to_num_bits(*min_value);
    const size_t width_max = NodeHeader::signed_to_num_bits(*max_value);
    auto max_bit = std::max(width_min, width_max);
    auto byte_size = NodeHeader::calc_size<Encoding::WTypBits>(size, max_bit);
    REALM_ASSERT(byte_size % 8 == 0); // 8 bytes aligned value
    auto& allocator = arr.get_alloc();

    // calling array destory here, works only because we are not going to use this array header anymore, see comment
    // in Array::init_from_mem. We can get away with this since we are basically restoring all the basic properties
    // for the array via Node::init_header(...) But width and capacity need particular attention.
    auto orginal_header = arr.get_header();
    auto origanal_ref = arr.get_ref();

    // this is slow. It is just for testing, but this is what it is needed. the ceil for the current bit size.
    // we need to have std::bit_ceil(v); AKA the next power of 2 closed to max_bit. We have somehow introduced a
    // regression here. I am impressed that most of the tests are working..
    // TODO: work a faster way to get this.
    size_t width = 1;
    while (width < max_bit)
        width *= 2;
    REALM_ASSERT(width <= 64);
    REALM_ASSERT(width == 0 || width == 1 || width == 2 || width == 4 || width == 8 || width == 16 || width == 32 ||
                 width == 64);

    auto mem = allocator.alloc(byte_size);
    auto header = mem.get_addr();
    NodeHeader::init_header(header, 'A', Encoding::WTypBits, flags, width, values.size());
    NodeHeader::set_capacity_in_header(byte_size, header);
    // the old array should not be deleted up until this function completes,
    // but once it finishes running, the ref of the old array is gone, so the memory is leaked.
    // previous header and ref must be stored temporary and deleted after this point
    arr.init_from_mem(mem);

    size_t i = 0;
    for (const auto& v : values)
        arr.set(i++, v);

    arr.update_parent();
    allocator.free_(origanal_ref, orginal_header);

    size_t w = arr.get_width();
    REALM_ASSERT(w == 0 || w == 1 || w == 2 || w == 4 || w == 8 || w == 16 || w == 32 || w == 64);
    REALM_ASSERT(arr.size() == values.size());
}

std::vector<int64_t> ArrayFlex::find_all(const Array& arr, int64_t, size_t start, size_t end) const
{
    REALM_ASSERT(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    if (get_encode_info(arr.get_header(), v_width, ndx_width, v_size, ndx_size)) {
        // TODO optimize this.
        REALM_ASSERT(start < ndx_size && end <= ndx_size);
        return fetch_signed_values_from_encoded_array(arr, v_width, ndx_width, v_size, end, start);
    }
    REALM_UNREACHABLE();
}

size_t ArrayFlex::find_first(const Array& arr, int64_t value) const
{
    REALM_ASSERT(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    if (get_encode_info(arr.get_header(), v_width, ndx_width, v_size, ndx_size)) {
        auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        const auto offset = v_size * v_width;
        bf_iterator index_iterator{data, offset, ndx_width, ndx_width, 0};
        for (size_t i = 0; i < ndx_size; ++i) {
            const auto index = index_iterator.get_value();
            bf_iterator it_value{data, static_cast<size_t>(index * v_width), v_width, v_width, 0};
            const auto arr_value = it_value.get_value();
            const auto ivalue = sign_extend_field(v_width, arr_value);
            if (ivalue == value)
                return i; // we need to pretend that the array was uncompressed, so we can't return index but i
            ++index_iterator;
        }
    }
    return realm::not_found;
}

int64_t ArrayFlex::sum(const Array& arr, size_t start, size_t end) const
{
    REALM_ASSERT(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    if (get_encode_info(arr.get_header(), v_width, ndx_width, v_size, ndx_size)) {
        auto values_and_indices = fetch_values_and_indices(arr, v_width, ndx_width, v_size, ndx_size);
        int64_t total_sum = 0;
        for (const auto& [v, ndx] : values_and_indices) {
            if (ndx >= start && ndx < end)
                total_sum += v;
        }
        return total_sum;
    }
    REALM_UNREACHABLE();
}

int64_t ArrayFlex::get(const char* header, size_t ndx)
{
    size_t v_width = 0;
    auto val = get_unsigned(header, ndx, v_width);
    if (val != realm::not_found) {
        const auto ivalue = sign_extend_field(v_width, val);
        return ivalue;
    }
    return val;
}

uint64_t ArrayFlex::get_unsigned(const char* header, size_t ndx, size_t& v_width)
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT(NodeHeader::get_kind(header) == 'B');
    REALM_ASSERT(NodeHeader::get_encoding(header) == Encoding::Flex);
    size_t ndx_width, v_size, ndx_size;
    if (get_encode_info(header, v_width, ndx_width, v_size, ndx_size)) {
        if (ndx >= ndx_size)
            return realm::not_found;
        auto data = (uint64_t*)NodeHeader::get_data_from_header(header);
        const auto offset = v_size * v_width;
        bf_iterator index_iterator{data, static_cast<size_t>(offset + (ndx * ndx_width)), ndx_width, ndx_width, 0};
        auto index_value = index_iterator.get_value();
        bf_iterator it_value{data, static_cast<size_t>(index_value * v_width), v_width, v_width, 0};
        const auto arr_value = it_value.get_value();
        return arr_value;
    }
    return realm::not_found;
}

uint64_t ArrayFlex::get_unsigned(const Array& arr, size_t ndx, size_t& v_width) const
{
    REALM_ASSERT(arr.is_attached());
    REALM_ASSERT(arr.get_kind(arr.get_header()) == 'B');
    size_t ndx_width, v_size, ndx_size;
    if (get_encode_info(arr.get_header(), v_width, ndx_width, v_size, ndx_size)) {

        if (ndx >= ndx_size)
            return realm::not_found;

        auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        const uint64_t offset = v_size * v_width;
        bf_iterator it_index{data, static_cast<size_t>(offset + (ndx * ndx_width)), ndx_width, ndx_size, 0};
        bf_iterator it_value{data, static_cast<size_t>(v_width * it_index.get_value()), v_width, v_width, 0};
        const auto v = it_value.get_value();
        return v;
    }
    REALM_UNREACHABLE();
}

size_t ArrayFlex::lower_bound(const Array& arr, uint64_t value) const
{
    // TODO: the implementation of this method is quite important and impacts the way the cluster tree is traversed.
    // TODO: redo the implementation once the tests are passing
    REALM_ASSERT(arr.is_attached());

    const auto header = arr.get_header();
    REALM_ASSERT(NodeHeader::get_kind(header) == 'B');

    size_t v_width, i_width, v_size, i_size;
    if (get_encode_info(header, v_width, i_width, v_size, i_size)) {
        auto values = fetch_unsigned_values_from_encoded_array(arr, v_width, i_width, v_size, i_size);
        // I might be wrong, but it seems that UnsignedArray values are kept sorted, so just call lower_bound here
        // NOTE: this is inefficient, the algorithm is moving from O(lg N) to O(N) with very expensive constant
        // factors. std::sort(values.begin(), values.end());
        return std::lower_bound(values.begin(), values.end(), value) - values.begin();
    }
    REALM_UNREACHABLE();
}

size_t ArrayFlex::upper_bound(const Array& arr, uint64_t value) const
{
    // TODO: the implementation of this method is quite important and impacts the way the cluster tree is traversed.
    // TODO: redo the implementation once the tests are passing
    REALM_ASSERT(arr.is_attached());

    const auto header = arr.get_header();
    REALM_ASSERT(NodeHeader::get_kind(header) == 'B');

    size_t v_width, i_width, v_size, i_size;
    if (get_encode_info(header, v_width, i_width, v_size, i_size)) {
        auto values = fetch_unsigned_values_from_encoded_array(arr, v_width, i_width, v_size, i_size);
        // I might be wrong, but it seems that UnsignedArray values are kept sorted, so just call lower_bound here
        // NOTE: this is inefficient, the algorithm is moving from O(lg N) to O(N) with very expensive constant
        // factors. std::sort(values.begin(), values.end());
        return std::upper_bound(values.begin(), values.end(), value) - values.begin();
    }
    REALM_UNREACHABLE();
}

size_t ArrayFlex::lower_bound(const Array& arr, int64_t value) const
{
    // same comment above applies to this method. Needs optimization
    REALM_ASSERT(arr.is_attached());
    const auto header = arr.get_header();
    REALM_ASSERT(NodeHeader::get_kind(header) == 'B');
    size_t v_width, i_width, v_size, i_size;
    if (get_encode_info(header, v_width, i_width, v_size, i_size)) {
        auto values = fetch_signed_values_from_encoded_array(arr, v_width, i_width, v_size, i_size);
        return std::lower_bound(values.begin(), values.end(), value) - values.begin();
    }
    REALM_UNREACHABLE();
}

size_t ArrayFlex::upper_bound(const Array& arr, int64_t value) const
{
    // same comment above applies to this method. Needs optimization
    REALM_ASSERT(arr.is_attached());
    const auto header = arr.get_header();
    REALM_ASSERT(NodeHeader::get_kind(header) == 'B');
    size_t v_width, i_width, v_size, i_size;
    if (get_encode_info(header, v_width, i_width, v_size, i_size)) {
        auto values = fetch_signed_values_from_encoded_array(arr, v_width, i_width, v_size, i_size);
        return std::upper_bound(values.begin(), values.end(), value) - values.begin();
    }
    REALM_UNREACHABLE();
}
