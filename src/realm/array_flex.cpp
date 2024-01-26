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

namespace impl {

static void copy_back(char* data, size_t w, size_t ndx, int64_t v)
{
    if (w == 0)
        realm::set_direct<0>(data, ndx, v);
    else if (w == 1)
        realm::set_direct<1>(data, ndx, v);
    else if (w == 2)
        realm::set_direct<2>(data, ndx, v);
    else if (w == 4)
        realm::set_direct<4>(data, ndx, v);
    else if (w == 8)
        realm::set_direct<8>(data, ndx, v);
    else if (w == 16)
        realm::set_direct<16>(data, ndx, v);
    else if (w == 32)
        realm::set_direct<32>(data, ndx, v);
    else if (w == 64)
        realm::set_direct<64>(data, ndx, v);
    else
        REALM_UNREACHABLE();
}

template <typename T>
inline T fetch_value(uint64_t* data, size_t ndx, size_t offset, size_t ndx_width, size_t v_width)
{
    const auto pos = static_cast<size_t>(realm::read_bitfield(data, offset + (ndx * ndx_width), ndx_width));
    const auto unsigned_val = realm::read_bitfield(data, v_width * pos, v_width);
    return std::is_same_v<T, int64_t> ? sign_extend_field(v_width, unsigned_val) : unsigned_val;
}

template <typename T>
inline size_t lower_bound(uint64_t* data, const T& key, size_t v_width, size_t ndx_width, size_t v_size,
                          size_t ndx_size)
{
    const auto offset = v_width * v_size;
    auto cnt = ndx_size;
    size_t step;
    size_t ndx = 0;
    size_t p = 0;
    while (cnt > 0) {
        ndx = p;
        step = cnt / 2;
        ndx += step;
        const auto v = fetch_value<T>(data, ndx, offset, ndx_width, v_width);
        if ((v < key)) {
            p = ++ndx;
            cnt -= step + 1;
        }
        else {
            cnt = step;
        }
    }
    return p;
}

template <typename T>
inline size_t upper_bound(uint64_t* data, const T key, size_t v_width, size_t ndx_width, size_t v_size,
                          size_t ndx_size)
{
    const auto offset = v_width * v_size;
    auto cnt = ndx_size;
    size_t step;
    size_t ndx = 0;
    size_t p = 0;
    while (cnt > 0) {
        ndx = p;
        step = cnt / 2;
        ndx += step;
        const auto v = fetch_value<T>(data, ndx, offset, ndx_width, v_width);
        if (!(key < v)) {
            p = ++ndx;
            cnt -= step + 1;
        }
        else {
            cnt = step;
        }
    }
    return p;
}

inline size_t find_linear(uint64_t* data, int64_t key, size_t v_width, size_t ndx_width, size_t v_size,
                          size_t ndx_size)
{
    const auto offset = v_size * v_width;
    bf_iterator index_iterator{data, offset, ndx_width, ndx_width, 0};
    for (size_t i = 0; i < ndx_size; ++i) {
        const auto index = index_iterator.get_value();
        bf_iterator it_value{data, static_cast<size_t>(index * v_width), v_width, v_width, 0};
        const auto v = sign_extend_field(v_width, it_value.get_value());
        if (v == key)
            return i;
        ++index_iterator;
    }
    return realm::not_found;
}

inline size_t find_binary(uint64_t* data, int64_t key, size_t v_width, size_t ndx_width, size_t v_size,
                          size_t ndx_size)
{
    size_t lo = 0;
    size_t hi = ndx_size;
    const auto ndx_offset = v_size * v_width;
    while (lo <= hi) {
        size_t mid = lo + (hi - lo) / 2;
        const auto ndx = static_cast<size_t>(realm::read_bitfield(data, ndx_offset + (mid * ndx_width), ndx_width));
        const auto unsigned_val = realm::read_bitfield(data, v_width * ndx, v_width);
        const auto v = sign_extend_field(v_width, unsigned_val);
        if (v == key)
            return ndx;
        else if (key < v)
            hi = mid - 1;
        else
            lo = mid + 1;
    }
    return realm::not_found;
}

} // namespace impl

bool ArrayFlex::encode(const Array& origin, Array& encoded) const
{
    REALM_ASSERT_DEBUG(origin.is_attached());
    const auto sz = origin.size();
    std::vector<int64_t> values;
    std::vector<size_t> indices;
    if (!is_encoded(origin) && try_encode(origin, encoded, values, indices)) {
        REALM_ASSERT_DEBUG(!values.empty());
        REALM_ASSERT_DEBUG(!indices.empty());
        REALM_ASSERT_DEBUG(indices.size() == sz);
        copy_into_encoded_array(encoded, values, indices);
        return true;
    }
    return false;
}

bool ArrayFlex::decode(Array& arr)
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    if (get_encode_info(arr.get_header(), v_width, ndx_width, v_size, ndx_size)) {
        auto values = fetch_signed_values_from_encoded_array(arr, v_width, ndx_width, v_size, ndx_size);
        REALM_ASSERT_DEBUG(values.size() == ndx_size);
        restore_array(arr, values); // restore array sets capacity
        return true;
    }
    return false;
}

bool ArrayFlex::is_encoded(const Array& arr) const
{
    // We are calling this when the header is not yet initiliased!
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    const auto header = arr.get_header();
    const auto kind = Node::get_kind(header);
    return kind == 'B' && Node::get_encoding(header) == Encoding::Flex;
}

size_t ArrayFlex::size(const Array& arr) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    using Encoding = NodeHeader::Encoding;
    const auto header = arr.get_header();
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(header) == 'B' && NodeHeader::get_encoding(header) == Encoding::Flex);
    return NodeHeader::get_arrayB_num_elements<NodeHeader::Encoding::Flex>(header);
}

void ArrayFlex::set_direct(const Array& arr, size_t ndx, int64_t value) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    const auto header = arr.get_header();
    if (get_encode_info(header, v_width, ndx_width, v_size, ndx_size)) {
        REALM_ASSERT_DEBUG(ndx < ndx_size);
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
    auto v = _get_unsigned(arr, ndx, v_width);
    const auto sign_v = sign_extend_field(v_width, v);
    return sign_v;
}

void ArrayFlex::get_chunk(const Array& arr, size_t ndx, int64_t res[8]) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    const auto header = arr.get_header();
    if (get_encode_info(header, v_width, ndx_width, v_size, ndx_size)) {
        REALM_ASSERT_DEBUG(ndx < ndx_width);
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
    size_t v_width = 0;
    size_t ndx_width = 0;
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
    REALM_ASSERT_DEBUG(arr.is_attached());
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
        auto v = sign_extend_field(v_width, it_value.get_value());
        REALM_ASSERT_DEBUG(v == values[i]);
        ++it_value;
    }
    for (size_t i = 0; i < indices.size(); ++i) {
        auto v = sign_extend_field(v_width, read_bitfield(data, indices[i] * v_width, v_width));
        REALM_ASSERT_DEBUG(values[indices[i]] == v);
        it_index.set_value(indices[i]);
        REALM_ASSERT_DEBUG(indices[i] == it_index.get_value());
        v = sign_extend_field(v_width, read_bitfield(data, indices[i] * v_width, v_width));
        REALM_ASSERT_DEBUG(values[indices[i]] == v);
        ++it_index;
    }
    REALM_ASSERT_DEBUG(arr.get_kind(header) == 'B');
    REALM_ASSERT_DEBUG(arr.get_encoding(header) == Encoding::Flex);
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
        REALM_ASSERT_DEBUG(values.back() == item);
    }

    std::sort(values.begin(), values.end());
    auto last = std::unique(values.begin(), values.end());
    values.erase(last, values.end());

    for (size_t i = 0; i < arr.size(); ++i) {
        auto pos = std::lower_bound(values.begin(), values.end(), arr.get(i));
        indices.push_back(std::distance(values.begin(), pos));
        REALM_ASSERT_DEBUG(values[indices[i]] == arr.get(i));
    }

    for (size_t i = 0; i < sz; ++i) {
        auto old_value = arr.get(i);
        auto new_value = values[indices[i]];
        REALM_ASSERT_DEBUG(new_value == old_value);
    }

    REALM_ASSERT_DEBUG(indices.size() == sz);
}

bool ArrayFlex::check_gain(const Array& arr, std::vector<int64_t>& values, std::vector<size_t>& indices,
                           size_t& v_width, size_t& ndx_width) const
{
    using Encoding = NodeHeader::Encoding;
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    const auto index = *std::max_element(indices.begin(), indices.end());
    v_width = std::max(Node::signed_to_num_bits(*min_value), Node::signed_to_num_bits(*max_value));
    ndx_width = index == 0 ? 1 : Node::unsigned_to_num_bits(index);
    REALM_ASSERT_DEBUG(v_width > 0);
    REALM_ASSERT_DEBUG(ndx_width > 0);
    // we should consider Encoding::Packed as well here.
    const auto uncompressed_size = arr.get_byte_size();
    auto byte_size = NodeHeader::calc_size<Encoding::Flex>(values.size(), indices.size(), v_width, ndx_width);
    return byte_size < uncompressed_size;
}

void ArrayFlex::setup_array_in_flex_format(const Array& origin, Array& arr, std::vector<int64_t>& values,
                                           std::vector<size_t>& indices, size_t v_width, size_t ndx_width) const
{
    using Encoding = NodeHeader::Encoding;
    // I'm assuming that flags are taken from the owning Array.
    uint8_t flags = NodeHeader::get_flags(origin.get_header());
    auto byte_size = NodeHeader::calc_size<Encoding::Flex>(values.size(), indices.size(), v_width, ndx_width);

    Allocator& allocator = arr.get_alloc();
    auto mem = allocator.alloc(byte_size);
    auto header = mem.get_addr();
    NodeHeader::init_header(header, 'B', Encoding::Flex, flags, v_width, ndx_width, values.size(), indices.size());
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(header) == 'B');
    REALM_ASSERT_DEBUG(NodeHeader::get_encoding(header) == Encoding::Flex);
    NodeHeader::set_capacity_in_header(byte_size, (char*)header);
    arr.init_from_mem(mem);
    REALM_ASSERT_DEBUG(arr.m_ref == mem.get_ref());
    REALM_ASSERT_DEBUG(arr.get_kind(header) == 'B');
    REALM_ASSERT_DEBUG(arr.get_encoding(header) == Encoding::Flex);
}

uint64_t ArrayFlex::_get_unsigned(const Array& arr, size_t ndx, size_t& v_width) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.get_kind(arr.get_header()) == 'B');
    return _get_unsigned(arr.get_header(), ndx, v_width);
}

uint64_t ArrayFlex::_get_unsigned(const char* header, size_t ndx, size_t& v_width)
{
    size_t ndx_width, v_size, ndx_size;
    if (get_encode_info(header, v_width, ndx_width, v_size, ndx_size)) {
        if (ndx >= ndx_size)
            return realm::not_found;
        auto data = (uint64_t*)NodeHeader::get_data_from_header(header);
        const uint64_t offset = v_size * v_width;
        bf_iterator it_index{data, static_cast<size_t>(offset + (ndx * ndx_width)), ndx_width, ndx_width, 0};
        bf_iterator it_value{data, static_cast<size_t>(v_width * it_index.get_value()), v_width, v_width, 0};
        const auto v = it_value.get_value();
        return v;
    }
    REALM_UNREACHABLE();
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

void ArrayFlex::restore_array(Array& arr, const std::vector<int64_t>& values) const
{
    //  do the reverse of compressing the array
    REALM_ASSERT_DEBUG(arr.is_attached());
    using Encoding = NodeHeader::Encoding;
    const auto flags = NodeHeader::get_flags(arr.get_header());
    const auto size = values.size();
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());

    // calling arr.destroy() is fine, as long as we don't use the memory deleted anymore.
    // Decompressing can only be happening within a write transactions, thus this invariant should hold.
    arr.destroy();
    auto& allocator = arr.get_alloc();

    auto signed_bit_width = std::max(Array::signed_to_num_bits(*min_value), Array::signed_to_num_bits(*max_value));
    auto byte_size = NodeHeader::calc_size<Encoding::WTypBits>(size, signed_bit_width);
    REALM_ASSERT_DEBUG(byte_size % 8 == 0); // 8 bytes aligned value

    auto width = std::max(Array::bit_width(*min_value), Array::bit_width(*max_value));
    REALM_ASSERT_DEBUG(width == 0 || width == 1 || width == 2 || width == 4 || width == 8 || width == 16 ||
                       width == 32 || width == 64);

    auto mem = allocator.alloc(byte_size);
    auto header = mem.get_addr();
    NodeHeader::init_header(header, 'A', Encoding::WTypBits, flags, width, values.size());
    NodeHeader::set_capacity_in_header(byte_size, header);
    arr.init_from_mem(mem);
    arr.update_parent();

    // copy straight into the array all the data, we don't need COW here.
    auto data = arr.get_data_from_header(arr.get_header());
    size_t ndx = 0;
    for (const auto& v : values)
        impl::copy_back(data, width, ndx++, v);

    REALM_ASSERT_DEBUG(width == arr.get_width());
    REALM_ASSERT_DEBUG(arr.size() == values.size());
}

size_t ArrayFlex::find_first(const Array& arr, int64_t value) const
{
    static constexpr auto MAX_SZ_LINEAR_FIND = 15;
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    if (get_encode_info(arr.get_header(), v_width, ndx_width, v_size, ndx_size)) {
        auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        if (ndx_size <= MAX_SZ_LINEAR_FIND) {
            return impl::find_linear(data, value, v_width, ndx_width, v_size, ndx_size);
        }
        return impl::find_binary(data, value, v_width, ndx_width, v_size, ndx_size);
    }
    return realm::not_found;
}

int64_t ArrayFlex::sum(const Array& arr, size_t start, size_t end) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, ndx_width, v_size, ndx_size;
    if (get_encode_info(arr.get_header(), v_width, ndx_width, v_size, ndx_size)) {
        REALM_ASSERT_DEBUG(ndx_size >= start && ndx_size <= end);
        const auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        const auto offset = v_size * v_width + (start * ndx_width);
        bf_iterator index_iterator{data, offset , ndx_width, ndx_width, 0};
        int64_t total_sum = 0;
        for (size_t i = start; i < end; ++i) {
            const auto offset = static_cast<size_t>(index_iterator.get_value() * v_width);
            total_sum += *(data + offset);
            ++index_iterator;
        }
        return total_sum;
    }
    REALM_UNREACHABLE();
}

int64_t ArrayFlex::get(const char* header, size_t ndx)
{
    size_t v_width;
    auto val = _get_unsigned(header, ndx, v_width);
    if (val != realm::not_found) {
        const auto ivalue = sign_extend_field(v_width, val);
        return ivalue;
    }
    return val;
}

uint64_t ArrayFlex::get_unsigned(const Array& arr, size_t ndx) const
{
    size_t v_width;
    return _get_unsigned(arr, ndx, v_width);
}

size_t ArrayFlex::lower_bound(const Array& arr, int64_t value) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    const auto header = arr.get_header();
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(header) == 'B');
    size_t v_width, ndx_width, v_size, ndx_size;
    if (get_encode_info(header, v_width, ndx_width, v_size, ndx_size)) {
        const auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        return impl::lower_bound(data, value, v_width, ndx_width, v_size, ndx_size);
    }
    REALM_UNREACHABLE();
}

size_t ArrayFlex::upper_bound(const Array& arr, int64_t value) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    const auto header = arr.get_header();
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(header) == 'B');
    size_t v_width, ndx_width, v_size, ndx_size;
    if (get_encode_info(header, v_width, ndx_width, v_size, ndx_size)) {
        const auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        return impl::upper_bound(data, value, v_width, ndx_width, v_size, ndx_size);
    }
    REALM_UNREACHABLE();
}
