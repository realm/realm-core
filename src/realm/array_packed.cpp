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

namespace impl {

template <typename T>
inline T fetch_value(uint64_t* data, size_t ndx, size_t v_width)
{
    const auto unsigned_val = realm::read_bitfield(data, v_width * ndx, v_width);
    return std::is_same_v<T, int64_t> ? sign_extend_field(v_width, unsigned_val) : unsigned_val;
}

template <typename T>
inline size_t lower_bound(uint64_t* data, const T& key, size_t v_width, size_t v_size)
{
    auto cnt = v_size;
    size_t step;
    size_t ndx = 0;
    size_t p = 0;
    while (cnt > 0) {
        ndx = p;
        step = cnt / 2;
        ndx += step;
        const auto v = fetch_value<T>(data, ndx, v_width);
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
inline size_t upper_bound(uint64_t* data, const T key, size_t v_width, size_t v_size)
{
    auto cnt = v_size;
    size_t step;
    size_t ndx = 0;
    size_t p = 0;
    while (cnt > 0) {
        ndx = p;
        step = cnt / 2;
        ndx += step;
        const auto v = fetch_value<T>(data, ndx, v_width);
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

inline size_t find_linear(uint64_t* data, int64_t key, size_t v_width, size_t v_size)
{
    for (size_t i = 0; i < v_size; ++i) {
        bf_iterator it_value{data, static_cast<size_t>(i * v_width), v_width, v_width, 0};
        const auto v = sign_extend_field(v_width, it_value.get_value());
        if (v == key)
            return i;
    }
    return realm::not_found;
}

inline size_t find_binary(uint64_t* data, int64_t key, size_t v_width, size_t v_size)
{
    size_t lo = 0;
    size_t hi = v_size;
    while (lo <= hi) {
        size_t mid = lo + (hi - lo) / 2;
        const auto unsigned_val = realm::read_bitfield(data, v_width * mid, v_width);
        const auto v = sign_extend_field(v_width, unsigned_val);
        if (v == key)
            return mid;
        else if (key < v)
            hi = mid - 1;
        else
            lo = mid + 1;
    }
    return realm::not_found;
}

} // namespace impl

// do not use values here, the values are computed to be used in flex format,
// thus all the duplicates are gone, but packed can still be better than flex.
// TODO: fix this
bool ArrayPacked::encode(const Array& origin, Array& dst, size_t byte_size, size_t v_width)
{
    setup_array_packed_format(origin, dst, byte_size, v_width);
    copy_into_packed_array(origin, dst);
    return true;
}

void ArrayPacked::setup_array_packed_format(const Array& origin, Array& arr, size_t byte_size, size_t v_width)
{
    using Encoding = NodeHeader::Encoding;
    uint8_t flags = NodeHeader::get_flags(origin.get_header()); // take flags from origi array
    Allocator& allocator = arr.get_alloc();
    auto mem = allocator.alloc(byte_size);
    auto header = mem.get_addr();
    NodeHeader::init_header(header, 'B', Encoding::Packed, flags, v_width, origin.size());
    NodeHeader::set_capacity_in_header(byte_size, (char*)header);
    arr.init_from_mem(mem);
    REALM_ASSERT_DEBUG(arr.m_ref == mem.get_ref());
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(header) == 'B');
    REALM_ASSERT_DEBUG(NodeHeader::get_encoding(header) == Encoding::Packed);
}

void ArrayPacked::copy_into_packed_array(const Array& origin, Array& arr)
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    using Encoding = NodeHeader::Encoding;
    auto header = arr.get_header();
    auto v_width = NodeHeader::get_element_size<Encoding::Packed>(header);
    auto v_size = origin.size();
    REALM_ASSERT_DEBUG(v_size == NodeHeader::get_num_elements<Encoding::Packed>(header));
    auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
    bf_iterator it_value{data, 0, v_width, v_width, 0};
    for (size_t i = 0; i < v_size; ++i) {
        it_value.set_value(origin.get(i));
        auto v = sign_extend_field(v_width, it_value.get_value());
        REALM_ASSERT_DEBUG(v == origin.get(i));
        ++it_value;
    }
    REALM_ASSERT_DEBUG(arr.get_kind(header) == 'B');
    REALM_ASSERT_DEBUG(arr.get_encoding(header) == Encoding::Packed);
}

bool ArrayPacked::decode(Array& arr)
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, v_size;
    if (get_encode_info(arr, v_width, v_size)) {
        auto values = fetch_signed_values_from_packed_array(arr, v_width, v_size);
        REALM_ASSERT_DEBUG(values.size() == v_size);
        restore_array(arr, values); // restore array sets capacity
        return true;
    }
    return false;
}

void ArrayPacked::set_direct(const Array& arr, size_t ndx, int64_t value) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, v_size;
    if (get_encode_info(arr, v_width, v_size)) {
        REALM_ASSERT_DEBUG(ndx < v_size);
        auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        bf_iterator it_value{data, static_cast<size_t>(ndx * v_width), v_width, v_width, 0};
        it_value.set_value(value);
    }
    REALM_UNREACHABLE();
}

int64_t ArrayPacked::get(const Array& arr, size_t ndx) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.is_encoded());
    return ArrayPacked::get(arr.get_header(), ndx);
}

int64_t ArrayPacked::get(const char* h, size_t ndx)
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(h) == 'B');
    REALM_ASSERT_DEBUG(NodeHeader::get_encoding(h) == NodeHeader::Encoding::Packed);
    const auto v_size = NodeHeader::get_num_elements<Encoding::Packed>(h);
    const auto v_width = NodeHeader::get_element_size<Encoding::Packed>(h);
    if (ndx >= v_size)
        return realm::not_found;
    const auto data = (uint64_t*)NodeHeader::get_data_from_header(h);
    const bf_iterator it_value{data, static_cast<size_t>(v_width * ndx), v_width, v_width, 0};
    return sign_extend_field(v_width, it_value.get_value());
}

void ArrayPacked::get_chunk(const Array& arr, size_t ndx, int64_t res[8]) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, v_size;
    if (get_encode_info(arr, v_width, v_size)) {
        REALM_ASSERT_DEBUG(ndx < v_size);
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

size_t ArrayPacked::find_first(const Array& arr, int64_t value) const
{
    static constexpr auto MAX_SZ_LINEAR_FIND = 15;
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, v_size;
    if (get_encode_info(arr, v_width, v_size)) {
        auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        if (v_size <= MAX_SZ_LINEAR_FIND) {
            return impl::find_linear(data, value, v_width, v_size);
        }
        return impl::find_binary(data, value, v_width, v_size);
    }
    return realm::not_found;
}

int64_t ArrayPacked::sum(const Array& arr, size_t start, size_t end) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, v_size;
    if (get_encode_info(arr, v_width, v_size)) {
        REALM_ASSERT_DEBUG(v_size >= start && v_size <= end);
        const auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        int64_t total_sum = 0;
        for (size_t i = start; i < end; ++i) {
            const auto offset = static_cast<size_t>(i * v_width);
            total_sum += *(data + offset);
        }
        return total_sum;
    }
    REALM_UNREACHABLE();
}

size_t ArrayPacked::lower_bound(const Array& arr, int64_t value) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, v_size;
    if (get_encode_info(arr, v_width, v_size)) {
        const auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        return impl::lower_bound(data, value, v_width, v_size);
    }
    REALM_UNREACHABLE();
}

size_t ArrayPacked::upper_bound(const Array& arr, int64_t value) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, v_size;
    if (get_encode_info(arr, v_width, v_size)) {
        const auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
        return impl::upper_bound(data, value, v_width, v_size);
    }
    REALM_UNREACHABLE();
}


bool inline ArrayPacked::get_encode_info(const Array& arr, size_t& v_width, size_t& v_size)
{
    using Encoding = NodeHeader::Encoding;
    if (arr.is_encoded()) {
        auto h = arr.get_header();
        const auto encoding = NodeHeader::get_encoding(h);
        REALM_ASSERT_DEBUG(encoding == Encoding::Packed);
        v_width = NodeHeader::get_element_size<Encoding::Packed>(h);
        v_size = NodeHeader::get_num_elements<Encoding::Packed>(h);
        return true;
    }
    return false;
}

std::vector<int64_t> ArrayPacked::fetch_signed_values_from_packed_array(const Array& arr, size_t v_width,
                                                                        size_t v_size) const
{
    std::vector<int64_t> values;
    values.reserve(v_size);
    auto data = (uint64_t*)NodeHeader::get_data_from_header(arr.get_header());
    bf_iterator it_value{data, 0, v_width, v_width, 0};
    for (size_t i = 0; i < v_size; ++i) {
        const auto value = it_value.get_value();
        const auto ivalue = sign_extend_field(v_width, value);
        values.push_back(ivalue);
        ++it_value;
    }
    return values;
}

void ArrayPacked::restore_array(Array& arr, const std::vector<int64_t>& values) const
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
        ArrayEncode::set_direct(data, width, ndx++, v);

    REALM_ASSERT_DEBUG(width == arr.get_width());
    REALM_ASSERT_DEBUG(arr.size() == values.size());
}
