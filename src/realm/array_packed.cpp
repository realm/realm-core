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

size_t ArrayPacked::find_first(const Array& arr, int64_t key, size_t start, size_t end, bool (*cmp)(int64_t, int64_t))
{
    size_t v_width, v_size;
    get_encode_info(arr, v_width, v_size);
    auto data = (uint64_t*)arr.m_data;
    if (v_size <= 30) {
        for (size_t i = start; i < end; ++i) {
            const bf_iterator it_value{data, static_cast<size_t>(v_width * i), v_width, v_width, 0};
            auto v = sign_extend_field(v_width, it_value.get_value());
            if (cmp(v, key))
                return i;
        }
    }
    else {
        int lo = (int)start;
        int hi = (int)end;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            const bf_iterator it_value{data, static_cast<size_t>(v_width * mid), v_width, v_width, 0};
            auto v = sign_extend_field(v_width, it_value.get_value());
            if (cmp(v, key))
                return mid;
            else if (key < v)
                hi = mid - 1;
            else
                lo = mid + 1;
        }
    }
    return realm::not_found;
}
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
    auto v = sign_extend_field(v_width, it_value.get_value());
    return v;
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
    REALM_ASSERT_DEBUG(!values.empty());
    REALM_ASSERT_DEBUG(arr.is_attached());
    using Encoding = NodeHeader::Encoding;
    const auto flags = NodeHeader::get_flags(arr.get_header());
    const auto size = values.size();
    auto [min_v, max_v] = std::minmax_element(values.begin(), values.end());
    auto width = std::max(Array::bit_width(*min_v), Array::bit_width(*max_v));
    REALM_ASSERT_DEBUG(width == 0 || width == 1 || width == 2 || width == 4 || width == 8 || width == 16 ||
                       width == 32 || width == 64);
    auto byte_size = NodeHeader::calc_size<Encoding::WTypBits>(size, width);
    byte_size += 64;
    REALM_ASSERT_DEBUG(byte_size % 8 == 0); // 8 bytes aligned value
    auto old_ref = arr.get_ref();
    auto old_header = arr.get_header();

    auto& allocator = arr.get_alloc();
    auto mem = allocator.alloc(byte_size);
    auto header = mem.get_addr();
    NodeHeader::init_header(header, 'A', Encoding::WTypBits, flags, width, values.size());
    NodeHeader::set_capacity_in_header(byte_size, header);
    auto data = NodeHeader::get_data_from_header(header);
    size_t ndx = 0;
    for (const auto& v : values)
        ArrayEncode::set_direct(data, width, ndx++, v);
    arr.init_from_mem(mem);
    arr.update_parent();
    REALM_ASSERT_DEBUG(width == arr.get_width());
    REALM_ASSERT_DEBUG(arr.size() == values.size());
    allocator.free_(old_ref, old_header);
}
