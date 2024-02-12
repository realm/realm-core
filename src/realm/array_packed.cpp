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

void ArrayPacked::init_array(char* h, uint8_t flags, size_t v_width, size_t v_size) const
{
    using Encoding = NodeHeader::Encoding;
    NodeHeader::init_header((char*)h, 'B', Encoding::Packed, flags, v_width, v_size);
}

void ArrayPacked::copy_data(const Array& origin, Array& arr) const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.m_kind == 'B');
    REALM_ASSERT_DEBUG(arr.m_encoding == Encoding::Packed);
    const auto h = arr.get_header();
    size_t v_width, v_size;
    get_encode_info(h, v_width, v_size);
    auto data = (uint64_t*)arr.m_data;
    bf_iterator it_value{data, 0, v_width, v_width, 0};
    for (size_t i = 0; i < v_size; ++i) {
        it_value.set_value(origin.get(i));
        REALM_ASSERT_DEBUG(sign_extend_field(v_width, it_value.get_value()) == origin.get(i));
        ++it_value;
    }
}

NodeHeader::Encoding ArrayPacked::get_encoding() const
{
    return NodeHeader::Encoding::Packed;
}

std::vector<int64_t> ArrayPacked::fetch_signed_values_from_encoded_array(const Array& arr) const
{
    size_t v_width, v_size;
    get_encode_info(arr.get_header(), v_width, v_size);
    std::vector<int64_t> values;
    values.reserve(v_size);
    auto data = (uint64_t*)arr.m_data;
    bf_iterator it_value{data, 0, v_width, v_width, 0};
    for (size_t i = 0; i < v_size; ++i) {
        const auto value = get(arr, i);
        values.push_back(value);
        ++it_value;
    }
    return values;
}


void ArrayPacked::set_direct(const Array& arr, size_t ndx, int64_t value) const
{
    size_t v_width, v_size;
    get_encode_info(arr.get_header(), v_width, v_size);
    REALM_ASSERT_DEBUG(ndx < v_size);
    auto data = (uint64_t*)arr.m_data;
    bf_iterator it_value{data, static_cast<size_t>(ndx * v_width), v_width, v_width, 0};
    it_value.set_value(value);
}

int64_t ArrayPacked::get(const Array& arr, size_t ndx) const
{
    size_t v_width, v_size;
    get_encode_info(arr.get_header(), v_width, v_size);
    return do_get((uint64_t*)arr.m_data, ndx, v_width, v_size);
}

int64_t ArrayPacked::get(const char* h, size_t ndx)
{
    size_t v_width, v_size;
    get_encode_info(h, v_width, v_size);
    const auto data_area = (uint64_t*)(NodeHeader::get_data_from_header(h));
    return do_get(data_area, ndx, v_width, v_size);
}

int64_t ArrayPacked::do_get(uint64_t* data, size_t ndx, size_t v_width, size_t v_size)
{
    if (ndx >= v_size)
        return realm::not_found;

    bf_iterator it{data, 0, v_width, v_width, ndx};
    //    auto field_size = v_width;
    //    auto field_position = ndx * field_size;
    //    auto first_word_ptr = data_area + (field_position >> 6);

    auto result = it.get_value();
    //    auto in_word_position = field_position & 0x3F;
    //    auto first_word = first_word_ptr[0];
    //    uint64_t result = first_word >> in_word_position;
    //    if (in_word_position + field_size > 64) {
    //        auto first_word_size = 64 - in_word_position;
    //        auto second_word = first_word_ptr[1];
    //        result |= second_word << first_word_size;
    //    }
    //    if (field_size < 64)
    //        result &= (1ULL << field_size) - 1;

    return sign_extend_field(v_width, result);
}

void ArrayPacked::get_chunk(const Array& arr, size_t ndx, int64_t res[8]) const
{
    size_t v_width, v_size;
    get_encode_info(arr.get_header(), v_width, v_size);
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

int64_t ArrayPacked::sum(const Array& arr, size_t start, size_t end) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    size_t v_width, v_size;
    const auto* h = arr.get_header();
    get_encode_info(h, v_width, v_size);
    REALM_ASSERT_DEBUG(v_size >= start && v_size <= end);
    const auto data = (uint64_t*)arr.m_data;
    int64_t total_sum = 0;
    bf_iterator it_value{data, static_cast<size_t>(v_width * start), v_width, v_width, 0};
    for (size_t i = start; i < end; ++i) {
        const auto v = sign_extend_field(v_width, it_value.get_value());
        total_sum += v;
        ++it_value;
    }
    return total_sum;
}

void inline ArrayPacked::get_encode_info(const char* h, size_t& v_width, size_t& v_size)
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(h) == 'B');
    REALM_ASSERT_DEBUG(NodeHeader::get_encoding(h) == NodeHeader::Encoding::Packed);
    v_width = NodeHeader::get_element_size<Encoding::Packed>(h);
    v_size = NodeHeader::get_num_elements<Encoding::Packed>(h);
}

template <typename F>
size_t ArrayPacked::find_first(const Array& arr, int64_t key, size_t start, size_t end, F cmp)
{
    constexpr auto LIMIT = 100;
    const auto h = arr.get_header();
    size_t v_width, v_size;
    get_encode_info(h, v_width, v_size);

    auto data = (uint64_t*)arr.m_data;
    if (v_size <= LIMIT) {
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
