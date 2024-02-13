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
    // this can be boosted a little bit, with and size shold be known at this stage.
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
    REALM_ASSERT(arr.is_encoded());
    // v_width = arr.m_width
    size_t v_size = arr.m_size;
    // get_encode_info(arr.get_header(), v_width, v_size);
    std::vector<int64_t> values;
    values.reserve(v_size);
    // auto data = (uint64_t*)arr.m_data;
    // bf_iterator it_value{data, 0, v_width, v_width, 0};
    for (size_t i = 0; i < v_size; ++i) {
        //        const auto value = it_value.get_value();
        //        values.push_back(sign_extend_field(v_width, value));
        //        ++it_value;
        values.push_back(get(arr, i));
    }
    return values;
}


void ArrayPacked::set_direct(const Array& arr, size_t ndx, int64_t value) const
{
    REALM_ASSERT_DEBUG(arr.is_encoded());
    const auto v_width = arr.m_width;
    const auto v_size = arr.m_size;
    //    size_t v_width, v_size;
    //    get_encode_info(arr.get_header(), v_width, v_size);
    REALM_ASSERT_DEBUG(ndx < v_size);
    auto data = (uint64_t*)arr.m_data;
    bf_iterator it_value{data, static_cast<size_t>(ndx * v_width), v_width, v_width, 0};
    it_value.set_value(value);
}

int64_t ArrayPacked::get(const Array& arr, size_t ndx) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.is_encoded());
    const auto w = arr.m_encode.m_v_width;
    const auto sz = arr.m_encode.m_v_size;
    return do_get((uint64_t*)arr.m_data, ndx, w, sz);
}

int64_t ArrayPacked::get(const char* data, size_t ndx, size_t width, size_t sz) const
{
    return do_get((uint64_t*)data, ndx, width, sz);
}

int64_t ArrayPacked::do_get(uint64_t* data, size_t ndx, size_t v_width, size_t v_size) const
{
    if (ndx >= v_size)
        return realm::not_found;

    //    const auto mask = (1ULL << v_width) - 1;
    //    const auto offset = ndx * v_width;
    //    const auto pos = offset >> 6;
    //    const auto buff = (data + pos);
    //    uint64_t res = buff[0];
    //    const auto word_pos = offset % 64;
    //    res >>= word_pos;
    //    if (word_pos + v_width > 64) {
    //        // data among 2 words
    //        uint64_t rest = buff[1];
    //        rest <<= 64 - word_pos;
    //        res |= rest;
    //    }
    //    return sign_extend_field(v_width, res & mask);

    bf_iterator it{data, 0, v_width, v_width, ndx};
    const auto result = it.get_value();
    return sign_extend_field(v_width, result);
}

void ArrayPacked::get_chunk(const Array& arr, size_t ndx, int64_t res[8]) const
{
    //    size_t v_width, v_size;
    //    get_encode_info(arr.get_header(), v_width, v_size);
    const auto v_size = arr.m_size;
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

// int64_t ArrayPacked::sum(const Array& arr, size_t start, size_t end) const
//{
//     REALM_ASSERT_DEBUG(arr.is_attached());
//     size_t v_width = arr.m_width, v_size = arr.m_size;
//     //    const auto* h = arr.get_header();
//     //    get_encode_info(h, v_width, v_size);
//     REALM_ASSERT_DEBUG(v_size >= start && v_size <= end);
//     // const auto data = (uint64_t*)arr.m_data;
//     int64_t total_sum = 0;
//     // bf_iterator it_value{data, start, v_width, v_width, v_width};
//     for (size_t i = start; i < end; ++i) {
//         const auto v = get(arr, i); // sign_extend_field(v_width, it_value.get_value());
//         total_sum += v;
//         //++it_value;
//     }
//     return total_sum;
// }

void inline ArrayPacked::get_encode_info(const char* h, size_t& v_width, size_t& v_size)
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(h) == 'B');
    REALM_ASSERT_DEBUG(NodeHeader::get_encoding(h) == NodeHeader::Encoding::Packed);
    v_width = NodeHeader::get_element_size<Encoding::Packed>(h);
    v_size = NodeHeader::get_num_elements<Encoding::Packed>(h);
}

// template <typename F>
// size_t ArrayPacked::find_first(const Array& arr, int64_t key, size_t start, size_t end, F cmp)
//{
//     constexpr auto LIMIT = 30;
//     const auto h = arr.get_header();
//     size_t v_width, v_size;
//     get_encode_info(h, v_width, v_size);
//
//     // auto data = (uint64_t*)arr.m_data;
//     if (v_size <= LIMIT) {
//         for (size_t i = start; i < end; ++i) {
//             const auto v = get(arr, i);
//             //            const bf_iterator it_value{data, static_cast<size_t>(v_width * i), v_width, v_width, 0};
//             //            auto v = sign_extend_field(v_width, it_value.get_value());
//             if (cmp(v, key))
//                 return i;
//         }
//     }
//     else {
//         int lo = (int)start;
//         int hi = (int)end;
//         while (lo <= hi) {
//             int mid = lo + (hi - lo) / 2;
//             const auto v = get(arr, mid);
//             //            const bf_iterator it_value{data, static_cast<size_t>(v_width * mid), v_width, v_width,
//             0};
//             //            auto v = sign_extend_field(v_width, it_value.get_value());
//             if (cmp(v, key))
//                 return mid;
//             else if (key < v)
//                 hi = mid - 1;
//             else
//                 lo = mid + 1;
//         }
//     }
//     return realm::not_found;
// }
