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
    // this can be boosted a little bit, with and size should be known at this stage.
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.m_encoder.get_kind() == 'B');
    REALM_ASSERT_DEBUG(arr.m_encoder.get_encoding() == Encoding::Packed);
    // we don't need to access the header, init from mem must have been called
    const auto v_width = arr.m_width;
    const auto v_size = arr.m_size;
    auto data = (uint64_t*)arr.m_data;
    bf_iterator it_value{data, 0, v_width, v_width, 0};
    for (size_t i = 0; i < v_size; ++i) {
        it_value.set_value(origin.get(i));
        REALM_ASSERT_DEBUG(sign_extend_field(v_width, it_value.get_value()) == origin.get(i));
        ++it_value;
    }
}

std::vector<int64_t> ArrayPacked::fetch_all_values(const Array& arr) const
{
    REALM_ASSERT(arr.is_encoded());
    std::vector<int64_t> res;
    res.reserve(arr.m_size);
    for (size_t i = 0; i < arr.m_size; ++i) {
        res.push_back(arr.get(i));
    }
    return res;
    // return get_all_values(arr, arr.m_width, arr.m_size, 0, arr.size());
}

void ArrayPacked::set_direct(const Array& arr, size_t ndx, int64_t value) const
{
    REALM_ASSERT_DEBUG(arr.is_encoded());
    const auto v_width = arr.m_encoder.m_v_width;
    const auto v_size = arr.m_encoder.m_v_size;
    REALM_ASSERT_DEBUG(ndx < v_size);
    auto data = (uint64_t*)arr.m_data;
    bf_iterator it_value{data, static_cast<size_t>(ndx * v_width), v_width, v_width, 0};
    it_value.set_value(value);
}

int64_t ArrayPacked::get(const Array& arr, size_t ndx) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.is_encoded());
    const auto w = arr.m_encoder.m_v_width;
    const auto sz = arr.m_encoder.m_v_size;
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
    bf_iterator it{data, 0, v_width, v_width, ndx};
    const auto result = it.get_value();
    return sign_extend_field(v_width, result);
}

void ArrayPacked::get_chunk(const Array& arr, size_t ndx, int64_t res[8]) const
{
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

void inline ArrayPacked::get_encode_info(const char* h, size_t& v_width, size_t& v_size)
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(h) == 'B');
    REALM_ASSERT_DEBUG(NodeHeader::get_encoding(h) == NodeHeader::Encoding::Packed);
    v_width = NodeHeader::get_element_size<Encoding::Packed>(h);
    v_size = NodeHeader::get_num_elements<Encoding::Packed>(h);
}

template <typename F>
std::vector<int64_t> ArrayPacked::find_all(const Array& arr, int64_t, size_t start, size_t end, F) const
{
    const auto w = arr.m_width;
    const auto sz = arr.m_size;

    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.is_encoded());
    REALM_ASSERT_DEBUG(end <= sz);


    // we use the size in bits
    constexpr auto word_size = sizeof(int64_t) * 8;

    const auto starting_bit = w * start;
    const auto ending_bit = w * end;
    const auto starting_word = starting_bit / word_size;
    const auto ending_word = ending_bit / word_size;
    const auto data = (uint64_t*)arr.m_data;
    const auto start_data = (data + starting_word);
    const auto end_data = (data + ending_word);
    const auto shift_bits = starting_bit & (word_size - 1);
    const auto mask = (1ULL << w) - 1;
    const auto bytes_to_read = 1 + (end_data - start_data);

    size_t counter = starting_bit;
    auto word_limit = (starting_word + 1) * word_size;
    std::vector<int64_t> res;
    std::vector<uint64_t> raw_values;

    const auto add_value = [&w, &res, &counter](int64_t v, uint64_t& byte, size_t shift) {
        if (w < word_size)
            v = sign_extend_field(w, v);
        res.push_back(v);
        byte >>= shift;
        counter += w;
    };

    //    auto pos = start_data;
    //    while(pos <= end_data) {
    //        auto v = *pos;
    //        if(pos == start_data)
    //            v >>= shift_bits;
    //        while (counter < ending_bit && counter + w <= word_limit) {
    //            const auto sv = v & mask;
    //            if(!cmp(sv, value))
    //                return res;
    //            add_value(sv, sv, w);
    //        }
    //        if (counter < ending_bit && counter + w > word_limit) {
    //            const auto rest = word_limit - counter;
    //            const auto sv = (*(pos+1) << rest) | v;
    //            if(!cmp(sv, value))
    //                return res;
    //            add_value(sv, sv, w - rest);
    //        }
    //        ++pos;
    //    }

    realm::safe_copy_n(start_data, bytes_to_read, std::back_inserter(raw_values));

    if (shift_bits)
        raw_values[0] >>= shift_bits;


    for (size_t i = 0; i < raw_values.size(); ++i) {
        while (counter < ending_bit && counter + w <= word_limit) {
            const auto sv = raw_values[i] & mask;
            add_value(sv, raw_values[i], w);
        }
        if (counter < ending_bit && counter + w > word_limit) {
            const auto rest = word_limit - counter;
            const auto sv = (raw_values[i + 1] << rest) | raw_values[i];
            add_value(sv, raw_values[i + 1], w - rest);
        }
        word_limit += word_size;
    }
    //    REALM_ASSERT_DEBUG(res.size() == (end - start));
    // #if REALM_DEBUG
    //    for (size_t i = 0; i < res.size(); ++i) {
    //        REALM_ASSERT_DEBUG(arr.get(start++) == res[i]);
    //    }
    // #endif
    return res;
}

// template <typename F>
// size_t ArrayPacked::c(const Array& arr, int64_t key, size_t start, size_t end, F cmp)
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
