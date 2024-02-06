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

#include <realm/array_encode.hpp>
#include <realm/array.hpp>
#include <realm/array_flex.hpp>
#include <realm/array_packed.hpp>

#include <vector>
#include <algorithm>

using namespace realm;

static ArrayFlex s_flex;
static ArrayPacked s_packed;

bool ArrayEncode::encode(const Array& origin, Array& dst) const
{
    std::vector<int64_t> values;
    std::vector<size_t> indices;
    try_encode(origin, values, indices);

    // For testing, compacts all the interger leaves we encounter.
    if (!values.empty()) {
        size_t v_width, ndx_width;
        //        const auto packed_size = packed_encoded_array_size(values, origin.size(), v_width);
        //        return s_packed.encode(origin, dst, packed_size, v_width);
        const auto flex_size = flex_encoded_array_size(values, indices, v_width, ndx_width);
        return s_flex.encode(origin, dst, flex_size, values, indices, v_width, ndx_width);
    }
    return false;

    // check what makes more sense, Packed, Flex or just keep array as it is.
    // return false;

    //    if (!values.empty()) {
    //        size_t v_width, ndx_width;
    //        const auto uncompressed_size = origin.get_byte_size();
    //        const auto packed_size = packed_encoded_array_size(values, origin.size(), v_width);
    //        const auto flex_size = flex_encoded_array_size(values, indices, v_width, ndx_width);
    //        if (flex_size < packed_size && flex_size < uncompressed_size) {
    //            return s_flex.encode(origin, dst, flex_size, values, indices, v_width, ndx_width);
    //        }
    //        else if (packed_size < uncompressed_size)
    //            return s_packed.encode(origin, dst, packed_size, v_width);
    //    }
    //    return false;
}

bool ArrayEncode::decode(Array& arr) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());

    const auto h = arr.get_header();
    auto values_fetcher = [this](const auto h) {
        return is_packed(h) ? s_packed.fetch_signed_values_from_packed_array(h)
                            : s_flex.fetch_signed_values_from_encoded_array(h);
    };

    const auto& values = values_fetcher(h);
    //  do the reverse of compressing the array
    REALM_ASSERT_DEBUG(!values.empty());
    using Encoding = NodeHeader::Encoding;
    const auto flags = NodeHeader::get_flags(arr.get_header());
    const auto size = values.size();
    const auto [min_v, max_v] = std::minmax_element(values.begin(), values.end());
    auto width = std::max(Array::bit_width(*min_v), Array::bit_width(*max_v));
    REALM_ASSERT_DEBUG(width == 0 || width == 1 || width == 2 || width == 4 || width == 8 || width == 16 ||
                       width == 32 || width == 64);
    auto byte_size = NodeHeader::calc_size<Encoding::WTypBits>(size, width);
    byte_size += 64; // this is some slab allocator magic number, this padding is needed in order to account for bit
                     // width expansion.

    REALM_ASSERT_DEBUG(byte_size % 8 == 0); // nevertheless all the values my be aligned to 8

    auto& allocator = arr.get_alloc(); // get allocator

    // store tmp header and ref, because these will be deleted once the array is restored.
    auto old_ref = arr.get_ref();
    auto old_h = arr.get_header();

    auto mem = allocator.alloc(byte_size);
    auto header = mem.get_addr();
    NodeHeader::init_header(header, 'A', Encoding::WTypBits, flags, width, values.size());
    NodeHeader::set_capacity_in_header(byte_size, header);
    arr.init_from_mem(mem);
    auto data = arr.m_data;
    size_t ndx = 0;
    // this is copying the bits straight, without doing any COW.
    // Restoring the array is basically COW.
    for (const auto& v : values)
        copy_direct(data, width, ndx++, v);

    // very important: since the ref of the current array has changed, the parent must be informed.
    // Otherwise we will lose the link between parent array and child array.
    arr.update_parent();
    REALM_ASSERT_DEBUG(width == arr.get_width());
    REALM_ASSERT_DEBUG(arr.size() == values.size());

    // free memory no longer used. Very important to avoid to leak memory. Either in the slab or in the C++  heap.
    allocator.free_(old_ref, old_h);
    return true;
}

size_t ArrayEncode::size(const char* h)
{
    using Encoding = NodeHeader::Encoding;
    return is_packed(h) ? NodeHeader::get_num_elements<Encoding::Packed>(h)
                        : NodeHeader::get_arrayB_num_elements<Encoding::Flex>(h);
}

int64_t ArrayEncode::get(const Array& arr, size_t ndx) const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(arr.m_kind == 'B');
    REALM_ASSERT_DEBUG(arr.m_encoding == Encoding::Flex || arr.m_encoding == Encoding::Packed);
    const auto h = arr.get_header();
    return arr.m_encoding == NodeHeader::Encoding::Flex ? s_flex.get(h, ndx) : s_packed.get(h, ndx);
}

int64_t ArrayEncode::get(const char* header, size_t ndx)
{
    // REALM_ASSERT_DEBUG(is_flex(header) || is_packed(header));
    const auto is_packed = NodeHeader::get_encoding(header) == NodeHeader::Encoding::Packed;
    return is_packed ? s_packed.get(header, ndx) : s_flex.get(header, ndx);
}

void ArrayEncode::get_chunk(const Array& arr, size_t ndx, int64_t res[8]) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    const auto* h = arr.get_header();
    REALM_ASSERT_DEBUG(arr.get_kind(h) == 'B');
    return is_packed(h) ? s_packed.get_chunk(h, ndx, res) : s_flex.get_chunk(h, ndx, res);
}

bool ArrayEncode::is_encoded(const char* h)
{
    return NodeHeader::get_kind(h) == 'B';
}

bool inline ArrayEncode::is_packed(const char* h) const
{
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(h) == 'B');
    return NodeHeader::get_encoding(h) == NodeHeader::Encoding::Packed;
}

bool ArrayEncode::is_flex(const char* h) const
{
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(h) == 'B');
    return NodeHeader::get_encoding(h) == NodeHeader::Encoding::Flex;
}

void ArrayEncode::set_direct(const Array& arr, size_t ndx, int64_t value) const
{
    const auto h = arr.get_header();
    REALM_ASSERT_DEBUG(is_packed(h) || is_flex(h));
    is_packed(arr.get_header()) ? s_packed.set_direct(h, ndx, value) : s_flex.set_direct(h, ndx, value);
}

template <typename F>
size_t ArrayEncode::find_first(const Array& arr, int64_t value, size_t start, size_t end, F cmp) const
{
    return is_packed(arr.get_header()) ? s_packed.find_first(arr, value, start, end, cmp)
                                       : s_flex.find_first(arr, value, start, end, cmp);
}

int64_t ArrayEncode::sum(const Array& arr, size_t start, size_t end) const
{
    return is_packed(arr.get_header()) ? s_packed.sum(arr, start, end) : s_flex.sum(arr, start, end);
}

void ArrayEncode::copy_direct(char* data, size_t w, size_t ndx, int64_t v) const
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

size_t ArrayEncode::flex_encoded_array_size(const std::vector<int64_t>& values, const std::vector<size_t>& indices,
                                            size_t& v_width, size_t& ndx_width) const
{
    using Encoding = NodeHeader::Encoding;
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    ndx_width = NodeHeader::unsigned_to_num_bits(values.size());
    v_width = std::max(Node::signed_to_num_bits(*min_value), Node::signed_to_num_bits(*max_value));
    REALM_ASSERT_DEBUG(v_width > 0);
    REALM_ASSERT_DEBUG(ndx_width > 0);
    return NodeHeader::calc_size<Encoding::Flex>(values.size(), indices.size(), v_width, ndx_width);
}

size_t ArrayEncode::packed_encoded_array_size(std::vector<int64_t>& values, size_t sz, size_t& v_width) const
{
    using Encoding = NodeHeader::Encoding;
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    v_width = std::max(Node::signed_to_num_bits(*min_value), Node::signed_to_num_bits(*max_value));
    REALM_ASSERT_DEBUG(v_width > 0);
    return NodeHeader::calc_size<Encoding::Packed>(sz, v_width);
}

void ArrayEncode::try_encode(const Array& arr, std::vector<int64_t>& values, std::vector<size_t>& indices) const
{
    // The main idea is to encode the values in flex format. If Packed is better it will chosen by
    // ArrayEncode::encode. The algorithm is O(n lg n), it gives us nice properties, but we could use an efficient
    // hash table and try to boost perf during insertion. The two formats are represented as following, the array is
    // mutated in either of these 2 formats:
    //  Packed: || node header || ..... values ..... ||
    //  Flex:   || node header || ..... values ..... || ..... indices ..... ||

    const auto sz = arr.size();
    REALM_ASSERT_DEBUG(sz > 0);
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

#if REALM_DEBUG
    for (size_t i = 0; i < sz; ++i) {
        auto old_value = arr.get(i);
        auto new_value = values[indices[i]];
        REALM_ASSERT_DEBUG(new_value == old_value);
    }
#endif
    REALM_ASSERT_DEBUG(indices.size() == sz);
}
