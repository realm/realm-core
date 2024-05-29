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

#include <realm/integer_compressor.hpp>
#include <realm/array.hpp>
#include <realm/integer_flex_compressor.hpp>
#include <realm/integer_packed_compressor.hpp>
#include <realm/array_with_find.hpp>
#include <realm/query_conditions.hpp>

#include <vector>
#include <algorithm>

using namespace realm;

namespace {

template <typename T, typename... Arg>
inline void init_compress_array(Array& arr, size_t byte_size, Arg&&... args)
{
    Allocator& allocator = arr.get_alloc();
    auto mem = allocator.alloc(byte_size);
    auto h = mem.get_addr();
    T::init_header(h, std::forward<Arg>(args)...);
    NodeHeader::set_capacity_in_header(byte_size, h);
    arr.init_from_mem(mem);
}

} // namespace

bool IntegerCompressor::always_compress(const Array& origin, Array& arr, NodeHeader::Encoding encoding) const
{
    using Encoding = NodeHeader::Encoding;
    std::vector<int64_t> values;
    std::vector<unsigned> indices;
    compress_values(origin, values, indices);
    if (!values.empty()) {
        const uint8_t flags = NodeHeader::get_flags(origin.get_header());
        uint8_t v_width = std::max(Node::signed_to_num_bits(values.front()), Node::signed_to_num_bits(values.back()));

        if (encoding == Encoding::Packed) {
            const auto packed_size = NodeHeader::calc_size(indices.size(), v_width, NodeHeader::Encoding::Packed);
            init_compress_array<PackedCompressor>(arr, packed_size, flags, v_width, origin.size());
            PackedCompressor::copy_data(origin, arr);
        }
        else if (encoding == Encoding::Flex) {
            uint8_t ndx_width = NodeHeader::unsigned_to_num_bits(values.size());
            const auto flex_size = NodeHeader::calc_size(values.size(), indices.size(), v_width, ndx_width);
            init_compress_array<FlexCompressor>(arr, flex_size, flags, v_width, ndx_width, values.size(),
                                                indices.size());
            FlexCompressor::copy_data(arr, values, indices);
        }
        else {
            REALM_UNREACHABLE();
        }
        return true;
    }
    return false;
}

bool IntegerCompressor::compress(const Array& origin, Array& arr) const
{
    if (origin.m_width < 2 || origin.m_size == 0)
        return false;

#if REALM_COMPRESS
    return always_compress(origin, arr, NodeHeader::Encoding::Flex);
#else
    std::vector<int64_t> values;
    std::vector<unsigned> indices;
    compress_values(origin, values, indices);
    REALM_ASSERT(!values.empty());
    const auto uncompressed_size = origin.get_byte_size();
    uint8_t ndx_width = NodeHeader::unsigned_to_num_bits(values.size());
    uint8_t v_width = std::max(Node::signed_to_num_bits(values.front()), Node::signed_to_num_bits(values.back()));
    const auto packed_size = NodeHeader::calc_size(indices.size(), v_width, NodeHeader::Encoding::Packed);
    const auto flex_size = NodeHeader::calc_size(values.size(), indices.size(), v_width, ndx_width);
    // heuristic: only compress to packed if gain at least 11.1%
    const auto adjusted_packed_size = packed_size + packed_size / 8;
    // heuristic: only compress to flex if gain at least 20%
    const auto adjusted_flex_size = flex_size + flex_size / 4;
    if (adjusted_flex_size < adjusted_packed_size && adjusted_flex_size < uncompressed_size) {
        const uint8_t flags = NodeHeader::get_flags(origin.get_header());
        init_compress_array<FlexCompressor>(arr, flex_size, flags, v_width, ndx_width, values.size(), indices.size());
        FlexCompressor::copy_data(arr, values, indices);
        return true;
    }
    else if (adjusted_packed_size < uncompressed_size) {
        const uint8_t flags = NodeHeader::get_flags(origin.get_header());
        init_compress_array<PackedCompressor>(arr, packed_size, flags, v_width, origin.size());
        PackedCompressor::copy_data(origin, arr);
        return true;
    }
    return false;
#endif
}

bool IntegerCompressor::decompress(Array& arr) const
{
    int64_t min_v = std::numeric_limits<int64_t>::max();
    int64_t max_v = std::numeric_limits<int64_t>::min();
    REALM_ASSERT_DEBUG(arr.is_attached());
    auto values_fetcher = [&]() {
        const auto sz = arr.size();
        if (is_packed()) {
            std::vector<int64_t> res;
            res.reserve(sz);
            for (size_t i = 0; i < sz; ++i) {
                auto val = arr.get(i);
                if (val > max_v)
                    max_v = val;
                if (val < min_v)
                    min_v = val;
                res.push_back(val);
            }
            return res;
        }
        // in flex format this is faster.
        min_v = FlexCompressor::min(*this);
        max_v = FlexCompressor::max(*this);
        return FlexCompressor::get_all(*this, 0, sz);
    };
    const auto& values = values_fetcher();
    //  do the reverse of compressing the array
    REALM_ASSERT_DEBUG(!values.empty());
    using Encoding = NodeHeader::Encoding;
    const auto flags = NodeHeader::get_flags(arr.get_header());
    const auto size = values.size();

    const auto width = std::max(Array::bit_width(min_v), Array::bit_width(max_v));
    REALM_ASSERT_DEBUG(width == 0 || width == 1 || width == 2 || width == 4 || width == 8 || width == 16 ||
                       width == 32 || width == 64);
    // 64 is some slab allocator magic number.
    // The padding is needed in order to account for bit width expansion.
    const auto byte_size = 64 + NodeHeader::calc_size(size, width, Encoding::WTypBits);
    REALM_ASSERT_DEBUG(byte_size % 8 == 0); // nevertheless all the values my be aligned to 8

    // Create new array with the correct width
    const auto mem = arr.get_alloc().alloc(byte_size);
    const auto header = mem.get_addr();
    init_header(header, Encoding::WTypBits, flags, width, size);
    NodeHeader::set_capacity_in_header(byte_size, header);

    // Destroy old array before initializing
    arr.destroy();
    arr.init_from_mem(mem);

    // this is copying the bits straight, without doing any COW, since the array is basically restored, we just need
    // to copy the data straight back into it. This makes decompressing the array equivalent to copy on write for
    // normal arrays, in fact for a compressed array, we skip COW and we just decompress, getting the same result.
    if (width > 0) {
        auto setter = arr.m_vtable->setter;
        for (size_t ndx = 0; ndx < size; ++ndx)
            setter(arr, ndx, values[ndx]);
    }

    // very important: since the ref of the current array has changed, the parent must be informed.
    // Otherwise we will lose the link between parent array and child array.
    arr.update_parent();
    REALM_ASSERT_DEBUG(width == arr.get_width());
    REALM_ASSERT_DEBUG(arr.size() == values.size());

    return true;
}

bool IntegerCompressor::init(const char* h)
{
    m_encoding = NodeHeader::get_encoding(h);
    // avoid to check wtype here, it is another access to the header, that we can avoid.
    // We just need to know if the encoding is packed or flex.
    // This makes Array::init_from_mem faster.
    if (REALM_LIKELY(!(is_packed() || is_flex())))
        return false;

    if (is_packed()) {
        init_packed(h);
    }
    else {
        init_flex(h);
    }
    return true;
}
int64_t IntegerCompressor::get_packed(const Array& arr, size_t ndx)
{
    return PackedCompressor::get(arr.m_integer_compressor, ndx);
}

int64_t IntegerCompressor::get_flex(const Array& arr, size_t ndx)
{
    return FlexCompressor::get(arr.m_integer_compressor, ndx);
}

std::vector<int64_t> IntegerCompressor::get_all_packed(const Array& arr, size_t begin, size_t end)
{
    return PackedCompressor::get_all(arr.m_integer_compressor, begin, end);
}

std::vector<int64_t> IntegerCompressor::get_all_flex(const Array& arr, size_t begin, size_t end)
{
    return FlexCompressor::get_all(arr.m_integer_compressor, begin, end);
}

void IntegerCompressor::get_chunk_packed(const Array& arr, size_t ndx, int64_t res[8])
{
    PackedCompressor::get_chunk(arr.m_integer_compressor, ndx, res);
}

void IntegerCompressor::get_chunk_flex(const Array& arr, size_t ndx, int64_t res[8])
{
    FlexCompressor::get_chunk(arr.m_integer_compressor, ndx, res);
}

void IntegerCompressor::set_packed(Array& arr, size_t ndx, int64_t val)
{
    PackedCompressor::set_direct(arr.m_integer_compressor, ndx, val);
}

void IntegerCompressor::set_flex(Array& arr, size_t ndx, int64_t val)
{
    FlexCompressor::set_direct(arr.m_integer_compressor, ndx, val);
}

template <class Cond>
bool IntegerCompressor::find_packed(const Array& arr, int64_t val, size_t begin, size_t end, size_t base_index,
                                    QueryStateBase* st)
{
    return PackedCompressor::find_all<Cond>(arr, val, begin, end, base_index, st);
}

template <class Cond>
bool IntegerCompressor::find_flex(const Array& arr, int64_t val, size_t begin, size_t end, size_t base_index,
                                  QueryStateBase* st)
{
    return FlexCompressor::find_all<Cond>(arr, val, begin, end, base_index, st);
}

void IntegerCompressor::set_vtable(Array& arr)
{
    static const Array::VTable vtable_packed = {get_packed,
                                                get_chunk_packed,
                                                get_all_packed,
                                                set_packed,
                                                {
                                                    find_packed<Equal>,
                                                    find_packed<NotEqual>,
                                                    find_packed<Greater>,
                                                    find_packed<Less>,
                                                }};
    static const Array::VTable vtable_flex = {get_flex,
                                              get_chunk_flex,
                                              get_all_flex,
                                              set_flex,
                                              {
                                                  find_flex<Equal>,
                                                  find_flex<NotEqual>,
                                                  find_flex<Greater>,
                                                  find_flex<Less>,
                                              }};
    if (is_packed()) {
        arr.m_vtable = &vtable_packed;
    }
    else {
        arr.m_vtable = &vtable_flex;
    }
}

int64_t IntegerCompressor::get(size_t ndx) const
{
    if (is_packed()) {
        return PackedCompressor::get(*this, ndx);
    }
    else {
        return FlexCompressor::get(*this, ndx);
    }
}

void IntegerCompressor::compress_values(const Array& arr, std::vector<int64_t>& values,
                                        std::vector<unsigned>& indices) const
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
    }

    std::sort(values.begin(), values.end());
    auto last = std::unique(values.begin(), values.end());
    values.erase(last, values.end());

    for (size_t i = 0; i < sz; ++i) {
        auto pos = std::lower_bound(values.begin(), values.end(), arr.get(i));
        indices.push_back(unsigned(std::distance(values.begin(), pos)));
        REALM_ASSERT_DEBUG(values[indices[i]] == arr.get(i));
    }
}
