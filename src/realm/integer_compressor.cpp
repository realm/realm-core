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

class ArraySetter {
public:
    ArraySetter()
    {
        m_width_setters.resize(65);
        m_width_setters[0] = &realm::set_direct<0>;
        m_width_setters[1] = &realm::set_direct<1>;
        m_width_setters[2] = &realm::set_direct<2>;
        m_width_setters[4] = &realm::set_direct<4>;
        m_width_setters[8] = &realm::set_direct<8>;
        m_width_setters[16] = &realm::set_direct<16>;
        m_width_setters[32] = &realm::set_direct<32>;
        m_width_setters[64] = &realm::set_direct<64>;
    }
    void set(uint8_t width, char* data, size_t pos, int_fast64_t value) const
    {
        const auto setter = m_width_setters[width];
        REALM_ASSERT_DEBUG(setter != nullptr);
        (setter)(data, pos, value);
    }

private:
    using SetDirect = void (*)(char*, size_t, int_fast64_t);
    std::vector<SetDirect> m_width_setters;
};
static ArraySetter s_array_setter;


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
    std::vector<size_t> indices;
    compress_values(origin, values, indices);
    if (!values.empty()) {
        uint8_t v_width, ndx_width;
        const uint8_t flags = NodeHeader::get_flags(origin.get_header());

        if (encoding == Encoding::Packed) {
            const auto packed_size = packed_disk_size(values, origin.size(), v_width);
            init_compress_array<PackedCompressor>(arr, packed_size, flags, v_width, origin.size());
            PackedCompressor::copy_data(origin, arr);
        }
        else if (encoding == Encoding::Flex) {
            const auto flex_size = flex_disk_size(values, indices, v_width, ndx_width);
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
#if REALM_COMPRESS
    return always_compress(origin, arr, NodeHeader::Encoding::Flex);
#else
    std::vector<int64_t> values;
    std::vector<size_t> indices;
    compress_values(origin, values, indices);
    if (!values.empty()) {
        uint8_t v_width, ndx_width;
        const auto uncompressed_size = origin.get_byte_size();
        const auto packed_size = packed_disk_size(values, origin.size(), v_width);
        const auto flex_size = flex_disk_size(values, indices, v_width, ndx_width);
        // heuristic: only compress to packed if gain at least 12.5%
        const auto adjusted_packed_size = packed_size + packed_size / 8;
        // heuristic: only compress to flex if gain at least 25%
        const auto adjusted_flex_size = flex_size + flex_size / 4;
        if (adjusted_flex_size < adjusted_packed_size && adjusted_flex_size < uncompressed_size) {
            const uint8_t flags = NodeHeader::get_flags(origin.get_header());
            init_compress_array<FlexCompressor>(arr, flex_size, flags, v_width, ndx_width, values.size(),
                                                indices.size());
            FlexCompressor::copy_data(arr, values, indices);
            return true;
        }
        else if (adjusted_packed_size < uncompressed_size) {
            const uint8_t flags = NodeHeader::get_flags(origin.get_header());
            init_compress_array<PackedCompressor>(arr, packed_size, flags, v_width, origin.size());
            PackedCompressor::copy_data(origin, arr);
            return true;
        }
    }
    return false;
#endif
}

bool IntegerCompressor::decompress(Array& arr) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    auto values_fetcher = [&arr, this]() {
        const auto sz = arr.size();
        if (is_packed()) {
            std::vector<int64_t> res;
            res.reserve(sz);
            for (size_t i = 0; i < sz; ++i)
                res.push_back(arr.get(i));
            return res;
        }
        // in flex format this is faster.
        return arr.get_all(0, sz);
    };
    const auto& values = values_fetcher();
    //  do the reverse of compressing the array
    REALM_ASSERT_DEBUG(!values.empty());
    using Encoding = NodeHeader::Encoding;
    const auto flags = NodeHeader::get_flags(arr.get_header());
    const auto size = values.size();
    const auto [min_v, max_v] = std::minmax_element(values.begin(), values.end());
    const auto width = std::max(Array::bit_width(*min_v), Array::bit_width(*max_v));
    REALM_ASSERT_DEBUG(width == 0 || width == 1 || width == 2 || width == 4 || width == 8 || width == 16 ||
                       width == 32 || width == 64);
    // 64 is some slab allocator magic number.
    // The padding is needed in order to account for bit width expansion.
    const auto byte_size = 64 + NodeHeader::calc_size(size, width, Encoding::WTypBits);

    REALM_ASSERT_DEBUG(byte_size % 8 == 0); // nevertheless all the values my be aligned to 8

    auto& allocator = arr.get_alloc(); // get allocator

    // store tmp header and ref, because these will be deleted once the array is restored.
    auto old_ref = arr.get_ref();
    auto old_h = arr.get_header();

    const auto mem = allocator.alloc(byte_size);
    const auto header = mem.get_addr();
    init_header(header, Encoding::WTypBits, flags, width, values.size());
    NodeHeader::set_capacity_in_header(byte_size, header);
    arr.init_from_mem(mem);

    // this is copying the bits straight, without doing any COW, since the array is basically restored, we just need
    // to copy the data straight back into it. This makes decompressing the array equivalent to copy on write for
    // normal arrays, in fact for a compressed array, we skip COW and we just decompress, getting the same result.
    for (size_t ndx = 0; ndx < size; ++ndx)
        s_array_setter.set(width, arr.m_data, ndx, values[ndx]);

    // very important: since the ref of the current array has changed, the parent must be informed.
    // Otherwise we will lose the link between parent array and child array.
    arr.update_parent();
    REALM_ASSERT_DEBUG(width == arr.get_width());
    REALM_ASSERT_DEBUG(arr.size() == values.size());

    // free memory no longer used. Very important to avoid to leak memory. Either in the slab or in the C++  heap.
    allocator.free_(old_ref, old_h);
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

size_t IntegerCompressor::flex_disk_size(const std::vector<int64_t>& values, const std::vector<size_t>& indices,
                                         uint8_t& v_width, uint8_t& ndx_width) const
{
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    ndx_width = NodeHeader::unsigned_to_num_bits(values.size());
    v_width = std::max(Node::signed_to_num_bits(*min_value), Node::signed_to_num_bits(*max_value));
    REALM_ASSERT_DEBUG(v_width > 0);
    REALM_ASSERT_DEBUG(ndx_width > 0);
    return NodeHeader::calc_size(values.size(), indices.size(), v_width, ndx_width);
}

size_t IntegerCompressor::packed_disk_size(std::vector<int64_t>& values, size_t sz, uint8_t& v_width) const
{
    using Encoding = NodeHeader::Encoding;
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    v_width = std::max(Node::signed_to_num_bits(*min_value), Node::signed_to_num_bits(*max_value));
    REALM_ASSERT_DEBUG(v_width > 0);
    return NodeHeader::calc_size(sz, v_width, Encoding::Packed);
}

void IntegerCompressor::compress_values(const Array& arr, std::vector<int64_t>& values,
                                        std::vector<size_t>& indices) const
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
