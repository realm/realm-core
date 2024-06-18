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
#include <realm/integer_delta_compressor.hpp>
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
    enum CompressOption { none, flex, packed, delta };

    if (origin.m_width < 2 || origin.m_size == 0)
        return false;

#if REALM_COMPRESS
    return always_compress(origin, arr, NodeHeader::Encoding::Flex);
#else
    size_t sizes[4];
    std::vector<int64_t> values;
    std::vector<unsigned> indices;
    compress_values(origin, values, indices);
    REALM_ASSERT(!values.empty());
    sizes[none] = origin.get_byte_size();
    uint8_t ndx_width = NodeHeader::unsigned_to_num_bits(values.size());
    auto val_sz = values.size();
    auto end_ofs = val_sz - 1;
    if (val_sz > 1)
        end_ofs -= 1;
    uint8_t ofs_width = NodeHeader::unsigned_to_num_bits(values[end_ofs] - values[0] + 1);
    uint8_t v_width = std::max(Node::signed_to_num_bits(values.front()), Node::signed_to_num_bits(values.back()));
    const auto packed_size = NodeHeader::calc_size(indices.size(), v_width, NodeHeader::Encoding::Packed);
    const auto flex_size = NodeHeader::calc_size(val_sz, indices.size(), v_width, ndx_width);
    const auto offset_size = NodeHeader::calc_size(2, indices.size(), v_width, ofs_width);
    // heuristic: only compress to packed if gain at least 11.1%
    sizes[packed] = packed_size + packed_size / 8;
    // heuristic: only compress to flex if gain at least 20%
    sizes[flex] = flex_size + flex_size / 4;
    sizes[delta] = offset_size + offset_size / 8;
    auto comp = std::min({none, flex, packed, delta}, [&sizes](const CompressOption& a, const CompressOption& b) {
        return sizes[a] < sizes[b];
    });
    switch (comp) {
        case none:
            break;
        case flex: {
            const uint8_t flags = NodeHeader::get_flags(origin.get_header());
            init_compress_array<FlexCompressor>(arr, flex_size, flags, v_width, ndx_width, val_sz, indices.size());
            FlexCompressor::copy_data(arr, values, indices);
            return true;
        }
        case packed: {
            const uint8_t flags = NodeHeader::get_flags(origin.get_header());
            init_compress_array<PackedCompressor>(arr, packed_size, flags, v_width, origin.size());
            PackedCompressor::copy_data(origin, arr);
            return true;
        }
        case delta: {
            const uint8_t flags = NodeHeader::get_flags(origin.get_header());
            init_compress_array<DeltaCompressor>(arr, offset_size, flags, v_width, ofs_width, 2, indices.size());
            DeltaCompressor::copy_data(origin, arr, values);
            return true;
        }
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
    auto setter = arr.m_vtable->setter;
    for (size_t ndx = 0; ndx < size; ++ndx)
        setter(arr, ndx, values[ndx]);

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
    switch (m_encoding) {
        case NodeHeader::Encoding::Packed:
            init_packed(h);
            break;
        case NodeHeader::Encoding::Flex:
            init_flex(h);
            break;
        case NodeHeader::Encoding::Delta:
            init_delta(h);
            break;
        default:
            return false;
    }
    return true;
}

template <class T>
int64_t IntegerCompressor::get_compressed(const Array& arr, size_t ndx)
{
    return T::get(arr.m_integer_compressor, ndx);
}

template <class T>
std::vector<int64_t> IntegerCompressor::get_all_compressed(const Array& arr, size_t begin, size_t end)
{
    return T::get_all(arr.m_integer_compressor, begin, end);
}

template <class T>
void IntegerCompressor::get_chunk_compressed(const Array& arr, size_t ndx, int64_t res[8])
{
    T::get_chunk(arr.m_integer_compressor, ndx, res);
}

template <class T>
void IntegerCompressor::set_compressed(Array& arr, size_t ndx, int64_t val)
{
    T::set_direct(arr.m_integer_compressor, ndx, val);
}

template <class T, class Cond>
bool IntegerCompressor::find_compressed(const Array& arr, int64_t val, size_t begin, size_t end, size_t base_index,
                                        QueryStateBase* st)
{
    return T::template find_all<Cond>(arr, val, begin, end, base_index, st);
}


void IntegerCompressor::set_vtable(Array& arr)
{
    static const Array::VTable vtable_packed = {get_compressed<PackedCompressor>,
                                                get_chunk_compressed<PackedCompressor>,
                                                get_all_compressed<PackedCompressor>,
                                                set_compressed<PackedCompressor>,
                                                {
                                                    find_compressed<PackedCompressor, Equal>,
                                                    find_compressed<PackedCompressor, NotEqual>,
                                                    find_compressed<PackedCompressor, Greater>,
                                                    find_compressed<PackedCompressor, Less>,
                                                }};
    static const Array::VTable vtable_flex = {get_compressed<FlexCompressor>,
                                              get_chunk_compressed<FlexCompressor>,
                                              get_all_compressed<FlexCompressor>,
                                              set_compressed<FlexCompressor>,
                                              {
                                                  find_compressed<FlexCompressor, Equal>,
                                                  find_compressed<FlexCompressor, NotEqual>,
                                                  find_compressed<FlexCompressor, Greater>,
                                                  find_compressed<FlexCompressor, Less>,
                                              }};
    static const Array::VTable vtable_delta = {get_compressed<DeltaCompressor>,
                                               get_chunk_compressed<DeltaCompressor>,
                                               get_all_compressed<DeltaCompressor>,
                                               set_compressed<DeltaCompressor>,
                                               {
                                                   find_compressed<DeltaCompressor, Equal>,
                                                   find_compressed<DeltaCompressor, NotEqual>,
                                                   find_compressed<DeltaCompressor, Greater>,
                                                   find_compressed<DeltaCompressor, Less>,
                                               }};
    switch (m_encoding) {
        case NodeHeader::Encoding::Packed:
            arr.m_vtable = &vtable_packed;
            break;
        case NodeHeader::Encoding::Flex:
            arr.m_vtable = &vtable_flex;
            break;
        case NodeHeader::Encoding::Delta:
            arr.m_vtable = &vtable_delta;
            break;
        default:
            REALM_UNREACHABLE();
            break;
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
    // The main idea is to compress the values in flex format. If Packed is better it will be chosen by
    // IntegerCompressor::compress. The algorithm is O(n lg n), it gives us nice properties, but we could use an
    // efficient hash table and try to boost perf during insertion, although leaf arrays are relatively small in
    // general (256 entries). The two compresion formats are packed and flex, and the data in the array is re-arranged
    // in the following ways (if compressed):
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
