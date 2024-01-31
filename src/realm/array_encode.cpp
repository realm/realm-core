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

void ArrayEncode::set_direct(char* data, size_t w, size_t ndx, int64_t v)
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


inline void arrange_data_in_flex_format(const Array& arr, std::vector<int64_t>& values, std::vector<size_t>& indices)
{
    // The idea is to either pick up Packed format or Flex Format.
    //  Packed format allows different bid width sizes (not aligned to the next power of 2). Thus generally better
    //  when all the values are different Flex format treats duplicates and allows to save space, storing only indices
    //  and values.
    //
    //
    //  Packed: || node header || ..... values ..... ||
    //  Flex:   || node header || ..... values ..... || ..... indices ..... ||
    //
    //  The algorithm is O(n lg n) for now, but we could use an efficient hash table and try to boost perf.

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

inline size_t compute_packed_size(std::vector<int64_t>& values, size_t sz, size_t& v_width)
{
    using Encoding = NodeHeader::Encoding;
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    v_width = std::max(Node::signed_to_num_bits(*min_value), Node::signed_to_num_bits(*max_value));
    REALM_ASSERT_DEBUG(v_width > 0);
    return NodeHeader::calc_size<Encoding::Packed>(sz, v_width);
}

inline size_t compute_flex_size(std::vector<int64_t>& values, std::vector<size_t>& indices, size_t& v_width,
                                size_t& ndx_width)
{
    using Encoding = NodeHeader::Encoding;
    const auto [min_value, max_value] = std::minmax_element(values.begin(), values.end());
    ndx_width = NodeHeader::unsigned_to_num_bits(values.size());
    v_width = std::max(Node::signed_to_num_bits(*min_value), Node::signed_to_num_bits(*max_value));
    REALM_ASSERT_DEBUG(v_width > 0);
    REALM_ASSERT_DEBUG(ndx_width > 0);
    return NodeHeader::calc_size<Encoding::Flex>(values.size(), indices.size(), v_width, ndx_width);
}

bool ArrayEncode::encode(const Array& origin, Array& dst)
{
    // check what makes more sense, Packed, Flex or just keep array as it is.
    std::vector<int64_t> values;
    std::vector<size_t> indices;
    arrange_data_in_flex_format(origin, values, indices);

    if (!values.empty()) {
        size_t v_width, ndx_width;
        const auto uncompressed_size = origin.get_byte_size();
        const auto packed_size = compute_packed_size(values, origin.size(), v_width);
        if (!indices.empty()) {
            const auto flex_size = compute_flex_size(values, indices, v_width, ndx_width);
            if (flex_size < uncompressed_size && packed_size > flex_size) {
                return ArrayFlex::encode(origin, dst, flex_size, values, indices, v_width, ndx_width);
            }
        }
        if (packed_size < uncompressed_size)
            return ArrayPacked::encode(origin, dst, packed_size, v_width);
    }
    return false;
}

size_t ArrayEncode::size(const char* h)
{
    using Encoding = NodeHeader::Encoding;
    return is_packed(h) ? NodeHeader::get_num_elements<Encoding::Packed>(h)
                        : NodeHeader::get_arrayB_num_elements<Encoding::Flex>(h);
}

int64_t ArrayEncode::get(const char* header, size_t ndx)
{
    using Encoding = NodeHeader::Encoding;
    const auto kind = NodeHeader::get_kind(header);
    const auto encoding = NodeHeader::get_encoding(header);
    // we only deal with packed and flex encodings for now
    REALM_ASSERT_DEBUG(kind == 'B');
    REALM_ASSERT_DEBUG(encoding == Encoding::Packed || encoding == Encoding::Flex);
    switch (encoding) {
        case Encoding::Packed: {
            return ArrayPacked::get(header, ndx);
        }
        case Encoding::Flex: {
            return ArrayFlex::get(header, ndx);
        }
        default:
            break;
    }
    // if we hit this, there is a foundamental error in our code.
    REALM_UNREACHABLE();
}

bool ArrayEncode::is_packed(const char* h)
{
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(h));
    return NodeHeader::get_encoding(h) == NodeHeader::Encoding::Packed;
}
