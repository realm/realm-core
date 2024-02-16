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
#include <realm/array_with_find.hpp>
#include <realm/query_conditions.hpp>

#include <vector>
#include <algorithm>

using namespace realm;

static ArrayFlex s_flex;
static ArrayPacked s_packed;

template size_t ArrayEncode::find_first<Equal>(const Array&, int64_t, size_t, size_t) const;
template size_t ArrayEncode::find_first<NotEqual>(const Array&, int64_t, size_t, size_t) const;
template size_t ArrayEncode::find_first<Greater>(const Array&, int64_t, size_t, size_t) const;
template size_t ArrayEncode::find_first<Less>(const Array&, int64_t, size_t, size_t) const;

template bool ArrayEncode::find_all<Equal>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
template bool ArrayEncode::find_all<NotEqual>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
template bool ArrayEncode::find_all<Greater>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
template bool ArrayEncode::find_all<Less>(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

template <typename T, typename... Arg>
inline void encode_array(const T& encoder, Array& arr, size_t byte_size, Arg&&... args)
{
    Allocator& allocator = arr.get_alloc();
    auto mem = allocator.alloc(byte_size);
    auto h = mem.get_addr();
    encoder.init_array(h, std::forward<Arg>(args)...);
    NodeHeader::set_capacity_in_header(byte_size, h);
    arr.init_from_mem(mem);
    REALM_ASSERT_DEBUG(NodeHeader::get_kind(arr.get_header()) == 'B');
    // REALM_ASSERT_DEBUG(NodeHeader::get_encoding(arr.get_header()) == arr.m_encoder.m_encoding);
}

template <typename T, typename... Arg>
inline void copy_into_encoded_array(const T& encoder, Arg&&... args)
{
    encoder.copy_data(std::forward<Arg>(args)...);
}

template <typename Encoder>
std::vector<int64_t> fetch_values(const Encoder& encoder, const Array& arr)
{
    std::vector<int64_t> res;
    const auto sz = arr.size();
    res.reserve(sz);
    for (size_t i = 0; i < sz; ++i)
        res.push_back(encoder.get(arr, i));
    return res;
}

bool ArrayEncode::always_encode(const Array& origin, Array& arr, bool packed) const
{
    std::vector<int64_t> values;
    std::vector<size_t> indices;
    encode_values(origin, values, indices);
    if (!values.empty()) {
        size_t v_width, ndx_width;
        const uint8_t flags = NodeHeader::get_flags(origin.get_header());

        if (packed) {
            const auto packed_size = packed_encoded_array_size(values, origin.size(), v_width);
            encode_array(s_packed, arr, packed_size, flags, v_width, origin.size());
            copy_into_encoded_array(s_packed, origin, arr);
        }
        else {
            const auto flex_size = flex_encoded_array_size(values, indices, v_width, ndx_width);
            encode_array(s_flex, arr, flex_size, flags, v_width, ndx_width, values.size(), indices.size());
            copy_into_encoded_array(s_flex, arr, values, indices);
        }
        return true;
    }
    return false;
}

bool ArrayEncode::encode(const Array& origin, Array& arr) const
{
    // return false;
    // return always_encode(origin, arr, false); // true packed, false flex

    std::vector<int64_t> values;
    std::vector<size_t> indices;
    encode_values(origin, values, indices);
    if (!values.empty()) {
        size_t v_width, ndx_width;
        const auto uncompressed_size = origin.get_byte_size();
        const auto packed_size = packed_encoded_array_size(values, origin.size(), v_width);
        const auto flex_size = flex_encoded_array_size(values, indices, v_width, ndx_width);

        if (flex_size < packed_size && flex_size < uncompressed_size) {
            const uint8_t flags = NodeHeader::get_flags(origin.get_header());
            encode_array(s_flex, arr, flex_size, flags, v_width, ndx_width, values.size(), indices.size());
            copy_into_encoded_array(s_flex, arr, values, indices);
            return true;
        }
        else if (packed_size < uncompressed_size) {
            const uint8_t flags = NodeHeader::get_flags(origin.get_header());
            encode_array(s_packed, arr, packed_size, flags, v_width, origin.size());
            copy_into_encoded_array(s_packed, origin, arr);
            return true;
        }
    }
    return false;
}

bool ArrayEncode::decode(Array& arr) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    auto values_fetcher = [&arr, this]() {
        return is_packed() ? fetch_values(s_packed, arr) : fetch_values(s_flex, arr);
    };
    const auto& values = values_fetcher();
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
    size_t ndx = 0;
    // this is copying the bits straight, without doing any COW.
    // Restoring the array is basically COW.
    for (const auto& v : values)
        set(arr.m_data, width, ndx++, v);

    // very important: since the ref of the current array has changed, the parent must be informed.
    // Otherwise we will lose the link between parent array and child array.
    arr.update_parent();
    REALM_ASSERT_DEBUG(width == arr.get_width());
    REALM_ASSERT_DEBUG(arr.size() == values.size());

    // free memory no longer used. Very important to avoid to leak memory. Either in the slab or in the C++  heap.
    allocator.free_(old_ref, old_h);
    return true;
}

void ArrayEncode::init(const char* h)
{
    using Encoding = NodeHeader::Encoding;
    m_kind = NodeHeader::get_kind(h);
    m_encoding = NodeHeader::get_encoding(h);
    if (m_encoding == Encoding::Packed) {
        m_v_width = NodeHeader::get_element_size<Encoding::Packed>(h);
        m_v_size = NodeHeader::get_num_elements<Encoding::Packed>(h);
        m_v_mask = 1UL << (m_v_width - 1);
    }
    else if (m_encoding == Encoding::Flex) {
        m_v_width = NodeHeader::get_elementA_size<Encoding::Flex>(h);
        m_v_size = NodeHeader::get_arrayA_num_elements<Encoding::Flex>(h);
        m_ndx_width = NodeHeader::get_elementB_size<Encoding::Flex>(h);
        m_ndx_size = NodeHeader::get_arrayB_num_elements<Encoding::Flex>(h);
        m_v_mask = 1UL << (m_v_width - 1);
    }
}

int64_t ArrayEncode::get(const Array& arr, size_t ndx) const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(arr.is_attached());
    REALM_ASSERT_DEBUG(m_kind == 'B');
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Flex || m_encoding == Encoding::Packed);
    return is_packed() ? s_packed.get(arr, ndx) : s_flex.get(arr, ndx);
}

int64_t ArrayEncode::get(const char* data, size_t ndx) const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_kind == 'B');
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Flex || m_encoding == Encoding::Packed);
    return m_encoding == Encoding::Packed
               ? s_packed.get(data, ndx, m_v_width, m_v_size, m_v_mask)
               : s_flex.get(data, ndx, m_v_width, m_v_size, m_ndx_width, m_ndx_size, m_v_mask);
}

void ArrayEncode::get_chunk(const Array& arr, size_t ndx, int64_t res[8]) const
{
    REALM_ASSERT_DEBUG(arr.is_attached());
    return is_packed() ? s_packed.get_chunk(arr, ndx, res) : s_flex.get_chunk(arr, ndx, res);
}

void ArrayEncode::set_direct(const Array& arr, size_t ndx, int64_t value) const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    is_packed() ? s_packed.set_direct(arr, ndx, value) : s_flex.set_direct(arr, ndx, value);
}

template <typename Cond>
size_t ArrayEncode::find_first(const Array& arr, int64_t value, size_t start, size_t end) const
{
    QueryStateFindFirst state;
    find_all<Cond>(arr, value, start, end, 0, &state);
    return state.m_state;
}

inline bool find_all_match(size_t start, size_t end, size_t baseindex, QueryStateBase* state)
{
    REALM_ASSERT_DEBUG(state->match_count() < state->limit());
    const auto process = state->limit() - state->match_count();
    const auto end2 = end - start > process ? start + process : end;
    for (; start < end2; start++)
        if (!state->match(start + baseindex))
            return false;
    return true;
}

template <typename Cond>
inline bool do_find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                        QueryStateBase* state)
{
    bool (*cmp)(int64_t, int64_t) = nullptr;
    if constexpr (std::is_same_v<Cond, Equal>)
        cmp = [](int64_t v, int64_t value) {
            return v == value;
        };
    else if constexpr (std::is_same_v<Cond, NotEqual>)
        cmp = [](int64_t v, int64_t value) {
            return v != value;
        };
    else if constexpr (std::is_same_v<Cond, Greater>)
        cmp = [](int64_t v, int64_t value) {
            return v > value;
        };
    else if constexpr (std::is_same_v<Cond, Less>)
        cmp = [](int64_t v, int64_t value) {
            return v < value;
        };
    REALM_ASSERT_DEBUG(cmp != nullptr);

    // fastest so far but ~6 times slower than master for not randomized inputs (vals within 1 ... 1000)
    const auto& encoder = arr.get_encoder();
    for (; start < end; start++) {
        const auto v = encoder.get(arr, start);
        if (cmp(v, value) && !state->match(start + baseindex))
            return false;
    }
    return true;
}

template <typename Cond>
bool ArrayEncode::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                           QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    REALM_ASSERT_DEBUG(start <= arr.m_size && (end <= arr.m_size || end == size_t(-1)) && start <= end);

    Cond c;

    if (end == npos)
        end = arr.m_size;

    if (!(arr.m_size > start && start < end))
        return true;

    const auto lbound = arr.m_lbound;
    const auto ubound = arr.m_ubound;

    if (!c.can_match(value, lbound, ubound))
        return true;

    if (c.will_match(value, lbound, ubound))
        return find_all_match(start, end, baseindex, state);

    return do_find_all<Cond>(arr, value, start, end, baseindex, state);
}

int64_t ArrayEncode::sum(const Array& arr, size_t start, size_t end) const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    REALM_ASSERT_DEBUG(arr.m_size >= start && arr.m_size <= end);

    int64_t total_sum = 0;
    for (size_t i = start; i < end; ++i) {
        const auto v = is_packed() ? s_packed.get(arr, i) : s_flex.get(arr, i);
        total_sum += v;
    }
    return total_sum;
}

void ArrayEncode::set(char* data, size_t w, size_t ndx, int64_t v) const
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

bool inline ArrayEncode::is_packed() const
{
    REALM_ASSERT_DEBUG(m_kind == 'B');
    return m_encoding == NodeHeader::Encoding::Packed;
}

bool inline ArrayEncode::is_flex() const
{
    REALM_ASSERT_DEBUG(m_kind == 'B');
    return m_encoding == NodeHeader::Encoding::Flex;
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

void ArrayEncode::encode_values(const Array& arr, std::vector<int64_t>& values, std::vector<size_t>& indices) const
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
