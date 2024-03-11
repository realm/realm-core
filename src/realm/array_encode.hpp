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

#ifndef REALM_ARRAY_ENCODE_HPP
#define REALM_ARRAY_ENCODE_HPP

#include <cstdint>
#include <cstddef>
#include <vector>
#include <realm/query_conditions.hpp>


namespace realm {

class Array;
class QueryStateBase;
class ArrayEncode {
public:
    // commit => encode, COW/insert => decode
    bool encode(const Array&, Array&) const;
    bool decode(Array&) const;

    void init(const char* h);

    // init from mem B
    inline size_t size() const;
    inline size_t width() const;
    inline size_t ndx_size() const;
    inline size_t ndx_width() const;
    inline NodeHeader::Encoding get_encoding() const;
    inline size_t v_size() const;
    inline uint64_t width_mask() const;
    inline uint64_t ndx_mask() const;
    inline uint64_t msb() const;
    inline uint64_t ndx_msb() const;
    inline size_t field_count() const;
    inline size_t ndx_field_count() const;
    inline size_t bit_count_per_iteration() const;
    inline size_t ndx_bit_count_per_iteration() const;

    // get/set
    int64_t get(const Array&, size_t) const;
    int64_t get(const char* data, size_t) const;
    void get_chunk(const Array&, size_t ndx, int64_t res[8]) const;
    void set_direct(const Array&, size_t, int64_t) const;

    // query interface
    template <typename Cond>
    size_t find_first(const Array&, int64_t, size_t, size_t) const;
    template <typename Cond>
    inline bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    int64_t sum(const Array&, size_t start, size_t end) const;

private:
    // Branch misprediction could kill performance, in order to avoid to dispatch computation
    // towards the right encoder via some if/else (which makes even 400% slower queries) we set up
    // a bunch of function pointers to the proper implementation code in order to avoid to repeat
    // the same if/else check over and over again.

    // vtable impl
    using Getter = int64_t (ArrayEncode::*)(const Array&, size_t) const;
    using GetterFromData = int64_t (ArrayEncode::*)(const char*, size_t) const;
    using GetterChunk = void (ArrayEncode::*)(const Array&, size_t, int64_t[8]) const;
    using SetterDirect = void (ArrayEncode::*)(const Array&, size_t, int64_t) const;
    using Finder = bool (ArrayEncode::*)(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    using Accumulator = int64_t (ArrayEncode::*)(const Array&, size_t, size_t) const;

    Getter m_getter;
    GetterFromData m_getter_from_data;
    GetterChunk m_getter_chunk;
    SetterDirect m_setter_direct;
    // we only call find using ==, !=, <, > operators
    Finder m_finder[cond_VTABLE_FINDER_COUNT];
    Accumulator m_accumulator;

    // getting and setting interface
    int64_t get_packed(const Array&, size_t) const;
    int64_t get_flex(const Array&, size_t) const;
    int64_t get_from_data_packed(const char*, size_t) const;
    int64_t get_from_data_flex(const char*, size_t) const;
    void get_chunk_packed(const Array&, size_t, int64_t[8]) const;
    void get_chunk_flex(const Array&, size_t, int64_t[8]) const;
    void set_direct_packed(const Array&, size_t, int64_t) const;
    void set_direct_flex(const Array&, size_t, int64_t) const;
    // query interface
    template <typename Cond>
    bool find_all_packed(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    template <typename Cond>
    bool find_all_flex(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    int64_t sum_packed(const Array&, size_t, size_t) const;
    int64_t sum_flex(const Array&, size_t, size_t) const;

    // internal impl
    void init_widths(const char* h);
    void init_vtable();
    void init_masks();
    void set(char* data, size_t w, size_t ndx, int64_t v) const;
    size_t flex_encoded_array_size(const std::vector<int64_t>&, const std::vector<size_t>&, size_t&, size_t&) const;
    size_t packed_encoded_array_size(std::vector<int64_t>&, size_t, size_t&) const;
    void encode_values(const Array&, std::vector<int64_t>&, std::vector<size_t>&) const;
    inline bool is_packed() const;
    inline bool is_flex() const;
    bool always_encode(const Array&, Array&, bool) const; // for testing

private:
    using Encoding = NodeHeader::Encoding;
    Encoding m_encoding{NodeHeader::Encoding::WTypBits};
    size_t m_v_width = 0, m_v_size = 0, m_ndx_width = 0, m_ndx_size = 0;
    uint64_t m_v_mask = 0, m_ndx_mask = 0;

    // these can all be computed during compression.
    uint64_t m_MSBs = 0, m_ndx_MSBs = 0;
    size_t m_field_count = 0, m_ndx_field_count = 0;
    size_t m_bit_count_pr_iteration = 0, m_ndx_bit_count_pr_iteration = 0;

    friend class ArrayPacked;
    friend class ArrayFlex;
};

inline bool ArrayEncode::is_packed() const
{
    return m_encoding == NodeHeader::Encoding::Packed;
}

inline bool ArrayEncode::is_flex() const
{
    return m_encoding == NodeHeader::Encoding::Flex;
}

inline size_t ArrayEncode::size() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_encoding == Encoding::Packed ? v_size() : ndx_size();
}

inline size_t ArrayEncode::v_size() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_v_size;
}

inline size_t ArrayEncode::ndx_size() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_ndx_size;
}

inline size_t ArrayEncode::width() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_v_width;
}

inline size_t ArrayEncode::ndx_width() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_ndx_width;
}

inline NodeHeader::Encoding ArrayEncode::get_encoding() const
{
    return m_encoding;
}

inline uint64_t ArrayEncode::width_mask() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_v_mask;
}

inline uint64_t ArrayEncode::ndx_mask() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_ndx_mask;
}

inline uint64_t ArrayEncode::msb() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_MSBs;
}

inline uint64_t ArrayEncode::ndx_msb() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_ndx_MSBs;
}

inline size_t ArrayEncode::field_count() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_field_count;
}

inline size_t ArrayEncode::ndx_field_count() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_ndx_field_count;
}

inline size_t ArrayEncode::bit_count_per_iteration() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_bit_count_pr_iteration;
}

inline size_t ArrayEncode::ndx_bit_count_per_iteration() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_ndx_bit_count_pr_iteration;
}

template <typename Cond>
inline bool ArrayEncode::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                                  QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    REALM_ASSERT_DEBUG(m_finder);
    if constexpr (std::is_same_v<Cond, Equal>) {
        return (this->*m_finder[cond_Equal])(arr, value, start, end, baseindex, state);
    }
    if constexpr (std::is_same_v<Cond, NotEqual>) {
        return (this->*m_finder[cond_NotEqual])(arr, value, start, end, baseindex, state);
    }
    if constexpr (std::is_same_v<Cond, Less>) {
        return (this->*m_finder[cond_Less])(arr, value, start, end, baseindex, state);
    }
    if constexpr (std::is_same_v<Cond, Greater>) {
        return (this->*m_finder[cond_Greater])(arr, value, start, end, baseindex, state);
    }
    REALM_UNREACHABLE();
}

} // namespace realm
#endif // REALM_ARRAY_ENCODE_HPP
