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

#ifndef REALM_INTEGER_COMPRESSOR_HPP
#define REALM_INTEGER_COMPRESSOR_HPP

#include <cstdint>
#include <cstddef>
#include <vector>
#include <realm/query_conditions.hpp>
#include <realm/array_direct.hpp>

namespace realm {

class Array;
class QueryStateBase;
class IntegerCompressor {
public:
    // commit => encode, COW/insert => decode
    bool compress(const Array&, Array&) const;
    bool decompress(Array&) const;

    bool init(const char*);

    // init from mem B
    inline const uint64_t* data() const;
    inline size_t size() const;
    inline NodeHeader::Encoding get_encoding() const;
    inline uint8_t v_width() const;
    inline uint8_t ndx_width() const;
    inline size_t v_size() const;
    inline size_t ndx_size() const;

    inline uint64_t v_mask() const;
    inline uint64_t ndx_mask() const;
    inline uint64_t msb() const;
    inline uint64_t ndx_msb() const;
    inline uint64_t bitmask_v() const;
    inline uint64_t bitmask_ndx() const;

    // get/set
    inline int64_t get(size_t) const;
    inline std::vector<int64_t> get_all(size_t b, size_t e) const;
    inline void get_chunk(size_t ndx, int64_t res[8]) const;
    inline void set_direct(size_t, int64_t) const;

    // query interface
    template <typename Cond>
    inline bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

private:
    // Same idea we have for Array, we want to avoid to constantly checking whether we
    // have compressed in packed or flex, and jump straight to the right implementation,
    // avoiding branch mis-predictions, which made some queries run ~6/7x slower.
    using Getter = int64_t (IntegerCompressor::*)(size_t) const;
    using GetterAll = std::vector<int64_t> (IntegerCompressor::*)(size_t, size_t) const;
    using ChunkGetterChunk = void (IntegerCompressor::*)(size_t, int64_t[8]) const;
    using DirectSetter = void (IntegerCompressor::*)(size_t, int64_t) const;
    using Finder = bool (IntegerCompressor::*)(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    using FinderTable = std::array<Finder, cond_VTABLE_FINDER_COUNT>;

    struct VTable {
        Getter m_getter{nullptr};
        GetterAll m_getter_all{nullptr};
        ChunkGetterChunk m_chunk_getter{nullptr};
        DirectSetter m_direct_setter{nullptr};
        FinderTable m_finder;
    };
    struct VTableForPacked;
    struct VTableForFlex;
    const VTable* m_vtable = nullptr;

    // getting and setting interface specifically for encoding formats
    inline void init_packed(const char*);
    inline void init_flex(const char*);
    int64_t get_packed(size_t) const;
    int64_t get_flex(size_t) const;
    
    std::vector<int64_t> get_all_packed(size_t, size_t) const;
    std::vector<int64_t> get_all_flex(size_t, size_t) const;
    
    void get_chunk_packed(size_t, int64_t[8]) const;
    void get_chunk_flex(size_t, int64_t[8]) const;
    void set_direct_packed(size_t, int64_t) const;
    void set_direct_flex(size_t, int64_t) const;
    // query interface
    template <typename Cond>
    bool find_all_packed(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    template <typename Cond>
    bool find_all_flex(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;

    // internal impl
    size_t flex_disk_size(const std::vector<int64_t>&, const std::vector<size_t>&, size_t&, size_t&) const;
    size_t packed_disk_size(std::vector<int64_t>&, size_t, size_t&) const;
    void compress_values(const Array&, std::vector<int64_t>&, std::vector<size_t>&) const;
    inline bool is_packed() const;
    inline bool is_flex() const;

    // for testing
    bool always_compress(const Array&, Array&, Node::Encoding) const;

private:
    using Encoding = NodeHeader::Encoding;
    Encoding m_encoding{NodeHeader::Encoding::WTypBits};
    uint64_t* m_data;
    uint8_t m_v_width = 0, m_ndx_width = 0;
    size_t m_v_size = 0, m_ndx_size = 0;
};

inline void IntegerCompressor::init_packed(const char* h)
{
    m_data = (uint64_t*)NodeHeader::get_data_from_header(h);
    m_v_width = NodeHeader::get_element_size(h, Encoding::Packed);
    m_v_size = NodeHeader::get_num_elements(h, Encoding::Packed);
}

inline void IntegerCompressor::init_flex(const char* h)
{
    m_data = (uint64_t*)NodeHeader::get_data_from_header(h);
    m_v_width = NodeHeader::get_elementA_size(h);
    m_v_size = NodeHeader::get_arrayA_num_elements(h);
    m_ndx_width = NodeHeader::get_elementB_size(h);
    m_ndx_size = NodeHeader::get_arrayB_num_elements(h);
}

inline const uint64_t* IntegerCompressor::data() const
{
    return m_data;
}

inline bool IntegerCompressor::is_packed() const
{
    return m_encoding == NodeHeader::Encoding::Packed;
}

inline bool IntegerCompressor::is_flex() const
{
    return m_encoding == NodeHeader::Encoding::Flex;
}

inline size_t IntegerCompressor::size() const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    return m_encoding == NodeHeader::Encoding::Packed ? v_size() : ndx_size();
}

inline size_t IntegerCompressor::v_size() const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    return m_v_size;
}

inline size_t IntegerCompressor::ndx_size() const
{
    REALM_ASSERT_DEBUG(is_flex());
    return m_ndx_size;
}

inline uint8_t IntegerCompressor::v_width() const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    return m_v_width;
}

inline uint8_t IntegerCompressor::ndx_width() const
{
    REALM_ASSERT_DEBUG(is_flex());
    return m_ndx_width;
}

inline NodeHeader::Encoding IntegerCompressor::get_encoding() const
{
    return m_encoding;
}

inline uint64_t IntegerCompressor::v_mask() const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    return 1ULL << (m_v_width - 1);
}

inline uint64_t IntegerCompressor::ndx_mask() const
{
    REALM_ASSERT_DEBUG(is_flex());
    return 1ULL << (m_ndx_width - 1);
}

inline uint64_t IntegerCompressor::msb() const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    return populate(m_v_width, v_mask());
}

inline uint64_t IntegerCompressor::ndx_msb() const
{
    REALM_ASSERT_DEBUG(is_flex());
    return populate(m_ndx_width, ndx_mask());
}

inline uint64_t IntegerCompressor::bitmask_v() const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    return (1ULL << m_v_width) - 1;
}

inline uint64_t IntegerCompressor::bitmask_ndx() const
{
    REALM_ASSERT_DEBUG(is_flex());
    return (1ULL << m_ndx_width) - 1;
}

inline int64_t IntegerCompressor::get(size_t ndx) const
{
    REALM_ASSERT_DEBUG(ndx < size());
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    REALM_ASSERT_DEBUG(m_vtable->m_getter);
    return (this->*(m_vtable->m_getter))(ndx);
}

inline std::vector<int64_t> IntegerCompressor::get_all(size_t b, size_t e) const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    REALM_ASSERT_DEBUG(m_vtable->m_getter_all);
    return (this->*(m_vtable->m_getter_all))(b, e);
}


inline void IntegerCompressor::get_chunk(size_t ndx, int64_t res[8]) const
{
    REALM_ASSERT_DEBUG(ndx < size());
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    REALM_ASSERT_DEBUG(m_vtable->m_chunk_getter);
    (this->*(m_vtable->m_chunk_getter))(ndx, res);
}

inline void IntegerCompressor::set_direct(size_t ndx, int64_t value) const
{
    REALM_ASSERT_DEBUG(ndx < size());
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    REALM_ASSERT_DEBUG(m_vtable->m_direct_setter);
    (this->*(m_vtable->m_direct_setter))(ndx, value);
}

template <typename Cond>
inline bool IntegerCompressor::find_all(const Array& arr, int64_t value, size_t start, size_t end, size_t baseindex,
                                        QueryStateBase* state) const
{
    REALM_ASSERT_DEBUG(is_packed() || is_flex());
    if constexpr (std::is_same_v<Cond, Equal>) {
        return (this->*(m_vtable->m_finder[cond_Equal]))(arr, value, start, end, baseindex, state);
    }
    if constexpr (std::is_same_v<Cond, NotEqual>) {
        return (this->*(m_vtable->m_finder[cond_NotEqual]))(arr, value, start, end, baseindex, state);
    }
    if constexpr (std::is_same_v<Cond, Less>) {
        return (this->*(m_vtable->m_finder[cond_Less]))(arr, value, start, end, baseindex, state);
    }
    if constexpr (std::is_same_v<Cond, Greater>) {
        return (this->*(m_vtable->m_finder[cond_Greater]))(arr, value, start, end, baseindex, state);
    }
    REALM_UNREACHABLE();
}

} // namespace realm
#endif // REALM_INTEGER_COMPRESSOR_HPP
