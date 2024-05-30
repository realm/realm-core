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
    void set_vtable(Array&);

    // init from mem B
    inline uint64_t* data() const;
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

    int64_t get(size_t) const;

private:
    // getting and setting interface specifically for encoding formats
    inline void init_packed(const char*);
    inline void init_flex(const char*);

    static int64_t get_packed(const Array& arr, size_t ndx);
    static int64_t get_flex(const Array& arr, size_t ndx);

    static std::vector<int64_t> get_all_packed(const Array& arr, size_t begin, size_t end);
    static std::vector<int64_t> get_all_flex(const Array& arr, size_t begin, size_t end);

    static void get_chunk_packed(const Array& arr, size_t ndx, int64_t res[8]);
    static void get_chunk_flex(const Array& arr, size_t ndx, int64_t res[8]);
    static void set_packed(Array& arr, size_t ndx, int64_t val);
    static void set_flex(Array& arr, size_t ndx, int64_t val);
    // query interface
    template <class Cond>
    static bool find_packed(const Array& arr, int64_t val, size_t begin, size_t end, size_t base_index,
                            QueryStateBase* st);
    template <class Cond>
    static bool find_flex(const Array& arr, int64_t val, size_t begin, size_t end, size_t base_index,
                          QueryStateBase* st);

    // internal impl
    void compress_values(const Array&, std::vector<int64_t>&, std::vector<unsigned>&) const;
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

inline uint64_t* IntegerCompressor::data() const
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
    return 0xFFFFFFFFFFFFFFFFULL >> (64 - m_v_width);
}

inline uint64_t IntegerCompressor::bitmask_ndx() const
{
    REALM_ASSERT_DEBUG(is_flex());
    return 0xFFFFFFFFFFFFFFFFULL >> (64 - m_ndx_width);
}

} // namespace realm
#endif // REALM_INTEGER_COMPRESSOR_HPP
