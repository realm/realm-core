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

#include <realm/node_header.hpp>

#include <cstdint>
#include <cstddef>
#include <vector>

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
    inline size_t width_mask() const;
    inline NodeHeader::Encoding get_encoding() const
    {
        return m_encoding;
    }

    // get/set
    int64_t get(const Array&, size_t) const;
    int64_t get(const char* data, size_t) const;
    void get_chunk(const Array&, size_t ndx, int64_t res[8]) const;
    void set_direct(const Array&, size_t, int64_t) const;

    // query interface
    template <typename Cond>
    size_t find_first(const Array&, int64_t, size_t, size_t) const;
    template <typename Cond>
    bool find_all(const Array&, int64_t, size_t, size_t, size_t, QueryStateBase*) const;
    // sum
    int64_t sum(const Array&, size_t start, size_t end) const;

    inline bool is_packed() const;
    inline bool is_flex() const;

private:
    void set(char* data, size_t w, size_t ndx, int64_t v) const;
    size_t flex_encoded_array_size(const std::vector<int64_t>&, const std::vector<size_t>&, size_t&, size_t&) const;
    size_t packed_encoded_array_size(std::vector<int64_t>&, size_t, size_t&) const;

    void encode_values(const Array&, std::vector<int64_t>&, std::vector<size_t>&) const;
    bool always_encode(const Array&, Array&, bool) const; // for testing
private:
    using Encoding = NodeHeader::Encoding;
    Encoding m_encoding{NodeHeader::Encoding::WTypBits}; // this is not ok .... probably
    size_t m_v_width = 0, m_v_size = 0, m_ndx_width = 0, m_ndx_size = 0;
    size_t m_v_mask = 0;

    friend class ArrayPacked;
    friend class ArrayFlex;
};


inline size_t ArrayEncode::size() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_kind == 'B');
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_encoding == Encoding::Packed ? m_v_size : m_ndx_size;
}

inline size_t ArrayEncode::width() const
{
    using Encoding = NodeHeader::Encoding;
    REALM_ASSERT_DEBUG(m_kind == 'B');
    REALM_ASSERT_DEBUG(m_encoding == Encoding::Packed || m_encoding == Encoding::Flex);
    return m_v_width;
}

} // namespace realm
#endif // REALM_ARRAY_ENCODE_HPP
