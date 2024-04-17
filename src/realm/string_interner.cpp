/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
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

#include <realm/string_interner.hpp>
#include <realm/string_data.hpp>

namespace realm {


StringInterner::StringInterner() {}

StringInterner::~StringInterner() {}

StringID StringInterner::intern(StringData sd)
{
    // special case for null string
    if (sd.data() == nullptr) {
        null_seen = true;
        return 0;
    }
    bool learn = true;
    auto c_str = compressor.compress(sd, learn);
    auto it = m_compressed_string_map.find(c_str);
    if (it != m_compressed_string_map.end()) {
        REALM_ASSERT_DEBUG(sd == m_strings[it->second - 1]);
        return it->second;
    }
    m_strings.push_back(sd);
    m_compressed_strings.push_back(c_str);
    auto id = m_compressed_strings.size();
    m_compressed_string_map[c_str] = id;
    return id;
}

std::optional<StringID> StringInterner::lookup(StringData sd)
{
    if (sd.data() == nullptr) {
        if (null_seen)
            return 0;
        return {};
    }
    bool dont_learn = false;
    auto c_str = compressor.compress(sd, dont_learn);
    auto it = m_compressed_string_map.find(c_str);
    if (it != m_compressed_string_map.end()) {
        REALM_ASSERT_DEBUG(sd == m_strings[it->second - 1]);
        return it->second;
    }
    return {};
}

int StringInterner::compare(StringID A, StringID B)
{
    REALM_ASSERT_DEBUG(A < m_compressed_strings.size());
    REALM_ASSERT_DEBUG(B < m_compressed_strings.size());
    if (A == B && A == 0)
        return 0;
    if (A == 0)
        return -1;
    if (B == 0)
        return 1;
    return compressor.compare(m_compressed_strings[A], m_compressed_strings[B]);
}

int StringInterner::compare(StringData s, StringID A)
{
    REALM_ASSERT_DEBUG(A < m_compressed_strings.size());
    if (s.data() == nullptr && A == 0)
        return 0;
    if (s.data() == nullptr)
        return 1;
    if (A == 0)
        return -1;
    return compressor.compare(s, m_compressed_strings[A]);
}

// We're handing out StringData which has no ownership, but must be able to
// access the underlying decompressed string. We keep only a limited number of these
// decompressed strings available. A value of 8 allows Core Unit tests to pass.
// A value of 4 does not. This approach is called empirical software construction :-D
constexpr size_t per_thread_decompressed = 8;

thread_local std::vector<std::string> keep_alive(per_thread_decompressed);
thread_local size_t string_index = 0;

StringData StringInterner::get(StringID id)
{
    if (id == 0)
        return StringData{nullptr};
    REALM_ASSERT_DEBUG(id <= m_compressed_strings.size());
    std::string str = compressor.decompress(m_compressed_strings[id - 1]);
    REALM_ASSERT_DEBUG(str == m_strings[id - 1]);
    // decompressed string must be kept in memory for a while....
    if (keep_alive.size() < per_thread_decompressed) {
        keep_alive.push_back(str);
        return keep_alive.back();
    }
    keep_alive[string_index] = str;
    auto return_index = string_index;
    // bump index with wrap-around
    string_index++;
    if (string_index == keep_alive.size())
        string_index = 0;
    return keep_alive[return_index];
}

} // namespace realm
