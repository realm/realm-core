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
    if (sd.data() == nullptr)
        return 0;
    std::string string(sd);
    auto it = m_string_map.find(string);
    if (it != m_string_map.end())
        return it->second;
    m_strings.push_back(sd);
    auto id = m_strings.size();
    m_string_map[string] = id;
    return id;
}

std::optional<StringID> StringInterner::lookup(StringData sd)
{
    if (sd.data() == nullptr)
        return 0;
    std::string string(sd);
    auto it = m_string_map.find(string);
    if (it != m_string_map.end())
        return it->second;
    return {};
}

int StringInterner::compare(StringID A, StringID B)
{
    return 0;
}

int StringInterner::compare(StringData, StringID A)
{
    return 0;
}

StringData StringInterner::get(StringID id)
{
    if (id == 0)
        return StringData{nullptr};
    REALM_ASSERT(id <= m_strings.size());
    std::string& str = m_strings[id - 1];
    return str;
}

} // namespace realm
