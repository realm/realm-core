////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include "keypath_mapping.hpp"

#include <functional>

namespace realm {
namespace query_parser {

using namespace realm::util;

std::size_t TableAndColHash::operator()(const std::pair<TableKey, std::string>& p) const
{
    auto h1 = std::hash<std::string>{}(p.second);
    return h1 ^ p.first.value;
}


bool KeyPathMapping::add_mapping(ConstTableRef table, std::string name, std::string alias)
{
    auto table_key = table->get_key();
    if (m_mapping.find({table_key, name}) == m_mapping.end()) {
        m_mapping[{table_key, name}] = alias;
        return true;
    }
    return false;
}

void KeyPathMapping::remove_mapping(ConstTableRef table, std::string name)
{
    auto table_key = table->get_key();
    auto it = m_mapping.find({table_key, name});
    REALM_ASSERT_DEBUG(it != m_mapping.end());
    m_mapping.erase(it);
}

bool KeyPathMapping::has_mapping(ConstTableRef table, const std::string& name) const
{
    return (m_mapping.size() > 0) && m_mapping.find({table->get_key(), name}) != m_mapping.end();
}

util::Optional<std::string> KeyPathMapping::get_mapping(TableKey table_key, const std::string& name) const
{
    util::Optional<std::string> ret;
    if (m_mapping.size() > 0) {
        auto it = m_mapping.find({table_key, name});
        if (it != m_mapping.end()) {
            ret = it->second;
        }
    }
    return ret;
}

// This may be premature optimisation, but it'll be super fast and it doesn't
// bother dragging in anything locale specific for case insensitive comparisons.
bool is_backlinks_prefix(const std::string& s)
{
    return s.size() == 6 && s[0] == '@' && (s[1] == 'l' || s[1] == 'L') && (s[2] == 'i' || s[2] == 'I') &&
           (s[3] == 'n' || s[3] == 'N') && (s[4] == 'k' || s[4] == 'K') && (s[5] == 's' || s[5] == 'S');
}

bool is_length_suffix(const std::string& s)
{
    return s.size() == 6 && (s[0] == 'l' || s[0] == 'L') && (s[1] == 'e' || s[1] == 'E') &&
           (s[2] == 'n' || s[2] == 'N') && (s[3] == 'g' || s[3] == 'G') && (s[4] == 't' || s[4] == 'T') &&
           (s[5] == 'h' || s[5] == 'H');
}


void KeyPathMapping::set_backlink_class_prefix(std::string prefix)
{
    m_backlink_class_prefix = prefix;
}

} // namespace query_parser
} // namespace realm
