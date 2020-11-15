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

std::size_t TableAndColHash::operator()(const std::pair<ConstTableRef, std::string>& p) const
{
    // in practice, table names are unique between tables and column names are unique within a table
    std::string combined = std::string(p.first->get_name()) + p.second;
    return std::hash<std::string>{}(combined);
}


bool KeyPathMapping::add_mapping(ConstTableRef table, std::string name, std::string alias)
{
    if (m_mapping.find({table, name}) == m_mapping.end()) {
        m_mapping[{table, name}] = alias;
        return true;
    }
    return false;
}

void KeyPathMapping::remove_mapping(ConstTableRef table, std::string name)
{
    auto it = m_mapping.find({table, name});
    REALM_ASSERT_DEBUG(it != m_mapping.end());
    m_mapping.erase(it);
}

bool KeyPathMapping::has_mapping(ConstTableRef table, std::string name)
{
    return m_mapping.find({table, name}) != m_mapping.end();
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


void KeyPathMapping::set_allow_backlinks(bool allow)
{
    m_allow_backlinks = allow;
}

void KeyPathMapping::set_backlink_class_prefix(std::string prefix)
{
    m_backlink_class_prefix = prefix;
}

} // namespace query_parser
} // namespace realm
