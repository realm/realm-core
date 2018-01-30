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
#include "parser_utils.hpp"


namespace realm {
namespace parser {

KeyPathMapping::KeyPathMapping()
{
}

void KeyPathMapping::add_mapping(TableRef table, std::string name, std::string alias)
{
    m_mapping[{table, name}] = alias;
}

std::string KeyPathMapping::process_keypath(TableRef table, std::string path)
{
    KeyPath keypath = key_path_from_string(path);
    if (keypath.size() == 0) {
        return path;
    }
    auto it = m_mapping.find({table, keypath[0]});
    if (it != m_mapping.end()) {
        keypath[0] = it->second;
    }
    return key_path_to_string(keypath);
}

void KeyPathMapping::remove_mapping(TableRef table, std::string name)
{
    auto it = m_mapping.find({table, name});
    REALM_ASSERT_DEBUG(it != m_mapping.end());
    m_mapping.erase(it);
}

} // namespace parser
} // namespace realm
