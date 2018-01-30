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

#ifndef REALM_KEYPATH_MAPPING_HPP
#define REALM_KEYPATH_MAPPING_HPP

#include <realm/table.hpp>

#include <map>
#include <string>

namespace realm {
namespace parser {

// This class holds state which allows aliasing variable names in key paths used in queries.
// It is currently used to allow variable naming in subqueries such as 'SUBQUERY(list, $obj, $obj.intCol = 5).@count'
// It could also be concievably used to allow querying named backlinks if bindings provide the mappings themselves.
class KeyPathMapping
{
public:
    KeyPathMapping();
    void add_mapping(TableRef table, std::string name, std::string alias);
    void remove_mapping(TableRef table, std::string name);
    std::string process_keypath(TableRef table, std::string path);
protected:
    std::map<std::pair<TableRef, std::string>, std::string> m_mapping;
};

} // namespace parser
} // namespace realm

#endif // REALM_KEYPATH_MAPPING_HPP
