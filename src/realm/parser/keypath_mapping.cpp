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

namespace realm {
namespace parser {

KeyPathMapping::KeyPathMapping()
{
}

void KeyPathMapping::add_mapping(ConstTableRef table, std::string name, std::string alias)
{
    m_mapping[{table, name}] = alias;
}

void KeyPathMapping::remove_mapping(ConstTableRef table, std::string name)
{
    auto it = m_mapping.find({table, name});
    REALM_ASSERT_DEBUG(it != m_mapping.end());
    m_mapping.erase(it);
}

// This may be premature optimisation, but it'll be super fast and it doesn't
// bother dragging in anything locale specific for case insensitive comparisons.
bool is_backlinks_prefix(std::string s) {
    return s.size() == 6 && s[0] == '@'
        && (s[1] == 'l' || s[1] == 'L')
        && (s[2] == 'i' || s[2] == 'I')
        && (s[3] == 'n' || s[3] == 'N')
        && (s[4] == 'k' || s[4] == 'K')
        && (s[5] == 's' || s[5] == 'S');
}

KeyPathElement KeyPathMapping::process_next_path(ConstTableRef table, KeyPath& keypath, size_t& index)
{
    REALM_ASSERT_DEBUG(index < keypath.size());

    // Perform substitution if the alias is found in the mapping
    auto it = m_mapping.find({table, keypath[index]});
    if (it != m_mapping.end()) {
        // the alias needs to be mapped because it might be more than a single path
        // for example named backlinks must alias to @links.class_Name.property
        KeyPath mapped_path = key_path_from_string(it->second);
        keypath.erase(keypath.begin() + index);
        keypath.insert(keypath.begin() + index, mapped_path.begin(), mapped_path.end());
    }

    // Process backlinks which consumes 3 parts of the keypath
    if (is_backlinks_prefix(keypath[index])) {
        precondition(index + 2 < keypath.size(), "'@links' must be proceeded by type name and a property name");

        Table::BacklinkOrigin info = table->find_backlink_origin(keypath[index + 1], keypath[index + 2]);
        precondition(bool(info), util::format("No property '%1' found in type '%2' which links to type '%3'",
                    keypath[index + 2], get_printable_table_name(keypath[index + 1]), get_printable_table_name(*table)))
        index = index + 3;
        KeyPathElement element;
        element.table = info->first;
        element.col_ndx = info->second;
        element.col_type = type_LinkList; // backlinks should be operated on as a list
        element.is_backlink = true;
        return element;
    }

    // Process a single property
    size_t col_ndx = table->get_column_index(keypath[index]);
    precondition(col_ndx != realm::not_found,
                 util::format("No property '%1' on object of type '%2'", keypath[index], get_printable_table_name(*table)));

    DataType cur_col_type = table->get_column_type(col_ndx);

    index++;
    KeyPathElement element;
    element.table = table;
    element.col_ndx =  col_ndx;
    element.col_type = cur_col_type;
    element.is_backlink = false;
    return element;
}


Table* KeyPathMapping::table_getter(TableRef table, const std::vector<KeyPathElement>& links)
{
    if (links.empty()) {
        return table.get();
    }
    // mutates m_link_chain on table
    for (size_t i = 0; i < links.size() - 1; i++) {
        if (links[i].is_backlink) {
            table->backlink(*links[i].table, links[i].col_ndx);
        }
        else {
            table->link(links[i].col_ndx);
        }
    }
    return table.get();
}


} // namespace parser
} // namespace realm
