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
namespace parser {

std::size_t TableAndColHash::operator()(const std::pair<ConstTableRef, std::string>& p) const
{
    // in practice, table names are unique between tables and column names are unique within a table
    std::string combined = std::string(p.first->get_name()) + p.second;
    return std::hash<std::string>{}(combined);
}


KeyPathMapping::KeyPathMapping()
    : m_allow_backlinks(true)
    , m_mapping()
{
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
        if (index + 1 == keypath.size()) {
            // we do support @links.@count and @links.@size so if @links is the end, that's what we are doing
            // any other operation on @links would have thrown a predicate error from the parser level
            index = index + 1;
            KeyPathElement element;
            element.table = table;
            element.col_key = ColKey(); // unused
            element.col_type = type_LinkList;
            element.is_backlink = false;
            element.is_list_of_primitives = false;
            element.is_primitive_element_length_op = false;
            return element;
        }
        realm_precondition(index + 2 < keypath.size(), "'@links' must be proceeded by type name and a property name");
        std::string origin_table_name = m_backlink_class_prefix + keypath[index + 1];
        Table::BacklinkOrigin info = table->find_backlink_origin(origin_table_name, keypath[index + 2]);
        realm_precondition(bool(info), util::format("No property '%1' found in type '%2' which links to type '%3'",
                                                    keypath[index + 2], get_printable_table_name(keypath[index + 1]),
                                                    get_printable_table_name(*table)));

        if (!m_allow_backlinks) {
            throw BacklinksRestrictedError(
                util::format("Querying over backlinks is disabled but backlinks were found in the inverse "
                             "relationship of property '%1' on type '%2'",
                             keypath[index + 2], get_printable_table_name(keypath[index + 1])));
        }

        index = index + 3;
        KeyPathElement element;
        element.table = info->first;
        element.col_key = info->second;
        element.col_type = type_LinkList; // backlinks should be operated on as a list
        element.is_backlink = true;
        element.is_list_of_primitives = false;
        element.is_primitive_element_length_op = false;
        return element;
    }

    // Process a single property
    ColKey col_key = table->get_column_key(keypath[index]);
    realm_precondition(col_key != ColKey(), util::format("No property '%1' on object of type '%2'", keypath[index],
                                                         get_printable_table_name(*table)));

    DataType cur_col_type = table->get_column_type(col_key);

    bool is_primitive_list =
        table->get_column_attr(col_key).test(ColumnAttr::col_attr_List) && cur_col_type != type_LinkList;
    bool is_length_op = false;
    if (is_primitive_list) {
        if (index + 2 == keypath.size() && is_length_suffix(keypath[index + 1])) {
            realm_precondition(cur_col_type == type_String || cur_col_type == type_Binary,
                               util::format("The '.length' operation only applies to string or binary "
                                            "elements within a list of primitives, but this is a list of type '%1'.",
                                            data_type_to_str(cur_col_type)));
            is_length_op = true;
            ++index; // consume .length too
        }
    }

    index++;
    KeyPathElement element;
    element.table = table;
    element.col_key = col_key;
    element.col_type = cur_col_type;
    element.is_backlink = false;
    element.is_list_of_primitives = is_primitive_list;
    element.is_primitive_element_length_op = is_length_op;
    return element;
}

void KeyPathMapping::set_allow_backlinks(bool allow)
{
    m_allow_backlinks = allow;
}

void KeyPathMapping::set_backlink_class_prefix(std::string prefix)
{
    m_backlink_class_prefix = prefix;
}

LinkChain KeyPathMapping::link_chain_getter(ConstTableRef table, const std::vector<KeyPathElement>& links,
                                            ExpressionComparisonType type)
{
    LinkChain lc(table, type);
    if (links.empty()) {
        return lc;
    }
    // mutates m_link_chain on table
    for (size_t i = 0; i < links.size() - 1; i++) {
        if (links[i].is_backlink) {
            lc.backlink(*links[i].table, links[i].col_key);
        }
        else {
            lc.link(links[i].col_key);
        }
    }
    return lc;
}

std::vector<KeyPathElement> generate_link_chain_from_string(Query& q, const std::string& key_path_string,
                                                            KeyPathMapping& mapping)
{
    ConstTableRef cur_table = q.get_table();
    KeyPath key_path = key_path_from_string(key_path_string);
    size_t index = 0;
    std::vector<KeyPathElement> link_chain;
    while (index < key_path.size()) {
        KeyPathElement element = mapping.process_next_path(cur_table, key_path, index);
        if (index != key_path.size()) {
            realm_precondition(element.col_type == type_Link || element.col_type == type_LinkList,
                               util::format("Property '%1' is not a link in object of type '%2'",
                                            element.table->get_column_name(element.col_key),
                                            get_printable_table_name(*element.table)));
            if (element.table == cur_table) {
                if (element.col_key == ColKey()) {
                    cur_table = element.table;
                }
                else {
                    cur_table = element.table->get_link_target(element.col_key); // advance through forward link
                }
            }
            else {
                cur_table = element.table; // advance through backlink
            }
        }
        link_chain.push_back(element);
    }
    return link_chain;
}

} // namespace parser
} // namespace realm
