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

#include "property_expression.hpp"
#include "parser_utils.hpp"

#include <realm/table.hpp>

#include <sstream>

namespace realm {
namespace parser {

PropertyExpression::PropertyExpression(Query &q, const std::string &key_path_string, parser::KeyPathMapping& mapping)
: query(q)
{
    ConstTableRef cur_table = query.get_table();
    KeyPath key_path = key_path_from_string(key_path_string);
    size_t index = 0;

    while (index < key_path.size()) {
        KeyPathElement element = mapping.process_next_path(cur_table, key_path, index);
        if (index != key_path.size()) {
            realm_precondition(element.col_type == type_Link || element.col_type == type_LinkList,
                         util::format("Property '%1' is not a link in object of type '%2'",
                                      element.table->get_column_name(element.col_ndx),
                                      get_printable_table_name(*element.table)));
            if (element.table == cur_table) {
                if (element.col_ndx == realm::npos) {
                    cur_table = element.table;
                } else {
                    cur_table = element.table->get_link_target(element.col_ndx); // advance through forward link
                }
            } else {
                cur_table = element.table; // advance through backlink
            }
        }
        link_chain.push_back(element);
    }
}

Table* PropertyExpression::table_getter() const
{
    auto& tbl = query.get_table();
    return KeyPathMapping::table_getter(tbl, link_chain);
}

} // namespace parser
} // namespace realm
