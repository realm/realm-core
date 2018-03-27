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

PropertyExpression::PropertyExpression(Query &q, const std::string &key_path_string)
: query(q)
{
    KeyPath key_path = key_path_from_string(key_path_string);
    TableRef cur_table = query.get_table();
    for (size_t index = 0; index < key_path.size(); index++) {
        ColKey cur_col_key = cur_table->get_column_key(key_path[index]);

        StringData object_name = get_printable_table_name(*cur_table);

        precondition(cur_col_key,
                     util::format("No property '%1' on object of type '%2'", key_path[index], object_name));

        DataType cur_col_type = cur_table->get_column_type(cur_col_key);
        if (index != key_path.size() - 1) {
            precondition(cur_col_type == type_Link || cur_col_type == type_LinkList,
                         util::format("Property '%1' is not a link in object of type '%2'", key_path[index], object_name));
            indexes.push_back(cur_col_key);
            cur_table = cur_table->get_link_target(cur_col_key);
        }
        else {
            col_key = cur_col_key;
            col_type = cur_col_type;
        }
    }
}

Table* PropertyExpression::table_getter() const
{
    auto& tbl = query.get_table();
    for (ColKey col : indexes) {
        tbl->link(col); // mutates m_link_chain on table
    }
    return tbl;
}

} // namespace parser
} // namespace realm
