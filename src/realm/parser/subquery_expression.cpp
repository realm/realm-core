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

#include "subquery_expression.hpp"
#include "parser_utils.hpp"

#include <realm/table.hpp>

#include <sstream>

namespace realm {
namespace parser {

using namespace util;

SubqueryExpression::SubqueryExpression(Query& q, const std::string& key_path_string, const std::string& variable_name,
                                       parser::KeyPathMapping& mapping)
    : var_name(variable_name)
    , query(q)
{
    ConstTableRef cur_table = query.get_table();
    KeyPath key_path = key_path_from_string(key_path_string);
    size_t index = 0;

    while (index < key_path.size()) {
        KeyPathElement element = mapping.process_next_path(cur_table, key_path, index);
        if (index != key_path.size()) {
            ColumnType col_type = element.col_key.get_type();
            realm_precondition(col_type == col_type_Link || col_type == col_type_LinkList,
                               util::format("Property '%1' is not a link in object of type '%2'",
                                            element.table->get_column_name(element.col_key),
                                            get_printable_table_name(*element.table)));
            if (element.operation == KeyPathElement::KeyPathOperation::BacklinkTraversal) {
                cur_table = element.table; // advance through backlink
            }
            else {
                cur_table = cur_table->get_link_target(element.col_key); // advance through forward link
            }
        }
        else {
            realm_precondition(
                !element.is_list_of_primitives(),
                util::format("A subquery can not operate on a list of primitive values (property '%1')",
                             element.table->get_column_name(element.col_key)));

            realm_precondition(element.col_key.get_type() == col_type_LinkList ||
                                   element.operation == KeyPathElement::KeyPathOperation::BacklinkTraversal,
                               util::format("A subquery must operate on a list property, but '%1' is type '%2'",
                                            element.table->get_column_name(element.col_key),
                                            data_type_to_str(DataType(element.col_key.get_type()))));
            ConstTableRef subquery_table;
            if (element.operation == KeyPathElement::KeyPathOperation::BacklinkTraversal) {
                subquery_table = element.table; // advance through backlink
            }
            else {
                subquery_table = cur_table->get_link_target(element.col_key); // advance through forward link
            }

            subquery = subquery_table->where();
        }
        link_chain.push_back(element);
    }
}

Query& SubqueryExpression::get_subquery()
{
    return subquery;
}

LinkChain SubqueryExpression::link_chain_getter() const
{
    auto& tbl = query.get_table();
    return KeyPathMapping::link_chain_getter(tbl, link_chain);
}

} // namespace parser
} // namespace realm
