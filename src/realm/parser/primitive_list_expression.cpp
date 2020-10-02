////////////////////////////////////////////////////////////////////////////
//
// Copyright 2020 Realm Inc.
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

#include "primitive_list_expression.hpp"
#include "parser_utils.hpp"

#include <realm/query_expression.hpp>
#include <realm/table.hpp>

namespace realm {
namespace parser {

PrimitiveListExpression::PrimitiveListExpression(Query& q, std::vector<KeyPathElement>&& chain,
                                                 ExpressionComparisonType type)
    : query(q)
    , link_chain(std::move(chain))
    , comparison_type(type)
{
}

LinkChain PrimitiveListExpression::link_chain_getter() const
{
    auto& tbl = query.get_table();
    return KeyPathMapping::link_chain_getter(tbl, link_chain, comparison_type);
}

template <>
SizeOperator<int64_t> PrimitiveListExpression::size_of_list() const
{
    ColKey col = get_dest_col_key();
    ColumnType type = col.get_type();

    if (type == col_type_Int)
        return link_chain_getter().template column<Lst<Int>>(col).size();
    else if (type == col_type_Bool)
        return link_chain_getter().template column<Lst<Bool>>(col).size();
    else if (type == col_type_String)
        return link_chain_getter().template column<Lst<String>>(col).size();
    else if (type == col_type_Binary)
        return link_chain_getter().template column<Lst<Binary>>(col).size();
    else if (type == col_type_Timestamp)
        return link_chain_getter().template column<Lst<Timestamp>>(col).size();
    else if (type == col_type_Float)
        return link_chain_getter().template column<Lst<Float>>(col).size();
    else if (type == col_type_Double)
        return link_chain_getter().template column<Lst<Double>>(col).size();
    else if (type == col_type_Decimal)
        return link_chain_getter().template column<Lst<Decimal128>>(col).size();
    else if (type == col_type_ObjectId)
        return link_chain_getter().template column<Lst<ObjectId>>(col).size();
    throw std::runtime_error(
        util::format("query contains unsupported list of primitives type %1 for operation .@count", type));
}

} // namespace parser
} // namespace realm
