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

#ifndef REALM_PRIMITIVE_LIST_EXPRESSION_HPP
#define REALM_PRIMITIVE_LIST_EXPRESSION_HPP

#include <realm/parser/keypath_mapping.hpp>
#include <realm/query.hpp>
#include <realm/table.hpp>

namespace realm {
namespace parser {

struct PrimitiveListExpression {
    Query& query;
    std::vector<KeyPathElement> link_chain;
    ExpressionComparisonType comparison_type;
    DataType get_dest_type() const;
    ColKey get_dest_col_key() const;
    ConstTableRef get_dest_table() const;
    template <class T>
    T size_of_list() const;

    PrimitiveListExpression(Query& q, std::vector<KeyPathElement>&& chain, ExpressionComparisonType type);

    LinkChain link_chain_getter() const;

    template <typename RetType>
    auto value_of_type_for_query() const
    {
        return this->link_chain_getter().template column<Lst<RetType>>(get_dest_col_key());
    }
};

inline DataType PrimitiveListExpression::get_dest_type() const
{
    REALM_ASSERT_DEBUG(link_chain.size() > 0);
    return DataType(link_chain.back().col_key.get_type());
}

inline ColKey PrimitiveListExpression::get_dest_col_key() const
{
    REALM_ASSERT_DEBUG(link_chain.size() > 0);
    return link_chain.back().col_key;
}

inline ConstTableRef PrimitiveListExpression::get_dest_table() const
{
    REALM_ASSERT_DEBUG(link_chain.size() > 0);
    return link_chain.back().table;
}

} // namespace parser
} // namespace realm

#endif // REALM_PRIMITIVE_LIST_EXPRESSION_HPP
