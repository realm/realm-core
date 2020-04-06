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

} // namespace parser
} // namespace realm
