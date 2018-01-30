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

#ifndef REALM_SUBQUERY_EXPRESSION_HPP
#define REALM_SUBQUERY_EXPRESSION_HPP

#include <realm/parser/keypath_mapping.hpp>
#include <realm/query.hpp>
#include <realm/query_expression.hpp>
#include <realm/table.hpp>

#include "parser_utils.hpp"

namespace realm {
namespace parser {

template <typename RetType, class Enable = void>
struct SubqueryGetter;

struct SubqueryExpression
{
    std::vector<size_t> indexes;
    std::string var_name;
    size_t col_ndx;
    DataType col_type;
    Query &query;
    Query subquery;

    SubqueryExpression(Query &q, const std::string &key_path_string, const std::string &variable_name, parser::KeyPathMapping &mapping);
    Query& get_subquery();

    Table* table_getter() const;

    template <typename T>
    auto value_of_type_for_query() const
    {
        return SubqueryGetter<T>::convert(*this);
    }
};


// Certain operations are disabled for some types (eg. a sum of timestamps is invalid).
// The operations that are supported have a specialisation with std::enable_if for that type below
// any type/operation combination that is not specialised will get the runtime error from the following
// default implementation. The return type is just a dummy to make things compile.
template <typename RetType, class Enable>
struct SubqueryGetter {
    static Columns<RetType> convert(const SubqueryExpression&) {
        throw std::runtime_error(util::format("Predicate error: comparison of type '%1' with result of a subquery count is not supported.",
                                              type_to_str<RetType>()));
    }
};

template <typename RetType>
struct SubqueryGetter<RetType,
typename std::enable_if_t<
std::is_same<RetType, Int>::value ||
std::is_same<RetType, Float>::value ||
std::is_same<RetType, Double>::value
> >{
    static SubQueryCount convert(const SubqueryExpression& expr)
    {
        return expr.table_getter()->template column<LinkList>(expr.col_ndx, expr.subquery).count();
    }
};

} // namespace parser
} // namespace realm

#endif // REALM_SUBQUERY_EXPRESSION_HPP

