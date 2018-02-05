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

#ifndef REALM_PROPERTY_EXPRESSION_HPP
#define REALM_PROPERTY_EXPRESSION_HPP

#include <realm/query.hpp>
#include <realm/table.hpp>

namespace realm {
namespace parser {

struct PropertyExpression
{
    std::vector<size_t> indexes;
    size_t col_ndx;
    DataType col_type;
    Query &query;

    PropertyExpression(Query &q, const std::string &key_path_string);

    Table* table_getter() const;

    template <typename RetType>
    auto value_of_type_for_query() const
    {
        return this->table_getter()->template column<RetType>(this->col_ndx);
    }
};

} // namespace parser
} // namespace realm

#endif // REALM_PROPERTY_EXPRESSION_HPP

