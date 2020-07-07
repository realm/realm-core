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

#ifndef REALM_EXPRESSION_CONTAINER_HPP
#define REALM_EXPRESSION_CONTAINER_HPP

#include <realm/util/any.hpp>

#include "collection_operator_expression.hpp"
#include "parser.hpp"
#include "primitive_list_expression.hpp"
#include "property_expression.hpp"
#include "query_builder.hpp"
#include "subquery_expression.hpp"
#include "value_expression.hpp"

namespace realm {
namespace parser {

class ExpressionContainer
{
public:
    ExpressionContainer(Query& query, const parser::Expression& e, query_builder::Arguments& args,
                        parser::KeyPathMapping& mapping);

    bool is_null();

    PropertyExpression& get_property();
    PrimitiveListExpression& get_primitive_list();
    ValueExpression& get_value();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Min, PropertyExpression>& get_min();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Max, PropertyExpression>& get_max();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum, PropertyExpression>& get_sum();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg, PropertyExpression>& get_avg();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Count, PropertyExpression>& get_count();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Min, PrimitiveListExpression>& get_primitive_min();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Max, PrimitiveListExpression>& get_primitive_max();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum, PrimitiveListExpression>& get_primitive_sum();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg, PrimitiveListExpression>& get_primitive_avg();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Count, PrimitiveListExpression>&
    get_primitive_count();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString, PrimitiveListExpression>&
    get_primitive_string_length();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary, PrimitiveListExpression>&
    get_primitive_binary_length();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::BacklinkCount, PropertyExpression>&
    get_backlink_count();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString, PropertyExpression>& get_size_string();
    CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary, PropertyExpression>& get_size_binary();
    SubqueryExpression& get_subexpression();

    std::vector<KeyPathElement> get_keypaths();

    DataType check_type_compatibility(DataType type);
    DataType get_comparison_type(ExpressionContainer& rhs);

    enum class ExpressionInternal {
        exp_Value,
        exp_Property,
        exp_PrimitiveList,
        exp_OpMin,
        exp_OpMax,
        exp_OpSum,
        exp_OpAvg,
        exp_OpCount,
        exp_OpMinPrimitive,
        exp_OpMaxPrimitive,
        exp_OpSumPrimitive,
        exp_OpAvgPrimitive,
        exp_OpCountPrimitive,
        exp_OpSizeStringPrimitive,
        exp_OpSizeBinaryPrimitive,
        exp_OpSizeString,
        exp_OpSizeBinary,
        exp_OpBacklinkCount,
        exp_SubQuery
    };

    ExpressionInternal type;
private:
    util::Any storage;
};

} // namespace parser
} // namespace realm

#endif // REALM_EXPRESSION_CONTAINER_HPP
