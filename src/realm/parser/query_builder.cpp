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

#include "query_builder.hpp"

#include "parser.hpp"
#include "parser_utils.hpp"
#include "property_expression.hpp"
#include "expression_container.hpp"

#include <realm.hpp>
#include <realm/query_expression.hpp>
#include <realm/column_type.hpp>
#include <realm/utilities.hpp>

#include <sstream>

using namespace realm;
using namespace util;
using namespace parser;
using namespace query_builder;

namespace {

REALM_NOINLINE REALM_COLD REALM_NORETURN void throw_logic_error(const char* msg)
{
    throw std::logic_error(msg);
}
REALM_NOINLINE REALM_COLD REALM_NORETURN void throw_logic_error(std::string msg)
{
    throw std::logic_error(std::move(msg));
}
REALM_NOINLINE REALM_COLD REALM_NORETURN void throw_runtime_error(const char* msg)
{
    throw std::runtime_error(msg);
}
REALM_NOINLINE REALM_COLD REALM_NORETURN void throw_runtime_error(std::string msg)
{
    throw std::runtime_error(std::move(msg));
}

template <typename T, parser::Expression::KeyPathOp OpType, typename ExpressionType>
REALM_FORCEINLINE Query do_make_null_comparison_query(Predicate::Operator,
                                                      const CollectionOperatorExpression<OpType, ExpressionType>&)
{
    throw_logic_error("Comparing a collection aggregate operation to 'null' is not supported.");
}

template <typename T>
REALM_FORCEINLINE Query do_make_null_comparison_query(Predicate::Operator, const ValueExpression&)
{
    throw_logic_error("Comparing a value to 'null' is not supported.");
}

template <typename T>
Query do_make_null_comparison_query(Predicate::Operator op, const PropertyExpression& expr)
{
    Columns<T> column = expr.link_chain_getter().template column<T>(expr.get_dest_col_key());
    switch (op) {
        case Predicate::Operator::NotEqual:
            return column != realm::null();
        case Predicate::Operator::In:
        case Predicate::Operator::Equal:
            return column == realm::null();
        default:
            throw_logic_error("Only 'equal' and 'not equal' operators supported when comparing against 'null'.");
    }
}

template <typename T>
Query do_make_null_comparison_query(Predicate::Operator op, const PrimitiveListExpression& expr)
{
    Columns<Lst<T>> column = expr.value_of_type_for_query<T>();
    switch (op) {
        case Predicate::Operator::NotEqual:
            return column != realm::null();
        case Predicate::Operator::In:
        case Predicate::Operator::Equal:
            return column == realm::null();
        default:
            throw_logic_error("Only 'equal' and 'not equal' operators supported when comparing against 'null'.");
    }
}

template <>
REALM_FORCEINLINE Query do_make_null_comparison_query<Link>(Predicate::Operator, const PrimitiveListExpression&)
{
    throw_logic_error("Invalid query, list of primitive links is not a valid Realm contruct");
}

template <>
Query do_make_null_comparison_query<Link>(Predicate::Operator op, const PropertyExpression& expr)
{
    REALM_ASSERT_DEBUG(!expr.dest_type_is_backlink()); // this case should be handled at a higher layer
    switch (op) {
        case Predicate::Operator::NotEqual: {
            Columns<Link> column = expr.value_of_type_for_query<Link>();
            return column != realm::null();
        }
        case Predicate::Operator::In:
        case Predicate::Operator::Equal: {
            Columns<Link> column = expr.value_of_type_for_query<Link>();
            return column == realm::null();
        }
        default:
            throw_logic_error("Only 'equal' and 'not equal' operators supported for object comparison.");
    }
}

// add a clause for numeric constraints based on operator type
template <typename A, typename B>
Query make_numeric_constraint_query(Predicate::Operator operatorType, A lhs, B rhs)
{
    switch (operatorType) {
        case Predicate::Operator::LessThan:
            return lhs < rhs;
        case Predicate::Operator::LessThanOrEqual:
            return lhs <= rhs;
        case Predicate::Operator::GreaterThan:
            return lhs > rhs;
        case Predicate::Operator::GreaterThanOrEqual:
            return lhs >= rhs;
        case Predicate::Operator::In:
        case Predicate::Operator::Equal:
            return lhs == rhs;
        case Predicate::Operator::NotEqual:
            return lhs != rhs;
        default:
            throw_logic_error("Unsupported operator for numeric queries.");
    }
}

const char* operator_description(const Predicate::Operator& op)
{
    switch (op) {
        case Predicate::Operator::None:
            return "NONE";
        case realm::parser::Predicate::Operator::Equal:
            return "==";
        case realm::parser::Predicate::Operator::NotEqual:
            return "!=";
        case realm::parser::Predicate::Operator::LessThan:
            return "<";
        case realm::parser::Predicate::Operator::LessThanOrEqual:
            return "<=";
        case realm::parser::Predicate::Operator::GreaterThan:
            return ">";
        case realm::parser::Predicate::Operator::GreaterThanOrEqual:
            return ">=";
        case realm::parser::Predicate::Operator::BeginsWith:
            return "BEGINSWITH";
        case realm::parser::Predicate::Operator::EndsWith:
            return "ENDSWITH";
        case realm::parser::Predicate::Operator::Contains:
            return "CONTAINS";
        case realm::parser::Predicate::Operator::Like:
            return "LIKE";
        case realm::parser::Predicate::Operator::In:
            return "IN";
    }
    REALM_ASSERT_DEBUG(false);
    return "";
}

template <typename A, typename B>
Query make_bool_constraint_query(Predicate::Operator operatorType, A lhs, B rhs)
{
    switch (operatorType) {
        case Predicate::Operator::In:
        case Predicate::Operator::Equal:
            return lhs == rhs;
        case Predicate::Operator::NotEqual:
            return lhs != rhs;
        default:
            throw_logic_error(util::format(
                "Unsupported operator '%1' in query. Only equal (==) and not equal (!=) are supported for this type.",
                operator_description(operatorType)));
    }
}

// (string column OR list of primitive strings) vs (string literal OR string column)
template <typename LHS, typename RHS>
std::enable_if_t<realm::is_any<LHS, Columns<String>, Columns<Lst<String>>>::value &&
                     realm::is_any<RHS, StringData, Columns<String>>::value,
                 Query>
make_string_constraint_query(const Predicate::Comparison& cmp, LHS&& lhs, RHS&& rhs)
{
    bool case_sensitive = (cmp.option != Predicate::OperatorOption::CaseInsensitive);
    switch (cmp.op) {
        case Predicate::Operator::BeginsWith:
            return lhs.begins_with(rhs, case_sensitive);
        case Predicate::Operator::EndsWith:
            return lhs.ends_with(rhs, case_sensitive);
        case Predicate::Operator::Contains:
            return lhs.contains(rhs, case_sensitive);
        case Predicate::Operator::Equal:
            return lhs.equal(rhs, case_sensitive);
        case Predicate::Operator::NotEqual:
            return lhs.not_equal(rhs, case_sensitive);
        case Predicate::Operator::Like:
            return lhs.like(rhs, case_sensitive);
        default:
            throw_logic_error(
                util::format("Unsupported operator '%1' for string queries.", operator_description(cmp.op)));
    }
}

// ((string literal) vs (string column OR list of primitive strings)) OR ((string column) vs (list of primitive
// strings column))
template <typename LHS, typename RHS>
std::enable_if_t<(realm::is_any<LHS, StringData>::value &&
                  realm::is_any<RHS, Columns<String>, Columns<Lst<String>>>::value) ||
                     (std::is_same_v<LHS, Columns<String>> && std::is_same_v<RHS, Columns<Lst<String>>>),
                 Query>
make_string_constraint_query(const Predicate::Comparison& cmp, LHS&& lhs, RHS&& rhs)
{
    bool case_sensitive = (cmp.option != Predicate::OperatorOption::CaseInsensitive);
    switch (cmp.op) {
        case Predicate::Operator::In:
        case Predicate::Operator::Equal:
            return rhs.equal(lhs, case_sensitive);
        case Predicate::Operator::NotEqual:
            return rhs.not_equal(lhs, case_sensitive);
            // operators CONTAINS, BEGINSWITH, ENDSWITH, LIKE are not supported in this direction
            // These queries are not the same: "'asdf' CONTAINS string_property" vs "string_property CONTAINS 'asdf'"
        default:
            throw_logic_error(
                util::format("Unsupported query comparison '%1' for a single string vs a string property.",
                             operator_description(cmp.op)));
    }
}

REALM_FORCEINLINE Query make_string_constraint_query(const Predicate::Comparison&, Columns<Lst<String>>&&,
                                                     Columns<Lst<String>>&&)
{
    throw_logic_error("Comparing two primitive string lists against each other is not implemented yet.");
}


template <typename LHS, typename RHS>
REALM_FORCEINLINE
    std::enable_if_t<(std::is_same_v<LHS, Columns<Lst<BinaryData>>> && std::is_same_v<RHS, Columns<Lst<BinaryData>>>),
                     Query>
    make_binary_constraint_query(const Predicate::Comparison&, RHS&&, LHS&&)
{
    throw_logic_error("Unsupported operation for binary comparison.");
}

// (column OR list of primitives) vs (literal OR column)
template <typename LHS, typename RHS>
std::enable_if_t<realm::is_any<LHS, Columns<BinaryData>, Columns<Lst<BinaryData>>>::value &&
                     realm::is_any<RHS, BinaryData, Columns<BinaryData>>::value,
                 Query>
make_binary_constraint_query(const Predicate::Comparison& cmp, LHS&& column, RHS&& value)
{
    bool case_sensitive = (cmp.option != Predicate::OperatorOption::CaseInsensitive);
    switch (cmp.op) {
        case Predicate::Operator::BeginsWith:
            return column.begins_with(value, case_sensitive);
        case Predicate::Operator::EndsWith:
            return column.ends_with(value, case_sensitive);
        case Predicate::Operator::Contains:
            return column.contains(value, case_sensitive);
        case Predicate::Operator::Equal:
            return column.equal(value, case_sensitive);
        case Predicate::Operator::NotEqual:
            return column.not_equal(value, case_sensitive);
        case Predicate::Operator::Like:
            return column.like(value, case_sensitive);
        default:
            throw_logic_error("Unsupported operator for binary queries.");
    }
}

// ((literal value) vs (column OR list of primitive)) OR ((column) vs (list of primitive column))
template <typename LHS, typename RHS>
std::enable_if_t<(realm::is_any<LHS, BinaryData>::value &&
                  realm::is_any<RHS, Columns<BinaryData>, Columns<Lst<BinaryData>>>::value) ||
                     (std::is_same_v<LHS, Columns<BinaryData>> && std::is_same_v<RHS, Columns<Lst<BinaryData>>>),
                 Query>
make_binary_constraint_query(const Predicate::Comparison& cmp, LHS&& value, RHS&& column)
{
    switch (cmp.op) {
        case Predicate::Operator::In:
        case Predicate::Operator::Equal:
            return column == value;
        case Predicate::Operator::NotEqual:
            return column != value;
        default:
            throw_logic_error("Substring comparison not supported for keypath substrings.");
    }
}

template <typename LHS, typename RHS>
REALM_FORCEINLINE Query make_link_constraint_query(Predicate::Operator, const LHS&, const RHS&)
{
    throw_runtime_error("Object comparisons are currently only supported between a property and an argument.");
}

template <>
Query make_link_constraint_query(Predicate::Operator op, const PropertyExpression& prop_expr,
                                 const ValueExpression& value_expr)
{
    ObjKey obj_key = value_expr.arguments->object_index_for_argument(string_to<int>(value_expr.value->s));
    realm_precondition(prop_expr.link_chain.size() == 1, "KeyPath queries not supported for object comparisons.");
    switch (op) {
        case Predicate::Operator::NotEqual: {
            ColKey col = prop_expr.get_dest_col_key();
            return Query().Not().links_to(col, obj_key);
        }
        case Predicate::Operator::In:
        case Predicate::Operator::Equal: {
            ColKey col = prop_expr.get_dest_col_key();
            return Query().links_to(col, obj_key);
        }
        default:
            throw_logic_error("Only 'equal' and 'not equal' operators supported for object comparison.");
    }
}

template <>
Query make_link_constraint_query(Predicate::Operator op, const ValueExpression& value_expr,
                                 const PropertyExpression& prop_expr)
{
    // since equality is commutative we can just call the above function to the same effect
    return make_link_constraint_query(op, prop_expr, value_expr);
}

template <typename A, typename B, typename KnownType, typename PossibleMixedType>
Query make_mixed_type_numeric_comparison_query(const Predicate::Comparison& cmp, A& lhs, B& rhs)
{
    if constexpr (std::is_same_v<B, ValueExpression>) {
        if (rhs.template is_type<PossibleMixedType>()) {
            return make_numeric_constraint_query(cmp.op, lhs.template value_of_type_for_query<KnownType>(),
                                                 rhs.template value_of_type_for_query<PossibleMixedType>());
        }
    }
    if constexpr (std::is_same_v<A, ValueExpression>) {
        if (lhs.template is_type<PossibleMixedType>()) {
            return make_numeric_constraint_query(cmp.op, lhs.template value_of_type_for_query<PossibleMixedType>(),
                                                 rhs.template value_of_type_for_query<KnownType>());
        }
    }
    return make_numeric_constraint_query(cmp.op, lhs.template value_of_type_for_query<KnownType>(),
                                         rhs.template value_of_type_for_query<KnownType>());
}

template <typename A, typename B>
Query do_make_comparison_query(const Predicate::Comparison& cmp, A& lhs, B& rhs, DataType type)
{
    switch (type) {
        case type_Bool:
            return make_bool_constraint_query(cmp.op, lhs.template value_of_type_for_query<bool>(),
                                              rhs.template value_of_type_for_query<bool>());
        case type_Timestamp:
            return make_mixed_type_numeric_comparison_query<A, B, Timestamp, ObjectId>(cmp, lhs, rhs);
        case type_Double:
            return make_numeric_constraint_query(cmp.op, lhs.template value_of_type_for_query<Double>(),
                                                 rhs.template value_of_type_for_query<Double>());
        case type_Float:
            return make_numeric_constraint_query(cmp.op, lhs.template value_of_type_for_query<Float>(),
                                                 rhs.template value_of_type_for_query<Float>());
        case type_Int:
            return make_numeric_constraint_query(cmp.op, lhs.template value_of_type_for_query<Int>(),
                                                 rhs.template value_of_type_for_query<Int>());
        case type_String:
            return make_string_constraint_query(cmp, lhs.template value_of_type_for_query<String>(),
                                                rhs.template value_of_type_for_query<String>());
        case type_Binary:
            return make_binary_constraint_query(cmp, lhs.template value_of_type_for_query<Binary>(),
                                                rhs.template value_of_type_for_query<Binary>());
        case type_Link:
            return make_link_constraint_query(cmp.op, lhs, rhs);
        case type_ObjectId:
            return make_mixed_type_numeric_comparison_query<A, B, ObjectId, Timestamp>(cmp, lhs, rhs);
        case type_Decimal:
            return make_numeric_constraint_query(cmp.op, lhs.template value_of_type_for_query<Decimal128>(),
                                                 rhs.template value_of_type_for_query<Decimal128>());
        case type_UUID:
            return make_bool_constraint_query(cmp.op, lhs.template value_of_type_for_query<UUID>(),
                                              rhs.template value_of_type_for_query<UUID>());
        default:
            throw_logic_error(util::format("Object type '%1' not supported", data_type_to_str(type)));
    }
}

template <>
REALM_FORCEINLINE Query do_make_comparison_query(const Predicate::Comparison&, ValueExpression&, ValueExpression&,
                                                 DataType)
{
    throw_runtime_error("Invalid predicate: comparison between two literals is not supported.");
}

enum class NullLocation {
    NullOnLHS,
    NullOnRHS
};

template <class T>
Query do_make_null_comparison_query(const Predicate::Comparison& cmp, const T& expr, DataType type,
                                    NullLocation location)
{
    if constexpr (std::is_same_v<T, PropertyExpression>) {
        if (expr.dest_type_is_backlink()) {
            throw_logic_error("Comparing linking object properties to 'null' is not supported");
        }
    }
    if (type == type_LinkList) {
        throw_logic_error("Comparing a list property to 'null' is not supported");
    }
    switch (type) {
        case realm::type_Bool:
            return do_make_null_comparison_query<bool>(cmp.op, expr);
        case realm::type_Timestamp:
            return do_make_null_comparison_query<Timestamp>(cmp.op, expr);
        case realm::type_Double:
            return do_make_null_comparison_query<Double>(cmp.op, expr);
        case realm::type_Float:
            return do_make_null_comparison_query<Float>(cmp.op, expr);
        case realm::type_Int:
            return do_make_null_comparison_query<Int>(cmp.op, expr);
        case realm::type_String: {
            if (location == NullLocation::NullOnLHS) {
                return make_string_constraint_query(cmp, StringData(),
                                                    expr.template value_of_type_for_query<String>());
            }
            else {
                return make_string_constraint_query(cmp, expr.template value_of_type_for_query<String>(),
                                                    StringData());
            }
        }
        case realm::type_Binary: {
            if (location == NullLocation::NullOnLHS) {
                return make_binary_constraint_query(cmp, BinaryData(),
                                                    expr.template value_of_type_for_query<Binary>());
            }
            else {
                return make_binary_constraint_query(cmp, expr.template value_of_type_for_query<Binary>(),
                                                    BinaryData());
            }
        }
        case realm::type_ObjectId:
            return do_make_null_comparison_query<ObjectId>(cmp.op, expr);
        case realm::type_Decimal:
            return do_make_null_comparison_query<Decimal128>(cmp.op, expr);
        case realm::type_Link:
            return do_make_null_comparison_query<Link>(cmp.op, expr);
        case realm::type_UUID:
            return do_make_null_comparison_query<UUID>(cmp.op, expr);
        default:
            throw_logic_error(util::format("Object type '%1' not supported", util::data_type_to_str(type)));
    }
}

Query make_null_comparison_query(const Predicate::Comparison& cmp, ExpressionContainer& exp, NullLocation location)
{
    switch (exp.type) {
        case ExpressionContainer::ExpressionInternal::exp_Value:
            throw_runtime_error(
                "Unsupported query comparing 'null' and a literal. A comparison must include at least one keypath.");
        case ExpressionContainer::ExpressionInternal::exp_Property:
            return do_make_null_comparison_query(cmp, exp.get_property(), exp.get_property().get_dest_type(),
                                                 location);
        case ExpressionContainer::ExpressionInternal::exp_PrimitiveList:
            return do_make_null_comparison_query(cmp, exp.get_primitive_list(),
                                                 exp.get_primitive_list().get_dest_type(), location);
        case ExpressionContainer::ExpressionInternal::exp_OpMin:
            return do_make_null_comparison_query(cmp, exp.get_min(), exp.get_min().operative_col_type, location);
        case ExpressionContainer::ExpressionInternal::exp_OpMax:
            return do_make_null_comparison_query(cmp, exp.get_max(), exp.get_max().operative_col_type, location);
        case ExpressionContainer::ExpressionInternal::exp_OpSum:
            return do_make_null_comparison_query(cmp, exp.get_sum(), exp.get_sum().operative_col_type, location);
        case ExpressionContainer::ExpressionInternal::exp_OpAvg:
            return do_make_null_comparison_query(cmp, exp.get_avg(), exp.get_avg().operative_col_type, location);
        case ExpressionContainer::ExpressionInternal::exp_OpMinPrimitive:
            return do_make_null_comparison_query(cmp, exp.get_primitive_min(),
                                                 exp.get_primitive_min().operative_col_type, location);
        case ExpressionContainer::ExpressionInternal::exp_OpMaxPrimitive:
            return do_make_null_comparison_query(cmp, exp.get_primitive_max(),
                                                 exp.get_primitive_max().operative_col_type, location);
        case ExpressionContainer::ExpressionInternal::exp_OpSumPrimitive:
            return do_make_null_comparison_query(cmp, exp.get_primitive_sum(),
                                                 exp.get_primitive_sum().operative_col_type, location);
        case ExpressionContainer::ExpressionInternal::exp_OpAvgPrimitive:
            return do_make_null_comparison_query(cmp, exp.get_primitive_avg(),
                                                 exp.get_primitive_avg().operative_col_type, location);
        case ExpressionContainer::ExpressionInternal::exp_SubQuery:
        case ExpressionContainer::ExpressionInternal::exp_OpCount:
        case ExpressionContainer::ExpressionInternal::exp_OpBacklinkCount:
        case ExpressionContainer::ExpressionInternal::exp_OpSizeString:
        case ExpressionContainer::ExpressionInternal::exp_OpSizeBinary:
        case ExpressionContainer::ExpressionInternal::exp_OpCountPrimitive:
            throw_runtime_error("Invalid predicate: comparison between 'null' and @size or @count");
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeStringPrimitive:
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeBinaryPrimitive:
            throw_runtime_error("Invalid predicate: comparison between primitive list '.length' and 'null'");
    }
    throw_logic_error("unreachable");
}

template <typename LHS_T>
Query internal_make_comparison_query(LHS_T& lhs, const Predicate::Comparison& cmp, ExpressionContainer& rhs,
                                     DataType comparison_type)
{
    switch (rhs.type) {
        case ExpressionContainer::ExpressionInternal::exp_Value:
            return do_make_comparison_query(cmp, lhs, rhs.get_value(), comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_Property:
            return do_make_comparison_query(cmp, lhs, rhs.get_property(), comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpMin:
            return do_make_comparison_query(cmp, lhs, rhs.get_min(), comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpMax:
            return do_make_comparison_query(cmp, lhs, rhs.get_max(), comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpSum:
            return do_make_comparison_query(cmp, lhs, rhs.get_sum(), comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpAvg:
            return do_make_comparison_query(cmp, lhs, rhs.get_avg(), comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpCount:
            return do_make_comparison_query(cmp, lhs, rhs.get_count(), comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpBacklinkCount:
            return do_make_comparison_query(cmp, lhs, rhs.get_backlink_count(), comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpSizeString:
            return do_make_comparison_query(cmp, lhs, rhs.get_size_string(), comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpSizeBinary:
            return do_make_comparison_query(cmp, lhs, rhs.get_size_binary(), comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_SubQuery:
            return do_make_comparison_query(cmp, lhs, rhs.get_subexpression(), comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_PrimitiveList:
            return do_make_comparison_query(cmp, lhs, rhs.get_primitive_list(), comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpMinPrimitive:
            return do_make_comparison_query(cmp, lhs, rhs.get_primitive_min(), comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpMaxPrimitive:
            return do_make_comparison_query(cmp, lhs, rhs.get_primitive_max(), comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSumPrimitive:
            return do_make_comparison_query(cmp, lhs, rhs.get_primitive_sum(), comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpAvgPrimitive:
            return do_make_comparison_query(cmp, lhs, rhs.get_primitive_avg(), comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpCountPrimitive:
            return do_make_comparison_query(cmp, lhs, rhs.get_primitive_count(), comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeStringPrimitive:
            return do_make_comparison_query(cmp, lhs, rhs.get_primitive_string_length(), comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeBinaryPrimitive:
            return do_make_comparison_query(cmp, lhs, rhs.get_primitive_binary_length(), comparison_type);
    }
    throw_logic_error("unreachable");
}

Query make_comparison_query(ExpressionContainer& lhs, const Predicate::Comparison& cmp, ExpressionContainer& rhs)
{
    DataType comparison_type = lhs.get_comparison_type(rhs);
    switch (lhs.type) {
        case ExpressionContainer::ExpressionInternal::exp_Value:
            return internal_make_comparison_query(lhs.get_value(), cmp, rhs, comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_Property:
            return internal_make_comparison_query(lhs.get_property(), cmp, rhs, comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpMin:
            return internal_make_comparison_query(lhs.get_min(), cmp, rhs, comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpMax:
            return internal_make_comparison_query(lhs.get_max(), cmp, rhs, comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpSum:
            return internal_make_comparison_query(lhs.get_sum(), cmp, rhs, comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpAvg:
            return internal_make_comparison_query(lhs.get_avg(), cmp, rhs, comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpCount:
            return internal_make_comparison_query(lhs.get_count(), cmp, rhs, comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpBacklinkCount:
            return internal_make_comparison_query(lhs.get_backlink_count(), cmp, rhs, comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpSizeString:
            return internal_make_comparison_query(lhs.get_size_string(), cmp, rhs, comparison_type);
        case ExpressionContainer::ExpressionInternal::exp_OpSizeBinary:
            return internal_make_comparison_query(lhs.get_size_binary(), cmp, rhs, comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_SubQuery:
            return internal_make_comparison_query(lhs.get_subexpression(), cmp, rhs, comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_PrimitiveList:
            return internal_make_comparison_query(lhs.get_primitive_list(), cmp, rhs, comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpMinPrimitive:
            return internal_make_comparison_query(lhs.get_primitive_min(), cmp, rhs, comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpMaxPrimitive:
            return internal_make_comparison_query(lhs.get_primitive_max(), cmp, rhs, comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSumPrimitive:
            return internal_make_comparison_query(lhs.get_primitive_sum(), cmp, rhs, comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpAvgPrimitive:
            return internal_make_comparison_query(lhs.get_primitive_avg(), cmp, rhs, comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpCountPrimitive:
            return internal_make_comparison_query(lhs.get_primitive_count(), cmp, rhs, comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeStringPrimitive:
            return internal_make_comparison_query(lhs.get_primitive_string_length(), cmp, rhs, comparison_type);
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeBinaryPrimitive:
            return internal_make_comparison_query(lhs.get_primitive_binary_length(), cmp, rhs, comparison_type);
    }
    throw_logic_error("unreachable");
}

// precheck some expressions to make sure we support them and if not, provide a meaningful error message
void preprocess_for_comparison_types(Predicate::Comparison& cmpr, ExpressionContainer& lhs, ExpressionContainer& rhs)
{
    auto get_cmp_type_name = [&](parser::Expression::ComparisonType compare_type) {
        if (compare_type == parser::Expression::ComparisonType::Any) {
            return util::format("'%1' or 'SOME'", comparison_type_to_str(parser::Expression::ComparisonType::Any));
        }
        return util::format("'%1'", comparison_type_to_str(compare_type));
    };

    auto verify_comparison_type = [&](ExpressionContainer expression,
                                      parser::Expression::ComparisonType compare_type) {
        size_t list_count = 0;
        size_t primitive_list_count = 0;
        std::vector<KeyPathElement> link_chain = expression.get_keypaths();
        for (KeyPathElement e : link_chain) {
            if (e.col_key.get_type() == col_type_LinkList ||
                e.operation == KeyPathElement::KeyPathOperation::BacklinkTraversal) {
                list_count++;
            }
            else if (e.is_list_of_primitives()) {
                primitive_list_count++;
            }
        }
        if (compare_type != parser::Expression::ComparisonType::Unspecified) {
            realm_precondition(
                expression.type == ExpressionContainer::ExpressionInternal::exp_Property ||
                    expression.type == ExpressionContainer::ExpressionInternal::exp_PrimitiveList ||
                    expression.type == ExpressionContainer::ExpressionInternal::exp_OpSizeBinaryPrimitive ||
                    expression.type == ExpressionContainer::ExpressionInternal::exp_OpSizeStringPrimitive,
                util::format("The expression after %1 must be a keypath containing a list",
                             get_cmp_type_name(compare_type)));
            realm_precondition(
                list_count > 0 || primitive_list_count > 0,
                util::format("The keypath following %1 must contain a list", get_cmp_type_name(compare_type)));
            realm_precondition(
                list_count == 1 || primitive_list_count == 1,
                util::format("The keypath following %1 must contain only one list", get_cmp_type_name(compare_type)));
        }
    };

    verify_comparison_type(lhs, cmpr.expr[0].comparison_type);
    verify_comparison_type(rhs, cmpr.expr[1].comparison_type);

    if (lhs.type == ExpressionContainer::ExpressionInternal::exp_PrimitiveList &&
        rhs.type == ExpressionContainer::ExpressionInternal::exp_PrimitiveList) {
        throw_logic_error(
            util::format("Ordered comparison between two primitive lists is not implemented yet ('%1' and '%2')",
                         cmpr.expr[0].s, cmpr.expr[1].s));
    }

    // Check that operator "IN" has a RHS keypath which is a list
    if (cmpr.op == Predicate::Operator::In) {
        realm_precondition(rhs.type == ExpressionContainer::ExpressionInternal::exp_Property ||
                               rhs.type == ExpressionContainer::ExpressionInternal::exp_PrimitiveList ||
                               rhs.type == ExpressionContainer::ExpressionInternal::exp_OpSizeStringPrimitive ||
                               rhs.type == ExpressionContainer::ExpressionInternal::exp_OpSizeBinaryPrimitive,
                           "The expression following 'IN' must be a keypath to a list");
        auto get_list_count = [](const std::vector<KeyPathElement>& target_link_chain) {
            size_t list_count = 0;
            for (KeyPathElement e : target_link_chain) {
                if (e.col_key.get_type() == col_type_LinkList || e.is_list_of_primitives() ||
                    e.operation == KeyPathElement::KeyPathOperation::BacklinkTraversal) {
                    list_count++;
                }
            }
            return list_count;
        };
        // For list vs list comparisons, all the right code paths are hooked up, but we just don't define the
        // actual behaviour, see the FIXME in query_expressions.hpp in Value::compare about many-to-many links
        // Without this check here, we would assert in debug mode and always return false in release mode.
        size_t lhs_list_count = get_list_count(lhs.get_keypaths());
        realm_precondition(lhs_list_count == 0, "The keypath preceeding 'IN' must not contain a list, list vs "
                                                "list comparisons are not currently supported");

        size_t rhs_list_count = get_list_count(rhs.get_keypaths());
        realm_precondition(rhs_list_count > 0, "The keypath following 'IN' must contain a list");
        realm_precondition(rhs_list_count == 1, "The keypath following 'IN' must contain only one list");
    }
}


bool is_property_operation(parser::Expression::Type type)
{
    return type == parser::Expression::Type::KeyPath || type == parser::Expression::Type::SubQuery;
}

Query make_comparison_query(Query& query, const Predicate& pred, Arguments& args, parser::KeyPathMapping& mapping)
{
    Predicate::Comparison cmpr = pred.cmpr;
    auto lhs_type = cmpr.expr[0].type, rhs_type = cmpr.expr[1].type;

    if (!is_property_operation(lhs_type) && !is_property_operation(rhs_type)) {
        // value vs value expressions are not supported (ex: 2 < 3 or null != null)
        throw_logic_error("Predicate expressions must compare a keypath and another keypath or a constant value");
    }
    ExpressionContainer lhs(query, cmpr.expr[0], args, mapping);
    ExpressionContainer rhs(query, cmpr.expr[1], args, mapping);

    preprocess_for_comparison_types(cmpr, lhs, rhs);

    if (lhs.is_null()) {
        return make_null_comparison_query(cmpr, rhs, NullLocation::NullOnLHS);
    }
    else if (rhs.is_null()) {
        return make_null_comparison_query(cmpr, lhs, NullLocation::NullOnRHS);
    }
    else {
        return make_comparison_query(lhs, cmpr, rhs);
    }
}

void update_query_with_predicate(Query& query, const Predicate& pred, Arguments& arguments,
                                 parser::KeyPathMapping& mapping)
{
    if (pred.negate) {
        query.Not();
    }

    switch (pred.type) {
        case Predicate::Type::And:
            query.group();
            for (auto &sub : pred.cpnd.sub_predicates) {
                update_query_with_predicate(query, sub, arguments, mapping);
            }
            if (!pred.cpnd.sub_predicates.size()) {
                query.and_query(std::unique_ptr<realm::Expression>(new TrueExpression));
            }
            query.end_group();
            break;

        case Predicate::Type::Or:
            query.group();
            for (auto &sub : pred.cpnd.sub_predicates) {
                query.Or();
                update_query_with_predicate(query, sub, arguments, mapping);
            }
            if (!pred.cpnd.sub_predicates.size()) {
                query.and_query(std::unique_ptr<realm::Expression>(new FalseExpression));
            }
            query.end_group();
            break;

        case Predicate::Type::Comparison: {
            query.and_query(make_comparison_query(query, pred, arguments, mapping));
            break;
        }
        case Predicate::Type::True:
            query.and_query(std::unique_ptr<realm::Expression>(new TrueExpression));
            break;

        case Predicate::Type::False:
            query.and_query(std::unique_ptr<realm::Expression>(new FalseExpression));
            break;

        default:
            throw_logic_error("Invalid predicate type");
    }
}
} // anonymous namespace

namespace realm {
namespace query_builder {

void apply_predicate(Query& query, const Predicate& predicate, Arguments& arguments, parser::KeyPathMapping mapping)
{

    if (predicate.type == Predicate::Type::True && !predicate.negate) {
        // early out for a predicate which should return all results
        return;
    }

    update_query_with_predicate(query, predicate, arguments, mapping);

    // Test the constructed query in core
    std::string validateMessage = query.validate();
    realm_precondition(validateMessage.empty(), validateMessage.c_str());
}

void apply_ordering(DescriptorOrdering& ordering, ConstTableRef target, const parser::DescriptorOrderingState& state,
                    Arguments&, parser::KeyPathMapping mapping)
{
    for (const DescriptorOrderingState::SingleOrderingState& cur_ordering : state.orderings) {
        if (cur_ordering.type == DescriptorOrderingState::SingleOrderingState::DescriptorType::Limit) {
            ordering.append_limit(cur_ordering.limit);
        }
        else if (cur_ordering.type == DescriptorOrderingState::SingleOrderingState::DescriptorType::Distinct ||
                 cur_ordering.type == DescriptorOrderingState::SingleOrderingState::DescriptorType::Sort) {
            bool is_distinct =
                cur_ordering.type == DescriptorOrderingState::SingleOrderingState::DescriptorType::Distinct;
            std::vector<std::vector<ColKey>> property_columns;
            std::vector<bool> ascendings;
            for (const DescriptorOrderingState::PropertyState& cur_property : cur_ordering.properties) {
                KeyPath path = key_path_from_string(cur_property.key_path);
                std::vector<ColKey> columns;
                ConstTableRef cur_table = target;
                for (size_t ndx_in_path = 0; ndx_in_path < path.size(); ++ndx_in_path) {
                    ColKey col_key = cur_table->get_column_key(path[ndx_in_path]);
                    if (!col_key) {
                        throw_runtime_error(util::format(
                            "No property '%1' found on object type '%2' specified in '%3' clause", path[ndx_in_path],
                            cur_table->get_name(), is_distinct ? "distinct" : "sort"));
                    }
                    columns.push_back(col_key);
                    if (ndx_in_path < path.size() - 1) {
                        cur_table = cur_table->get_link_target(col_key);
                    }
                }
                property_columns.push_back(columns);
                ascendings.push_back(cur_property.ascending);
            }

            if (is_distinct) {
                ordering.append_distinct(DistinctDescriptor(property_columns));
            }
            else {
                ordering.append_sort(SortDescriptor(property_columns, ascendings),
                                     SortDescriptor::MergeMode::prepend);
            }
        }
        else if (cur_ordering.type == DescriptorOrderingState::SingleOrderingState::DescriptorType::Include) {
            REALM_ASSERT(target->is_group_level());
            using tf = _impl::TableFriend;
            Group* g = tf::get_parent_group(*target);
            REALM_ASSERT(g);

            // by definition, included paths contain at least one backlink
            bool backlink_paths_allowed = mapping.backlinks_allowed();
            mapping.set_allow_backlinks(true);

            std::vector<std::vector<LinkPathPart>> properties;
            for (const DescriptorOrderingState::PropertyState& cur_property : cur_ordering.properties) {
                KeyPath path = key_path_from_string(cur_property.key_path);
                size_t index = 0;
                std::vector<LinkPathPart> links;
                ConstTableRef cur_table = target;

                while (index < path.size()) {
                    KeyPathElement element = mapping.process_next_path(cur_table, path, index); // throws if invalid
                    // backlinks use type_LinkList since list operations apply to them (and is_backlink is set)
                    if (element.col_key.get_type() != col_type_Link &&
                        element.col_key.get_type() != col_type_LinkList) {
                        throw InvalidPathError(
                            util::format("Property '%1' is not a link in object of type '%2' in 'INCLUDE' clause",
                                         element.table->get_column_name(element.col_key),
                                         get_printable_table_name(*element.table)));
                    }
                    if (element.table == cur_table) {
                        if (!element.col_key) {
                            cur_table = element.table;
                        }
                        else {
                            cur_table =
                                element.table->get_link_target(element.col_key); // advance through forward link
                        }
                    }
                    else {
                        cur_table = element.table; // advance through backlink
                    }
                    ConstTableRef tr;
                    if (element.operation == KeyPathElement::KeyPathOperation::BacklinkTraversal) {
                        tr = element.table;
                        links.push_back(LinkPathPart(element.col_key, tr));
                    }
                    else
                        links.push_back(LinkPathPart(element.col_key));
                }
                properties.emplace_back(std::move(links));
            }
            ordering.append_include(IncludeDescriptor{target, properties});
            mapping.set_allow_backlinks(backlink_paths_allowed);
        }
        else {
            REALM_UNREACHABLE();
        }
    }
}

void apply_ordering(DescriptorOrdering& ordering, ConstTableRef target, const parser::DescriptorOrderingState& state,
                    parser::KeyPathMapping mapping)
{
    NoArguments args;
    apply_ordering(ordering, target, state, args, mapping);
}

} // namespace query_builder
} // namespace realm
