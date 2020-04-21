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
#include "expression_container.hpp"

#include <realm.hpp>
#include <realm/query_expression.hpp>
#include <realm/column_type.hpp>
#include <realm/utilities.hpp>

#include <sstream>

using namespace realm;
using namespace parser;
using namespace query_builder;

namespace {


template <typename T, parser::Expression::KeyPathOp OpType, typename ExpressionType>
void do_add_null_comparison_to_query(Query&, Predicate::Operator,
                                     const CollectionOperatorExpression<OpType, ExpressionType>&)
{
    throw std::logic_error("Comparing a collection aggregate operation to 'null' is not supported.");
}

template<typename T>
void do_add_null_comparison_to_query(Query &, Predicate::Operator, const ValueExpression &)
{
    throw std::logic_error("Comparing a value to 'null' is not supported.");
}

template<typename T>
void do_add_null_comparison_to_query(Query &query, Predicate::Operator op, const PropertyExpression &expr)
{
    Columns<T> column = expr.link_chain_getter().template column<T>(expr.get_dest_col_key());
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.and_query(column != realm::null());
            break;
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal:
            query.and_query(column == realm::null());
            break;
        default:
            throw std::logic_error("Only 'equal' and 'not equal' operators supported when comparing against 'null'.");
    }
}

template <typename T>
void do_add_null_comparison_to_query(Query& query, Predicate::Operator op, const PrimitiveListExpression& expr)
{
    Columns<Lst<T>> column = expr.value_of_type_for_query<T>();
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.and_query(column != realm::null());
            break;
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal:
            query.and_query(column == realm::null());
            break;
        default:
            throw std::logic_error("Only 'equal' and 'not equal' operators supported when comparing against 'null'.");
    }
}

template <>
void do_add_null_comparison_to_query<Link>(Query&, Predicate::Operator, const PrimitiveListExpression&)
{
    throw std::logic_error("Invalid query, list of primitive links is not a valid Realm contruct");
}

template<>
void do_add_null_comparison_to_query<Link>(Query &query, Predicate::Operator op, const PropertyExpression &expr)
{
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.Not();
            REALM_FALLTHROUGH;
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal: {
            Columns<Link> column = expr.value_of_type_for_query<Link>();
            query.and_query(column == realm::null());
        } break;
        default:
            throw std::logic_error("Only 'equal' and 'not equal' operators supported for object comparison.");
    }
}

// add a clause for numeric constraints based on operator type
template <typename A, typename B>
void add_numeric_constraint_to_query(Query& query,
                                     Predicate::Operator operatorType,
                                     A lhs,
                                     B rhs)
{
    switch (operatorType) {
        case Predicate::Operator::LessThan:
            query.and_query(lhs < rhs);
            break;
        case Predicate::Operator::LessThanOrEqual:
            query.and_query(lhs <= rhs);
            break;
        case Predicate::Operator::GreaterThan:
            query.and_query(lhs > rhs);
            break;
        case Predicate::Operator::GreaterThanOrEqual:
            query.and_query(lhs >= rhs);
            break;
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal:
            query.and_query(lhs == rhs);
            break;
        case Predicate::Operator::NotEqual:
            query.and_query(lhs != rhs);
            break;
        default:
            throw std::logic_error("Unsupported operator for numeric queries.");
    }
}

template <typename A, typename B>
void add_bool_constraint_to_query(Query &query, Predicate::Operator operatorType, A lhs, B rhs) {
    switch (operatorType) {
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal:
            query.and_query(lhs == rhs);
            break;
        case Predicate::Operator::NotEqual:
            query.and_query(lhs != rhs);
            break;
        default:
            throw std::logic_error("Unsupported operator for numeric queries.");
    }
}

std::string operator_description(const Predicate::Operator& op)
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
}

// (string column OR list of primitive strings) vs (string literal OR string column)
template <typename LHS, typename RHS>
std::enable_if_t<realm::is_any<LHS, Columns<String>, Columns<Lst<String>>>::value &&
                     realm::is_any<RHS, StringData, Columns<String>>::value,
                 void>
add_string_constraint_to_query(Query& query, const Predicate::Comparison& cmp, LHS&& lhs, RHS&& rhs)
{
    bool case_sensitive = (cmp.option != Predicate::OperatorOption::CaseInsensitive);
    switch (cmp.op) {
        case Predicate::Operator::BeginsWith:
            query.and_query(lhs.begins_with(rhs, case_sensitive));
            break;
        case Predicate::Operator::EndsWith:
            query.and_query(lhs.ends_with(rhs, case_sensitive));
            break;
        case Predicate::Operator::Contains:
            query.and_query(lhs.contains(rhs, case_sensitive));
            break;
        case Predicate::Operator::Equal:
            query.and_query(lhs.equal(rhs, case_sensitive));
            break;
        case Predicate::Operator::NotEqual:
            query.and_query(lhs.not_equal(rhs, case_sensitive));
            break;
        case Predicate::Operator::Like:
            query.and_query(lhs.like(rhs, case_sensitive));
            break;
        default:
            throw std::logic_error(
                util::format("Unsupported operator '%1' for string queries.", operator_description(cmp.op)));
    }
}

// ((string literal) vs (string column OR list of primitive strings)) OR ((string column) vs (list of primitive
// strings column))
template <typename LHS, typename RHS>
std::enable_if_t<(realm::is_any<LHS, StringData>::value &&
                  realm::is_any<RHS, Columns<String>, Columns<Lst<String>>>::value) ||
                     (std::is_same_v<LHS, Columns<String>> && std::is_same_v<RHS, Columns<Lst<String>>>),
                 void>
add_string_constraint_to_query(Query& query, const Predicate::Comparison& cmp, LHS&& lhs, RHS&& rhs)
{
    bool case_sensitive = (cmp.option != Predicate::OperatorOption::CaseInsensitive);
    switch (cmp.op) {
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal:
            query.and_query(rhs.equal(lhs, case_sensitive));
            break;
        case Predicate::Operator::NotEqual:
            query.and_query(rhs.not_equal(lhs, case_sensitive));
            break;
            // operators CONTAINS, BEGINSWITH, ENDSWITH, LIKE are not supported in this direction
            // These queries are not the same: "'asdf' CONTAINS string_property" vs "string_property CONTAINS 'asdf'"
        default:
            throw std::logic_error(
                util::format("Unsupported query comparison '%1' for a single string vs a string property.",
                             operator_description(cmp.op)));
    }
}

void add_string_constraint_to_query(Query&, const Predicate::Comparison&, Columns<Lst<String>>&&,
                                    Columns<Lst<String>>&&)
{
    throw std::logic_error("Comparing two primitive string lists against each other is not implemented yet.");
}


template <typename LHS, typename RHS>
std::enable_if_t<(std::is_same_v<LHS, Columns<Lst<BinaryData>>> && std::is_same_v<RHS, Columns<Lst<BinaryData>>>),
                 void>
add_binary_constraint_to_query(Query&, const Predicate::Comparison&, RHS&&, LHS&&)
{
    throw std::logic_error("Unsupported operation for binary comparison.");
}

// (column OR list of primitives) vs (literal OR column)
template <typename LHS, typename RHS>
std::enable_if_t<realm::is_any<LHS, Columns<BinaryData>, Columns<Lst<BinaryData>>>::value &&
                     realm::is_any<RHS, BinaryData, Columns<BinaryData>>::value,
                 void>
add_binary_constraint_to_query(Query& query, const Predicate::Comparison& cmp, LHS&& column, RHS&& value)
{
    bool case_sensitive = (cmp.option != Predicate::OperatorOption::CaseInsensitive);
    switch (cmp.op) {
        case Predicate::Operator::BeginsWith:
            query.and_query(column.begins_with(value, case_sensitive));
            break;
        case Predicate::Operator::EndsWith:
            query.and_query(column.ends_with(value, case_sensitive));
            break;
        case Predicate::Operator::Contains:
            query.and_query(column.contains(value, case_sensitive));
            break;
        case Predicate::Operator::Equal:
            query.and_query(column.equal(value, case_sensitive));
            break;
        case Predicate::Operator::NotEqual:
            query.and_query(column.not_equal(value, case_sensitive));
            break;
        case Predicate::Operator::Like:
            query.and_query(column.like(value, case_sensitive));
            break;
        default:
            throw std::logic_error("Unsupported operator for binary queries.");
    }
}

// ((literal value) vs (column OR list of primitive)) OR ((column) vs (list of primitive column))
template <typename LHS, typename RHS>
std::enable_if_t<(realm::is_any<LHS, BinaryData>::value &&
                  realm::is_any<RHS, Columns<BinaryData>, Columns<Lst<BinaryData>>>::value) ||
                     (std::is_same_v<LHS, Columns<BinaryData>> && std::is_same_v<RHS, Columns<Lst<BinaryData>>>),
                 void>
add_binary_constraint_to_query(realm::Query& query, const Predicate::Comparison& cmp, LHS&& value, RHS&& column)
{
    switch (cmp.op) {
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal:
            query.and_query(column == value);
            break;
        case Predicate::Operator::NotEqual:
            query.and_query(column != value);
            break;
        default:
            throw std::logic_error("Substring comparison not supported for keypath substrings.");
    }
}

template <typename LHS, typename RHS>
void add_link_constraint_to_query(realm::Query &,
                                  Predicate::Operator,
                                  const LHS &,
                                  const RHS &) {
    throw std::runtime_error("Object comparisons are currently only supported between a property and an argument.");
}

template <>
void add_link_constraint_to_query(realm::Query &query,
                                  Predicate::Operator op,
                                  const PropertyExpression &prop_expr,
                                  const ValueExpression &value_expr) {
    ObjKey obj_key = value_expr.arguments->object_index_for_argument(string_to<int>(value_expr.value->s));
    realm_precondition(prop_expr.link_chain.size() == 1, "KeyPath queries not supported for object comparisons.");
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.Not();
            REALM_FALLTHROUGH;
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal: {
            ColKey col = prop_expr.get_dest_col_key();
            query.links_to(col, obj_key);
            break;
        }
        default:
            throw std::logic_error("Only 'equal' and 'not equal' operators supported for object comparison.");
    }
}

template <>
void add_link_constraint_to_query(realm::Query &query,
                                  Predicate::Operator op,
                                  const ValueExpression &value_expr,
                                  const PropertyExpression &prop_expr) {
    // since equality is commutative we can just call the above function to the same effect
    add_link_constraint_to_query(query, op, prop_expr, value_expr);
}


template <typename A, typename B>
void do_add_comparison_to_query(Query& query, const Predicate::Comparison& cmp, A& lhs, B& rhs, DataType type)
{

    switch (type) {
        case type_Bool:
            add_bool_constraint_to_query(query, cmp.op,
                                         lhs. template value_of_type_for_query<bool>(),
                                         rhs. template value_of_type_for_query<bool>());
            break;
        case type_Timestamp:
            add_numeric_constraint_to_query(query, cmp.op,
                                            lhs. template value_of_type_for_query<Timestamp>(),
                                            rhs. template value_of_type_for_query<Timestamp>());
            break;
        case type_Double:
            add_numeric_constraint_to_query(query, cmp.op,
                                            lhs. template value_of_type_for_query<Double>(),
                                            rhs. template value_of_type_for_query<Double>());
            break;
        case type_Float:
            add_numeric_constraint_to_query(query, cmp.op,
                                            lhs. template value_of_type_for_query<Float>(),
                                            rhs. template value_of_type_for_query<Float>());
            break;
        case type_Int:
            add_numeric_constraint_to_query(query, cmp.op,
                                            lhs. template value_of_type_for_query<Int>(),
                                            rhs. template value_of_type_for_query<Int>());
            break;
        case type_String:
            add_string_constraint_to_query(query, cmp,
                                           lhs. template value_of_type_for_query<String>(),
                                           rhs. template value_of_type_for_query<String>());
            break;
        case type_Binary:
            add_binary_constraint_to_query(query, cmp,
                                           lhs. template value_of_type_for_query<Binary>(),
                                           rhs. template value_of_type_for_query<Binary>());
            break;
        case type_Link:
            add_link_constraint_to_query(query, cmp.op, lhs, rhs);
            break;
        case type_ObjectId:
            add_numeric_constraint_to_query(query, cmp.op, lhs.template value_of_type_for_query<ObjectId>(),
                                            rhs.template value_of_type_for_query<ObjectId>());
            break;
        case type_Decimal:
            add_numeric_constraint_to_query(query, cmp.op, lhs.template value_of_type_for_query<Decimal128>(),
                                            rhs.template value_of_type_for_query<Decimal128>());
            break;
        default:
            throw std::logic_error(util::format("Object type '%1' not supported", data_type_to_str(type)));
    }
}

template <>
void do_add_comparison_to_query(Query&, const Predicate::Comparison&, ValueExpression&, ValueExpression&, DataType)
{
    throw std::runtime_error("Invalid predicate: comparison between two literals is not supported.");
}

enum class NullLocation {
    NullOnLHS,
    NullOnRHS
};

template <class T>
void do_add_null_comparison_to_query(Query& query, const Predicate::Comparison& cmp, const T& expr, DataType type,
                                     NullLocation location)
{
    if (type == type_LinkList) { // this handles backlinks as well since they are set to type LinkList
        throw std::logic_error("Comparing a list property to 'null' is not supported");
    }
    switch (type) {
        case realm::type_Bool:
            do_add_null_comparison_to_query<bool>(query, cmp.op, expr);
            break;
        case realm::type_Timestamp:
            do_add_null_comparison_to_query<Timestamp>(query, cmp.op, expr);
            break;
        case realm::type_Double:
            do_add_null_comparison_to_query<Double>(query, cmp.op, expr);
            break;
        case realm::type_Float:
            do_add_null_comparison_to_query<Float>(query, cmp.op, expr);
            break;
        case realm::type_Int:
            do_add_null_comparison_to_query<Int>(query, cmp.op, expr);
            break;
        case realm::type_String: {
            if (location == NullLocation::NullOnLHS) {
                add_string_constraint_to_query(query, cmp, StringData(), expr. template value_of_type_for_query<String>());
            }
            else {
                add_string_constraint_to_query(query, cmp, expr. template value_of_type_for_query<String>(), StringData());
            }
            break;
        }
        case realm::type_Binary: {
            if (location == NullLocation::NullOnLHS) {
                add_binary_constraint_to_query(query, cmp, BinaryData(), expr. template value_of_type_for_query<Binary>());
            }
            else {
                add_binary_constraint_to_query(query, cmp, expr. template value_of_type_for_query<Binary>(), BinaryData());
            }
            break;
        }
        case realm::type_ObjectId:
            do_add_null_comparison_to_query<ObjectId>(query, cmp.op, expr);
            break;
        case realm::type_Decimal:
            do_add_null_comparison_to_query<Decimal128>(query, cmp.op, expr);
            break;
        case realm::type_Link:
            do_add_null_comparison_to_query<Link>(query, cmp.op, expr);
            break;
        default:
            throw std::logic_error(util::format("Object type '%1' not supported", util::data_type_to_str(type)));
    }
}

void add_null_comparison_to_query(Query& query, const Predicate::Comparison& cmp, ExpressionContainer& exp,
                                  NullLocation location)
{
    switch (exp.type) {
        case ExpressionContainer::ExpressionInternal::exp_Value:
            throw std::runtime_error("Unsupported query comparing 'null' and a literal. A comparison must include at least one keypath.");
        case ExpressionContainer::ExpressionInternal::exp_Property:
            do_add_null_comparison_to_query(query, cmp, exp.get_property(), exp.get_property().get_dest_type(),
                                            location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_PrimitiveList:
            do_add_null_comparison_to_query(query, cmp, exp.get_primitive_list(),
                                            exp.get_primitive_list().get_dest_type(), location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpMin:
            do_add_null_comparison_to_query(query, cmp, exp.get_min(), exp.get_min().operative_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpMax:
            do_add_null_comparison_to_query(query, cmp, exp.get_max(), exp.get_max().operative_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpSum:
            do_add_null_comparison_to_query(query, cmp, exp.get_sum(), exp.get_sum().operative_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpAvg:
            do_add_null_comparison_to_query(query, cmp, exp.get_avg(), exp.get_avg().operative_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpMinPrimitive:
            do_add_null_comparison_to_query(query, cmp, exp.get_primitive_min(),
                                            exp.get_primitive_min().operative_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpMaxPrimitive:
            do_add_null_comparison_to_query(query, cmp, exp.get_primitive_max(),
                                            exp.get_primitive_max().operative_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpSumPrimitive:
            do_add_null_comparison_to_query(query, cmp, exp.get_primitive_sum(),
                                            exp.get_primitive_sum().operative_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpAvgPrimitive:
            do_add_null_comparison_to_query(query, cmp, exp.get_primitive_avg(),
                                            exp.get_primitive_avg().operative_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_SubQuery:
            REALM_FALLTHROUGH;
        case ExpressionContainer::ExpressionInternal::exp_OpCount:
            REALM_FALLTHROUGH;
        case ExpressionContainer::ExpressionInternal::exp_OpBacklinkCount:
            REALM_FALLTHROUGH;
        case ExpressionContainer::ExpressionInternal::exp_OpSizeString:
            REALM_FALLTHROUGH;
        case ExpressionContainer::ExpressionInternal::exp_OpSizeBinary:
            REALM_FALLTHROUGH;
        case ExpressionContainer::ExpressionInternal::exp_OpCountPrimitive:
            throw std::runtime_error("Invalid predicate: comparison between 'null' and @size or @count");
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeStringPrimitive:
            REALM_FALLTHROUGH;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeBinaryPrimitive:
            throw std::runtime_error("Invalid predicate: comparison between primitive list '.length' and 'null'");
    }
}

template <typename LHS_T>
void internal_add_comparison_to_query(Query& query, LHS_T& lhs, const Predicate::Comparison& cmp,
                                      ExpressionContainer& rhs, DataType comparison_type)
{
    switch (rhs.type) {
        case ExpressionContainer::ExpressionInternal::exp_Value:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_value(), comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_Property:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_property(), comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpMin:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_min(), comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpMax:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_max(), comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpSum:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_sum(), comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpAvg:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_avg(), comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpCount:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_count(), comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpBacklinkCount:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_backlink_count(), comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpSizeString:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_size_string(), comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpSizeBinary:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_size_binary(), comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_SubQuery:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_subexpression(), comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_PrimitiveList:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_primitive_list(), comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpMinPrimitive:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_primitive_min(), comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpMaxPrimitive:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_primitive_max(), comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSumPrimitive:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_primitive_sum(), comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpAvgPrimitive:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_primitive_avg(), comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpCountPrimitive:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_primitive_count(), comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeStringPrimitive:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_primitive_string_length(), comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeBinaryPrimitive:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_primitive_binary_length(), comparison_type);
            return;
    }
}

void add_comparison_to_query(Query& query, ExpressionContainer& lhs, const Predicate::Comparison& cmp,
                             ExpressionContainer& rhs)
{
    DataType comparison_type = lhs.get_comparison_type(rhs);
    switch (lhs.type) {
        case ExpressionContainer::ExpressionInternal::exp_Value:
            internal_add_comparison_to_query(query, lhs.get_value(), cmp, rhs, comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_Property:
            internal_add_comparison_to_query(query, lhs.get_property(), cmp, rhs, comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpMin:
            internal_add_comparison_to_query(query, lhs.get_min(), cmp, rhs, comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpMax:
            internal_add_comparison_to_query(query, lhs.get_max(), cmp, rhs, comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpSum:
            internal_add_comparison_to_query(query, lhs.get_sum(), cmp, rhs, comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpAvg:
            internal_add_comparison_to_query(query, lhs.get_avg(), cmp, rhs, comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpCount:
            internal_add_comparison_to_query(query, lhs.get_count(), cmp, rhs, comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpBacklinkCount:
            internal_add_comparison_to_query(query, lhs.get_backlink_count(), cmp, rhs, comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpSizeString:
            internal_add_comparison_to_query(query, lhs.get_size_string(), cmp, rhs, comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpSizeBinary:
            internal_add_comparison_to_query(query, lhs.get_size_binary(), cmp, rhs, comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_SubQuery:
            internal_add_comparison_to_query(query, lhs.get_subexpression(), cmp, rhs, comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_PrimitiveList:
            internal_add_comparison_to_query(query, lhs.get_primitive_list(), cmp, rhs, comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpMinPrimitive:
            internal_add_comparison_to_query(query, lhs.get_primitive_min(), cmp, rhs, comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpMaxPrimitive:
            internal_add_comparison_to_query(query, lhs.get_primitive_max(), cmp, rhs, comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSumPrimitive:
            internal_add_comparison_to_query(query, lhs.get_primitive_sum(), cmp, rhs, comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpAvgPrimitive:
            internal_add_comparison_to_query(query, lhs.get_primitive_avg(), cmp, rhs, comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpCountPrimitive:
            internal_add_comparison_to_query(query, lhs.get_primitive_count(), cmp, rhs, comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeStringPrimitive:
            internal_add_comparison_to_query(query, lhs.get_primitive_string_length(), cmp, rhs, comparison_type);
            return;
        case realm::parser::ExpressionContainer::ExpressionInternal::exp_OpSizeBinaryPrimitive:
            internal_add_comparison_to_query(query, lhs.get_primitive_binary_length(), cmp, rhs, comparison_type);
            return;
    }
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
            if (e.col_type == type_LinkList || e.is_backlink) {
                list_count++;
            }
            else if (e.is_list_of_primitives) {
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
        throw std::logic_error(
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
                if (e.col_type == type_LinkList || e.is_backlink || e.is_list_of_primitives) {
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

void add_comparison_to_query(Query& query, const Predicate& pred, Arguments& args, parser::KeyPathMapping& mapping)
{
    Predicate::Comparison cmpr = pred.cmpr;
    auto lhs_type = cmpr.expr[0].type, rhs_type = cmpr.expr[1].type;

    if (!is_property_operation(lhs_type) && !is_property_operation(rhs_type)) {
        // value vs value expressions are not supported (ex: 2 < 3 or null != null)
        throw std::logic_error("Predicate expressions must compare a keypath and another keypath or a constant value");
    }
    ExpressionContainer lhs(query, cmpr.expr[0], args, mapping);
    ExpressionContainer rhs(query, cmpr.expr[1], args, mapping);

    preprocess_for_comparison_types(cmpr, lhs, rhs);

    if (lhs.is_null()) {
        add_null_comparison_to_query(query, cmpr, rhs, NullLocation::NullOnLHS);
    }
    else if (rhs.is_null()) {
        add_null_comparison_to_query(query, cmpr, lhs, NullLocation::NullOnRHS);
    }
    else {
        add_comparison_to_query(query, lhs, cmpr, rhs);
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
            add_comparison_to_query(query, pred, arguments, mapping);
            break;
        }
        case Predicate::Type::True:
            query.and_query(std::unique_ptr<realm::Expression>(new TrueExpression));
            break;

        case Predicate::Type::False:
            query.and_query(std::unique_ptr<realm::Expression>(new FalseExpression));
            break;

        default:
            throw std::logic_error("Invalid predicate type");
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
                        throw std::runtime_error(util::format(
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
                ordering.append_sort(SortDescriptor(property_columns, ascendings));
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
                    if (element.col_type != type_Link && element.col_type != type_LinkList) {
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
                    if (element.is_backlink) {
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
