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


template<typename T, parser::Expression::KeyPathOp OpType>
void do_add_null_comparison_to_query(Query &, Predicate::Operator, const CollectionOperatorExpression<OpType> &)
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
    Columns<T> column = expr.table_getter()->template column<T>(expr.get_dest_ndx());
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

template<>
void do_add_null_comparison_to_query<Link>(Query &query, Predicate::Operator op, const PropertyExpression &expr)
{
    realm_precondition(expr.link_chain.size() == 1, "KeyPath queries not supported for object comparisons.");
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.Not();
            REALM_FALLTHROUGH;
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal:
            query.and_query(query.get_table()->column<Link>(expr.get_dest_ndx()).is_null());
            break;
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

void add_string_constraint_to_query(Query &query,
                                    Predicate::Comparison cmp,
                                    Columns<String> &&column,
                                    StringData &&value) {
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
            throw std::logic_error("Unsupported operator for string queries.");
    }
}

void add_string_constraint_to_query(realm::Query &query,
                                    Predicate::Comparison cmp,
                                    StringData &&value,
                                    Columns<String> &&column) {
    bool case_sensitive = (cmp.option != Predicate::OperatorOption::CaseInsensitive);
    switch (cmp.op) {
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal:
            query.and_query(column.equal(value, case_sensitive));
            break;
        case Predicate::Operator::NotEqual:
            query.and_query(column.not_equal(value, case_sensitive));
            break;
            // operators CONTAINS, BEGINSWITH, ENDSWITH, LIKE are not supported in this direction
            // These queries are not the same: "'asdf' CONTAINS string_property" vs "string_property CONTAINS 'asdf'"
        default:
            throw std::logic_error("Unsupported operator for keypath substring queries.");
    }
}

void add_string_constraint_to_query(realm::Query &query,
                                    Predicate::Comparison cmp,
                                    Columns<String> &&lhs_col,
                                    Columns<String> &&rhs_col) {
    bool case_sensitive = (cmp.option != Predicate::OperatorOption::CaseInsensitive);
    switch (cmp.op) {
        case Predicate::Operator::BeginsWith:
            query.and_query(lhs_col.begins_with(rhs_col, case_sensitive));
            break;
        case Predicate::Operator::EndsWith:
            query.and_query(lhs_col.ends_with(rhs_col, case_sensitive));
            break;
        case Predicate::Operator::Contains:
            query.and_query(lhs_col.contains(rhs_col, case_sensitive));
            break;
        case Predicate::Operator::Equal:
            query.and_query(lhs_col.equal(rhs_col, case_sensitive));
            break;
        case Predicate::Operator::NotEqual:
            query.and_query(lhs_col.not_equal(rhs_col, case_sensitive));
            break;
        case Predicate::Operator::Like:
            query.and_query(lhs_col.like(rhs_col, case_sensitive));
            break;
        default:
            throw std::logic_error("Unsupported operator for string queries.");
    }
}

void add_binary_constraint_to_query(Query &query,
                                    Predicate::Comparison cmp,
                                    Columns<Binary> &&column,
                                    BinaryData &&value) {
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

void add_binary_constraint_to_query(realm::Query &query,
                                    Predicate::Comparison cmp,
                                    BinaryData &&value,
                                    Columns<Binary> &&column) {
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

void add_binary_constraint_to_query(Query &query,
                                    Predicate::Comparison cmp,
                                    Columns<Binary> &&lhs_col,
                                    Columns<Binary> &&rhs_col) {
    bool case_sensitive = (cmp.option != Predicate::OperatorOption::CaseInsensitive);
    switch (cmp.op) {
        case Predicate::Operator::BeginsWith:
            query.and_query(lhs_col.begins_with(rhs_col, case_sensitive));
            break;
        case Predicate::Operator::EndsWith:
            query.and_query(lhs_col.ends_with(rhs_col, case_sensitive));
            break;
        case Predicate::Operator::Contains:
            query.and_query(lhs_col.contains(rhs_col, case_sensitive));
            break;
        case Predicate::Operator::Equal:
            query.and_query(lhs_col.equal(rhs_col, case_sensitive));
            break;
        case Predicate::Operator::NotEqual:
            query.and_query(lhs_col.not_equal(rhs_col, case_sensitive));
            break;
        default:
            throw std::logic_error("Unsupported operator for binary queries.");
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
    size_t row_index = value_expr.arguments->object_index_for_argument(stot<int>(value_expr.value->s));
    realm_precondition(prop_expr.link_chain.size() == 1, "KeyPath queries not supported for object comparisons.");
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.Not();
            REALM_FALLTHROUGH;
        case Predicate::Operator::In:
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal: {
            size_t col = prop_expr.get_dest_ndx();
            query.links_to(col, query.get_table()->get_link_target(col)->get(row_index));
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
void do_add_comparison_to_query(Query &query, Predicate::Comparison cmp, A &lhs, B &rhs, DataType type)
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
        default:
            throw std::logic_error(util::format("Object type '%1' not supported", data_type_to_str(type)));
    }
}

template <>
void do_add_comparison_to_query(Query&, Predicate::Comparison, ValueExpression&, ValueExpression&, DataType)
{
    throw std::runtime_error("Invalid predicate: comparison between two literals is not supported.");
}

enum class NullLocation {
    NullOnLHS,
    NullOnRHS
};

template <class T>
void do_add_null_comparison_to_query(Query &query, Predicate::Comparison cmp, const T &expr, DataType type, NullLocation location)
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
        case realm::type_Link:
            do_add_null_comparison_to_query<Link>(query, cmp.op, expr);
            break;
        default:
            throw std::logic_error(util::format("Object type '%1' not supported", util::data_type_to_str(type)));
    }
}

void add_null_comparison_to_query(Query &query, Predicate::Comparison cmp, ExpressionContainer& exp, NullLocation location)
{
    switch (exp.type) {
        case ExpressionContainer::ExpressionInternal::exp_Value:
            throw std::runtime_error("Unsupported query comparing 'null' and a literal. A comparison must include at least one keypath.");
        case ExpressionContainer::ExpressionInternal::exp_Property:
            do_add_null_comparison_to_query(query, cmp, exp.get_property(), exp.get_property().get_dest_type(), location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpMin:
            do_add_null_comparison_to_query(query, cmp, exp.get_min(), exp.get_min().post_link_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpMax:
            do_add_null_comparison_to_query(query, cmp, exp.get_max(), exp.get_max().post_link_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpSum:
            do_add_null_comparison_to_query(query, cmp, exp.get_sum(), exp.get_sum().post_link_col_type, location);
            break;
        case ExpressionContainer::ExpressionInternal::exp_OpAvg:
            do_add_null_comparison_to_query(query, cmp, exp.get_avg(), exp.get_avg().post_link_col_type, location);
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
            throw std::runtime_error("Invalid predicate: comparison between 'null' and @size or @count");
    }
}

template <typename LHS_T>
void internal_add_comparison_to_query(Query& query, LHS_T& lhs, Predicate::Comparison cmp, ExpressionContainer& rhs, DataType comparison_type)
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
    }
}

void add_comparison_to_query(Query &query, ExpressionContainer& lhs, Predicate::Comparison cmp, ExpressionContainer& rhs)
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
    }
}


std::pair<std::string, std::string> separate_list_parts(PropertyExpression& pe) {
    std::string pre_and_list = "";
    std::string post_list = "";
    bool past_list = false;
    for (KeyPathElement& e : pe.link_chain) {
        std::string cur_name;
        if (e.is_backlink) {
            cur_name = std::string("@links") + util::serializer::value_separator + std::string(e.table->get_name()) + util::serializer::value_separator + std::string(e.table->get_column_name(e.col_ndx));
        } else {
            cur_name = e.table->get_column_name(e.col_ndx);
        }
        if (!past_list) {
            if (!pre_and_list.empty()) {
                pre_and_list += util::serializer::value_separator;
            }
            pre_and_list += cur_name;
        } else {
            realm_precondition(!e.is_backlink && e.col_type != type_LinkList, util::format("The keypath after '%1' must not contain any additional list properties, but '%2' is a list.", pre_and_list, cur_name));
            if (!post_list.empty()) {
                post_list += util::serializer::value_separator;
            }
            post_list += cur_name;
        }
        if (e.is_backlink || e.col_type == type_LinkList) {
            past_list = true;
        }
    }
    return {pre_and_list, post_list};
}


// some query types are not supported in core but can be produced by a transformation:
// "ALL path.to.list.property >= 20"    --> "SUBQUERY(path.to.list, $x, $x.property >= 20).@count == path.to.list.@count"
// "NONE path.to.list.property >= 20"   --> "SUBQUERY(path.to.list, $x, $x.property >= 20).@count == 0"
void preprocess_for_comparison_types(Query &query, Predicate::Comparison &cmpr, ExpressionContainer &lhs, ExpressionContainer &rhs, Arguments &args, parser::KeyPathMapping& mapping)
{
    auto get_cmp_type_name = [&]() {
        if (cmpr.compare_type == Predicate::ComparisonType::Any) {
            return util::format("'%1' or 'SOME'", comparison_type_to_str(Predicate::ComparisonType::Any));
        }
        return util::format("'%1'", comparison_type_to_str(cmpr.compare_type));
    };

    if (cmpr.compare_type != Predicate::ComparisonType::Unspecified)
    {
        realm_precondition(lhs.type == ExpressionContainer::ExpressionInternal::exp_Property,
                           util::format("The expression after %1 must be a keypath containing a list",
                                        get_cmp_type_name()));
        size_t list_count = 0;
        for (KeyPathElement e : lhs.get_property().link_chain) {
            if (e.col_type == type_LinkList || e.is_backlink) {
                list_count++;
            }
        }
        realm_precondition(list_count > 0, util::format("The keypath following %1 must contain a list",
                                                         get_cmp_type_name()));
        realm_precondition(list_count == 1, util::format("The keypath following %1 must contain only one list",
                                                        get_cmp_type_name()));
    }

    if (cmpr.compare_type == Predicate::ComparisonType::All || cmpr.compare_type == Predicate::ComparisonType::None) {
        realm_precondition(rhs.type == ExpressionContainer::ExpressionInternal::exp_Value,
                           util::format("The comparison in an %1 clause must be between a keypath and a value",
                                        get_cmp_type_name()));

        parser::Expression exp(parser::Expression::Type::SubQuery);
        std::pair<std::string, std::string> path_parts = separate_list_parts(lhs.get_property());
        exp.subquery_path = path_parts.first;

        util::serializer::SerialisationState temp_state;
        std::string var_name;
        for (var_name = temp_state.get_variable_name(query.get_table());
             mapping.has_mapping(query.get_table(), var_name);
             var_name = temp_state.get_variable_name(query.get_table())) {
            temp_state.subquery_prefix_list.push_back(var_name);
        }

        exp.subquery_var = var_name;
        exp.subquery = std::make_shared<Predicate>(Predicate::Type::Comparison);
        exp.subquery->cmpr.expr[0] = parser::Expression(parser::Expression::Type::KeyPath, var_name + util::serializer::value_separator + path_parts.second);
        exp.subquery->cmpr.op = cmpr.op;
        exp.subquery->cmpr.option = cmpr.option;
        exp.subquery->cmpr.expr[1] = cmpr.expr[1];
        cmpr.expr[0] = exp;

        lhs = ExpressionContainer(query, cmpr.expr[0], args, mapping);
        cmpr.op = parser::Predicate::Operator::Equal;
        cmpr.option = parser::Predicate::OperatorOption::None;

        if (cmpr.compare_type == Predicate::ComparisonType::All) {
            cmpr.expr[1] = parser::Expression(path_parts.first, parser::Expression::KeyPathOp::Count, "");
            rhs = ExpressionContainer(query, cmpr.expr[1], args, mapping);
        } else if (cmpr.compare_type == Predicate::ComparisonType::None) {
            cmpr.expr[1] = parser::Expression(parser::Expression::Type::Number, "0");
            rhs = ExpressionContainer(query, cmpr.expr[1], args, mapping);
        } else {
            REALM_UNREACHABLE();
        }
    }

    // Check that operator "IN" has a RHS keypath which is a list
    if (cmpr.op == Predicate::Operator::In) {
        realm_precondition(rhs.type == ExpressionContainer::ExpressionInternal::exp_Property,
                           "The expression following 'IN' must be a keypath to a list");
        auto get_list_count = [](const std::vector<KeyPathElement>& target_link_chain) {
            size_t list_count = 0;
            for (KeyPathElement e : target_link_chain) {
                if (e.col_type == type_LinkList || e.is_backlink) {
                    list_count++;
                }
            }
            return list_count;
        };
        if (lhs.type == ExpressionContainer::ExpressionInternal::exp_Property) {
            // For list vs list comparisons, all the right code paths are hooked up, but we just don't define the
            // actual behaviour, see the FIXME in query_expressions.hpp in Value::compare about many-to-many links
            // Without this check here, we would assert in debug mode and always return false in release mode.
            size_t lhs_list_count = get_list_count(lhs.get_property().link_chain);
            realm_precondition(lhs_list_count == 0, "The keypath preceeding 'IN' must not contain a list, list vs list comparisons are not currently supported");
        }

        size_t rhs_list_count = get_list_count(rhs.get_property().link_chain);
        realm_precondition(rhs_list_count > 0, "The keypath following 'IN' must contain a list");
        realm_precondition(rhs_list_count == 1, "The keypath following 'IN' must contain only one list");
    }
}


bool is_property_operation(parser::Expression::Type type)
{
    return type == parser::Expression::Type::KeyPath || type == parser::Expression::Type::SubQuery;
}

void add_comparison_to_query(Query &query, const Predicate &pred, Arguments &args, parser::KeyPathMapping& mapping)
{
    Predicate::Comparison cmpr = pred.cmpr;
    auto lhs_type = cmpr.expr[0].type, rhs_type = cmpr.expr[1].type;

    if (!is_property_operation(lhs_type) && !is_property_operation(rhs_type)) {
        // value vs value expressions are not supported (ex: 2 < 3 or null != null)
        throw std::logic_error("Predicate expressions must compare a keypath and another keypath or a constant value");
    }
    ExpressionContainer lhs(query, cmpr.expr[0], args, mapping);
    ExpressionContainer rhs(query, cmpr.expr[1], args, mapping);

    preprocess_for_comparison_types(query, cmpr, lhs, rhs, args, mapping);

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

void update_query_with_predicate(Query &query, const Predicate &pred, Arguments &arguments, parser::KeyPathMapping& mapping)
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

void apply_predicate(Query &query, const Predicate &predicate, Arguments &arguments, parser::KeyPathMapping mapping)
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

void apply_ordering(DescriptorOrdering& ordering, ConstTableRef target, const parser::DescriptorOrderingState& state, Arguments&, parser::KeyPathMapping mapping)
{
    for (const DescriptorOrderingState::SingleOrderingState& cur_ordering : state.orderings) {
        if (cur_ordering.type == DescriptorOrderingState::SingleOrderingState::DescriptorType::Limit) {
            ordering.append_limit(cur_ordering.limit);
        }
        else if (cur_ordering.type == DescriptorOrderingState::SingleOrderingState::DescriptorType::Distinct
                   || cur_ordering.type == DescriptorOrderingState::SingleOrderingState::DescriptorType::Sort) {
            bool is_distinct = cur_ordering.type == DescriptorOrderingState::SingleOrderingState::DescriptorType::Distinct;
            std::vector<std::vector<size_t>> property_indices;
            std::vector<bool> ascendings;
            for (const DescriptorOrderingState::PropertyState& cur_property : cur_ordering.properties) {
                KeyPath path = key_path_from_string(cur_property.key_path);
                std::vector<size_t> indices;
                ConstTableRef cur_table = target;
                for (size_t ndx_in_path = 0; ndx_in_path < path.size(); ++ndx_in_path) {
                    size_t col_ndx = cur_table->get_column_index(path[ndx_in_path]);
                    if (col_ndx == realm::not_found) {
                        throw std::runtime_error(
                            util::format("No property '%1' found on object type '%2' specified in '%3' clause",
                                path[ndx_in_path], cur_table->get_name(),
                                         is_distinct ? "distinct" : "sort"));
                    }
                    indices.push_back(col_ndx);
                    if (ndx_in_path < path.size() - 1) {
                        cur_table = cur_table->get_link_target(col_ndx);
                    }
                }
                property_indices.push_back(indices);
                ascendings.push_back(cur_property.ascending);
            }

            if (is_distinct) {
                ordering.append_distinct(DistinctDescriptor{*target.get(), property_indices});
            }
            else {
                ordering.append_sort(SortDescriptor{*target.get(), property_indices, ascendings});
            }
        }
        else if (cur_ordering.type == DescriptorOrderingState::SingleOrderingState::DescriptorType::Include) {
            REALM_ASSERT(target->is_group_level());
            using tf = _impl::TableFriend;
            Group* g = tf::get_parent_group(*target);
            REALM_ASSERT(g);

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
                        throw InvalidPathError(util::format("Property '%1' is not a link in object of type '%2' in 'INCLUDE' clause",
                                                        element.table->get_column_name(element.col_ndx),
                                                        get_printable_table_name(*element.table)));
                   }
                    if (element.table == cur_table) {
                        if (element.col_ndx == realm::npos) {
                            cur_table = element.table;
                        } else {
                            cur_table = element.table->get_link_target(element.col_ndx); // advance through forward link
                        }
                    } else {
                        cur_table = element.table; // advance through backlink
                    }
                    ConstTableRef tr;
                    if (element.is_backlink) {
                        tr = element.table;
                    }
                    links.push_back(LinkPathPart(element.col_ndx, tr));
                }
                properties.emplace_back(std::move(links));
            }
            ordering.append_include(IncludeDescriptor{*target.get(), properties});
        }
        else {
            REALM_UNREACHABLE();
        }
    }
}

void apply_ordering(DescriptorOrdering& ordering, ConstTableRef target, const parser::DescriptorOrderingState& state, parser::KeyPathMapping mapping)
{
    NoArguments args;
    apply_ordering(ordering, target, state, args, mapping);
}

} // namespace query_builder
} // namespace realm
