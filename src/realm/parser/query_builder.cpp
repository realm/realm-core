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
    Columns<T> column = expr.table_getter()->template column<T>(expr.col_key);
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.and_query(column != realm::null());
            break;
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
    precondition(expr.indexes.empty(), "KeyPath queries not supported for object comparisons.");
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.Not();
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal:
            query.and_query(query.get_table()->column<Link>(expr.col_key).is_null());
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
    ObjKey obj_key = value_expr.arguments->object_index_for_argument(stot<int>(value_expr.value->s));
    precondition(prop_expr.indexes.empty(), "KeyPath queries not supported for object comparisons.");
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.Not();
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal: {
            ColKey col = prop_expr.col_key;
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
    if (type == type_LinkList) { // when backlinks are supported, this should check those as well
        throw std::logic_error("Comparing Lists to 'null' is not supported");
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
            do_add_null_comparison_to_query(query, cmp, exp.get_property(), exp.get_property().col_type, location);
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
        case ExpressionContainer::ExpressionInternal::exp_OpCount:
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
        case ExpressionContainer::ExpressionInternal::exp_OpSizeString:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_size_string(), comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpSizeBinary:
            do_add_comparison_to_query(query, cmp, lhs, rhs.get_size_binary(), comparison_type);
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
        case ExpressionContainer::ExpressionInternal::exp_OpSizeString:
            internal_add_comparison_to_query(query, lhs.get_size_string(), cmp, rhs, comparison_type);
            return;
        case ExpressionContainer::ExpressionInternal::exp_OpSizeBinary:
            internal_add_comparison_to_query(query, lhs.get_size_binary(), cmp, rhs, comparison_type);
            return;
    }
}

void add_comparison_to_query(Query &query, const Predicate &pred, Arguments &args)
{
    const Predicate::Comparison &cmpr = pred.cmpr;
    auto lhs_type = cmpr.expr[0].type, rhs_type = cmpr.expr[1].type;

    if (lhs_type != parser::Expression::Type::KeyPath && rhs_type != parser::Expression::Type::KeyPath) {
        // value vs value expressions are not supported (ex: 2 < 3 or null != null)
        throw std::logic_error("Predicate expressions must compare a keypath and another keypath or a constant value");
    }

    ExpressionContainer lhs(query, cmpr.expr[0], args);
    ExpressionContainer rhs(query, cmpr.expr[1], args);

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

void update_query_with_predicate(Query &query, const Predicate &pred, Arguments &arguments)
{
    if (pred.negate) {
        query.Not();
    }

    switch (pred.type) {
        case Predicate::Type::And:
            query.group();
            for (auto &sub : pred.cpnd.sub_predicates) {
                update_query_with_predicate(query, sub, arguments);
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
                update_query_with_predicate(query, sub, arguments);
            }
            if (!pred.cpnd.sub_predicates.size()) {
                query.and_query(std::unique_ptr<realm::Expression>(new FalseExpression));
            }
            query.end_group();
            break;

        case Predicate::Type::Comparison: {
            add_comparison_to_query(query, pred, arguments);
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

void apply_predicate(Query &query, const Predicate &predicate, Arguments &arguments)
{

    if (predicate.type == Predicate::Type::True && !predicate.negate) {
        // early out for a predicate which should return all results
        return;
    }

    update_query_with_predicate(query, predicate, arguments);

    // Test the constructed query in core
    std::string validateMessage = query.validate();
    precondition(validateMessage.empty(), validateMessage.c_str());
}

struct EmptyArgContext
{
    template<typename T>
    T unbox(std::string) {
        return T{}; //dummy
    }
    bool is_null(std::string) {
        return false;
    }
};

void apply_predicate(Query &query, const Predicate &predicate)
{
    EmptyArgContext ctx;
    std::string empty_string;
    realm::query_builder::ArgumentConverter<std::string, EmptyArgContext> args(ctx, &empty_string, 0);

    apply_predicate(query, predicate, args);
}

void apply_ordering(DescriptorOrdering& ordering, TableRef target, const parser::DescriptorOrderingState& state, Arguments&)
{
    for (const DescriptorOrderingState::SingleOrderingState& cur_ordering : state.orderings) {
        std::vector<std::vector<ColKey>> property_indices;
        std::vector<bool> ascendings;
        for (const DescriptorOrderingState::PropertyState& cur_property : cur_ordering.properties) {
            KeyPath path = key_path_from_string(cur_property.key_path);
            std::vector<ColKey> indices;
            TableRef cur_table = target;
            for (size_t ndx_in_path = 0; ndx_in_path < path.size(); ++ndx_in_path) {
                ColKey col_key = cur_table->get_column_key(path[ndx_in_path]);
                if (!col_key) {
                    throw std::runtime_error(
                        util::format("No property '%1' found on object type '%2' specified in '%3' clause",
                            path[ndx_in_path], cur_table->get_name(), cur_ordering.is_distinct ? "distinct" : "sort"));
                }
                indices.push_back(col_key);
                if (ndx_in_path < path.size() - 1) {
                    cur_table = cur_table->get_link_target(col_key);
                }
            }
            property_indices.push_back(indices);
            ascendings.push_back(cur_property.ascending);
        }

        if (cur_ordering.is_distinct) {
            ordering.append_distinct(DistinctDescriptor(*target, property_indices));
        } else {
            ordering.append_sort(SortDescriptor(*target, property_indices, ascendings));
        }
    }
}

void apply_ordering(DescriptorOrdering& ordering, TableRef target, const parser::DescriptorOrderingState& state)
{
    EmptyArgContext ctx;
    std::string empty_string;
    realm::query_builder::ArgumentConverter<std::string, EmptyArgContext> args(ctx, &empty_string, 0);

    apply_ordering(ordering, target, state, args);
}

} // namespace query_builder
} // namespace realm
