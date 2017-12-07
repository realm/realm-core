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

#include <realm.hpp>
#include <realm/query_expression.hpp>
#include <realm/column_type.hpp>

#include <time.h>
#include <sstream>

using namespace realm;
using namespace parser;
using namespace query_builder;

namespace {
template<typename T>
T stot(std::string const& s) {
    std::istringstream iss(s);
    T value;
    iss >> value;
    if (iss.fail()) {
        throw std::invalid_argument(util::format("Cannot convert string '%1'", s));
    }
    return value;
}

Timestamp get_timestamp_if_valid(int64_t seconds, int32_t nanoseconds) {
    const bool both_non_negative = seconds >= 0 && nanoseconds >= 0;
    const bool both_non_positive = seconds <= 0 && nanoseconds <= 0;
    if (both_non_negative || both_non_positive) {
        return Timestamp(seconds, nanoseconds);
    }
    throw std::runtime_error("Invalid timestamp format");
}

Timestamp from_timestamp_values(std::vector<std::string> const& time_inputs) {

    if (time_inputs.size() == 2) {
        // internal format seconds, nanoseconds
        int64_t seconds = stot<int64_t>(time_inputs[0]);
        int32_t nanoseconds = stot<int32_t>(time_inputs[1]);
        return get_timestamp_if_valid(seconds, nanoseconds);
    }
    else if (time_inputs.size() == 6 || time_inputs.size() == 7) {
        // readable format YYYY-MM-DD-HH:MM:SS:NANOS nanos optional
        tm created;
        created.tm_year = stot<int>(time_inputs[0]) - 1900; // epoch offset (see man mktime)
        created.tm_mon = stot<int>(time_inputs[1]) - 1; // converts from 1-12 to 0-11
        created.tm_mday = stot<int>(time_inputs[2]);
        created.tm_hour = stot<int>(time_inputs[3]);
        created.tm_min = stot<int>(time_inputs[4]);
        created.tm_sec = stot<int>(time_inputs[5]);

        std::time_t unix_time = timegm(&created); // UTC time

        int64_t seconds = int64_t(static_cast<int32_t>(unix_time)); // unix_time comes as a int32_t
        int32_t nanoseconds = 0;
        if (time_inputs.size() == 7) {
            nanoseconds = stot<int32_t>(time_inputs[6]);
        }
        return get_timestamp_if_valid(seconds, nanoseconds);
    }
    throw std::runtime_error("Unexpected timestamp format.");
}

// check a precondition and throw an exception if it is not met
// this should be used iff the condition being false indicates a bug in the caller
// of the function checking its preconditions
#define precondition(condition, message) if (!REALM_LIKELY(condition)) { throw std::logic_error(message); }

using KeyPath = std::vector<std::string>;
KeyPath key_path_from_string(const std::string &s) {
    std::stringstream ss(s);
    std::string item;
    KeyPath key_path;
    while (std::getline(ss, item, '.')) {
        key_path.push_back(item);
    }
    return key_path;
}

struct PropertyExpression
{
    std::vector<size_t> indexes;
    std::function<Table *()> table_getter;
    size_t col_ndx;
    DataType col_type;

    PropertyExpression(Query &query, const std::string &key_path_string)
    {
        KeyPath key_path = key_path_from_string(key_path_string);
        TableRef cur_table = query.get_table();;
        for (size_t index = 0; index < key_path.size(); index++) {
            size_t cur_col_ndx = cur_table->get_column_index(key_path[index]);
            StringData table_name = cur_table->get_name();

            precondition(cur_col_ndx != realm::not_found,
                         util::format("No property '%1' on object of type '%2'", key_path[index], table_name));

            DataType cur_col_type = cur_table->get_column_type(cur_col_ndx);
            if (index != key_path.size() - 1) {
                precondition(cur_col_type == type_Link,
                             util::format("Property '%1' is not a link in object of type '%2'", key_path[index], table_name));
                indexes.push_back(cur_col_ndx);
            }
            else {
                col_ndx = cur_col_ndx;
                col_type = cur_col_type;
            }
        }

        table_getter = [&] {
            auto& tbl = query.get_table();
            for (size_t col : indexes) {
                tbl->link(col); // mutates m_link_chain on table
            }
            return tbl.get();
        };
    }
};

const char* collection_operator_to_str(parser::Expression::KeyPathOp op)
{
    switch (op) {
        case parser::Expression::KeyPathOp::None:
            return "None";
        case parser::Expression::KeyPathOp::Min:
            return "Min";
        case parser::Expression::KeyPathOp::Max:
            return "Max";
        case parser::Expression::KeyPathOp::Sum:
            return "Sum";
        case parser::Expression::KeyPathOp::Avg:
            return "Avg";
        case parser::Expression::KeyPathOp::Size:
            return "Size";
        case parser::Expression::KeyPathOp::Count:
            return "Count";
    }
}

struct CollectionOperatorExpression
{
    std::function<Table *()> table_getter;
    PropertyExpression pe;
    parser::Expression::KeyPathOp op;
    size_t post_link_col_ndx;
    DataType post_link_col_type;
    CollectionOperatorExpression(PropertyExpression exp, parser::Expression::KeyPathOp o, std::string suffix_path)
    : pe(exp)
    , op(o)
    , post_link_col_ndx(realm::not_found)
    {
        table_getter = pe.table_getter;

        const bool requires_expression_link = op != parser::Expression::KeyPathOp::Size;
        if (requires_expression_link) {
            if (pe.col_type != type_LinkList) {
                throw std::runtime_error(std::string("Column aggregates must operate on a List"));
            }
            TableRef post_table = pe.table_getter()->get_link_target(pe.col_ndx);
            if (!post_table) {
                throw std::runtime_error(util::format("Internal error: could not reach destination table through '%1'", suffix_path));
            }
        }
        else {
            post_link_col_type = pe.col_type;
        }

        const bool requires_suffix_path = !(op == parser::Expression::KeyPathOp::Size
                                            || op == parser::Expression::KeyPathOp::Count);

        if (requires_suffix_path) {
            KeyPath suffix_key_path = key_path_from_string(suffix_path);
            if (suffix_path.size() == 0 || suffix_key_path.size() == 0) {
                throw std::runtime_error(std::string("A property must be provided at the destination object to perform operation ") + collection_operator_to_str(op));
            }
            if (suffix_key_path.size() > 1) {
                throw std::runtime_error(std::string("Collection aggregate operations are only supported for direct properties at this time: ") + suffix_path);
            }
            post_link_col_ndx = pe.table_getter()->get_link_target(pe.col_ndx)->get_column_index(suffix_key_path[0]);
            if (post_link_col_ndx == realm::not_found) {
                throw std::runtime_error(std::string("Destination column not found: ") + suffix_key_path[0]);
            }
            post_link_col_type = pe.table_getter()->get_link_target(pe.col_ndx)->get_column_type(post_link_col_ndx);
        }
        else {
            if (!suffix_path.empty()) {
                throw std::runtime_error(util::format("An extraneous property on the destination object '%1' was found for aggregate operation %2", suffix_path, collection_operator_to_str(op)));
            }
        }
    }
};

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
        default:
            throw std::logic_error("Substring comparison not supported for keypath substrings.");
    }
}

void add_binary_constraint_to_query(Query &query,
                                    Predicate::Operator op,
                                    Columns<Binary> &&column,
                                    BinaryData &&value) {
    switch (op) {
        case Predicate::Operator::BeginsWith:
            query.begins_with(column.column_ndx(), BinaryData(value));
            break;
        case Predicate::Operator::EndsWith:
            query.ends_with(column.column_ndx(), BinaryData(value));
            break;
        case Predicate::Operator::Contains:
            query.contains(column.column_ndx(), BinaryData(value));
            break;
        case Predicate::Operator::Equal:
            query.equal(column.column_ndx(), BinaryData(value));
            break;
        case Predicate::Operator::NotEqual:
            query.not_equal(column.column_ndx(), BinaryData(value));
            break;
        default:
            throw std::logic_error("Unsupported operator for binary queries.");
    }
}

void add_binary_constraint_to_query(realm::Query &query,
                                    Predicate::Operator op,
                                    BinaryData value,
                                    Columns<Binary> &&column) {
    switch (op) {
        case Predicate::Operator::Equal:
            query.equal(column.column_ndx(), BinaryData(value));
            break;
        case Predicate::Operator::NotEqual:
            query.not_equal(column.column_ndx(), BinaryData(value));
            break;
        default:
            throw std::logic_error("Substring comparison not supported for keypath substrings.");
    }
}

void add_link_constraint_to_query(realm::Query &query,
                                  Predicate::Operator op,
                                  const PropertyExpression &prop_expr,
                                  size_t row_index) {
    precondition(prop_expr.indexes.empty(), "KeyPath queries not supported for object comparisons.");
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.Not();
            //REALM_FALLTHROUGH;
        case Predicate::Operator::Equal: {
            size_t col = prop_expr.col_ndx;
            query.links_to(col, query.get_table()->get_link_target(col)->get(row_index));
            break;
        }
        default:
            throw std::logic_error("Only 'equal' and 'not equal' operators supported for object comparison.");
    }
}

auto link_argument(const PropertyExpression&, const parser::Expression &argExpr, Arguments &args)
{
    return args.object_index_for_argument(stot<int>(argExpr.s));
}

auto link_argument(const parser::Expression &argExpr, const PropertyExpression&, Arguments &args)
{
    return args.object_index_for_argument(stot<int>(argExpr.s));
}


template <typename RetType, typename TableGetter>
struct ColumnGetter {
    static Columns<RetType> convert(TableGetter&& table, const PropertyExpression& expr, Arguments&)
    {
        return table()->template column<RetType>(expr.col_ndx);
    }
};

template <typename RetType, typename TableGetter, class AggOpType>
struct CollectionOperatorGetter {
    static SubColumnAggregate<RetType, AggOpType > convert(TableGetter&&, const CollectionOperatorExpression&, Arguments&) {
        throw std::runtime_error("Predicate error constructing collection aggregate operation");
    }
};

template <typename RetType, typename TableGetter>
struct CollectionOperatorGetter<RetType, TableGetter, aggregate_operations::Minimum<RetType> >{
    static SubColumnAggregate<RetType, aggregate_operations::Minimum<RetType> > convert(TableGetter&& table, const CollectionOperatorExpression& expr, Arguments&)
    {
        return table()->template column<Link>(expr.pe.col_ndx).template column<RetType>(expr.post_link_col_ndx).min();
    }
};

template <typename RetType, typename TableGetter>
struct CollectionOperatorGetter<RetType, TableGetter, aggregate_operations::Maximum<RetType> >{
    static SubColumnAggregate<RetType, aggregate_operations::Maximum<RetType> > convert(TableGetter&& table, const CollectionOperatorExpression& expr, Arguments&)
    {
        return table()->template column<Link>(expr.pe.col_ndx).template column<RetType>(expr.post_link_col_ndx).max();
    }
};

template <typename RetType, typename TableGetter>
struct CollectionOperatorGetter<RetType, TableGetter, aggregate_operations::Sum<RetType> >{
    static SubColumnAggregate<RetType, aggregate_operations::Sum<RetType> > convert(TableGetter&& table, const CollectionOperatorExpression& expr, Arguments&)
    {
        return table()->template column<Link>(expr.pe.col_ndx).template column<RetType>(expr.post_link_col_ndx).sum();
    }
};

template <typename RetType, typename TableGetter>
struct CollectionOperatorGetter<RetType, TableGetter, aggregate_operations::Average<RetType> >{
    static SubColumnAggregate<RetType, aggregate_operations::Average<RetType> > convert(TableGetter&& table, const CollectionOperatorExpression& expr, Arguments&)
    {
        return table()->template column<Link>(expr.pe.col_ndx).template column<RetType>(expr.post_link_col_ndx).average();
    }
};

template <typename RetType, typename TableGetter>
struct CollectionOperatorGetter<RetType, TableGetter, LinkCount >{
    static LinkCount convert(TableGetter&& table, const CollectionOperatorExpression& expr, Arguments&)
    {
        return table()->template column<Link>(expr.pe.col_ndx).count();
    }
};

template <typename RetType, typename TableGetter>
struct CollectionOperatorGetter<RetType, TableGetter, SizeOperator<Size<RetType> > >{
    static SizeOperator<Size<RetType> > convert(TableGetter&& table, const CollectionOperatorExpression& expr, Arguments&)
    {
        return table()->template column<RetType>(expr.pe.col_ndx).size();
    }
};



template <typename RequestedType, typename TableGetter>
struct ValueGetter;



template <typename TableGetter>
struct ValueGetter<Timestamp, TableGetter> {
    static Timestamp convert(TableGetter&&, const parser::Expression & value, Arguments &args)
    {
        if (value.type == parser::Expression::Type::Argument) {
            return args.timestamp_for_argument(stot<int>(value.s));
        } else if (value.type == parser::Expression::Type::Timestamp) {
            return from_timestamp_values(value.time_inputs);
        } else if (value.type == parser::Expression::Type::Null) {
            return Timestamp(realm::null());
        }
        throw std::logic_error("Attempting to compare Timestamp property to a non-Timestamp value");
    }
};

template <typename TableGetter>
struct ValueGetter<bool, TableGetter> {
    static bool convert(TableGetter&&, const parser::Expression & value, Arguments &args)
    {
        if (value.type == parser::Expression::Type::Argument) {
            return args.bool_for_argument(stot<int>(value.s));
        }
        if (value.type != parser::Expression::Type::True && value.type != parser::Expression::Type::False) {
            throw std::logic_error("Attempting to compare bool property to a non-bool value");
        }
        return value.type == parser::Expression::Type::True;
    }
};

template <typename TableGetter>
struct ValueGetter<Double, TableGetter> {
    static Double convert(TableGetter&&, const parser::Expression & value, Arguments &args)
    {
        if (value.type == parser::Expression::Type::Argument) {
            return args.double_for_argument(stot<int>(value.s));
        }
        return stot<double>(value.s);
    }
};

template <typename TableGetter>
struct ValueGetter<Float, TableGetter> {
    static Float convert(TableGetter&&, const parser::Expression & value, Arguments &args)
    {
        if (value.type == parser::Expression::Type::Argument) {
            return args.float_for_argument(stot<int>(value.s));
        }
        return stot<float>(value.s);
    }
};

template <typename TableGetter>
struct ValueGetter<Int, TableGetter> {
    static Int convert(TableGetter&&, const parser::Expression & value, Arguments &args)
    {
        if (value.type == parser::Expression::Type::Argument) {
            return args.long_for_argument(stot<int>(value.s));
        }
        return stot<long long>(value.s);
    }
};

template <typename TableGetter>
struct ValueGetter<String, TableGetter> {
    static StringData convert(TableGetter&&, const parser::Expression & value, Arguments &args)
    {
        if (value.type == parser::Expression::Type::Argument) {
            return args.string_for_argument(stot<int>(value.s));
        }
        if (value.type != parser::Expression::Type::String) {
            throw std::logic_error("Attempting to compare String property to a non-String value");
        }
        return value.s;
    }
};

template <typename TableGetter>
struct ValueGetter<Binary, TableGetter> {
    static BinaryData convert(TableGetter&&, const parser::Expression & value, Arguments &args)
    {
        if (value.type == parser::Expression::Type::Argument) {
            return args.binary_for_argument(stot<int>(value.s));
        }
        throw std::logic_error("Binary properties must be compared against a binary argument.");
    }
};

template <typename RetType, typename Value, typename TableGetter>
auto value_of_type_for_query(TableGetter&& tables, Value&& value, Arguments &args)
{
    const bool isColumn = std::is_same<PropertyExpression, typename std::remove_reference<Value>::type>::value;
    using helper = std::conditional_t<isColumn, ColumnGetter<RetType, TableGetter>, ValueGetter<RetType, TableGetter> >;
    return helper::convert(tables, value, args);
}

template <typename RetType, typename OpType, typename ValueType, typename Value, typename TableGetter>
auto value_of_agg_type_for_query(TableGetter&& tables, Value&& value, Arguments &args)
{
    const bool is_collection_operator = std::is_same<CollectionOperatorExpression, typename std::remove_reference<Value>::type>::value;
    using helper = std::conditional_t<is_collection_operator, CollectionOperatorGetter<RetType, TableGetter, OpType>, ValueGetter<ValueType, TableGetter> >;
    return helper::convert(tables, value, args);
}

const char* data_type_to_str(DataType type)
{
    switch (type) {
        case type_Int:
            return "Int";
        case type_Bool:
            return "Bool";
        case type_Float:
            return "Float";
        case type_Double:
            return "Double";
        case type_String:
            return "String";
        case type_Binary:
            return "Binary";
        case type_OldDateTime:
            return "DateTime";
        case type_Timestamp:
            return "Timestamp";
        case type_Table:
            return "Table";
        case type_Mixed:
            return "Mixed";
        case type_Link:
            return "Link";
        case type_LinkList:
            return "LinkList";
    }
    return "type_Unknown"; // LCOV_EXCL_LINE
}

template <typename A, typename B>
void do_add_collection_op_comparison_to_query(Query &query, Predicate::Comparison cmp,
                                const CollectionOperatorExpression &expr, A &lhs, B &rhs, Arguments &args)
{
    DataType type = expr.post_link_col_type;
    using OpType = parser::Expression::KeyPathOp;

#define ENABLE_SUPPORT_WITH_VALUE(TYPE, OP, VALUE_T) \
    if (type == type_##TYPE) { \
        add_numeric_constraint_to_query(query, cmp.op,  \
            value_of_agg_type_for_query< realm::TYPE, OP, VALUE_T >(expr.table_getter, lhs, args), \
            value_of_agg_type_for_query< realm::TYPE, OP, VALUE_T >(expr.table_getter, rhs, args)); \
        return; \
    }
#define ENABLE_SUPPORT_OF(TYPE, OP) ENABLE_SUPPORT_WITH_VALUE(TYPE, OP, TYPE)

    switch (expr.op) {
        case OpType::Min:
            ENABLE_SUPPORT_OF(Double, aggregate_operations::Minimum<Double>)
            ENABLE_SUPPORT_OF(Float, aggregate_operations::Minimum<Float>)
            ENABLE_SUPPORT_OF(Int, aggregate_operations::Minimum<Int>)
            break;
        case OpType::Max:
            ENABLE_SUPPORT_OF(Double, aggregate_operations::Maximum<Double>)
            ENABLE_SUPPORT_OF(Float, aggregate_operations::Maximum<Float>)
            ENABLE_SUPPORT_OF(Int, aggregate_operations::Maximum<Int>)
            break;
        case OpType::Avg:
            ENABLE_SUPPORT_OF(Double, aggregate_operations::Average<Double>)
            ENABLE_SUPPORT_OF(Float, aggregate_operations::Average<Float>)
            ENABLE_SUPPORT_OF(Int, aggregate_operations::Average<Int>)
            break;
        case OpType::Sum:
            ENABLE_SUPPORT_OF(Double, aggregate_operations::Sum<Double>)
            ENABLE_SUPPORT_OF(Float, aggregate_operations::Sum<Float>)
            ENABLE_SUPPORT_OF(Int, aggregate_operations::Sum<Int>)
            break;
        case OpType::Count:
            add_numeric_constraint_to_query(query, cmp.op,
                value_of_agg_type_for_query<Int, LinkCount, Int>(expr.table_getter, lhs, args),
                value_of_agg_type_for_query<Int, LinkCount, Int>(expr.table_getter, rhs, args));
            return;
        case OpType::Size:
            ENABLE_SUPPORT_WITH_VALUE(String, SizeOperator<Size<StringData> >, Int)
            ENABLE_SUPPORT_WITH_VALUE(Binary, SizeOperator<Size<BinaryData> >, Int)
            // FIXME: Subtable support ?
            break;
        case OpType::None:
            break;
    }
    throw std::runtime_error(util::format("Aggregate '%1' not supported on type '%2'",
                                          collection_operator_to_str(expr.op),
                                          data_type_to_str(expr.post_link_col_type)));
}

template <typename A, typename B>
void do_add_comparison_to_query(Query &query, Predicate::Comparison cmp,
                                const PropertyExpression &expr, A &lhs, B &rhs, Arguments &args)
{
    DataType type = expr.col_type;
    switch (type) {
        case type_Bool:
            add_bool_constraint_to_query(query, cmp.op, value_of_type_for_query<bool>(expr.table_getter, lhs, args),
                                                        value_of_type_for_query<bool>(expr.table_getter, rhs, args));
            break;
        case type_Timestamp:
            add_numeric_constraint_to_query(query, cmp.op, value_of_type_for_query<Timestamp>(expr.table_getter, lhs, args),
                                                           value_of_type_for_query<Timestamp>(expr.table_getter, rhs, args));
            break;
        case type_Double:
            add_numeric_constraint_to_query(query, cmp.op, value_of_type_for_query<Double>(expr.table_getter, lhs, args),
                                                           value_of_type_for_query<Double>(expr.table_getter, rhs, args));
            break;
        case type_Float:
            add_numeric_constraint_to_query(query, cmp.op, value_of_type_for_query<Float>(expr.table_getter, lhs, args),
                                                           value_of_type_for_query<Float>(expr.table_getter, rhs, args));
            break;
        case type_Int:
            add_numeric_constraint_to_query(query, cmp.op, value_of_type_for_query<Int>(expr.table_getter, lhs, args),
                                                           value_of_type_for_query<Int>(expr.table_getter, rhs, args));
            break;
        case type_String:
            add_string_constraint_to_query(query, cmp, value_of_type_for_query<String>(expr.table_getter, lhs, args),
                                                       value_of_type_for_query<String>(expr.table_getter, rhs, args));
            break;
        case type_Binary:
            add_binary_constraint_to_query(query, cmp.op, value_of_type_for_query<Binary>(expr.table_getter, lhs, args),
                                                          value_of_type_for_query<Binary>(expr.table_getter, rhs, args));
            break;
        case type_Link:
            add_link_constraint_to_query(query, cmp.op, expr, link_argument(lhs, rhs, args));
            break;
        default:
            throw std::logic_error(util::format("Object type '%1' not supported", data_type_to_str(expr.col_type)));
    }
}

template<typename T>
void do_add_null_comparison_to_query(Query &query, Predicate::Operator op, const PropertyExpression &expr)
{
    Columns<T> column = expr.table_getter()->template column<T>(expr.col_ndx);
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
void do_add_null_comparison_to_query<Binary>(Query &query, Predicate::Operator op, const PropertyExpression &expr)
{
    precondition(expr.indexes.empty(), "KeyPath queries not supported for data comparisons.");
    Columns<Binary> column = expr.table_getter()->template column<Binary>(expr.col_ndx);
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.not_equal(expr.col_ndx, realm::null());
            break;
        case Predicate::Operator::Equal:
            query.equal(expr.col_ndx, realm::null());
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
            //REALM_FALLTHROUGH;
        case Predicate::Operator::Equal:
            query.and_query(query.get_table()->column<Link>(expr.col_ndx).is_null());
            break;
        default:
            throw std::logic_error("Only 'equal' and 'not equal' operators supported for object comparison.");
    }
}

void do_add_null_comparison_to_query(Query &query, Predicate::Comparison cmp, const PropertyExpression &expr)
{
    DataType type = expr.col_type;
// FIXME: uncomment this and test it for LinkList and backlink column types
//    if (is_array(type)) {
//        throw std::logic_error("Comparing Lists to 'null' is not supported");
//    }
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
        case realm::type_String:
            do_add_null_comparison_to_query<String>(query, cmp.op, expr);
            break;
        case realm::type_Binary:
            do_add_null_comparison_to_query<Binary>(query, cmp.op, expr);
            break;
        case realm::type_Link:
            do_add_null_comparison_to_query<Link>(query, cmp.op, expr);
            break;
        default:
            throw std::logic_error(util::format("Object type '%1' not supported", data_type_to_str(expr.col_type)));
    }
}

bool expression_is_null(const parser::Expression &expr, Arguments &args) {
    if (expr.type == parser::Expression::Type::Null) {
        return true;
    }
    else if (expr.type == parser::Expression::Type::Argument) {
        return args.is_argument_null(stot<int>(expr.s));
    }
    return false;
}

void add_comparison_to_query(Query &query, const Predicate &pred, Arguments &args, const std::string &type)
{
    const Predicate::Comparison &cmpr = pred.cmpr;
    auto t0 = cmpr.expr[0].type, t1 = cmpr.expr[1].type;
    if (t0 == parser::Expression::Type::KeyPath && t1 != parser::Expression::Type::KeyPath) {
        PropertyExpression expr(query, cmpr.expr[0].s);
        if (cmpr.expr[0].collection_op != parser::Expression::KeyPathOp::None) {
            CollectionOperatorExpression wrapper(expr, cmpr.expr[0].collection_op, cmpr.expr[0].op_suffix);
            do_add_collection_op_comparison_to_query(query, cmpr, wrapper, wrapper, cmpr.expr[1], args);
        }
        else if (expression_is_null(cmpr.expr[1], args)) {
            do_add_null_comparison_to_query(query, cmpr, expr);
        }
        else {
            do_add_comparison_to_query(query, cmpr, expr, expr, cmpr.expr[1], args);
        }
    }
    else if (t0 != parser::Expression::Type::KeyPath && t1 == parser::Expression::Type::KeyPath) {
        PropertyExpression expr(query, cmpr.expr[1].s);
        if (cmpr.expr[1].collection_op != parser::Expression::KeyPathOp::None) {
            CollectionOperatorExpression wrapper(expr, cmpr.expr[1].collection_op, cmpr.expr[1].op_suffix);
            do_add_collection_op_comparison_to_query(query, cmpr, wrapper, cmpr.expr[0], wrapper, args);
        }
        else if (expression_is_null(cmpr.expr[0], args)) {
            do_add_null_comparison_to_query(query, cmpr, expr);
        }
        else {
            do_add_comparison_to_query(query, cmpr, expr, cmpr.expr[0], expr, args);
        }
    }
    else {
        throw std::logic_error("Predicate expressions must compare a keypath and another keypath or a constant value");
    }
}

void update_query_with_predicate(Query &query, const Predicate &pred, Arguments &arguments, const std::string &type)
{
    if (pred.negate) {
        query.Not();
    }

    switch (pred.type) {
        case Predicate::Type::And:
            query.group();
            for (auto &sub : pred.cpnd.sub_predicates) {
                update_query_with_predicate(query, sub, arguments, type);
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
                update_query_with_predicate(query, sub, arguments, type);
            }
            if (!pred.cpnd.sub_predicates.size()) {
                query.and_query(std::unique_ptr<realm::Expression>(new FalseExpression));
            }
            query.end_group();
            break;

        case Predicate::Type::Comparison: {
            add_comparison_to_query(query, pred, arguments, type);
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

void apply_predicate(Query &query, const Predicate &predicate, Arguments &arguments, const std::string &objectType)
{
    update_query_with_predicate(query, predicate, arguments, objectType);

    // Test the constructed query in core
    std::string validateMessage = query.validate();
    precondition(validateMessage.empty(), validateMessage.c_str());
}

}
}
