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
#include <realm/utilities.hpp>
#include <realm/util/base64.hpp>

#include <functional>
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
        tm created{};
        created.tm_year = stot<int>(time_inputs[0]) - 1900; // epoch offset (see man mktime)
        created.tm_mon = stot<int>(time_inputs[1]) - 1; // converts from 1-12 to 0-11
        created.tm_mday = stot<int>(time_inputs[2]);
        created.tm_hour = stot<int>(time_inputs[3]);
        created.tm_min = stot<int>(time_inputs[4]);
        created.tm_sec = stot<int>(time_inputs[5]);

        if (created.tm_year < 0) {
            // platform timegm functions do not throw errors, they return -1 which is also a valid time
            throw std::logic_error("Conversion of dates before 1900 is not supported.");
        }

        int64_t seconds = platform_timegm(created); // UTC time
        int32_t nanoseconds = 0;
        if (time_inputs.size() == 7) {
            nanoseconds = stot<int32_t>(time_inputs[6]);
            if (nanoseconds < 0) {
                throw std::logic_error("The nanoseconds of a Timestamp cannot be negative.");
            }
            if (seconds < 0) { // seconds determines the sign of the nanoseconds part
                nanoseconds *= -1;
            }
        }
        return get_timestamp_if_valid(seconds, nanoseconds);
    }
    throw std::runtime_error("Unexpected timestamp format.");
}

StringData from_base64(const std::string& input, util::StringBuffer& decode_buffer)
{
    // expects wrapper tokens B64"..."
    if (input.size() < 5
        || !(input[0] == 'B' || input[0] == 'b')
        || input[1] != '6' || input[2] != '4'
        || input[3] != '"' || input[input.size() - 1] != '"') {
        throw std::runtime_error("Unexpected base64 format");
    }
    const size_t encoded_size = input.size() - 5;
    size_t buffer_size = util::base64_decoded_size(encoded_size);
    decode_buffer.resize(buffer_size);
    StringData window(input.data() + 4, encoded_size);
    util::Optional<size_t> decoded_size = util::base64_decode(window, decode_buffer.data(), buffer_size);
    if (!decoded_size) {
        throw std::runtime_error("Invalid base64 value");
    }
    REALM_ASSERT_DEBUG_EX(*decoded_size <= encoded_size, *decoded_size, encoded_size);
    decode_buffer.resize(*decoded_size); // truncate
    StringData output_window(decode_buffer.data(), decode_buffer.size());
    return output_window;
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

StringData get_printable_table_name(const Table& table)
{
    StringData name = table.get_name();
    // the "class_" prefix is an implementation detail of the object store that shouldn't be exposed to users
    static const std::string prefix = "class_";
    if (name.size() > prefix.size() && strncmp(name.data(), prefix.data(), prefix.size()) == 0) {
        name = StringData(name.data() + prefix.size(), name.size() - prefix.size());
    }
    return name;
}

struct ValueExpression
{
    const parser::Expression* value;
    Arguments* arguments;
    std::function<Table *()> table_getter;

    ValueExpression(Query& query, Arguments* args, const parser::Expression* v)
    : value(v)
    , arguments(args) {
        table_getter = [&] {
            auto& tbl = query.get_table();
            return tbl.get();
        };
    }
    ValueExpression& operator=(const ValueExpression& other)
    {
        value = other.value;
        arguments = other.arguments;
        table_getter = other.table_getter;
        return *this;
    }
    bool is_null()
    {
        if (value->type == parser::Expression::Type::Null) {
            return true;
        }
        else if (value->type == parser::Expression::Type::Argument) {
            return arguments->is_argument_null(stot<int>(value->s));
        }
        return false;
    }

    template <typename RetType>
    RetType value_of_type_for_query();
};


template <>
Timestamp ValueExpression::value_of_type_for_query<Timestamp>()
{
    if (value->type == parser::Expression::Type::Argument) {
        return arguments->timestamp_for_argument(stot<int>(value->s));
    } else if (value->type == parser::Expression::Type::Timestamp) {
        return from_timestamp_values(value->time_inputs);
    } else if (value->type == parser::Expression::Type::Null) {
        return Timestamp(realm::null());
    }
    throw std::logic_error("Attempting to compare Timestamp property to a non-Timestamp value");
}

template <>
bool ValueExpression::value_of_type_for_query<bool>()
{
    if (value->type == parser::Expression::Type::Argument) {
        return arguments->bool_for_argument(stot<int>(value->s));
    }
    if (value->type != parser::Expression::Type::True && value->type != parser::Expression::Type::False) {
        if (value->type == parser::Expression::Type::Number) {
            // As a special exception we can handle 0 and 1.
            // Our bool values are actually stored as integers {0, 1}
            int64_t number_value = stot<int64_t>(value->s);
            if (number_value == 0) {
                return false;
            }
            else if (number_value == 1) {
                return true;
            }
        }
        throw std::logic_error("Attempting to compare bool property to a non-bool value");
    }
    return value->type == parser::Expression::Type::True;
}

template <>
Double ValueExpression::value_of_type_for_query<Double>()
{
    if (value->type == parser::Expression::Type::Argument) {
        return arguments->double_for_argument(stot<int>(value->s));
    }
    return stot<double>(value->s);
}

template <>
Float ValueExpression::value_of_type_for_query<Float>()
{
    if (value->type == parser::Expression::Type::Argument) {
        return arguments->float_for_argument(stot<int>(value->s));
    }
    return stot<float>(value->s);
}

template <>
Int ValueExpression::value_of_type_for_query<Int>()
{
    if (value->type == parser::Expression::Type::Argument) {
        return arguments->long_for_argument(stot<int>(value->s));
    }
    return stot<long long>(value->s);
}

template <>
StringData ValueExpression::value_of_type_for_query<StringData>()
{
    if (value->type == parser::Expression::Type::Argument) {
        return arguments->string_for_argument(stot<int>(value->s));
    }
    else if (value->type == parser::Expression::Type::String) {
        return value->s;
    }
    else if (value->type == parser::Expression::Type::Base64) {
        // the return value points to data in the lifetime of args
        return from_base64(value->s, arguments->buffer_space);
    }
    throw std::logic_error("Attempting to compare String property to a non-String value");
}

template <>
BinaryData ValueExpression::value_of_type_for_query<BinaryData>()
{
    if (value->type == parser::Expression::Type::Argument) {
        return arguments->binary_for_argument(stot<int>(value->s));
    }
    else if (value->type == parser::Expression::Type::String) {
        return BinaryData(value->s);
    }
    else if (value->type == parser::Expression::Type::Base64) {
        StringData converted = from_base64(value->s, arguments->buffer_space);
        // returning a pointer to data in the lifetime of args
        return BinaryData(converted.data(), converted.size());
    }
    throw std::logic_error("Binary properties must be compared against a binary argument.");
}

struct PropertyExpression
{
    std::vector<size_t> indexes;
    size_t col_ndx;
    DataType col_type;
    Query &query;

    PropertyExpression(Query &q, const std::string &key_path_string)
    : query(q)
    {
        KeyPath key_path = key_path_from_string(key_path_string);
        TableRef cur_table = query.get_table();
        for (size_t index = 0; index < key_path.size(); index++) {
            size_t cur_col_ndx = cur_table->get_column_index(key_path[index]);

            StringData object_name = get_printable_table_name(*cur_table);

            precondition(cur_col_ndx != realm::not_found,
                         util::format("No property '%1' on object of type '%2'", key_path[index], object_name));

            DataType cur_col_type = cur_table->get_column_type(cur_col_ndx);
            if (index != key_path.size() - 1) {
                precondition(cur_col_type == type_Link || cur_col_type == type_LinkList,
                             util::format("Property '%1' is not a link in object of type '%2'", key_path[index], object_name));
                indexes.push_back(cur_col_ndx);
                cur_table = cur_table->get_link_target(cur_col_ndx);
            }
            else {
                col_ndx = cur_col_ndx;
                col_type = cur_col_type;
            }
        }
    }

    Table* table_getter() const
    {
        auto& tbl = query.get_table();
        for (size_t col : indexes) {
            tbl->link(col); // mutates m_link_chain on table
        }
        return tbl.get();
    }

    template <typename RetType>
    auto value_of_type_for_query() const
    {
        return this->table_getter()->template column<RetType>(this->col_ndx);
    }
};
template <typename T>
const char* type_to_str()
{
    return typeid(T).name();
}
template <>
const char* type_to_str<bool>()
{
    return "Bool";
}
template <>
const char* type_to_str<Int>()
{
    return "Int";
}
template <>
const char* type_to_str<Float>()
{
    return "Float";
}
template <>
const char* type_to_str<Double>()
{
    return "Double";
}
template <>
const char* type_to_str<String>()
{
    return "String";
}
template <>
const char* type_to_str<Binary>()
{
    return "Binary";
}
template <>
const char* type_to_str<OldDateTime>()
{
    return "DateTime";
}
template <>
const char* type_to_str<Timestamp>()
{
    return "Timestamp";
}
template <>
const char* type_to_str<Table>()
{
    return "Table";
}
template <>
const char* type_to_str<Mixed>()
{
    return "Mixed";
}
template <>
const char* type_to_str<Link>()
{
    return "Link";
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

const char* collection_operator_to_str(parser::Expression::KeyPathOp op)
{
    switch (op) {
        case parser::Expression::KeyPathOp::None:
            return "NONE";
        case parser::Expression::KeyPathOp::Min:
            return "@min";
        case parser::Expression::KeyPathOp::Max:
            return "@max";
        case parser::Expression::KeyPathOp::Sum:
            return "@sum";
        case parser::Expression::KeyPathOp::Avg:
            return "@avg";
        case parser::Expression::KeyPathOp::SizeString:
            return "@size";
        case parser::Expression::KeyPathOp::SizeBinary:
            return "@size";
        case parser::Expression::KeyPathOp::Count:
            return "@count";
    }
    return "";
}

template <typename RetType, parser::Expression::KeyPathOp AggOpType, class Enable = void>
struct CollectionOperatorGetter;

template <parser::Expression::KeyPathOp OpType>
struct CollectionOperatorExpression
{
    static constexpr parser::Expression::KeyPathOp operation_type = OpType;
    std::function<Table *()> table_getter;
    PropertyExpression pe;
    size_t post_link_col_ndx;
    DataType post_link_col_type;
    CollectionOperatorExpression(PropertyExpression&& exp, std::string suffix_path)
    : pe(std::move(exp))
    , post_link_col_ndx(realm::not_found)
    {
        table_getter = std::bind(&PropertyExpression::table_getter, pe);

        const bool requires_suffix_path = !(OpType == parser::Expression::KeyPathOp::SizeString
                                            || OpType == parser::Expression::KeyPathOp::SizeBinary
                                            || OpType == parser::Expression::KeyPathOp::Count);

        if (requires_suffix_path) {
            Table* pre_link_table = pe.table_getter();
            REALM_ASSERT(pre_link_table);
            StringData list_property_name = pre_link_table->get_column_name(pe.col_ndx);
            precondition(pe.col_type == type_LinkList,
                         util::format("The '%1' operation must be used on a list property, but '%2' is not a list",
                                      collection_operator_to_str(OpType), list_property_name));

            TableRef post_link_table = pe.table_getter()->get_link_target(pe.col_ndx);
            REALM_ASSERT(post_link_table);
            StringData printable_post_link_table_name = get_printable_table_name(*post_link_table);

            KeyPath suffix_key_path = key_path_from_string(suffix_path);
            precondition(suffix_path.size() > 0 && suffix_key_path.size() > 0,
                         util::format("A property from object '%1' must be provided to perform operation '%2'",
                                      printable_post_link_table_name, collection_operator_to_str(OpType)));

            precondition(suffix_key_path.size() == 1,
                         util::format("Unable to use '%1' because collection aggreate operations are only supported "
                                      "for direct properties at this time", suffix_path));

            post_link_col_ndx = pe.table_getter()->get_link_target(pe.col_ndx)->get_column_index(suffix_key_path[0]);

            precondition(post_link_col_ndx != realm::not_found,
                         util::format("No property '%1' on object of type '%2'",
                                      suffix_path, printable_post_link_table_name));
            post_link_col_type = pe.table_getter()->get_link_target(pe.col_ndx)->get_column_type(post_link_col_ndx);
        }
        else {  // !requires_suffix_path
            post_link_col_type = pe.col_type;

            precondition(suffix_path.empty(),
                         util::format("An extraneous property '%1' was found for operation '%2'",
                                      suffix_path, collection_operator_to_str(OpType)));
        }
    }

    template <typename T>
    auto value_of_type_for_query() const
    {
        return CollectionOperatorGetter<T, OpType>::convert(*this);
    }
};

// Certain operations are disabled for some types (eg. a sum of timestamps is invalid).
// The operations that are supported have a specialisation with std::enable_if for that type below
// any type/operation combination that is not specialised will get the runtime error from the following
// default implementation. The return type is just a dummy to make things compile.
template <typename RetType, parser::Expression::KeyPathOp AggOpType, class Enable>
struct CollectionOperatorGetter {
    static Columns<RetType> convert(const CollectionOperatorExpression<AggOpType>& op) {
        throw std::runtime_error(util::format("Predicate error: comparison of type '%1' with result of '%2' is not supported.",
                                              type_to_str<RetType>(),
                                              collection_operator_to_str(op.operation_type)));
    }
};

template <typename RetType>
struct CollectionOperatorGetter<RetType, parser::Expression::KeyPathOp::Min,
    typename std::enable_if_t<
        std::is_same<RetType, Int>::value ||
        std::is_same<RetType, Float>::value ||
        std::is_same<RetType, Double>::value
    > >{
    static SubColumnAggregate<RetType, aggregate_operations::Minimum<RetType> > convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Min>& expr)
    {
        return expr.table_getter()->template column<Link>(expr.pe.col_ndx).template column<RetType>(expr.post_link_col_ndx).min();
    }
};

template <typename RetType>
struct CollectionOperatorGetter<RetType, parser::Expression::KeyPathOp::Max,
    typename std::enable_if_t<
        std::is_same<RetType, Int>::value ||
        std::is_same<RetType, Float>::value ||
        std::is_same<RetType, Double>::value
    > >{
    static SubColumnAggregate<RetType, aggregate_operations::Maximum<RetType> > convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Max>& expr)
    {
        return expr.table_getter()->template column<Link>(expr.pe.col_ndx).template column<RetType>(expr.post_link_col_ndx).max();
    }
};

template <typename RetType>
struct CollectionOperatorGetter<RetType, parser::Expression::KeyPathOp::Sum,
    typename std::enable_if_t<
        std::is_same<RetType, Int>::value ||
        std::is_same<RetType, Float>::value ||
        std::is_same<RetType, Double>::value
    > >{
    static SubColumnAggregate<RetType, aggregate_operations::Sum<RetType> > convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum>& expr)
    {
        return expr.table_getter()->template column<Link>(expr.pe.col_ndx).template column<RetType>(expr.post_link_col_ndx).sum();
    }
};

template <typename RetType>
struct CollectionOperatorGetter<RetType, parser::Expression::KeyPathOp::Avg,
    typename std::enable_if_t<
        std::is_same<RetType, Int>::value ||
        std::is_same<RetType, Float>::value ||
        std::is_same<RetType, Double>::value
    > >{
    static SubColumnAggregate<RetType, aggregate_operations::Average<RetType> > convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg>& expr)
    {
        return expr.table_getter()->template column<Link>(expr.pe.col_ndx).template column<RetType>(expr.post_link_col_ndx).average();
    }
};

template <>
struct CollectionOperatorGetter<Int, parser::Expression::KeyPathOp::Count>{
    static LinkCount convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::Count>& expr)
    {
        return expr.table_getter()->template column<Link>(expr.pe.col_ndx).count();
    }
};

template <>
struct CollectionOperatorGetter<Int, parser::Expression::KeyPathOp::SizeString>{
    static SizeOperator<Size<String> > convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString>& expr)
    {
        return expr.table_getter()->template column<String>(expr.pe.col_ndx).size();
    }
};

template <>
struct CollectionOperatorGetter<Int, parser::Expression::KeyPathOp::SizeBinary>{
    static SizeOperator<Size<Binary> > convert(const CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary>& expr)
    {
        return expr.table_getter()->template column<Binary>(expr.pe.col_ndx).size();
    }
};


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
    Columns<Binary> column = expr.table_getter()->template column<Binary>(expr.col_ndx);
    BinaryData null_binary;
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.and_query(column != null_binary);
            break;
        case Predicate::Operator::Equal:
            query.and_query(column == null_binary);
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
            query.and_query(query.get_table()->column<Link>(expr.col_ndx).is_null());
            break;
        default:
            throw std::logic_error("Only 'equal' and 'not equal' operators supported for object comparison.");
    }
}

template <class T>
void do_add_null_comparison_to_query(Query &query, Predicate::Comparison cmp, const T &expr, DataType type)
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
            throw std::logic_error(util::format("Object type '%1' not supported", data_type_to_str(type)));
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

void add_string_constraint_to_query(Query &,
                                    Predicate::Comparison,
                                    SizeOperator<Size<StringData>> &&,
                                    StringData &&) {
    throw std::logic_error("Comparing the size of a string with a string literal is an unsupported operation.");
}
void add_string_constraint_to_query(Query &,
                                    Predicate::Comparison,
                                    SizeOperator<Size<StringData>> &&,
                                    Columns<String> &&) {
    throw std::logic_error("Comparing the size of a string with a string property is an unsupported operation.");
}
void add_string_constraint_to_query(Query &,
                                    Predicate::Comparison,
                                    StringData &&,
                                    SizeOperator<Size<StringData>> &&) {
    throw std::logic_error("Comparing the size of a string with a string literal is an unsupported operation.");
}
void add_string_constraint_to_query(Query &,
                                    Predicate::Comparison,
                                    Columns<String> &&,
                                    SizeOperator<Size<StringData>> &&) {
    throw std::logic_error("Comparing the size of a string with a string property is an unsupported operation.");
}
void add_string_constraint_to_query(Query &query,
                                    Predicate::Comparison cmp,
                                    SizeOperator<Size<StringData>> &&lhs,
                                    SizeOperator<Size<StringData>> &&rhs) {
    // comparing the result of two size operations is a numeric constraint
    add_numeric_constraint_to_query(query, cmp.op, std::move(lhs), std::move(rhs));
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

void add_binary_constraint_to_query(Query &,
                                    Predicate::Operator,
                                    SizeOperator<Size<BinaryData>> &&,
                                    BinaryData &&) {
    throw std::logic_error("Comparing the size of a binary with a binary literal is an unsupported operation.");
}
void add_binary_constraint_to_query(Query &,
                                    Predicate::Operator,
                                    SizeOperator<Size<BinaryData>> &&,
                                    Columns<BinaryData> &&) {
    throw std::logic_error("Comparing the size of a binary with a binary property is an unsupported operation.");
}
void add_binary_constraint_to_query(Query &,
                                    Predicate::Operator,
                                    BinaryData &&,
                                    SizeOperator<Size<BinaryData>> &&) {
    throw std::logic_error("Comparing the size of a binary with a binary literal is an unsupported operation.");
}
void add_binary_constraint_to_query(Query &,
                                    Predicate::Operator,
                                    Columns<BinaryData> &&,
                                    SizeOperator<Size<BinaryData>> &&) {
    throw std::logic_error("Comparing the size of a binary with a binary property is an unsupported operation.");
}
void add_binary_constraint_to_query(Query &query,
                                    Predicate::Operator op,
                                    SizeOperator<Size<BinaryData>> &&lhs,
                                    SizeOperator<Size<BinaryData>> &&rhs) {
    // comparing the result of two size operations is a numeric constraint
    add_numeric_constraint_to_query(query, op, std::move(lhs), std::move(rhs));
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
            query.and_query(column == value);
            break;
        case Predicate::Operator::NotEqual:
            query.and_query(column != value);
            break;
        default:
            throw std::logic_error("Substring comparison not supported for keypath substrings.");
    }
}

void add_binary_constraint_to_query(Query &,
                                    Predicate::Operator,
                                    Columns<Binary> &&,
                                    Columns<Binary> &&) {
    throw std::logic_error("Comparing two binary columns is currently unsupported.");
}

void add_link_constraint_to_query(realm::Query &query,
                                  Predicate::Operator op,
                                  const PropertyExpression &prop_expr,
                                  size_t row_index) {
    precondition(prop_expr.indexes.empty(), "KeyPath queries not supported for object comparisons.");
    switch (op) {
        case Predicate::Operator::NotEqual:
            query.Not();
            REALM_FALLTHROUGH;
        case Predicate::Operator::Equal: {
            size_t col = prop_expr.col_ndx;
            query.links_to(col, query.get_table()->get_link_target(col)->get(row_index));
            break;
        }
        default:
            throw std::logic_error("Only 'equal' and 'not equal' operators supported for object comparison.");
    }
}

size_t link_argument(const PropertyExpression&, const PropertyExpression&, Arguments&)
{
    throw std::logic_error("Comparing two link properties is not supported at this time.");
}

size_t link_argument(const PropertyExpression&, const parser::Expression &argExpr, Arguments &args)
{
    return args.object_index_for_argument(stot<int>(argExpr.s));
}

size_t link_argument(const parser::Expression &argExpr, const PropertyExpression&, Arguments &args)
{
    return args.object_index_for_argument(stot<int>(argExpr.s));
}

//size_t link_argument(const CollectionOperatorExpression&, const parser::Expression &, Arguments &)
//{
//    throw std::logic_error("Unsupported link comparison");
//}
//
//size_t link_argument(const parser::Expression &, const CollectionOperatorExpression&, Arguments &)
//{
//    throw std::logic_error("Unsupported link comparison");
//}
//size_t link_argument(const CollectionOperatorExpression&, const PropertyExpression &, Arguments &)
//{
//    throw std::logic_error("Unsupported link comparison");
//}
//
//size_t link_argument(const PropertyExpression &, const CollectionOperatorExpression&, Arguments &)
//{
//    throw std::logic_error("Unsupported link comparison");
//}
//size_t link_argument(const CollectionOperatorExpression &, const CollectionOperatorExpression&, Arguments &)
//{
//    throw std::logic_error("Unsupported link comparison");
//}

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
            add_binary_constraint_to_query(query, cmp.op,
                                           lhs. template value_of_type_for_query<Binary>(),
                                           rhs. template value_of_type_for_query<Binary>());
            break;
        case type_Link:
            //add_link_constraint_to_query(query, cmp.op, expr, link_argument(lhs, rhs, args));
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

struct ExpressionContainer
{
    enum class ExpressionInternal
    {
        exp_Value,
        exp_Property,
        exp_OpMin,
        exp_OpMax,
        exp_OpSum,
        exp_OpAvg,
        exp_OpCount,
        exp_OpSizeString,
        exp_OpSizeBinary
    };

    ExpressionInternal type;
    util::Any storage;

    ExpressionContainer(Query& query, const parser::Expression& e, Arguments& args)
    {
        if (e.type == parser::Expression::Type::KeyPath) {
            PropertyExpression pe(query, e.s);
            switch (e.collection_op) {
                case parser::Expression::KeyPathOp::Min:
                    type = ExpressionInternal::exp_OpMin;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Min>(std::move(pe), e.op_suffix);
                    break;
                case parser::Expression::KeyPathOp::Max:
                    type = ExpressionInternal::exp_OpMax;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Max>(std::move(pe), e.op_suffix);
                    break;
                case parser::Expression::KeyPathOp::Sum:
                    type = ExpressionInternal::exp_OpSum;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum>(std::move(pe), e.op_suffix);
                    break;
                case parser::Expression::KeyPathOp::Avg:
                    type = ExpressionInternal::exp_OpAvg;
                    storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg>(std::move(pe), e.op_suffix);
                    break;
                case parser::Expression::KeyPathOp::Count:
                    REALM_FALLTHROUGH;
                case parser::Expression::KeyPathOp::SizeString:
                    REALM_FALLTHROUGH;
                case parser::Expression::KeyPathOp::SizeBinary:
                    if (pe.col_type == type_LinkList) {
                        type = ExpressionInternal::exp_OpCount;
                        storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::Count>(std::move(pe), e.op_suffix);
                    }
                    else if (pe.col_type == type_String) {
                        type = ExpressionInternal::exp_OpSizeString;
                        storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString>(std::move(pe), e.op_suffix);
                    }
                    else if (pe.col_type == type_Binary) {
                        type = ExpressionInternal::exp_OpSizeBinary;
                        storage = CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary>(std::move(pe), e.op_suffix);
                    }
                    else {
                        throw std::runtime_error("Invalid query: @size and @count can only operate on types list, binary, or string");
                    }
                    break;
                case parser::Expression::KeyPathOp::None:
                    type = ExpressionInternal::exp_Property;
                    storage = std::move(pe);
                    break;
            }
        }
        else {
            type = ExpressionInternal::exp_Value;
            storage = ValueExpression(query, &args, &e);
        }
    }

    void add_null_comparison_to_query(Query &query, Predicate::Comparison cmp)
    {

        switch (type) {
            case ExpressionInternal::exp_Value:
                throw std::runtime_error("Unsupported query. A comparison must include at least one keypath.");
            case ExpressionInternal::exp_Property:
                do_add_null_comparison_to_query(query, cmp, get_property(), get_property().col_type);
                break;
            case ExpressionInternal::exp_OpMin:
                do_add_null_comparison_to_query(query, cmp, get_min(), get_min().post_link_col_type);
                break;
            case ExpressionInternal::exp_OpMax:
                do_add_null_comparison_to_query(query, cmp, get_max(), get_max().post_link_col_type);
                break;
            case ExpressionInternal::exp_OpSum:
                do_add_null_comparison_to_query(query, cmp, get_sum(), get_sum().post_link_col_type);
                break;
            case ExpressionInternal::exp_OpAvg:
                do_add_null_comparison_to_query(query, cmp, get_avg(), get_avg().post_link_col_type);
                break;
            case ExpressionInternal::exp_OpCount:
                do_add_null_comparison_to_query(query, cmp, get_count(), get_count().post_link_col_type);
                break;
            case ExpressionInternal::exp_OpSizeString:
                do_add_null_comparison_to_query(query, cmp, get_size_string(), get_size_string().post_link_col_type);
                break;
            case ExpressionInternal::exp_OpSizeBinary:
                do_add_null_comparison_to_query(query, cmp, get_size_binary(), get_size_binary().post_link_col_type);
                break;
        }
    }

    PropertyExpression& get_property()
    {
        REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_Property);
        return util::any_cast<PropertyExpression&>(storage);
    }
    ValueExpression& get_value()
    {
        REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_Value);
        return util::any_cast<ValueExpression&>(storage);
    }
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Min>& get_min()
    {
        REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpMin);
        return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Min>&>(storage);
    }
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Max>& get_max()
    {
        REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpMax);
        return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Max>&>(storage);
    }
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum>& get_sum()
    {
        REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSum);
        return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Sum>&>(storage);
    }
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg>& get_avg()
    {
        REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpAvg);
        return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Avg>&>(storage);
    }
    CollectionOperatorExpression<parser::Expression::KeyPathOp::Count>& get_count()
    {
        REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpCount);
        return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::Count>&>(storage);
    }
    CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString>& get_size_string()
    {
        REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSizeString);
        return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeString>&>(storage);
    }
    CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary>& get_size_binary()
    {
        REALM_ASSERT_DEBUG(type == ExpressionInternal::exp_OpSizeBinary);
        return util::any_cast<CollectionOperatorExpression<parser::Expression::KeyPathOp::SizeBinary>&>(storage);
    }

    DataType get_comparison_type(ExpressionContainer& rhs) {
        if (type == ExpressionInternal::exp_Property) {
            return get_property().col_type;
        } else if (rhs.type == ExpressionInternal::exp_Property) {
            return rhs.get_property().col_type;
        } else if (type == ExpressionInternal::exp_OpMin) {
            return get_min().post_link_col_type;
        } else if (type == ExpressionInternal::exp_OpMax) {
            return get_max().post_link_col_type;
        } else if (type == ExpressionInternal::exp_OpSum) {
            return get_sum().post_link_col_type;
        } else if (type == ExpressionInternal::exp_OpAvg) {
            return get_avg().post_link_col_type;
        } else if (type == ExpressionInternal::exp_OpCount) {
            return type_Int;
        } else if (type == ExpressionInternal::exp_OpSizeString) {
            return type_Int;
        } else if (type == ExpressionInternal::exp_OpSizeBinary) {
            return type_Int;
        }
        // rhs checks
        else if (rhs.type == ExpressionInternal::exp_OpMin) {
            return rhs.get_min().post_link_col_type;
        } else if (rhs.type == ExpressionInternal::exp_OpMax) {
            return rhs.get_max().post_link_col_type;
        } else if (rhs.type == ExpressionInternal::exp_OpSum) {
            return rhs.get_sum().post_link_col_type;
        } else if (rhs.type == ExpressionInternal::exp_OpAvg) {
            return rhs.get_avg().post_link_col_type;
        } else if (rhs.type == ExpressionInternal::exp_OpCount) {
            return type_Int;
        } else if (rhs.type == ExpressionInternal::exp_OpSizeString) {
            return type_Int;
        } else if (rhs.type == ExpressionInternal::exp_OpSizeBinary) {
            return type_Int;
        }

        throw std::runtime_error("Unsupported query (type undeductable). A comparison must include at lease one keypath");
    }

    template <typename LHS_T>
    void internal_add_comparison_to_query(Query& query, LHS_T& lhs, Predicate::Comparison cmp, ExpressionContainer& rhs, DataType comparison_type)
    {
        switch (rhs.type) {
            case ExpressionInternal::exp_Value:
                do_add_comparison_to_query(query, cmp, lhs, rhs.get_value(), comparison_type);
                return;
            case ExpressionInternal::exp_Property:
                do_add_comparison_to_query(query, cmp, lhs, rhs.get_property(), comparison_type);
                return;
            case ExpressionInternal::exp_OpMin:
                do_add_comparison_to_query(query, cmp, lhs, rhs.get_min(), comparison_type);
                return;
            case ExpressionInternal::exp_OpMax:
                do_add_comparison_to_query(query, cmp, lhs, rhs.get_max(), comparison_type);
                return;
            case ExpressionInternal::exp_OpSum:
                do_add_comparison_to_query(query, cmp, lhs, rhs.get_sum(), comparison_type);
                return;
            case ExpressionInternal::exp_OpAvg:
                do_add_comparison_to_query(query, cmp, lhs, rhs.get_avg(), comparison_type);
                return;
            case ExpressionInternal::exp_OpCount:
                do_add_comparison_to_query(query, cmp, lhs, rhs.get_count(), comparison_type);
                return;
            case ExpressionInternal::exp_OpSizeString:
                do_add_comparison_to_query(query, cmp, lhs, rhs.get_size_string(), comparison_type);
                return;
            case ExpressionInternal::exp_OpSizeBinary:
                do_add_comparison_to_query(query, cmp, lhs, rhs.get_size_binary(), comparison_type);
                return;
        }
    }

    void add_comparison_to_query(Query &query, Predicate::Comparison cmp, ExpressionContainer& rhs)
    {
        DataType comparison_type = get_comparison_type(rhs);
        switch (type) {
            case ExpressionInternal::exp_Value:
                internal_add_comparison_to_query(query, get_value(), cmp, rhs, comparison_type);
                return;
            case ExpressionInternal::exp_Property:
                internal_add_comparison_to_query(query, get_property(), cmp, rhs, comparison_type);
                return;
            case ExpressionInternal::exp_OpMin:
                internal_add_comparison_to_query(query, get_min(), cmp, rhs, comparison_type);
                return;
            case ExpressionInternal::exp_OpMax:
                internal_add_comparison_to_query(query, get_max(), cmp, rhs, comparison_type);
                return;
            case ExpressionInternal::exp_OpSum:
                internal_add_comparison_to_query(query, get_sum(), cmp, rhs, comparison_type);
                return;
            case ExpressionInternal::exp_OpAvg:
                internal_add_comparison_to_query(query, get_avg(), cmp, rhs, comparison_type);
                return;
            case ExpressionInternal::exp_OpCount:
                internal_add_comparison_to_query(query, get_count(), cmp, rhs, comparison_type);
                return;
            case ExpressionInternal::exp_OpSizeString:
                internal_add_comparison_to_query(query, get_size_string(), cmp, rhs, comparison_type);
                return;
            case ExpressionInternal::exp_OpSizeBinary:
                internal_add_comparison_to_query(query, get_size_binary(), cmp, rhs, comparison_type);
                return;
        }
    }

    bool is_null() {
        if (type == ExpressionInternal::exp_Value) {
            return get_value().is_null();
        }
        return false;
    }
};

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
        rhs.add_null_comparison_to_query(query, cmpr);
    }
    else if (rhs.is_null()) {
        lhs.add_null_comparison_to_query(query, cmpr);
    }
    else {
        lhs.add_comparison_to_query(query, cmpr, rhs);
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
}
}
