/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <realm/util/serializer.hpp>

#include <realm/binary_data.hpp>
#include <realm/keys.hpp>
#include <realm/null.hpp>
#include <realm/query_expression.hpp>
#include <realm/string_data.hpp>
#include <realm/table.hpp>
#include <realm/timestamp.hpp>
#include <realm/util/base64.hpp>
#include <realm/util/string_buffer.hpp>

#include <cctype>
#include <cmath>

constexpr const char* null_string = "NULL";

namespace realm {
namespace util {
namespace serializer {

template <>
std::string print_value<>(BinaryData data)
{
    if (data.is_null()) {
        return null_string;
    }
    return print_value<StringData>(StringData(data.data(), data.size()));
}

template <>
std::string print_value<>(bool b)
{
    if (b) {
        return "true";
    }
    return "false";
}

template <typename T>
inline std::string print_with_nan_check(T val)
{
    // we standardize NaN because some implementations (windows) will
    // print the different types of NaN such as "nan(ind)" to indicate "indefinite"
    if (std::isnan(val)) {
        // preserving the sign of nan is not strictly required but is good etiquette
        if (std::signbit(val)) {
            return "-nan";
        }
        return "nan";
    }
    std::stringstream ss;
    ss << val;
    return ss.str();
}

template <>
std::string print_value<>(float val)
{
    return print_with_nan_check(val);
}

template <>
std::string print_value<>(double val)
{
    return print_with_nan_check(val);
}

template <>
std::string print_value<>(realm::null)
{
    return null_string;
}

bool contains_invalids(StringData data)
{
    // the custom whitelist is different from std::isprint because it doesn't include quotations
    const static std::string whitelist = " {|}~:;<=>?@!#$%&()*+,-./[]^_`";
    for (size_t i = 0; i < data.size(); ++i) {
        using unsigned_char_t = unsigned char;
        char c = data.data()[i];
        // std::isalnum takes an int, but is undefined for negative values so we must pass an unsigned char
        if (!std::isalnum(unsigned_char_t(c)) && whitelist.find(c) == std::string::npos) {
            return true;
        }
    }
    return false;
}

template <>
std::string print_value<>(StringData data)
{
    if (data.is_null()) {
        return null_string;
    }
    std::string out;
    const char* start = data.data();
    const size_t len = data.size();

    if (contains_invalids(data)) {
        util::StringBuffer encode_buffer;
        encode_buffer.resize(util::base64_encoded_size(len));
        util::base64_encode(start, len, encode_buffer.data(), encode_buffer.size());
        out = "B64\"" + encode_buffer.str() + "\"";
    }
    else {
        out.reserve(len + 2);
        out += '"';
        for (const char* i = start; i != start + len; ++i) {
            out += *i;
        }
        out += '"';
    }
    return out;
}

template <>
std::string print_value<>(realm::Timestamp t)
{
    if (t.is_null()) {
        return null_string;
    }
    std::stringstream ss;
    ss << "T" << t.get_seconds() << ":" << t.get_nanoseconds();
    return ss.str();
}

template <>
std::string print_value<>(realm::ObjectId oid)
{
    return "oid(" + oid.to_string() + ")";
}

template <>
std::string print_value<>(realm::ObjKey k)
{
    if (!k) {
        return null_string;
    }
    std::stringstream ss;
    ss << "O" << k.value;
    return ss.str();
}

template <>
std::string print_value<>(realm::ObjLink link)
{
    if (link.is_null()) {
        return null_string;
    }
    return util::format("{%1, %2}", link.get_table_key().value, link.get_obj_key().value);
}

template <>
std::string print_value<>(realm::UUID uuid)
{
    return "uuid(" + uuid.to_string() + ")";
}

template <>
std::string print_value<>(realm::Mixed mixed)
{
    if (mixed.is_null()) {
        return null_string;
    }
    switch (mixed.get_type()) {
        case type_Int:
            return print_value(mixed.get<Int>());
        case type_Bool:
            return print_value(mixed.get<Bool>());
        case type_String:
            return print_value(mixed.get<StringData>());
        case type_Binary:
            return print_value(mixed.get<BinaryData>());
        case type_Timestamp:
            return print_value(mixed.get<Timestamp>());
        case type_Float:
            return print_value(mixed.get<Float>());
        case type_Double:
            return print_value(mixed.get<Double>());
        case type_Decimal:
            return print_value(mixed.get<Decimal128>());
        case type_Link:
            return print_value(mixed.get<ObjKey>());
        case type_TypedLink:
            return print_value(mixed.get<ObjLink>());
        case type_ObjectId:
            return print_value(mixed.get<ObjectId>());
        case type_UUID:
            return print_value(mixed.get<UUID>());
        case type_LinkList:
        case type_Mixed:
        case type_OldDateTime:
        case type_OldTable:
            throw std::logic_error(util::format("unsupported type serializeation from mixed: %1", mixed.get_type()));
    }
    return "UnknownValue";
}

// The variable name must be unique with respect to the already chosen variables at
// this level of subquery nesting and with respect to the names of the columns in the table.
// This assumes that columns can start with '$' and that we might one day want to support
// referencing the parent table columns in the subquery. This is currently disabled by an assertion in the
// core SubQuery constructor.
std::string SerialisationState::get_variable_name(ConstTableRef table)
{
    std::string guess_prefix = "$";
    const char start_char = 'x';
    char add_char = start_char;

    auto next_guess = [&]() {
        add_char = (((add_char + 1) - 'a') % ('z' - 'a' + 1)) + 'a';
        if (add_char == start_char) {
            guess_prefix += add_char;
        }
    };

    while (true) {
        std::string guess = guess_prefix + add_char;
        bool found_duplicate = false;
        for (size_t i = 0; i < subquery_prefix_list.size(); ++i) {
            if (guess == subquery_prefix_list[i]) {
                found_duplicate = true;
                break;
            }
        }
        if (found_duplicate) {
            next_guess();
            continue;
        }
        if (table->get_column_key(guess) != ColKey()) {
            next_guess();
            continue;
        }
        return guess;
    }
}

std::string SerialisationState::get_column_name(ConstTableRef table, ColKey col_key)
{
    ColumnType col_type = table->get_real_column_type(col_key);
    if (col_type == col_type_BackLink) {
        const Table::BacklinkOrigin origin = table->find_backlink_origin(col_key);
        REALM_ASSERT(origin);
        std::string source_table_name = origin->first->get_name();
        std::string source_col_name = origin->first->get_column_name(origin->second);
        return "@links" + util::serializer::value_separator + source_table_name + util::serializer::value_separator +
               source_col_name;
    }
    else if (col_key != ColKey()) {
        return std::string(table->get_column_name(col_key));
    }
    return "";
}

std::string SerialisationState::describe_column(ConstTableRef table, ColKey col_key)
{
    if (table && col_key) {
        std::string desc;
        if (!subquery_prefix_list.empty()) {
            desc += subquery_prefix_list.back() + value_separator;
        }
        desc += get_column_name(table, col_key);
        return desc;
    }
    return "";
}

std::string SerialisationState::get_backlink_column_name(ConstTableRef from, ColKey col_key)
{
    ColumnType col_type = col_key.get_type();
    REALM_ASSERT_EX(col_type == col_type_Link || col_type == col_type_LinkList, col_type);

    auto target_table = from->get_opposite_table(col_key);
    auto backlink_col = from->get_opposite_column(col_key);
    return get_column_name(target_table, backlink_col);
}

std::string SerialisationState::describe_columns(const LinkMap& link_map, ColKey target_col_key)
{
    std::string desc;
    if (!subquery_prefix_list.empty()) {
        desc += subquery_prefix_list.back();
    }
    if (link_map.links_exist()) {
        if (!desc.empty()) {
            desc += util::serializer::value_separator;
        }
        desc += link_map.description(*this);
    }
    ConstTableRef target = link_map.get_target_table();
    if (target && target_col_key) {
        if (!desc.empty()) {
            desc += util::serializer::value_separator;
        }
        desc += get_column_name(target, target_col_key);
    }
    return desc;
}

std::string SerialisationState::describe_expression_type(ExpressionComparisonType type)
{
    switch (type) {
        case ExpressionComparisonType::Any:
            return ""; // ANY is implied
        case ExpressionComparisonType::All:
            return "ALL ";
        case ExpressionComparisonType::None:
            return "NONE ";
    }
    REALM_UNREACHABLE();
    return "";
}

} // namespace serializer
} // namespace util
} // namespace realm
