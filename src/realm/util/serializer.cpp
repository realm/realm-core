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

namespace realm {
namespace util {
namespace serializer {

template <>
std::string print_value<>(BinaryData data)
{
    if (data.is_null()) {
        return "NULL";
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

template <>
std::string print_value<>(realm::null)
{
    return "NULL";
}

bool contains_invalids(StringData data) {
    const static std::string whitelist = " {|}~:;<=>?@!#$%&()*+,-./[]^_`";
    for (size_t i = 0; i < data.size(); ++i) {
        using unsigned_char_t = unsigned char;
        char c = data.data()[i];
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
        return "NULL";
    }
    std::string out;
    const char* start = data.data();
    const size_t len = data.size();

    if (contains_invalids(data)) {
        util::StringBuffer encode_buffer;
        encode_buffer.resize(util::base64_encoded_size(len));
        util::base64_encode(start, len, encode_buffer.data(), encode_buffer.size());
        out = "B64\"" + encode_buffer.str() + "\"";
    } else {
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
    std::stringstream ss;
    if (t.is_null()) {
        ss << "null";
    }
    else {
        ss << "T" << t.get_seconds() << ":" << t.get_nanoseconds();
    }
    return ss.str();
}

template <>
std::string print_value<>(realm::ObjKey k)
{
    std::stringstream ss;
    if (!k) {
        ss << "NULL";
    }
    else {
        ss << "O" << k.value;
    }
    return ss.str();
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
    const Table* target = link_map.get_target_table();
    if (target && target_col_key) {
        if (!desc.empty()) {
            desc += util::serializer::value_separator;
        }
        desc += get_column_name(target->get_table_ref(), target_col_key);
    }
    return desc;
}

} // namespace serializer
} // namespace util
} // namespace realm
