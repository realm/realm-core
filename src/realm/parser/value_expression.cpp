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

#include "value_expression.hpp"
#include "parser_utils.hpp"

#include <realm/util/base64.hpp>

#include <sstream>
#include <time.h>

namespace realm {
namespace parser {

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
        struct tm created = tm();
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


ValueExpression::ValueExpression(Query& query, query_builder::Arguments* args, const parser::Expression* v)
: value(v)
, arguments(args) {
    table_getter = [&] {
        auto& tbl = query.get_table();
        return tbl.get();
    };
}

bool ValueExpression::is_null()
{
    if (value->type == parser::Expression::Type::Null) {
        return true;
    }
    else if (value->type == parser::Expression::Type::Argument) {
        return arguments->is_argument_null(stot<int>(value->s));
    }
    return false;
}

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
    // We can allow string types here in case people have numbers in their strings like "int == '23'"
    // it's just a convienence but if the string conversion fails we'll throw from the stot function.
    if (value->type != parser::Expression::Type::Number && value->type != parser::Expression::Type::String) {
        throw std::logic_error("Attempting to compare a numeric property to a non-numeric value");
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
        arguments->buffer_space.push_back({});
        arguments->buffer_space.back().append(value->s);
        return StringData(arguments->buffer_space.back().data(), arguments->buffer_space.back().size());
    }
    else if (value->type == parser::Expression::Type::Base64) {
        // the return value points to data in the lifetime of args
        arguments->buffer_space.push_back({});
        return from_base64(value->s, arguments->buffer_space.back());
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
        arguments->buffer_space.push_back({});
        arguments->buffer_space.back().append(value->s);
        return BinaryData(arguments->buffer_space.back().data(), arguments->buffer_space.back().size());
    }
    else if (value->type == parser::Expression::Type::Base64) {
        // the return value points to data in the lifetime of args
        arguments->buffer_space.push_back({});
        StringData converted = from_base64(value->s, arguments->buffer_space[arguments->buffer_space.size() - 1]);
        // returning a pointer to data in the lifetime of args
        return BinaryData(converted.data(), converted.size());
    }
    throw std::logic_error("Binary properties must be compared against a binary argument.");
}

} // namespace parser
} // namespace realm
