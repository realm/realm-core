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
#include <realm/null.hpp>
#include <realm/string_data.hpp>
#include <realm/timestamp.hpp>

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

template <>
std::string print_value<>(StringData data)
{
    if (data.is_null()) {
        return "NULL";
    }
    std::string out;
    const char* start = data.data();
    const size_t len = data.size();
    out.reserve(len + 2);
    out += '"';
    for (const char* i = start; i != start + len; ++i) {
        out += *i;
    }
    out += '"';
    return out;
}

template <>
std::string print_value<>(realm::Timestamp t)
{
    std::stringstream ss;
    ss << "T" << t.get_seconds() << ":" << t.get_nanoseconds();
    return ss.str();
}

} // namespace serializer
} // namespace util
} // namespace realm
