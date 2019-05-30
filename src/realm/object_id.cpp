/*************************************************************************
 *
 * Copyright 2019 Realm Inc.
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

#include <realm/object_id.hpp>
#include <realm/string_data.hpp>
#include <iomanip>
#include <ostream>
#include <sstream>
#include <cctype>

using namespace realm;

std::ostream& realm::operator<<(std::ostream& os, const ObjectID& object_id)
{
    return os << '{' << std::setw(4) << std::right << std::setfill('0') << std::hex << object_id.hi() << '-'
              << std::setw(4) << std::right << std::setfill('0') << std::hex << object_id.lo() << '}'
              << std::setfill(' ') << std::setw(0);
}

std::string ObjectID::to_string() const
{
    std::ostringstream ss;
    ss << *this;
    return ss.str();
}


ObjectID ObjectID::from_string(StringData string)
{
    if (string.size() < 5) // Must be at least "{0-0}"
        throw util::invalid_argument("Invalid object ID.");

    const char* begin = string.data();
    const char* end = string.data() + string.size();
    const char* last = end - 1;

    if (*begin != '{' || *last != '}')
        throw util::invalid_argument("Invalid object ID.");

    auto dash_pos = std::find(begin, end, '-');
    if (dash_pos == end)
        throw util::invalid_argument("Invalid object ID.");
    size_t dash_index = dash_pos - begin;

    const char* hi_begin = begin + 1;
    const char* lo_begin = dash_pos + 1;
    size_t hi_len = dash_index - 1;
    size_t lo_len = string.size() - dash_index - 2;

    if (hi_len == 0 || hi_len > 16 || lo_len == 0 || lo_len > 16) {
        throw util::invalid_argument("Invalid object ID.");
    }

    auto isxdigit = static_cast<int (*)(int)>(std::isxdigit);
    if (!std::all_of(hi_begin, hi_begin + hi_len, isxdigit) || !std::all_of(lo_begin, lo_begin + lo_len, isxdigit)) {
        throw util::invalid_argument("Invalid object ID.");
    }

    // hi_begin and lo_begin do not need to be copied into a NUL-terminated
    // buffer because we have checked above that they are immediately followed
    // by '-' or '}' respectively, and std::strtoull guarantees that it will
    // stop processing when it reaches either of those characters.
    return ObjectID(strtoull(hi_begin, nullptr, 16), strtoull(lo_begin, nullptr, 16));
}
