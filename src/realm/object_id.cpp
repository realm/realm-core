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
#include <realm/mixed.hpp>
#include <realm/util/sha_crypto.hpp>
#include <iomanip>
#include <ostream>
#include <sstream>
#include <cctype>

namespace realm {

std::ostream& operator<<(std::ostream& os, const ObjectID& object_id)
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

ObjectID object_id_for_primary_key(Mixed pk)
{
    if (pk.is_null())
        // Choose {1, 0} as the object ID for NULL. This could just as well have been {0, 0},
        // but then the null-representation for string and integer primary keys would have to
        // be different, as {0, 0} is a valid object ID for a row with an integer primary key.
        // Therefore, in the interest of simplicity, {1, 0} is chosen to represent NULL for
        // both integer and string primary keys.
        return ObjectID{1, 0};

    if (pk.get_type() == type_String) {
        // FIXME: Type-punning will only work on little-endian architectures
        // (which is everyone we care about).
        union {
            unsigned char buffer[20];
            struct {
                uint64_t lo;
                uint64_t hi;
            } oid;
        } data;
        // FIXME: Use a better hash function than SHA1
        auto val = pk.get_string();
        util::sha1(val.data(), val.size(), data.buffer);

        // On big-endian architectures, this is necessary instead:
        // uint64_t lo = uint64_t(buffer[0]) | (uint64_t(buffer[1]) << 8)
        //             | (uint64_t(buffer[2])  << 16) | (uint64_t(buffer[3])  << 24)
        //             | (uint64_t(buffer[4])  << 32) | (uint64_t(buffer[5])  << 40)
        //             | (uint64_t(buffer[6])  << 48) | (uint64_t(buffer[7])  << 56);
        // uint64_t hi = uint64_t(buffer[8]) | (uint64_t(buffer[9]) << 8)
        //             | (uint64_t(buffer[10]) << 16) | (uint64_t(buffer[11]) << 24)
        //             | (uint64_t(buffer[12]) << 32) | (uint64_t(buffer[13]) << 40)
        //             | (uint64_t(buffer[14]) << 48) | (uint64_t(buffer[15]) << 56);

        return ObjectID{data.oid.hi, data.oid.lo};
    }
    if (pk.get_type() == type_Int) {
        return ObjectID{0, uint64_t(pk.get_int())};
    }
    return {};
}

} // namespace realm
