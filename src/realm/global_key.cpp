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

#include <realm/global_key.hpp>
#include <realm/string_data.hpp>
#include <realm/mixed.hpp>
#include <realm/util/sha_crypto.hpp>
#include <iomanip>
#include <ostream>
#include <istream>
#include <sstream>
#include <cctype>

namespace realm {

std::ostream& operator<<(std::ostream& os, const GlobalKey& object_id)
{
    return os << '{' << std::setw(4) << std::right << std::setfill('0') << std::hex << object_id.hi() << '-'
              << std::setw(4) << std::right << std::setfill('0') << std::hex << object_id.lo() << '}'
              << std::setfill(' ') << std::setw(0);
}

std::istream& operator>>(std::istream& in, GlobalKey& object_id)
{
    try {
        std::istream::sentry sentry{in};
        if (REALM_LIKELY(sentry)) {
            std::string string;
            char ch;
            in.get(ch);
            while (REALM_LIKELY(in)) {
                string.push_back(ch); // Throws
                if (REALM_LIKELY(ch == '}'))
                    break;
                in.get(ch);
            }
            object_id = GlobalKey::from_string(string);
        }
    }
    catch (const util::invalid_argument&) {
        object_id = GlobalKey();
        in.setstate(std::ios_base::failbit);
    }
    return in;
}
std::string GlobalKey::to_string() const
{
    std::ostringstream ss;
    ss << *this;
    return ss.str();
}


GlobalKey GlobalKey::from_string(StringData string)
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
    return GlobalKey(strtoull(hi_begin, nullptr, 16), strtoull(lo_begin, nullptr, 16));
}

GlobalKey::GlobalKey(Mixed pk)
{
    if (pk.is_null()) {
        // Choose {1, 0} as the object ID for NULL. This could just as well have been {0, 0},
        // but then the null-representation for string and integer primary keys would have to
        // be different, as {0, 0} is a valid object ID for a row with an integer primary key.
        // Therefore, in the interest of simplicity, {1, 0} is chosen to represent NULL for
        // both integer and string primary keys.
        m_hi = 1;
        m_lo = 0;
        return;
    }

    union {
        unsigned char buffer[20];
        struct {
            uint64_t lo;
            uint64_t hi;
        } oid;
    } outp;

    switch (pk.get_type()) {
        case type_String: {
            auto val = pk.get_string();
            util::sha1(val.data(), val.size(), outp.buffer);
            m_hi = outp.oid.hi;
            m_lo = outp.oid.lo;
            break;
        }

        case type_ObjectId: {
            union ObjectIdBuffer {
                ObjectIdBuffer() {}
                char buffer[sizeof(ObjectId)];
                ObjectId id;
            } inp;
            inp.id = pk.get<ObjectId>();
            util::sha1(inp.buffer, sizeof(ObjectId), outp.buffer);
            m_hi = outp.oid.hi;
            m_lo = outp.oid.lo;
            break;
        }

        case type_Int:
            m_hi = 0;
            m_lo = uint64_t(pk.get_int());
            break;

        case type_UUID: {
            union UUIDBuffer {
                UUIDBuffer() {}
                UUID::UUIDBytes id;
                struct {
                    uint64_t upper;
                    uint64_t lower;
                } values;
            } inp;
            inp.id = pk.get<UUID>().to_bytes();
            m_hi = inp.values.upper;
            m_lo = inp.values.lower;
            break;
        }
        default:
            m_hi = -1;
            m_lo = -1;
            break;
    }
}

} // namespace realm
