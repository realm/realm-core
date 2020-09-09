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

#include <realm/uuid.hpp>
#include <realm/string_data.hpp>
#include <realm/util/assert.hpp>
#include <atomic>
#include <cctype>

static const char hex_digits[] = "0123456789abcdef";
static const size_t size_of_uuid_string = 36;
static const size_t size_of_uuid = 16;
static const char null_uuid[size_of_uuid] = {};

static const size_t hyphen_pos_0 = 8;
static const size_t hyphen_pos_1 = 13;
static const size_t hyphen_pos_2 = 18;
static const size_t hyphen_pos_3 = 23;
static const char hyphen = '-';

namespace realm {

bool UUID::is_valid_string(StringData str) noexcept
{
    if (str.size() != size_of_uuid_string) {
        return false;
    }
    if (str[hyphen_pos_0] != hyphen || str[hyphen_pos_1] != hyphen || str[hyphen_pos_2] != hyphen ||
        str[hyphen_pos_3] != hyphen) {
        return false;
    }
    for (size_t i = 0; i < size_of_uuid_string;
         (i == (hyphen_pos_0 - 1) || i == (hyphen_pos_1 - 1) || i == (hyphen_pos_2 - 1) || i == (hyphen_pos_3 - 1))
             ? i += 2
             : ++i) {
        if (!std::isxdigit(str[i])) {
            return false;
        }
    }
    return true;
}

UUID::UUID(const char* init) noexcept
{
    char buf[3];
    REALM_ASSERT_EX(is_valid_string(init), init);

    buf[2] = '\0';

    size_t j = 0;
    for (size_t i = 0; i < sizeof(m_bytes); i++) {
        buf[0] = init[j++];
        buf[1] = init[j++];
        if (j == hyphen_pos_0 || j == hyphen_pos_1 || j == hyphen_pos_2 || j == hyphen_pos_3) {
            j++;
        }
        m_bytes[i] = char(strtol(buf, nullptr, 16));
    }
}

UUID::UUID(const StringData& init) noexcept
    : UUID(init.data())
{
}

UUID::UUID() noexcept
    : m_bytes{}
{
}

bool UUID::is_null() const
{
    return memcmp(m_bytes, null_uuid, sizeof(m_bytes)) == 0;
}

std::string UUID::to_string() const
{
    std::string ret;
    size_t ret_size = 0;
    for (size_t i = 0; i < sizeof(m_bytes); i++) {
        ret += hex_digits[m_bytes[i] >> 4];
        ret += hex_digits[m_bytes[i] & 0xf];
        ret_size += 2;
        if (ret_size == hyphen_pos_0 || ret_size == hyphen_pos_1 || ret_size == hyphen_pos_2 ||
            ret_size == hyphen_pos_3) {
            ret += hyphen;
            ++ret_size;
        }
    }
    return ret;
}

size_t UUID::hash() const noexcept
{
    return murmur2_or_cityhash(m_bytes, sizeof(m_bytes));
}

} // namespace realm
