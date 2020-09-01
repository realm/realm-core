/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
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

#ifndef REALM_UUID_HPP
#define REALM_UUID_HPP

#include <realm/string_data.hpp>

namespace realm {

class UUID {
public:
    static bool is_valid_string(StringData) noexcept;

    /// Constructs an ObjectId from 36 hex characters.
    UUID(const char* init) noexcept;
    /// Constructs a null UUID
    UUID() noexcept;

    bool operator==(const UUID& other) const
    {
        return memcmp(m_bytes, other.m_bytes, sizeof(m_bytes)) == 0;
    }
    bool operator!=(const UUID& other) const
    {
        return memcmp(m_bytes, other.m_bytes, sizeof(m_bytes)) != 0;
    }
    bool operator>(const UUID& other) const
    {
        return memcmp(m_bytes, other.m_bytes, sizeof(m_bytes)) > 0;
    }
    bool operator<(const UUID& other) const
    {
        return memcmp(m_bytes, other.m_bytes, sizeof(m_bytes)) < 0;
    }
    bool operator>=(const UUID& other) const
    {
        return memcmp(m_bytes, other.m_bytes, sizeof(m_bytes)) >= 0;
    }
    bool operator<=(const UUID& other) const
    {
        return memcmp(m_bytes, other.m_bytes, sizeof(m_bytes)) <= 0;
    }
    bool is_null() const;
    std::string to_string() const;
    size_t hash() const noexcept;

private:
    uint8_t m_bytes[16] = {};
};

inline std::ostream& operator<<(std::ostream& ostr, const UUID& id)
{
    ostr << id.to_string();
    return ostr;
}

} // namespace realm

namespace std {
template <>
struct hash<realm::UUID> {
    size_t operator()(const realm::UUID& oid) const noexcept
    {
        return oid.hash();
    }
};
} // namespace std

#endif /* REALM_UUID_HPP */
