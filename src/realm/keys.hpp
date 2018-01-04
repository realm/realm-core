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

#ifndef REALM_KEYS_HPP
#define REALM_KEYS_HPP

#include <realm/util/to_string.hpp>

namespace realm {

struct TableKey {
    constexpr TableKey()
        : value(uint64_t(-1) >> 1) // free top bit
    {
    }
    explicit TableKey(int64_t val)
        : value(val)
    {
    }
    TableKey& operator=(int64_t val)
    {
        value = val;
        return *this;
    }
    bool operator==(const TableKey& rhs) const
    {
        return value == rhs.value;
    }
    bool operator!=(const TableKey& rhs) const
    {
        return value != rhs.value;
    }
    bool operator<(const TableKey& rhs) const
    {
        return value < rhs.value;
    }
    int64_t value;
};

inline std::ostream& operator<<(std::ostream& os, TableKey tk)
{
    os << tk.value;
    return os;
}

namespace util {

inline std::string to_string(TableKey tk)
{
    return to_string(tk.value);
}
}


struct ColKey {
    constexpr ColKey()
        : value(uint64_t(-1) >> 1) // free top bit
    {
    }
    explicit ColKey(int64_t val)
        : value(val)
    {
    }
    ColKey& operator=(int64_t val)
    {
        value = val;
        return *this;
    }
    bool operator==(const ColKey& rhs) const
    {
        return value == rhs.value;
    }
    bool operator!=(const ColKey& rhs) const
    {
        return value != rhs.value;
    }
    bool operator<(const ColKey& rhs) const
    {
        return value < rhs.value;
    }
    explicit operator bool() const
    {
        return value != ColKey().value;
    }
    int64_t value;
};

inline std::ostream& operator<<(std::ostream& os, ColKey ck)
{
    os << ck.value;
    return os;
}

namespace util {

inline std::string to_string(ColKey ck)
{
    return to_string(ck.value);
}

} // namespace util

} // namespace realm


#endif
