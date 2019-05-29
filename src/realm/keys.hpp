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
#include <realm/column_type.hpp>
#include <ostream>
#include <vector>

namespace realm {

struct TableKey {
    static constexpr int64_t null_value = uint64_t(-1) >> 1; // free top bit
    constexpr TableKey()
        : value(null_value)
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
    explicit operator bool() const
    {
        return value != null_value;
    }
    int64_t value;
};


inline std::ostream& operator<<(std::ostream& os, TableKey tk)
{
    os << "TableKey(" << tk.value << ")";
    return os;
}

namespace util {

inline std::string to_string(TableKey tk)
{
    return to_string(tk.value);
}
}

class TableVersions : public std::vector<std::pair<TableKey, uint64_t>> {
public:
    TableVersions()
    {
    }
    TableVersions(TableKey key, uint64_t version)
    {
        emplace_back(key, version);
    }
    bool operator==(const TableVersions& other) const;
};

struct ColKey {
    struct Idx {
        unsigned val;
    };

    constexpr ColKey()
        : value(uint64_t(-1) >> 1) // free top bit
    {
    }
    explicit ColKey(int64_t val)
        : value(val)
    {
    }
    explicit ColKey(Idx index, ColumnType type, ColumnAttrMask attrs, unsigned tag)
        : ColKey((index.val & 0xFFFFUL) | ((type & 0x3FUL) << 16) | ((attrs.m_value & 0xFFUL) << 22) |
                 ((tag & 0xFFFFFFFFUL) << 30))
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
    bool operator>(const ColKey& rhs) const
    {
        return value > rhs.value;
    }
    explicit operator bool() const
    {
        return value != ColKey().value;
    }
    Idx get_index() const
    {
        return Idx{static_cast<unsigned>(value) & 0xFFFFU};
    }
    ColumnType get_type() const
    {
        return ColumnType((static_cast<unsigned>(value) >> 16) & 0x3F);
    }
    ColumnAttrMask get_attrs() const
    {
        return ColumnAttrMask((static_cast<unsigned>(value) >> 22) & 0xFF);
    }
    unsigned get_tag() const
    {
        return (value >> 30) & 0xFFFFFFFFUL;
    }
    int64_t value;
};

inline std::ostream& operator<<(std::ostream& os, ColKey ck)
{
    os << "ColKey(" << ck.value << ")";
    return os;
}

struct ObjKey {
    constexpr ObjKey()
        : value(-1)
    {
    }
    explicit constexpr ObjKey(int64_t val)
        : value(val)
    {
    }
    ObjKey& operator=(int64_t val)
    {
        value = val;
        return *this;
    }
    bool operator==(const ObjKey& rhs) const
    {
        return value == rhs.value;
    }
    bool operator!=(const ObjKey& rhs) const
    {
        return value != rhs.value;
    }
    bool operator<(const ObjKey& rhs) const
    {
        return value < rhs.value;
    }
    bool operator>(const ObjKey& rhs) const
    {
        return value > rhs.value;
    }
    explicit operator bool() const
    {
        return value != -1;
    }
    int64_t value;

private:
    // operator bool will enable casting to integer. Prevent this.
    operator int64_t() const
    {
        return 0;
    }
};

class ObjKeys : public std::vector<ObjKey> {
public:
    ObjKeys(const std::vector<int64_t>& init)
    {
        reserve(init.size());
        for (auto i : init) {
            emplace_back(i);
        }
    }
    ObjKeys()
    {
    }
};


inline std::ostream& operator<<(std::ostream& ostr, ObjKey key)
{
    ostr << "ObjKey(" << key.value << ")";
    return ostr;
}

constexpr ObjKey null_key;

namespace util {

inline std::string to_string(ColKey ck)
{
    return to_string(ck.value);
}

} // namespace util

} // namespace realm


#endif
