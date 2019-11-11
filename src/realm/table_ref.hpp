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

#ifndef REALM_TABLE_REF_HPP
#define REALM_TABLE_REF_HPP

#include <cstddef>
#include <algorithm>
#include <ostream>
namespace realm {


class Table;

class TableRef {
public:
    Table* operator->() const;
    Table* checked() const;
    Table* checked_or_null() const;
    Table* unchecked() const { return m_table; }
    Table& operator*() const;
    operator bool() const;
    operator Table*() const
    {
        return checked();
    }

    explicit TableRef(const Table* t_ptr);
    TableRef(const TableRef&) noexcept;
    TableRef(std::nullptr_t) {}
    TableRef()
    {
    }
    TableRef& operator=(const TableRef& other);
    bool operator==(const TableRef& other) const
    {
        return m_table == other.m_table && m_instance_version == other.m_instance_version;
    }

    bool operator!=(const TableRef& other) const
    {
        return !(*this == other);
    }

    std::ostream& print(std::ostream& o) const
    {
        return o << "TableRef(" << m_table << ", " << m_instance_version << ")";
    }

protected:
    Table* m_table = nullptr;
    uint64_t m_instance_version = 0;
    friend class Group;
    friend class Table;
};


inline TableRef::TableRef(const TableRef& other) noexcept
    : m_table(other.m_table)
    , m_instance_version(other.m_instance_version)
{
}

inline TableRef& TableRef::operator=(const TableRef& other)
{
    m_table = other.m_table;
    m_instance_version = other.m_instance_version;
    return *this;
}

inline std::ostream& operator<<(std::ostream& o, const TableRef& tr)
{
    return tr.print(o);
}

} // namespace realm

#endif // REALM_TABLE_REF_HPP
