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
class TableRef;

class ConstTableRef {
public:
    ~ConstTableRef()
    {
    }
    ConstTableRef(const TableRef& other);
    ConstTableRef(const ConstTableRef& other);
    ConstTableRef(std::nullptr_t) {}
    ConstTableRef& operator=(const ConstTableRef& other);
    ConstTableRef& operator=(const TableRef& other);
    const Table* operator->() const;
    const Table* checked() const;
    const Table* checked_or_null() const;
    const Table* unchecked() const { return m_table; }
    const Table& operator*() const;
    explicit ConstTableRef(const Table* t_ptr);
    ConstTableRef()
    {
    }
    operator bool() const;
    operator const Table*() const
    {
        return checked();
    }

    bool operator==(const ConstTableRef& other) const
    {
        return m_table == other.m_table && m_instance_version == other.m_instance_version;
    }

    bool operator!=(const ConstTableRef& other) const
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

class TableRef : public ConstTableRef {
public:
    Table* operator->() const;
    Table* checked() const;
    Table* checked_or_null() const;
    Table* unchecked() const { return m_table; }
    Table& operator*() const;
    operator Table*() const
    {
        return checked();
    }

    explicit TableRef(Table* t_ptr)
        : ConstTableRef(t_ptr)
    {
    }
    explicit TableRef(const Table* t_ptr)
        : ConstTableRef(t_ptr)
    {
    }
    TableRef(std::nullptr_t) {}
    TableRef()
        : ConstTableRef()
    {
    }
    TableRef& operator=(const TableRef& other)
    {
        ConstTableRef::operator=(other);
        return *this;
    }
protected:
    friend class Group;
    friend class Table;
};


inline ConstTableRef::ConstTableRef(const TableRef& other)
    : m_table(other.m_table)
    , m_instance_version(other.m_instance_version)
{
}

inline ConstTableRef::ConstTableRef(const ConstTableRef& other)
    : m_table(other.m_table), m_instance_version(other.m_instance_version)
{
}

inline ConstTableRef& ConstTableRef::operator=(const ConstTableRef& other)
{
    m_table = other.m_table;
    m_instance_version = other.m_instance_version;
    return *this;
}

inline ConstTableRef& ConstTableRef::operator=(const TableRef& other)
{
    m_table = other.m_table;
    m_instance_version = other.m_instance_version;
    return *this;
}

inline std::ostream& operator<<(std::ostream& o, const ConstTableRef& tr)
{
    return tr.print(o);
}

} // namespace realm

#endif // REALM_TABLE_REF_HPP
