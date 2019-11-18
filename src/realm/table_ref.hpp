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
    ConstTableRef(std::nullptr_t) {}

    const Table* operator->() const;
    const Table& operator*() const
    {
        return *m_table;
    }
    ConstTableRef()
    {
    }
    operator bool() const;
    const Table* unchecked_ptr() const
    {
        return m_table;
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
    TableRef cast_away_const() const;
    static ConstTableRef unsafe_create(const Table* t_ptr);

protected:
    explicit ConstTableRef(const Table* t_ptr, uint64_t instance_version)
        : m_table(const_cast<Table*>(t_ptr))
        , m_instance_version(instance_version)
    {
    }
    friend class Group; // only Group::get_table() and friends can safely create a TableRef

    Table* m_table = nullptr;
    uint64_t m_instance_version = 0;
};

class TableRef : public ConstTableRef {
public:
    Table* operator->() const;
    Table& operator*() const
    {
        return *m_table;
    }
    Table* unchecked_ptr() const
    {
        return m_table;
    }
    TableRef(std::nullptr_t) {}
    TableRef()
        : ConstTableRef()
    {
    }
    static TableRef unsafe_create(Table* t_ptr);

private:
    explicit TableRef(Table* t_ptr, uint64_t instance_version)
        : ConstTableRef(t_ptr, instance_version)
    {
    }
    friend class Group; // only Group::get_table() and friends can safely create a TableRef
    friend class ConstTableRef;
};


inline ConstTableRef::ConstTableRef(const TableRef& other)
    : m_table(other.m_table)
    , m_instance_version(other.m_instance_version)
{
}

inline TableRef ConstTableRef::cast_away_const() const
{
    return TableRef(m_table, m_instance_version);
}

inline std::ostream& operator<<(std::ostream& o, const ConstTableRef& tr)
{
    return tr.print(o);
}

} // namespace realm

#endif // REALM_TABLE_REF_HPP
