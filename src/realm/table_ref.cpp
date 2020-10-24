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

#include <realm/table_ref.hpp>
#include <realm/table.hpp>

namespace realm {

ConstTableRef ConstTableRef::unsafe_create(const Table* t_ptr)
{
    return ConstTableRef(const_cast<Table*>(t_ptr), t_ptr ? t_ptr->get_instance_version() : 0);
}

TableRef TableRef::unsafe_create(Table* t_ptr)
{
    return TableRef(t_ptr, t_ptr ? t_ptr->get_instance_version() : 0);
}

ConstTableRef::operator bool() const noexcept
{
    return m_table != nullptr && m_table->get_instance_version() == m_instance_version;
}

void ConstTableRef::check() const
{
    if (m_table == nullptr) {
        throw InvalidTableRef("null");
    }
    if (m_table->get_instance_version() != m_instance_version) {
        // we cannot distinguish between removal of a table and expiration of the
        // tableref due to other changes. In the latter cases, we'd like to throw
        // a logic error: detached accessor. But in the former case, a user REALLY
        // would expect a NoSuchTable. So we choose that:
        throw InvalidTableRef(m_table->get_state());
    }
}

const Table& ConstTableRef::operator*() const
{
    check();
    return *m_table;
}

const Table* ConstTableRef::operator->() const
{
    check();
    return m_table;
}

Table& TableRef::operator*() const
{
    check();
    return *m_table;
}

Table* TableRef::operator->() const
{
    check();
    return m_table;
}


}
