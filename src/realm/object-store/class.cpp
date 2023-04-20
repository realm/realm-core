////////////////////////////////////////////////////////////////////////////
//
// Copyright 2023 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////


#include <realm/object-store/class.hpp>
#include <realm/object-store/shared_realm.hpp>

namespace realm {

Class::Class(const std::shared_ptr<Realm>& r, StringData object_type)
    : m_realm(r)
    , m_object_schema(&*r->schema().find(object_type))
    , m_table(r->read_group().get_table(m_object_schema->table_key))
{
}

size_t Class::size() const
{
    return m_table->size();
}

TableKey Class::get_key() const
{
    return m_table->get_key();
}

ColKey Class::get_column_key(StringData name) const
{
    return m_table->get_column_key(name);
}

Obj Class::create_object(Mixed pk)
{
    if (!pk.is_null()) {
        bool did_create;
        auto obj = m_table->create_object_with_primary_key(pk, &did_create);
        REALM_ASSERT(did_create);
        return obj;
    }
    return m_table->create_object();
}

Obj Class::get_object(Mixed pk)
{
    return m_table->get_object_with_primary_key(pk);
}

} // namespace realm
