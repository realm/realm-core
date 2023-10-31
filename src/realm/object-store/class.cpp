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

Class::Class(std::shared_ptr<Realm> r, const ObjectSchema* object_schema)
    : m_realm(std::move(r))
    , m_object_schema(object_schema)
    , m_table(m_realm->read_group().get_table(m_object_schema->table_key))
{
}

std::pair<Obj, bool> Class::create_object(Mixed pk)
{
    bool did_create;
    auto obj = m_table->create_object_with_primary_key(pk, &did_create);
    return {obj, did_create};
}

Obj Class::create_object()
{
    return m_table->create_object();
}

Obj Class::get_object(Mixed pk)
{
    return m_table->get_object_with_primary_key(pk);
}

} // namespace realm
