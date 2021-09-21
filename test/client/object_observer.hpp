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

#ifndef REALM_TEST_CLIENT_OBJECT_OBSERVER_HPP
#define REALM_TEST_CLIENT_OBJECT_OBSERVER_HPP

#include <algorithm>
#include <set>
#include <map>

#include <realm/impl/transact_log.hpp>
#include <realm/string_data.hpp>


namespace realm {
namespace test_client {

/// \brief Discover newly created objects.
class ObjectObserver : public _impl::NullInstructionObserver {
public:
    /// New objects will be added to the specified set, and if an object in the
    /// set gets removed from the Realm, it will also be removed from the set.
    ObjectObserver(std::map<TableKey, std::set<ObjKey>>& new_objects) noexcept
        : m_new_objects{new_objects}
    {
    }

    bool erase_class(TableKey table_key)
    {
        m_new_objects.erase(table_key);
        return true;
    }

    bool select_table(TableKey table_key)
    {
        m_selected_table = table_key;
        return true;
    }

    bool create_object(ObjKey key)
    {
        std::set<ObjKey>& objects = m_new_objects[m_selected_table];
        objects.insert(key);
        return true;
    }

    bool remove_object(ObjKey key)
    {
        auto i = m_new_objects.find(m_selected_table);
        if (i != m_new_objects.end()) {
            std::set<ObjKey>& objects = i->second;
            objects.erase(key);
        }
        return true;
    }

private:
    std::map<TableKey, std::set<ObjKey>>& m_new_objects;
    TableKey m_selected_table;
};

} // namespace test_client
} // namespace realm

#endif // REALM_TEST_CLIENT_OBJECT_OBSERVER_HPP
