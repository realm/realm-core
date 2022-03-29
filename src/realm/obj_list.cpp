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

#include <realm/table_view.hpp>
#include <set>

namespace realm {

ObjList::~ObjList() {}

TableView ObjList::links(ColKey col_key) const
{
    std::set<ObjKey> keys;
    for (size_t i = 0; i < size(); i++) {
        auto obj = get_object(i);
        keys.insert(obj.get<ObjKey>(col_key));
    }
    TableView ret(get_target_table());
    for (auto k : keys) {
        ret.m_key_values.add(k);
    }
    return ret;
}

TableView ObjList::intersection(const ObjList& list) const
{
    REALM_ASSERT(get_target_table() == list.get_target_table());
    std::set<ObjKey> mykeys;
    for (size_t i = 0; i < size(); i++) {
        mykeys.insert(get_key(i));
    }
    TableView ret(get_target_table());
    for (size_t i = 0; i < list.size(); i++) {
        auto key = list.get_key(i);
        if (mykeys.find(key) != mykeys.end()) {
            ret.m_key_values.add(key);
        }
    }
    return ret;
}

} // namespace realm
