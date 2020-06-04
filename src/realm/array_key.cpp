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

#include <realm/array_key.hpp>
#include <realm/table.hpp>

namespace realm {

// This is used for linklists
template <>
void ArrayKeyBase<0>::verify() const
{
#ifdef REALM_DEBUG
    Array::verify();
    // We have to get parent until we reach the containing cluster
    auto parent = get_parent();
    size_t origin_obj_ndx;
    size_t origin_col_ndx = get_ndx_in_parent();
    Cluster* cluster = nullptr;
    do {
        REALM_ASSERT(parent);
        auto arr = dynamic_cast<Array*>(parent);
        REALM_ASSERT(arr);
        parent = arr->get_parent();
        // When we reach the cluster, the previous value of
        // origin_col_ndx will be the object index in the cluster.
        origin_obj_ndx = origin_col_ndx;
        // When we reach the cluster, arr will point to the ArrayRef leaf and the
        // origin_col_ndx will give the position in the cluster array.
        origin_col_ndx = arr->get_ndx_in_parent();
        cluster = dynamic_cast<Cluster*>(parent);
    } while (parent && !cluster);

    REALM_ASSERT(cluster);
    const Table* origin_table = cluster->get_owning_table();
    ObjKey origin_key = cluster->get_real_key(origin_obj_ndx);
    ColKey link_col_key = cluster->get_col_key(origin_col_ndx);

    TableRef target_table = origin_table->get_opposite_table(link_col_key);

    auto verify_link = [origin_table, link_col_key, origin_key](const Obj& target_obj) {
        auto cnt = target_obj.get_backlink_count(*origin_table, link_col_key);
        for (size_t i = 0; i < cnt; i++) {
            if (target_obj.get_backlink(*origin_table, link_col_key, i) == origin_key)
                return;
        }
        REALM_ASSERT(false);
    };

    // Verify that forward link has a corresponding backlink
    for (size_t i = 0; i < size(); ++i) {
        if (ObjKey target_key = get(i)) {
            auto target_obj = target_key.is_unresolved() ? target_table->get_tombstone(target_key)
                                                         : target_table->get_object(target_key);
            verify_link(target_obj);
        }
    }
#endif
}

// This is used for single links
template <>
void ArrayKeyBase<1>::verify() const
{
#ifdef REALM_DEBUG
    Array::verify();
    REALM_ASSERT(dynamic_cast<Cluster*>(get_parent()));
    auto cluster = static_cast<Cluster*>(get_parent());
    const Table* origin_table = cluster->get_owning_table();
    ColKey link_col_key = cluster->get_col_key(get_ndx_in_parent());

    ConstTableRef target_table = origin_table->get_opposite_table(link_col_key);

    auto verify_link = [origin_table, link_col_key](const Obj& target_obj, ObjKey origin_key) {
        auto cnt = target_obj.get_backlink_count(*origin_table, link_col_key);
        for (size_t i = 0; i < cnt; i++) {
            if (target_obj.get_backlink(*origin_table, link_col_key, i) == origin_key)
                return;
        }
        REALM_ASSERT(false);
    };

    // Verify that forward link has a corresponding backlink
    for (size_t i = 0; i < size(); ++i) {
        if (ObjKey target_key = get(i)) {
            ObjKey origin_key = cluster->get_real_key(i);

            auto target_obj = target_key.is_unresolved() ? target_table->get_tombstone(target_key)
                                                         : target_table->get_object(target_key);
            verify_link(target_obj, origin_key);
        }
    }
#endif
}

} // namespace realm
