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

#include <realm/array_backlink.hpp>
#include <realm/util/assert.hpp>
#include <realm/table.hpp>
#include <realm/group.hpp>

using namespace realm;

// nullify forward links corresponding to any backward links at index 'ndx'.
void ArrayBacklink::nullify_fwd_links(size_t ndx, CascadeState& state)
{
    uint64_t value = Array::get(ndx);
    if (value != 0) {
        // Naming: Links go from source to target.
        // Backlinks go from target to source.
        // This array holds backlinks, hence it is the target.
        // The table which holds the corresponding fwd links is the source.

        // determine target table, column and key.
        REALM_ASSERT_DEBUG(dynamic_cast<Cluster*>(get_parent()));
        auto cluster = static_cast<Cluster*>(get_parent());
        const Table* target_table = cluster->m_tree_top.get_owner();
        ColKey::Idx target_col_ndx{unsigned(get_ndx_in_parent() - 1)}; // <- leaf_index here. Opaque.
        auto target_col_key = target_table->leaf_ndx2colkey(target_col_ndx);
        REALM_ASSERT(target_col_key.get_index().val == target_col_ndx.val);

        ObjKey target_key = cluster->get_real_key(ndx);

        // determine the source table/col - which is the one holding the forward links
        // FIXME: May have to be moved to Table so that we don't have spec access here.
        // FIXME: This mix of using keys and indexes are not good.
        TableRef source_table = _impl::TableFriend::get_opposite_link_table(*target_table, target_col_key);
        source_table->bump_content_version();
        const Spec& target_spec = cluster->m_tree_top.get_spec();
        size_t target_spec_ndx = target_table->leaf_ndx2spec_ndx(target_col_ndx);
        ColKey src_col_key = target_spec.get_origin_column_key(target_spec_ndx);
        TableKey src_table_key = source_table->get_key();

        Group::CascadeNotification notifications;
        // Now follow all backlinks to their origin and clear forward links.
        if ((value & 1) != 0) {
            // just a single one
            notifications.links.emplace_back(src_table_key, src_col_key, ObjKey(value >> 1), target_key);
        }
        else {
            // There is more than one backlink - Iterate through them all
            ref_type ref = to_ref(value);
            Array backlink_list(m_alloc);
            backlink_list.init_from_ref(ref);

            size_t sz = backlink_list.size();
            for (size_t i = 0; i < sz; i++) {
                notifications.links.emplace_back(src_table_key, src_col_key, ObjKey(backlink_list.get(i)),
                                                 target_key);
            }
        }

        if (state.notification_handler()) {
            state.send_notifications(notifications);
        }

        // Nullify links
        for (auto& l : notifications.links) {
            Obj obj = source_table->get_object(l.origin_key);
            obj.nullify_link(src_col_key, target_key);
        }
    }
}

void ArrayBacklink::add(size_t ndx, ObjKey key)
{
    uint64_t value = Array::get(ndx);

    // A backlink list of size 1 is stored as a single non-ref column value.
    if (value == 0) {
        set(ndx, key.value << 1 | 1); // Throws
        return;
    }

    // When increasing the size of the backlink list from 1 to 2, we need to
    // convert from the single non-ref column value representation, to a B+-tree
    // representation.
    Array backlink_list(m_alloc);
    if ((value & 1) != 0) {
        // Create new column to hold backlinks
        backlink_list.create(Array::type_Normal);
        set_as_ref(ndx, backlink_list.get_ref());
        backlink_list.add(value >> 1);
    }
    else {
        backlink_list.init_from_ref(to_ref(value));
        backlink_list.set_parent(this, ndx);
    }
    backlink_list.add(key.value); // Throws
}

// Return true if the last link was removed
bool ArrayBacklink::remove(size_t ndx, ObjKey key)
{
    uint64_t value = Array::get(ndx);
    REALM_ASSERT(value != 0);

    // If there is only a single backlink, it can be stored as
    // a tagged value
    if ((value & 1) != 0) {
        REALM_ASSERT_3(int64_t(value >> 1), ==, key.value);
        set(ndx, 0);
        return true;
    }

    // if there is a list of backlinks we have to find
    // the right one and remove it.
    Array backlink_list(m_alloc);
    backlink_list.init_from_ref(ref_type(value));
    backlink_list.set_parent(this, ndx);

    size_t backlink_ndx = backlink_list.find_first(key.value);
    REALM_ASSERT_3(backlink_ndx, !=, not_found);
    backlink_list.erase(backlink_ndx); // Throws

    // If there is only one backlink left we can inline it as tagged value
    if (backlink_list.size() == 1) {
        uint64_t key_value = backlink_list.get(0);
        backlink_list.destroy();

        set(ndx, key_value << 1 | 1);
    }

    return false;
}

void ArrayBacklink::erase(size_t ndx)
{
    uint64_t value = Array::get(ndx);
    if (value && (value & 1) == 0) {
        Array::destroy(ref_type(value), m_alloc);
    }
    Array::erase(ndx);
}

size_t ArrayBacklink::get_backlink_count(size_t ndx) const
{
    uint64_t value = Array::get(ndx);
    if (value == 0) {
        return 0;
    }

    // If there is only a single backlink, it can be stored as
    // a tagged value
    if ((value & 1) != 0) {
        return 1;
    }

    // return size of list
    MemRef mem(ref_type(value), m_alloc);
    return Array::get_size_from_header(mem.get_addr());
}

ObjKey ArrayBacklink::get_backlink(size_t ndx, size_t index) const
{
    uint64_t value = Array::get(ndx);
    REALM_ASSERT(value != 0);

    // If there is only a single backlink, it can be stored as
    // a tagged value
    if ((value & 1) != 0) {
        REALM_ASSERT(index == 0);
        return ObjKey(int64_t(value >> 1));
    }

    Array backlink_list(m_alloc);
    backlink_list.init_from_ref(ref_type(value));

    REALM_ASSERT(index < backlink_list.size());
    return ObjKey(backlink_list.get(index));
}
