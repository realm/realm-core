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
#include <realm/list.hpp>
#include <realm/dictionary.hpp>

using namespace realm;

// nullify forward links corresponding to any backward links at index 'ndx'.
void ArrayBacklink::nullify_fwd_links(size_t ndx, CascadeState& state)
{
    uint64_t value = Array::get(ndx);
    if (value == 0) {
        return;
    }

    // Naming: Links go from source to target.
    // Backlinks go from target to source.
    // This array holds backlinks, hence it is the target.
    // The table which holds the corresponding fwd links is the source.

    // determine target table, column and key.
    REALM_ASSERT_DEBUG(dynamic_cast<Cluster*>(get_parent()));
    auto cluster = static_cast<Cluster*>(get_parent());
    const Table* target_table = cluster->get_owning_table();
    ColKey target_col_key = cluster->get_col_key(get_ndx_in_parent());
    ObjKey target_key = cluster->get_real_key(ndx);

    // determine the source table/col - which is the one holding the forward links
    TableRef source_table = target_table->get_opposite_table(target_col_key);
    ColKey src_col_key = target_table->get_opposite_column(target_col_key);

    // Now follow all backlinks to their origin and clear forward links.
    if ((value & 1) != 0) {
        // just a single one
        state.enqueue_for_nullification(*source_table, src_col_key, ObjKey(value >> 1),
                                        {target_table->get_key(), target_key});
    }
    else {
        // There is more than one backlink - Iterate through them all
        ref_type ref = to_ref(value);
        Array backlink_list(m_alloc);
        backlink_list.init_from_ref(ref);

        size_t sz = backlink_list.size();
        for (size_t i = 0; i < sz; i++) {
            state.enqueue_for_nullification(*source_table, src_col_key, ObjKey(backlink_list.get(i)),
                                            {target_table->get_key(), target_key});
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

    size_t last_ndx = backlink_list.size() - 1;
    size_t backlink_ndx = backlink_list.find_first(key.value);
    REALM_ASSERT_3(backlink_ndx, !=, not_found);
    if (backlink_ndx != last_ndx)
        backlink_list.set(backlink_ndx, backlink_list.get(last_ndx));
    backlink_list.truncate(last_ndx); // Throws

    // If there is only one backlink left we can inline it as tagged value
    if (last_ndx == 1) {
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

void ArrayBacklink::verify() const
{
#ifdef REALM_DEBUG
    Array::verify();

    REALM_ASSERT(dynamic_cast<Cluster*>(get_parent()));
    auto cluster = static_cast<Cluster*>(get_parent());
    const Table* target_table = cluster->get_owning_table();
    ColKey backlink_col_key = cluster->get_col_key(get_ndx_in_parent());

    TableRef src_table = target_table->get_opposite_table(backlink_col_key);
    ColKey src_col_key = target_table->get_opposite_column(backlink_col_key);

    // Verify that each backlink has a corresponding forward link
    ColumnAttrMask src_attr = src_col_key.get_attrs();
    for (size_t i = 0; i < size(); ++i) {
        ObjKey target_key = cluster->get_real_key(i);
        auto cnt = get_backlink_count(i);
        for (size_t j = 0; j < cnt; ++j) {
            Obj src_obj = src_table->get_object(get_backlink(i, j));
            if (src_attr.test(col_attr_List)) {
                REALM_ASSERT(src_obj.get_list<ObjKey>(src_col_key).find_first(target_key) != npos);
            }
            else if (src_attr.test(col_attr_Dictionary)) {
                // The link is stored as type_TypedLink in Dictionary
                ObjLink link(target_table->get_key(), target_key);
                REALM_ASSERT(src_obj.get_dictionary(src_col_key).find_any(link) != npos);
            }
            else {
                REALM_ASSERT(src_obj.get_unfiltered_link(src_col_key) == target_key);
            }
        }
    }
#endif
}
