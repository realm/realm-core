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

using namespace realm;

void ArrayBacklink::erase(size_t ndx)
{
    auto cluster = dynamic_cast<Cluster*>(get_parent());
    size_t col_ndx = get_ndx_in_parent() - 1;
    Key target_key_value = Key(cluster->get_key(ndx));

    const Spec& spec = cluster->m_tree_top.get_spec();

    TableRef target_table = _impl::TableFriend::get_opposite_link_table(*cluster->m_tree_top.get_owner(), col_ndx);
    size_t origin_col = spec.get_origin_column_ndx(col_ndx);

    auto clear_link = [&target_table, origin_col, target_key_value](Key origin_key) {
        Obj obj = target_table->get_object(origin_key);
        obj.nullify_link(origin_col, target_key_value);
    };

    uint64_t value = Array::get(ndx);
    if (value != 0) {
        if ((value & 1) != 0) {
            clear_link(Key(value >> 1));
        }
        else {
            // There is more than one backlink
            // Iterate through them all
            ref_type ref = to_ref(value);
            Array backlink_list(m_alloc);
            backlink_list.init_from_ref(ref);

            size_t sz = backlink_list.size();
            for (size_t i = 0; i < sz; i++) {
                clear_link(Key(backlink_list.get(i)));
            }
            backlink_list.destroy();
        }
    }
    Array::erase(ndx);
}

void ArrayBacklink::add(size_t ndx, Key key)
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

void ArrayBacklink::remove(size_t ndx, Key key)
{
    int64_t value = Array::get(ndx);
    REALM_ASSERT(value != 0);

    // If there is only a single backlink, it can be stored as
    // a tagged value
    if ((value & 1) != 0) {
        REALM_ASSERT_3(value >> 1, ==, key.value);
        set(ndx, 0);
        return;
    }

    // if there is a list of backlinks we have to find
    // the right one and remove it.
    ref_type ref = to_ref(value);
    Array backlink_list(m_alloc);
    backlink_list.init_from_ref(ref);
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
}

size_t ArrayBacklink::get_backlink_count(size_t ndx) const
{
    int64_t value = Array::get(ndx);
    if (value == 0) {
        return 0;
    }

    // If there is only a single backlink, it can be stored as
    // a tagged value
    if ((value & 1) != 0) {
        return 1;
    }

    // return size of list
    MemRef mem(to_ref(value), m_alloc);
    return Array::get_size_from_header(mem.get_addr());
}

Key ArrayBacklink::get_backlink(size_t ndx, size_t index) const
{
    int64_t value = Array::get(ndx);
    REALM_ASSERT(value != 0);

    // If there is only a single backlink, it can be stored as
    // a tagged value
    if ((value & 1) != 0) {
        REALM_ASSERT(index == 0);
        return Key(value >> 1);
    }

    ref_type ref = to_ref(value);
    Array backlink_list(m_alloc);
    backlink_list.init_from_ref(ref);

    REALM_ASSERT(index < backlink_list.size());
    return Key(backlink_list.get(index));
}
