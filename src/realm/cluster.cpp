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

#include "realm/cluster.hpp"
#include "realm/dictionary_cluster_tree.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_basic.hpp"
#include "realm/array_bool.hpp"
#include "realm/array_string.hpp"
#include "realm/array_binary.hpp"
#include "realm/array_mixed.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/array_decimal128.hpp"
#include "realm/array_fixed_bytes.hpp"
#include "realm/array_key.hpp"
#include "realm/array_ref.hpp"
#include "realm/array_typed_link.hpp"
#include "realm/array_backlink.hpp"
#include "realm/column_type_traits.hpp"
#include "realm/replication.hpp"
#include "realm/dictionary.hpp"
#include <iostream>
#include <cmath>

namespace realm {

/******************************* ClusterNode *********************************/

void ClusterNode::IteratorState::clear()
{
    m_current_leaf.detach();
    m_key_offset = 0;
    m_current_index = size_t(-1);
}

void ClusterNode::IteratorState::init(State& s, ObjKey key)
{
    m_current_leaf.init(s.mem);
    m_current_index = s.index;
    m_key_offset = key.value - m_current_leaf.get_key_value(m_current_index);
    m_current_leaf.set_offset(m_key_offset);
}

const Table* ClusterNode::get_owning_table() const noexcept
{
    return m_tree_top.get_owning_table();
}

void ClusterNode::get(ObjKey k, ClusterNode::State& state) const
{
    if (!k || !try_get(k, state)) {
        throw KeyNotFound(util::format("No object with key '%1' in '%2'", k.value, get_owning_table()->get_name()));
    }
}


/********************************* Cluster ***********************************/

MemRef Cluster::create_empty_cluster(Allocator& alloc)
{
    Array arr(alloc);
    arr.create(Array::type_HasRefs); // Throws

    arr.add(RefOrTagged::make_tagged(0)); // Compact form
    return arr.get_mem();
}


template <class T>
inline void Cluster::do_create(ColKey col)
{
    T arr(m_alloc);
    arr.create();
    auto col_ndx = col.get_index();
    arr.set_parent(this, col_ndx.val + s_first_col_index);
    arr.update_parent();
}

void Cluster::create()
{
    Array::create(type_HasRefs, false, s_first_col_index);
    Array::set(0, RefOrTagged::make_tagged(0)); // Size = 0

    auto column_initialize = [this](ColKey col_key) {
        auto col_ndx = col_key.get_index();
        while (size() <= col_ndx.val + 1)
            add(0);
        auto type = col_key.get_type();
        auto attr = col_key.get_attrs();
        if (attr.test(col_attr_Collection)) {
            ArrayRef arr(m_alloc);
            arr.create();
            arr.set_parent(this, col_ndx.val + s_first_col_index);
            arr.update_parent();
            return false;
        }
        switch (type) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_create<ArrayIntNull>(col_key);
                }
                else {
                    do_create<ArrayInteger>(col_key);
                }
                break;
            case col_type_Bool:
                do_create<ArrayBoolNull>(col_key);
                break;
            case col_type_Float:
                do_create<ArrayFloatNull>(col_key);
                break;
            case col_type_Double:
                do_create<ArrayDoubleNull>(col_key);
                break;
            case col_type_String: {
                if (m_tree_top.is_string_enum_type(col_ndx)) {
                    do_create<ArrayInteger>(col_key);
                }
                else {
                    do_create<ArrayString>(col_key);
                }
                break;
            }
            case col_type_Binary:
                do_create<ArrayBinary>(col_key);
                break;
            case col_type_Mixed:
                do_create<ArrayMixed>(col_key);
                break;
            case col_type_Timestamp:
                do_create<ArrayTimestamp>(col_key);
                break;
            case col_type_Decimal:
                do_create<ArrayDecimal128>(col_key);
                break;
            case col_type_ObjectId:
                do_create<ArrayObjectIdNull>(col_key);
                break;
            case col_type_UUID:
                do_create<ArrayUUIDNull>(col_key);
                break;
            case col_type_Link:
                do_create<ArrayKey>(col_key);
                break;
            case col_type_TypedLink:
                do_create<ArrayTypedLink>(col_key);
                break;
            case col_type_BackLink:
                do_create<ArrayBacklink>(col_key);
                break;
            default:
                throw LogicError(LogicError::illegal_type);
        }
        return false;
    };
    m_tree_top.for_each_and_every_column(column_initialize);

    // By specifying the minimum size, we ensure that the array has a capacity
    // to hold m_size 64 bit refs.
    ensure_size(m_size * 8);
    // "ensure_size" may COW, but as array is just created, it has no parents, so
    // failing to update parent is not an error.
    clear_missing_parent_update();
}

void Cluster::init(MemRef mem)
{
    Array::init_from_mem(mem);
    auto rot = Array::get_as_ref_or_tagged(0);
    if (rot.is_tagged()) {
        m_keys.detach();
    }
    else {
        m_keys.init_from_ref(rot.get_as_ref());
    }
}

void Cluster::update_from_parent() noexcept
{
    Array::update_from_parent();
    auto rot = Array::get_as_ref_or_tagged(0);
    if (!rot.is_tagged()) {
        m_keys.update_from_parent();
    }
}

MemRef Cluster::ensure_writeable(ObjKey)
{
    // By specifying the minimum size, we ensure that the array has a capacity
    // to hold m_size 64 bit refs.
    copy_on_write(8 * m_size);

    return get_mem();
}

void Cluster::update_ref_in_parent(ObjKey, ref_type)
{
    REALM_UNREACHABLE();
}

size_t Cluster::node_size_from_header(Allocator& alloc, const char* header)
{
    auto rot = Array::get_as_ref_or_tagged(header, s_key_ref_or_size_index);
    if (rot.is_tagged()) {
        return size_t(rot.get_as_int());
    }
    else {
        return Array::get_size_from_header(alloc.translate(rot.get_as_ref()));
    }
}

template <class T>
inline void Cluster::set_spec(T&, ColKey::Idx) const
{
}

template <>
inline void Cluster::set_spec(ArrayString& arr, ColKey::Idx col_ndx) const
{
    m_tree_top.set_spec(arr, col_ndx);
}

template <class T>
inline void Cluster::do_insert_row(size_t ndx, ColKey col, Mixed init_val, bool nullable)
{
    using U = typename util::RemoveOptional<typename T::value_type>::type;

    T arr(m_alloc);
    auto col_ndx = col.get_index();
    arr.set_parent(this, col_ndx.val + s_first_col_index);
    set_spec<T>(arr, col_ndx);
    arr.init_from_parent();
    if (init_val.is_null()) {
        arr.insert(ndx, T::default_value(nullable));
    }
    else {
        arr.insert(ndx, init_val.get<U>());
    }
}

inline void Cluster::do_insert_key(size_t ndx, ColKey col_key, Mixed init_val, ObjKey origin_key)
{
    ObjKey target_key = init_val.is_null() ? ObjKey{} : init_val.get<ObjKey>();
    ArrayKey arr(m_alloc);
    auto col_ndx = col_key.get_index();
    arr.set_parent(this, col_ndx.val + s_first_col_index);
    arr.init_from_parent();
    arr.insert(ndx, target_key);

    // Insert backlink if link is not null
    if (target_key) {
        const Table* origin_table = m_tree_top.get_owning_table();
        ColKey opp_col = origin_table->get_opposite_column(col_key);
        TableRef opp_table = origin_table->get_opposite_table(col_key);
        Obj target_obj = opp_table->get_object(target_key);
        target_obj.add_backlink(opp_col, origin_key);
    }
}

inline void Cluster::do_insert_mixed(size_t ndx, ColKey col_key, Mixed init_value, ObjKey origin_key)
{
    ArrayMixed arr(m_alloc);
    arr.set_parent(this, col_key.get_index().val + s_first_col_index);
    arr.init_from_parent();
    arr.insert(ndx, init_value);

    // Insert backlink if needed
    if (init_value.is_type(type_TypedLink)) {
        // In case we are inserting in a Dictionary cluster, the backlink will
        // be handled in Dictionary::insert function
        if (Table* origin_table = const_cast<Table*>(m_tree_top.get_owning_table())) {
            ObjLink link = init_value.get<ObjLink>();
            auto target_table = origin_table->get_parent_group()->get_table(link.get_table_key());

            ColKey backlink_col_key = target_table->find_or_add_backlink_column(col_key, origin_table->get_key());
            target_table->get_object(link.get_obj_key()).add_backlink(backlink_col_key, origin_key);
        }
    }
}

inline void Cluster::do_insert_link(size_t ndx, ColKey col_key, Mixed init_val, ObjKey origin_key)
{
    ObjLink target_link = init_val.is_null() ? ObjLink{} : init_val.get<ObjLink>();
    ArrayTypedLink arr(m_alloc);
    auto col_ndx = col_key.get_index();
    arr.set_parent(this, col_ndx.val + s_first_col_index);
    arr.init_from_parent();
    arr.insert(ndx, target_link);

    // Insert backlink if link is not null
    if (target_link) {
        Table* origin_table = const_cast<Table*>(m_tree_top.get_owning_table());
        auto target_table = origin_table->get_parent_group()->get_table(target_link.get_table_key());

        ColKey backlink_col_key = target_table->find_or_add_backlink_column(col_key, origin_table->get_key());
        target_table->get_object(target_link.get_obj_key()).add_backlink(backlink_col_key, origin_key);
    }
}

void Cluster::insert_row(size_t ndx, ObjKey k, const FieldValues& init_values)
{
    // Ensure the cluster array is big enough to hold 64 bit values.
    copy_on_write(m_size * 8);

    if (m_keys.is_attached()) {
        m_keys.insert(ndx, k.value);
    }
    else {
        Array::set(s_key_ref_or_size_index, Array::get(s_key_ref_or_size_index) + 2); // Increments size by 1
    }

    auto val = init_values.begin();
    auto insert_in_column = [&](ColKey col_key) {
        auto col_ndx = col_key.get_index();
        auto attr = col_key.get_attrs();
        Mixed init_value;
        // init_values must be sorted in col_ndx order - this is ensured by ClustTree::insert()
        if (val != init_values.end() && val->col_key.get_index().val == col_ndx.val) {
            init_value = val->value;
            ++val;
        }

        auto type = col_key.get_type();
        if (attr.test(col_attr_Collection)) {
            REALM_ASSERT(init_value.is_null());
            ArrayRef arr(m_alloc);
            arr.set_parent(this, col_ndx.val + s_first_col_index);
            arr.init_from_parent();
            arr.insert(ndx, 0);
            return false;
        }

        bool nullable = attr.test(col_attr_Nullable);
        switch (type) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_insert_row<ArrayIntNull>(ndx, col_key, init_value, nullable);
                }
                else {
                    do_insert_row<ArrayInteger>(ndx, col_key, init_value, nullable);
                }
                break;
            case col_type_Bool:
                do_insert_row<ArrayBoolNull>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Float:
                do_insert_row<ArrayFloatNull>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Double:
                do_insert_row<ArrayDoubleNull>(ndx, col_key, init_value, nullable);
                break;
            case col_type_String:
                do_insert_row<ArrayString>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Binary:
                do_insert_row<ArrayBinary>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Mixed: {
                do_insert_mixed(ndx, col_key, init_value, ObjKey(k.value + get_offset()));
                break;
            }
            case col_type_Timestamp:
                do_insert_row<ArrayTimestamp>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Decimal:
                do_insert_row<ArrayDecimal128>(ndx, col_key, init_value, nullable);
                break;
            case col_type_ObjectId:
                do_insert_row<ArrayObjectIdNull>(ndx, col_key, init_value, nullable);
                break;
            case col_type_UUID:
                do_insert_row<ArrayUUIDNull>(ndx, col_key, init_value, nullable);
                break;
            case col_type_Link:
                do_insert_key(ndx, col_key, init_value, ObjKey(k.value + get_offset()));
                break;
            case col_type_TypedLink:
                do_insert_link(ndx, col_key, init_value, ObjKey(k.value + get_offset()));
                break;
            case col_type_BackLink: {
                ArrayBacklink arr(m_alloc);
                arr.set_parent(this, col_ndx.val + s_first_col_index);
                arr.init_from_parent();
                arr.insert(ndx, 0);
                break;
            }
            default:
                REALM_ASSERT(false);
                break;
        }
        return false;
    };
    m_tree_top.for_each_and_every_column(insert_in_column);
}

template <class T>
inline void Cluster::do_move(size_t ndx, ColKey col_key, Cluster* to)
{
    auto col_ndx = col_key.get_index().val + s_first_col_index;
    T src(m_alloc);
    src.set_parent(this, col_ndx);
    src.init_from_parent();

    T dst(m_alloc);
    dst.set_parent(to, col_ndx);
    dst.init_from_parent();

    src.move(dst, ndx);
}

void Cluster::move(size_t ndx, ClusterNode* new_node, int64_t offset)
{
    auto new_leaf = static_cast<Cluster*>(new_node);

    auto move_from_column = [&](ColKey col_key) {
        auto attr = col_key.get_attrs();
        auto type = col_key.get_type();

        if (attr.test(col_attr_Collection)) {
            do_move<ArrayRef>(ndx, col_key, new_leaf);
            return false;
        }

        switch (type) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_move<ArrayIntNull>(ndx, col_key, new_leaf);
                }
                else {
                    do_move<ArrayInteger>(ndx, col_key, new_leaf);
                }
                break;
            case col_type_Bool:
                do_move<ArrayBoolNull>(ndx, col_key, new_leaf);
                break;
            case col_type_Float:
                do_move<ArrayFloat>(ndx, col_key, new_leaf);
                break;
            case col_type_Double:
                do_move<ArrayDouble>(ndx, col_key, new_leaf);
                break;
            case col_type_String: {
                if (m_tree_top.is_string_enum_type(col_key.get_index()))
                    do_move<ArrayInteger>(ndx, col_key, new_leaf);
                else
                    do_move<ArrayString>(ndx, col_key, new_leaf);
                break;
            }
            case col_type_Binary:
                do_move<ArrayBinary>(ndx, col_key, new_leaf);
                break;
            case col_type_Mixed:
                do_move<ArrayMixed>(ndx, col_key, new_leaf);
                break;
            case col_type_Timestamp:
                do_move<ArrayTimestamp>(ndx, col_key, new_leaf);
                break;
            case col_type_Decimal:
                do_move<ArrayDecimal128>(ndx, col_key, new_leaf);
                break;
            case col_type_ObjectId:
                do_move<ArrayObjectIdNull>(ndx, col_key, new_leaf);
                break;
            case col_type_UUID:
                do_move<ArrayUUIDNull>(ndx, col_key, new_leaf);
                break;
            case col_type_Link:
                do_move<ArrayKey>(ndx, col_key, new_leaf);
                break;
            case col_type_TypedLink:
                do_move<ArrayTypedLink>(ndx, col_key, new_leaf);
                break;
            case col_type_BackLink:
                do_move<ArrayBacklink>(ndx, col_key, new_leaf);
                break;
            default:
                REALM_ASSERT(false);
                break;
        }
        return false;
    };
    m_tree_top.for_each_and_every_column(move_from_column);
    for (size_t i = ndx; i < m_keys.size(); i++) {
        new_leaf->m_keys.add(m_keys.get(i) - offset);
    }
    m_keys.truncate(ndx);
}

Cluster::~Cluster() {}

ColKey Cluster::get_col_key(size_t ndx_in_parent) const
{
    ColKey::Idx col_ndx{unsigned(ndx_in_parent - 1)}; // <- leaf_index here. Opaque.
    auto col_key = get_owning_table()->leaf_ndx2colkey(col_ndx);
    REALM_ASSERT(col_key.get_index().val == col_ndx.val);
    return col_key;
}

void Cluster::ensure_general_form()
{
    if (!m_keys.is_attached()) {
        size_t current_size = get_size_in_compact_form();
        m_keys.create(current_size, 255);
        m_keys.update_parent();
        for (size_t i = 0; i < current_size; i++) {
            m_keys.set(i, i);
        }
    }
}

template <class T>
inline void Cluster::do_insert_column(ColKey col_key, bool nullable)
{
    size_t sz = node_size();

    T arr(m_alloc);
    arr.create();
    auto val = T::default_value(nullable);
    for (size_t i = 0; i < sz; i++) {
        arr.add(val);
    }
    auto col_ndx = col_key.get_index();
    unsigned ndx = col_ndx.val + s_first_col_index;

    // Fill up if indexes are not consecutive
    while (size() < ndx)
        Array::add(0);

    if (ndx == size())
        Array::insert(ndx, from_ref(arr.get_ref()));
    else
        Array::set(ndx, from_ref(arr.get_ref()));
}

void Cluster::insert_column(ColKey col_key)
{
    auto attr = col_key.get_attrs();
    auto type = col_key.get_type();
    if (attr.test(col_attr_Collection)) {
        size_t sz = node_size();

        ArrayRef arr(m_alloc);
        arr.create(sz);
        auto col_ndx = col_key.get_index();
        unsigned idx = col_ndx.val + s_first_col_index;
        if (idx == size())
            Array::insert(idx, from_ref(arr.get_ref()));
        else
            Array::set(idx, from_ref(arr.get_ref()));
        return;
    }
    bool nullable = attr.test(col_attr_Nullable);
    switch (type) {
        case col_type_Int:
            if (nullable) {
                do_insert_column<ArrayIntNull>(col_key, nullable);
            }
            else {
                do_insert_column<ArrayInteger>(col_key, nullable);
            }
            break;
        case col_type_Bool:
            do_insert_column<ArrayBoolNull>(col_key, nullable);
            break;
        case col_type_Float:
            do_insert_column<ArrayFloatNull>(col_key, nullable);
            break;
        case col_type_Double:
            do_insert_column<ArrayDoubleNull>(col_key, nullable);
            break;
        case col_type_String:
            do_insert_column<ArrayString>(col_key, nullable);
            break;
        case col_type_Binary:
            do_insert_column<ArrayBinary>(col_key, nullable);
            break;
        case col_type_Mixed:
            do_insert_column<ArrayMixed>(col_key, nullable);
            break;
        case col_type_Timestamp:
            do_insert_column<ArrayTimestamp>(col_key, nullable);
            break;
        case col_type_Decimal:
            do_insert_column<ArrayDecimal128>(col_key, nullable);
            break;
        case col_type_ObjectId:
            do_insert_column<ArrayObjectIdNull>(col_key, nullable);
            break;
        case col_type_UUID:
            do_insert_column<ArrayUUIDNull>(col_key, nullable);
            break;
        case col_type_Link:
            do_insert_column<ArrayKey>(col_key, nullable);
            break;
        case col_type_TypedLink:
            do_insert_column<ArrayTypedLink>(col_key, nullable);
            break;
        case col_type_BackLink:
            do_insert_column<ArrayBacklink>(col_key, nullable);
            break;
        default:
            throw LogicError(LogicError::illegal_type);
            break;
    }
}

void Cluster::remove_column(ColKey col_key)
{
    auto col_ndx = col_key.get_index();
    unsigned idx = col_ndx.val + s_first_col_index;
    ref_type ref = to_ref(Array::get(idx));
    if (ref != 0) {
        Array::destroy_deep(ref, m_alloc);
    }
    if (idx == size() - 1)
        Array::erase(idx);
    else
        Array::set(idx, 0);
}

ref_type Cluster::insert(ObjKey k, const FieldValues& init_values, ClusterNode::State& state)
{
    int64_t current_key_value = -1;
    size_t sz;
    size_t ndx;
    ref_type ret = 0;

    auto on_error = [&] {
        throw KeyAlreadyUsed(
            util::format("When inserting key '%1' in '%2'", k.value, get_owning_table()->get_name()));
    };

    if (m_keys.is_attached()) {
        sz = m_keys.size();
        ndx = m_keys.lower_bound(uint64_t(k.value));
        if (ndx < sz) {
            current_key_value = m_keys.get(ndx);
            if (k.value == current_key_value) {
                on_error();
            }
        }
    }
    else {
        sz = size_t(Array::get(s_key_ref_or_size_index)) >> 1; // Size is stored as tagged integer
        if (uint64_t(k.value) < sz) {
            on_error();
        }
        // Key value is bigger than all other values, should be put last
        ndx = sz;
        if (uint64_t(k.value) > sz && sz < cluster_node_size) {
            ensure_general_form();
        }
    }

    REALM_ASSERT_DEBUG(sz <= cluster_node_size);
    if (REALM_LIKELY(sz < cluster_node_size)) {
        insert_row(ndx, k, init_values); // Throws
        state.mem = get_mem();
        state.index = ndx;
    }
    else {
        // Split leaf node
        Cluster new_leaf(0, m_alloc, m_tree_top);
        new_leaf.create();
        if (ndx == sz) {
            new_leaf.insert_row(0, ObjKey(0), init_values); // Throws
            state.split_key = k.value;
            state.mem = new_leaf.get_mem();
            state.index = 0;
        }
        else {
            // Current cluster must be in general form to get here
            REALM_ASSERT_DEBUG(m_keys.is_attached());
            new_leaf.ensure_general_form();
            move(ndx, &new_leaf, current_key_value);
            insert_row(ndx, k, init_values); // Throws
            state.mem = get_mem();
            state.split_key = current_key_value;
            state.index = ndx;
        }
        ret = new_leaf.get_ref();
    }

    return ret;
}

bool Cluster::try_get(ObjKey k, ClusterNode::State& state) const noexcept
{
    state.mem = get_mem();
    if (m_keys.is_attached()) {
        state.index = m_keys.lower_bound(uint64_t(k.value));
        return state.index != m_keys.size() && m_keys.get(state.index) == uint64_t(k.value);
    }
    else {
        if (uint64_t(k.value) < uint64_t(Array::get(s_key_ref_or_size_index) >> 1)) {
            state.index = size_t(k.value);
            return true;
        }
    }
    return false;
}

ObjKey Cluster::get(size_t ndx, ClusterNode::State& state) const
{
    state.index = ndx;
    state.mem = get_mem();
    return get_real_key(ndx);
}

template <class T>
inline void Cluster::do_erase(size_t ndx, ColKey col_key)
{
    auto col_ndx = col_key.get_index();
    T values(m_alloc);
    values.set_parent(this, col_ndx.val + s_first_col_index);
    set_spec<T>(values, col_ndx);
    values.init_from_parent();
    ObjLink link;
    if constexpr (std::is_same_v<T, ArrayTypedLink>) {
        link = values.get(ndx);
    }
    if constexpr (std::is_same_v<T, ArrayMixed>) {
        Mixed value = values.get(ndx);
        if (value.is_type(type_TypedLink)) {
            link = value.get<ObjLink>();
        }
    }
    if (link) {
        if (const Table* origin_table = m_tree_top.get_owning_table()) {
            auto target_obj = origin_table->get_parent_group()->get_object(link);

            ColKey backlink_col_key = target_obj.get_table()->find_backlink_column(col_key, origin_table->get_key());
            REALM_ASSERT(backlink_col_key);
            target_obj.remove_one_backlink(backlink_col_key, get_real_key(ndx)); // Throws
        }
    }
    values.erase(ndx);
}

inline void Cluster::do_erase_key(size_t ndx, ColKey col_key, CascadeState& state)
{
    ArrayKey values(m_alloc);
    auto col_ndx = col_key.get_index();
    values.set_parent(this, col_ndx.val + s_first_col_index);
    values.init_from_parent();

    ObjKey key = values.get(ndx);
    if (key != null_key) {
        remove_backlinks(get_real_key(ndx), col_key, std::vector<ObjKey>{key}, state);
    }
    values.erase(ndx);
}

size_t Cluster::get_ndx(ObjKey k, size_t ndx) const noexcept
{
    size_t index;
    if (m_keys.is_attached()) {
        index = m_keys.lower_bound(uint64_t(k.value));
        if (index == m_keys.size() || m_keys.get(index) != uint64_t(k.value)) {
            return realm::npos;
        }
    }
    else {
        index = size_t(k.value);
        if (index >= get_as_ref_or_tagged(s_key_ref_or_size_index).get_as_int()) {
            return realm::npos;
        }
    }
    return index + ndx;
}

size_t Cluster::erase(ObjKey key, CascadeState& state)
{
    size_t ndx = get_ndx(key, 0);
    if (ndx == realm::npos)
        throw KeyNotFound(util::format("When erasing key '%1' in '%2'", key.value, get_owning_table()->get_name()));
    std::vector<ColKey> backlink_column_keys;

    auto erase_in_column = [&](ColKey col_key) {
        auto col_type = col_key.get_type();
        auto attr = col_key.get_attrs();
        if (attr.test(col_attr_Collection)) {
            auto col_ndx = col_key.get_index();
            ArrayRef values(m_alloc);
            values.set_parent(this, col_ndx.val + s_first_col_index);
            values.init_from_parent();
            ref_type ref = values.get(ndx);

            if (ref) {
                const Table* origin_table = m_tree_top.get_owning_table();
                if (attr.test(col_attr_Dictionary)) {
                    if (col_type == col_type_Mixed || col_type == col_type_Link) {
                        Obj obj(origin_table->m_own_ref, get_mem(), key, ndx);
                        const Dictionary dict = obj.get_dictionary(col_key);
                        dict.remove_backlinks(state);
                    }
                }
                else if (col_type == col_type_LinkList) {
                    BPlusTree<ObjKey> links(m_alloc);
                    links.init_from_ref(ref);
                    if (links.size() > 0) {
                        remove_backlinks(ObjKey(key.value + m_offset), col_key, links.get_all(), state);
                    }
                }
                else if (col_type == col_type_TypedLink) {
                    BPlusTree<ObjLink> links(m_alloc);
                    links.init_from_ref(ref);
                    for (size_t i = 0; i < links.size(); i++) {
                        ObjLink link = links.get(i);
                        auto target_obj = origin_table->get_parent_group()->get_object(link);
                        ColKey backlink_col_key =
                            target_obj.get_table()->find_backlink_column(col_key, origin_table->get_key());
                        target_obj.remove_one_backlink(backlink_col_key, ObjKey(key.value + m_offset));
                    }
                }
                else if (col_type == col_type_Mixed) {
                    BPlusTree<Mixed> list(m_alloc);
                    list.init_from_ref(ref);
                    for (size_t i = 0; i < list.size(); i++) {
                        Mixed val = list.get(i);
                        if (val.is_type(type_TypedLink)) {
                            ObjLink link = val.get<ObjLink>();
                            auto target_obj = origin_table->get_parent_group()->get_object(link);
                            ColKey backlink_col_key =
                                target_obj.get_table()->find_backlink_column(col_key, origin_table->get_key());
                            target_obj.remove_one_backlink(backlink_col_key, ObjKey(key.value + m_offset));
                        }
                    }
                }
                Array::destroy_deep(ref, m_alloc);
            }

            values.erase(ndx);

            return false;
        }

        switch (col_type) {
            case col_type_Int:
                if (attr.test(col_attr_Nullable)) {
                    do_erase<ArrayIntNull>(ndx, col_key);
                }
                else {
                    do_erase<ArrayInteger>(ndx, col_key);
                }
                break;
            case col_type_Bool:
                do_erase<ArrayBoolNull>(ndx, col_key);
                break;
            case col_type_Float:
                do_erase<ArrayFloatNull>(ndx, col_key);
                break;
            case col_type_Double:
                do_erase<ArrayDoubleNull>(ndx, col_key);
                break;
            case col_type_String:
                do_erase<ArrayString>(ndx, col_key);
                break;
            case col_type_Binary:
                do_erase<ArrayBinary>(ndx, col_key);
                break;
            case col_type_Mixed:
                do_erase<ArrayMixed>(ndx, col_key);
                break;
            case col_type_Timestamp:
                do_erase<ArrayTimestamp>(ndx, col_key);
                break;
            case col_type_Decimal:
                do_erase<ArrayDecimal128>(ndx, col_key);
                break;
            case col_type_ObjectId:
                do_erase<ArrayObjectIdNull>(ndx, col_key);
                break;
            case col_type_UUID:
                do_erase<ArrayUUIDNull>(ndx, col_key);
                break;
            case col_type_Link:
                do_erase_key(ndx, col_key, state);
                break;
            case col_type_TypedLink:
                do_erase<ArrayTypedLink>(ndx, col_key);
                break;
            case col_type_BackLink:
                if (state.m_mode == CascadeState::Mode::None) {
                    do_erase<ArrayBacklink>(ndx, col_key);
                }
                else {
                    // Postpone the deletion of backlink entries or else the
                    // checks for if there's any remaining backlinks will
                    // check the wrong row for columns which have already
                    // had values erased from them.
                    backlink_column_keys.push_back(col_key);
                }
                break;
            default:
                REALM_ASSERT(false);
                break;
        }
        return false;
    };
    m_tree_top.for_each_and_every_column(erase_in_column);

    // Any remaining backlink columns to erase from?
    for (auto k : backlink_column_keys)
        do_erase<ArrayBacklink>(ndx, k);

    if (m_keys.is_attached()) {
        m_keys.erase(ndx);
    }
    else {
        size_t current_size = get_size_in_compact_form();
        if (ndx == current_size - 1) {
            // When deleting last, we can still maintain compact form
            set(0, RefOrTagged::make_tagged(current_size - 1));
        }
        else {
            ensure_general_form();
            m_keys.erase(ndx);
        }
    }

    return node_size();
}

void Cluster::nullify_incoming_links(ObjKey key, CascadeState& state)
{
    size_t ndx = get_ndx(key, 0);
    if (ndx == realm::npos)
        throw KeyNotFound(util::format("When nullify incoming links for key '%1' in '%2'", key.value,
                                       get_owning_table()->get_name()));

    // We must start with backlink columns in case the corresponding link
    // columns are in the same table so that we can nullify links before
    // erasing rows in the link columns.
    //
    // This phase also generates replication instructions documenting the side-
    // effects of deleting the object (i.e. link nullifications). These instructions
    // must come before the actual deletion of the object, but at the same time
    // the Replication object may need a consistent view of the row (not including
    // link columns). Therefore we first nullify links to this object, then
    // generate the instruction, and then delete the row in the remaining columns.

    auto nullify_fwd_links = [&](ColKey col_key) {
        ColKey::Idx leaf_ndx = col_key.get_index();
        auto type = col_key.get_type();
        REALM_ASSERT(type == col_type_BackLink);
        ArrayBacklink values(m_alloc);
        values.set_parent(this, leaf_ndx.val + s_first_col_index);
        values.init_from_parent();
        // Ensure that Cluster is writable and able to hold references to nodes in
        // the slab area before nullifying or deleting links. These operation may
        // both have the effect that other objects may be constructed and manipulated.
        // If those other object are in the same cluster that the object to be deleted
        // is in, then that will cause another accessor to this cluster to be created.
        // It would lead to an error if the cluster node was relocated without it being
        // reflected in the context here.
        values.copy_on_write();
        values.nullify_fwd_links(ndx, state);

        return false;
    };

    m_tree_top.get_owning_table()->for_each_backlink_column(nullify_fwd_links);
}

void Cluster::upgrade_string_to_enum(ColKey col_key, ArrayString& keys)
{
    auto col_ndx = col_key.get_index();
    Array indexes(m_alloc);
    indexes.create(Array::type_Normal, false);
    ArrayString values(m_alloc);
    ref_type ref = Array::get_as_ref(col_ndx.val + s_first_col_index);
    values.init_from_ref(ref);
    size_t sz = values.size();
    for (size_t i = 0; i < sz; i++) {
        auto v = values.get(i);
        size_t pos = keys.lower_bound(v);
        REALM_ASSERT_3(pos, !=, keys.size());
        indexes.add(pos);
    }
    Array::set(col_ndx.val + s_first_col_index, indexes.get_ref());
    Array::destroy_deep(ref, m_alloc);
}

void Cluster::init_leaf(ColKey col_key, ArrayPayload* leaf) const
{
    auto col_ndx = col_key.get_index();
    // FIXME: Move this validation into callers.
    // Currently, the query subsystem may call with an unvalidated key.
    // once fixed, reintroduce the noexcept declaration :-D
    if (auto t = m_tree_top.get_owning_table())
        t->check_column(col_key);
    ref_type ref = to_ref(Array::get(col_ndx.val + 1));
    if (leaf->need_spec()) {
        m_tree_top.set_spec(*leaf, col_ndx);
    }
    leaf->init_from_ref(ref);
    leaf->set_parent(const_cast<Cluster*>(this), col_ndx.val + 1);
}

void Cluster::add_leaf(ColKey col_key, ref_type ref)
{
    auto col_ndx = col_key.get_index();
    REALM_ASSERT((col_ndx.val + 1) == size());
    Array::insert(col_ndx.val + 1, from_ref(ref));
}

template <typename ArrayType>
void Cluster::verify(ref_type ref, size_t index, util::Optional<size_t>& sz) const
{
    ArrayType arr(get_alloc());
    set_spec(arr, ColKey::Idx{unsigned(index) - 1});
    arr.set_parent(const_cast<Cluster*>(this), index);
    arr.init_from_ref(ref);
    arr.verify();
    if (sz) {
        REALM_ASSERT(arr.size() == *sz);
    }
    else {
        sz = arr.size();
    }
}
namespace {

template <typename ArrayType>
void verify_list(ArrayRef& arr, size_t sz)
{
    for (size_t n = 0; n < sz; n++) {
        if (ref_type bp_tree_ref = arr.get(n)) {
            BPlusTree<ArrayType> links(arr.get_alloc());
            links.init_from_ref(bp_tree_ref);
            links.set_parent(&arr, n);
            links.verify();
        }
    }
}

template <typename SetType>
void verify_set(ArrayRef& arr, size_t sz)
{
    for (size_t n = 0; n < sz; ++n) {
        if (ref_type bp_tree_ref = arr.get(n)) {
            BPlusTree<SetType> elements(arr.get_alloc());
            elements.init_from_ref(bp_tree_ref);
            elements.set_parent(&arr, n);
            elements.verify();

            // FIXME: Check uniqueness of elements.
        }
    }
}

} // namespace

void Cluster::verify() const
{
#ifdef REALM_DEBUG
    util::Optional<size_t> sz;

    auto verify_column = [this, &sz](ColKey col_key) {
        size_t col = col_key.get_index().val + s_first_col_index;
        ref_type ref = Array::get_as_ref(col);
        auto attr = col_key.get_attrs();
        auto col_type = col_key.get_type();
        bool nullable = attr.test(col_attr_Nullable);

        if (attr.test(col_attr_List)) {
            ArrayRef arr(get_alloc());
            arr.set_parent(const_cast<Cluster*>(this), col);
            arr.init_from_ref(ref);
            arr.verify();
            if (sz) {
                REALM_ASSERT(arr.size() == *sz);
            }
            else {
                sz = arr.size();
            }

            switch (col_type) {
                case col_type_Int:
                    if (nullable) {
                        verify_list<util::Optional<int64_t>>(arr, *sz);
                    }
                    else {
                        verify_list<int64_t>(arr, *sz);
                    }
                    break;
                case col_type_Bool:
                    verify_list<Bool>(arr, *sz);
                    break;
                case col_type_Float:
                    verify_list<Float>(arr, *sz);
                    break;
                case col_type_Double:
                    verify_list<Double>(arr, *sz);
                    break;
                case col_type_String:
                    verify_list<String>(arr, *sz);
                    break;
                case col_type_Binary:
                    verify_list<Binary>(arr, *sz);
                    break;
                case col_type_Timestamp:
                    verify_list<Timestamp>(arr, *sz);
                    break;
                case col_type_Decimal:
                    verify_list<Decimal128>(arr, *sz);
                    break;
                case col_type_ObjectId:
                    verify_list<ObjectId>(arr, *sz);
                    break;
                case col_type_UUID:
                    verify_list<UUID>(arr, *sz);
                    break;
                case col_type_LinkList:
                    verify_list<ObjKey>(arr, *sz);
                    break;
                default:
                    // FIXME: Nullable primitives
                    break;
            }
            return false;
        }
        else if (attr.test(col_attr_Dictionary)) {
            ArrayRef arr(get_alloc());
            arr.set_parent(const_cast<Cluster*>(this), col);
            arr.init_from_ref(ref);
            arr.verify();
            if (sz) {
                REALM_ASSERT(arr.size() == *sz);
            }
            else {
                sz = arr.size();
            }
            for (size_t n = 0; n < sz; n++) {
                if (arr.get(n)) {
                    auto key_type = get_owning_table()->get_dictionary_key_type(col_key);
                    DictionaryClusterTree cluster(&arr, key_type, get_alloc(), n);
                    cluster.init_from_parent();
                    cluster.verify();
                }
            }
            return false;
        }
        else if (attr.test(col_attr_Set)) {
            ArrayRef arr(get_alloc());
            arr.set_parent(const_cast<Cluster*>(this), col);
            arr.init_from_ref(ref);
            arr.verify();
            if (sz) {
                REALM_ASSERT(arr.size() == *sz);
            }
            else {
                sz = arr.size();
            }
            switch (col_type) {
                case col_type_Int:
                    if (nullable) {
                        verify_set<util::Optional<int64_t>>(arr, *sz);
                    }
                    else {
                        verify_set<int64_t>(arr, *sz);
                    }
                    break;
                case col_type_Bool:
                    verify_set<Bool>(arr, *sz);
                    break;
                case col_type_Float:
                    verify_set<Float>(arr, *sz);
                    break;
                case col_type_Double:
                    verify_set<Double>(arr, *sz);
                    break;
                case col_type_String:
                    verify_set<String>(arr, *sz);
                    break;
                case col_type_Binary:
                    verify_set<Binary>(arr, *sz);
                    break;
                case col_type_Timestamp:
                    verify_set<Timestamp>(arr, *sz);
                    break;
                case col_type_Decimal:
                    verify_set<Decimal128>(arr, *sz);
                    break;
                case col_type_ObjectId:
                    verify_set<ObjectId>(arr, *sz);
                    break;
                case col_type_UUID:
                    verify_set<UUID>(arr, *sz);
                    break;
                case col_type_Link:
                    verify_set<ObjKey>(arr, *sz);
                    break;
                default:
                    // FIXME: Nullable primitives
                    break;
            }
            return false;
        }

        switch (col_type) {
            case col_type_Int:
                if (nullable) {
                    verify<ArrayIntNull>(ref, col, sz);
                }
                else {
                    verify<ArrayInteger>(ref, col, sz);
                }
                break;
            case col_type_Bool:
                verify<ArrayBoolNull>(ref, col, sz);
                break;
            case col_type_Float:
                verify<ArrayFloatNull>(ref, col, sz);
                break;
            case col_type_Double:
                verify<ArrayDoubleNull>(ref, col, sz);
                break;
            case col_type_String:
                verify<ArrayString>(ref, col, sz);
                break;
            case col_type_Binary:
                verify<ArrayBinary>(ref, col, sz);
                break;
            case col_type_Mixed:
                verify<ArrayMixed>(ref, col, sz);
                break;
            case col_type_Timestamp:
                verify<ArrayTimestamp>(ref, col, sz);
                break;
            case col_type_Decimal:
                verify<ArrayDecimal128>(ref, col, sz);
                break;
            case col_type_ObjectId:
                verify<ArrayObjectIdNull>(ref, col, sz);
                break;
            case col_type_UUID:
                verify<ArrayUUIDNull>(ref, col, sz);
                break;
            case col_type_Link:
                verify<ArrayKey>(ref, col, sz);
                break;
            case col_type_BackLink:
                verify<ArrayBacklink>(ref, col, sz);
                break;
            default:
                break;
        }
        return false;
    };

    m_tree_top.for_each_and_every_column(verify_column);
#endif
}

// LCOV_EXCL_START
void Cluster::dump_objects(int64_t key_offset, std::string lead) const
{
    std::cout << lead << "leaf - size: " << node_size() << std::endl;
    if (!m_keys.is_attached()) {
        std::cout << lead << "compact form" << std::endl;
    }

    for (unsigned i = 0; i < node_size(); i++) {
        int64_t key_value;
        if (m_keys.is_attached()) {
            key_value = m_keys.get(i);
        }
        else {
            key_value = int64_t(i);
        }
        std::cout << lead << "key: " << std::hex << key_value + key_offset << std::dec;
        m_tree_top.for_each_and_every_column([&](ColKey col) {
            size_t j = col.get_index().val + 1;
            if (col.get_attrs().test(col_attr_List)) {
                ref_type ref = Array::get_as_ref(j);
                ArrayRef refs(m_alloc);
                refs.init_from_ref(ref);
                std::cout << ", {";
                ref = refs.get(i);
                if (ref) {
                    if (col.get_type() == col_type_Int) {
                        // This is easy to handle
                        Array ints(m_alloc);
                        ints.init_from_ref(ref);
                        for (size_t n = 0; n < ints.size(); n++) {
                            std::cout << ints.get(n) << ", ";
                        }
                    }
                    else {
                        std::cout << col.get_type();
                    }
                }
                std::cout << "}";
                return false;
            }

            switch (col.get_type()) {
                case col_type_Int: {
                    bool nullable = col.get_attrs().test(col_attr_Nullable);
                    ref_type ref = Array::get_as_ref(j);
                    if (nullable) {
                        ArrayIntNull arr_int_null(m_alloc);
                        arr_int_null.init_from_ref(ref);
                        if (arr_int_null.is_null(i)) {
                            std::cout << ", null";
                        }
                        else {
                            std::cout << ", " << arr_int_null.get(i).value();
                        }
                    }
                    else {
                        Array arr(m_alloc);
                        arr.init_from_ref(ref);
                        std::cout << ", " << arr.get(i);
                    }
                    break;
                }
                case col_type_Bool: {
                    ArrayBoolNull arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    auto val = arr.get(i);
                    std::cout << ", " << (val ? (*val ? "true" : "false") : "null");
                    break;
                }
                case col_type_Float: {
                    ArrayFloatNull arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    auto val = arr.get(i);
                    if (val)
                        std::cout << ", " << *val;
                    else
                        std::cout << ", null";
                    break;
                }
                case col_type_Double: {
                    ArrayDoubleNull arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    auto val = arr.get(i);
                    if (val)
                        std::cout << ", " << *val;
                    else
                        std::cout << ", null";
                    break;
                    break;
                }
                case col_type_String: {
                    ArrayString arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    std::cout << ", " << arr.get(i);
                    break;
                }
                case col_type_Binary: {
                    ArrayBinary arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    std::cout << ", " << arr.get(i);
                    break;
                }
                case col_type_Mixed: {
                    ArrayMixed arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    std::cout << ", " << arr.get(i);
                    break;
                }
                case col_type_Timestamp: {
                    ArrayTimestamp arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    if (arr.is_null(i)) {
                        std::cout << ", null";
                    }
                    else {
                        std::cout << ", " << arr.get(i);
                    }
                    break;
                }
                case col_type_Decimal: {
                    ArrayDecimal128 arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    if (arr.is_null(i)) {
                        std::cout << ", null";
                    }
                    else {
                        std::cout << ", " << arr.get(i);
                    }
                    break;
                }
                case col_type_ObjectId: {
                    ArrayObjectIdNull arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    if (arr.is_null(i)) {
                        std::cout << ", null";
                    }
                    else {
                        std::cout << ", " << *arr.get(i);
                    }
                    break;
                }
                case col_type_UUID: {
                    ArrayUUIDNull arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    if (arr.is_null(i)) {
                        std::cout << ", null";
                    }
                    else {
                        std::cout << ", " << arr.get(i);
                    }
                    break;
                }
                case col_type_Link: {
                    ArrayKey arr(m_alloc);
                    ref_type ref = Array::get_as_ref(j);
                    arr.init_from_ref(ref);
                    std::cout << ", " << arr.get(i);
                    break;
                }
                case col_type_BackLink: {
                    break;
                }
                default:
                    std::cout << ", Error";
                    break;
            }
            return false;
        });
        std::cout << std::endl;
    }
}
// LCOV_EXCL_STOP

void Cluster::remove_backlinks(ObjKey origin_key, ColKey origin_col_key, const std::vector<ObjKey>& keys,
                               CascadeState& state) const
{
    const Table* origin_table = m_tree_top.get_owning_table();
    TableRef target_table = origin_table->get_opposite_table(origin_col_key);
    ColKey backlink_col_key = origin_table->get_opposite_column(origin_col_key);
    bool strong_links = target_table->is_embedded();

    for (auto key : keys) {
        if (key != null_key) {
            bool is_unres = key.is_unresolved();
            Obj target_obj = is_unres ? target_table->m_tombstones->get(key) : target_table->m_clusters.get(key);
            bool last_removed = target_obj.remove_one_backlink(backlink_col_key, origin_key); // Throws
            if (is_unres) {
                if (last_removed) {
                    // Check is there are more backlinks
                    if (!target_obj.has_backlinks(false)) {
                        // Tombstones can be erased right away - there is no cascading effect
                        target_table->m_tombstones->erase(key, state);
                    }
                }
            }
            else {
                state.enqueue_for_cascade(target_obj, strong_links, last_removed);
            }
        }
    }
}

void Cluster::remove_backlinks(ObjKey origin_key, ColKey origin_col_key, const std::vector<ObjLink>& links,
                               CascadeState& state) const
{
    const Table* origin_table = m_tree_top.get_owning_table();
    Group* group = origin_table->get_parent_group();
    TableKey origin_table_key = origin_table->get_key();

    for (auto link : links) {
        if (link) {
            bool is_unres = link.get_obj_key().is_unresolved();
            Obj target_obj = group->get_object(link);
            TableRef target_table = target_obj.get_table();
            ColKey backlink_col_key = target_table->find_or_add_backlink_column(origin_col_key, origin_table_key);

            bool last_removed = target_obj.remove_one_backlink(backlink_col_key, origin_key); // Throws
            if (is_unres) {
                if (last_removed) {
                    // Check is there are more backlinks
                    if (!target_obj.has_backlinks(false)) {
                        // Tombstones can be erased right away - there is no cascading effect
                        target_table->m_tombstones->erase(link.get_obj_key(), state);
                    }
                }
            }
            else {
                state.enqueue_for_cascade(target_obj, false, last_removed);
            }
        }
    }
}

} // namespace realm
