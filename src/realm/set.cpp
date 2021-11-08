/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
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


#include "realm/set.hpp"
#include "realm/array_basic.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_bool.hpp"
#include "realm/array_string.hpp"
#include "realm/array_binary.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/array_decimal128.hpp"
#include "realm/array_fixed_bytes.hpp"
#include "realm/array_typed_link.hpp"
#include "realm/array_mixed.hpp"
#include "realm/replication.hpp"

namespace realm {

// FIXME: This method belongs in obj.cpp.
SetBasePtr Obj::get_setbase_ptr(ColKey col_key) const
{
    auto attr = get_table()->get_column_attr(col_key);
    REALM_ASSERT(attr.test(col_attr_Set));
    bool nullable = attr.test(col_attr_Nullable);

    switch (get_table()->get_column_type(col_key)) {
        case type_Int: {
            if (nullable)
                return std::make_unique<Set<util::Optional<Int>>>(*this, col_key);
            else
                return std::make_unique<Set<Int>>(*this, col_key);
        }
        case type_Bool: {
            if (nullable)
                return std::make_unique<Set<util::Optional<Bool>>>(*this, col_key);
            else
                return std::make_unique<Set<Bool>>(*this, col_key);
        }
        case type_Float: {
            if (nullable)
                return std::make_unique<Set<util::Optional<Float>>>(*this, col_key);
            else
                return std::make_unique<Set<Float>>(*this, col_key);
        }
        case type_Double: {
            if (nullable)
                return std::make_unique<Set<util::Optional<Double>>>(*this, col_key);
            else
                return std::make_unique<Set<Double>>(*this, col_key);
        }
        case type_String: {
            return std::make_unique<Set<String>>(*this, col_key);
        }
        case type_Binary: {
            return std::make_unique<Set<Binary>>(*this, col_key);
        }
        case type_Timestamp: {
            return std::make_unique<Set<Timestamp>>(*this, col_key);
        }
        case type_Decimal: {
            return std::make_unique<Set<Decimal128>>(*this, col_key);
        }
        case type_ObjectId: {
            if (nullable)
                return std::make_unique<Set<util::Optional<ObjectId>>>(*this, col_key);
            else
                return std::make_unique<Set<ObjectId>>(*this, col_key);
        }
        case type_UUID: {
            if (nullable)
                return std::make_unique<Set<util::Optional<UUID>>>(*this, col_key);
            else
                return std::make_unique<Set<UUID>>(*this, col_key);
        }
        case type_TypedLink: {
            return std::make_unique<Set<ObjLink>>(*this, col_key);
        }
        case type_Mixed: {
            return std::make_unique<Set<Mixed>>(*this, col_key);
        }
        case type_Link: {
            return std::make_unique<LnkSet>(*this, col_key);
        }
        case type_LinkList:
            break;
    }
    REALM_TERMINATE("Unsupported column type.");
}

void SetBase::insert_repl(Replication* repl, size_t index, Mixed value) const
{
    repl->set_insert(*this, index, value);
}

void SetBase::erase_repl(Replication* repl, size_t index, Mixed value) const
{
    repl->set_erase(*this, index, value);
}

void SetBase::clear_repl(Replication* repl) const
{
    repl->set_clear(*this);
}

template <>
void Set<ObjKey>::do_insert(size_t ndx, ObjKey target_key)
{
    auto origin_table = m_obj.get_table();
    auto target_table_key = origin_table->get_opposite_table_key(m_col_key);
    m_obj.set_backlink(m_col_key, {target_table_key, target_key});
    m_tree->insert(ndx, target_key);
    if (target_key.is_unresolved()) {
        m_tree->set_context_flag(true);
    }
}

template <>
void Set<ObjKey>::do_erase(size_t ndx)
{
    auto origin_table = m_obj.get_table();
    auto target_table_key = origin_table->get_opposite_table_key(m_col_key);
    ObjKey old_key = get(ndx);
    CascadeState state(old_key.is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);

    bool recurse = m_obj.remove_backlink(m_col_key, {target_table_key, old_key}, state);

    m_tree->erase(ndx);

    if (recurse) {
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
    if (old_key.is_unresolved()) {
        // We might have removed the last unresolved link - check it

        // FIXME: Exploit the fact that the values are sorted and unresolved
        // keys have a negative value.
        _impl::check_for_last_unresolved(m_tree.get());
    }
}

template <>
void Set<ObjKey>::do_clear()
{
    size_t ndx = size();
    while (ndx--) {
        do_erase(ndx);
    }

    m_tree->set_context_flag(false);
}

template <>
void Set<ObjLink>::do_insert(size_t ndx, ObjLink target_link)
{
    m_obj.set_backlink(m_col_key, target_link);
    m_tree->insert(ndx, target_link);
}

template <>
void Set<ObjLink>::do_erase(size_t ndx)
{
    ObjLink old_link = get(ndx);
    CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);

    bool recurse = m_obj.remove_backlink(m_col_key, old_link, state);

    m_tree->erase(ndx);

    if (recurse) {
        auto table = m_obj.get_table();
        _impl::TableFriend::remove_recursive(*table, state); // Throws
    }
}

template <>
void Set<Mixed>::do_insert(size_t ndx, Mixed value)
{
    if (value.is_type(type_TypedLink)) {
        m_obj.set_backlink(m_col_key, value.get<ObjLink>());
    }
    m_tree->insert(ndx, value);
}

template <>
void Set<Mixed>::do_erase(size_t ndx)
{
    if (Mixed old_value = get(ndx); old_value.is_type(type_TypedLink)) {
        auto old_link = old_value.get<ObjLink>();

        CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All
                                                                  : CascadeState::Mode::Strong);
        bool recurse = m_obj.remove_backlink(m_col_key, old_link, state);

        m_tree->erase(ndx);

        if (recurse) {
            auto table = m_obj.get_table();
            _impl::TableFriend::remove_recursive(*table, state); // Throws
        }
    }
    else {
        m_tree->erase(ndx);
    }
}

template <>
void Set<Mixed>::do_clear()
{
    size_t ndx = size();
    while (ndx--) {
        do_erase(ndx);
    }
}

void LnkSet::remove_target_row(size_t link_ndx)
{
    // Deleting the object will automatically remove all links
    // to it. So we do not have to manually remove the deleted link
    ObjKey k = get(link_ndx);
    get_target_table()->remove_object(k);
}

void LnkSet::remove_all_target_rows()
{
    if (m_set.update()) {
        _impl::TableFriend::batch_erase_rows(*get_target_table(), *m_set.m_tree);
    }
}

bool LnkSet::is_subset_of(const CollectionBase& rhs) const
{
    return this->m_set.is_subset_of(rhs);
}

bool LnkSet::is_strict_subset_of(const CollectionBase& rhs) const
{
    return this->m_set.is_strict_subset_of(rhs);
}

bool LnkSet::is_superset_of(const CollectionBase& rhs) const
{
    return this->m_set.is_superset_of(rhs);
}

bool LnkSet::is_strict_superset_of(const CollectionBase& rhs) const
{
    return this->m_set.is_strict_superset_of(rhs);
}

bool LnkSet::intersects(const CollectionBase& rhs) const
{
    return this->m_set.intersects(rhs);
}

bool LnkSet::set_equals(const CollectionBase& rhs) const
{
    return this->m_set.set_equals(rhs);
}

void set_sorted_indices(size_t sz, std::vector<size_t>& indices, bool ascending)
{
    indices.resize(sz);
    if (ascending) {
        std::iota(indices.begin(), indices.end(), 0);
    }
    else {
        std::iota(indices.rbegin(), indices.rend(), 0);
    }
}

} // namespace realm
