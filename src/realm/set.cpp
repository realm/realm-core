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

// Handling of unresolved links:
#include "realm/list.hpp"

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
            [[fallthrough]];
        case type_OldDateTime:
            [[fallthrough]];
        case type_OldTable:
            REALM_ASSERT(false);
            break;
    }
    return {};
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

    if (old_key.is_unresolved()) {
        check_for_last_unresolved(*m_tree);
    }

    if (recurse) {
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
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
    if (!value.is_null() && value.get_type() == type_TypedLink) {
        m_obj.set_backlink(m_col_key, value.get<ObjLink>());
    }
    m_tree->insert(ndx, value);
}

template <>
void Set<Mixed>::do_erase(size_t ndx)
{
    if (Mixed old_value = get(ndx); old_value.get_type() == type_TypedLink) {
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

LnkSet::LnkSet(const Obj& owner, ColKey col_key)
    : Set<ObjKey>(owner, col_key)
{
    update_unresolved(m_unresolved, *m_tree);
}

bool LnkSet::init_from_parent() const
{
    Set<ObjKey>::init_from_parent();
    update_unresolved(m_unresolved, *m_tree);
    return m_valid;
}

TableRef LnkSet::get_target_table() const
{
    return m_obj.get_table()->get_link_target(m_col_key);
}

Obj LnkSet::get_object(size_t ndx) const
{
    ObjKey k = get(ndx);
    return get_target_table()->get_object(k);
}

void LnkSet::get_dependencies(TableVersions& versions) const
{
    if (is_attached()) {
        auto table = get_table();
        versions.emplace_back(table->get_key(), table->get_content_version());
    }
}

void LnkSet::sync_if_needed() const
{
    if (is_attached()) {
        const_cast<LnkSet*>(this)->update_if_needed();
    }
}

ObjKey LnkSet::get(size_t ndx) const
{
    auto real_ndx = virtual2real(m_unresolved, ndx);
    return Set<ObjKey>::get(real_ndx);
}

size_t LnkSet::find(ObjKey key) const
{
    auto ndx = Set<ObjKey>::find(key);
    if (ndx != npos) {
        ndx = real2virtual(m_unresolved, ndx);
    }
    return ndx;
}

std::pair<size_t, bool> LnkSet::insert(ObjKey key)
{
    auto [ndx, inserted] = Set<ObjKey>::insert(key);
    ndx = real2virtual(m_unresolved, ndx);
    return {ndx, inserted};
}

std::pair<size_t, bool> LnkSet::erase(ObjKey key)
{
    auto [ndx, erased] = Set<ObjKey>::erase(key);
    if (ndx != npos) {
        ndx = real2virtual(m_unresolved, ndx);
    }
    return {ndx, erased};
}

void LnkSet::clear()
{
    Set<ObjKey>::clear();
    m_unresolved.clear();
}

std::pair<size_t, bool> LnkSet::insert_null()
{
    auto [ndx, x] = Set<ObjKey>::insert_null();
    if (ndx != npos) {
        ndx = real2virtual(m_unresolved, ndx);
    }
    return {ndx, x};
}

std::pair<size_t, bool> LnkSet::erase_null()
{
    auto [ndx, x] = Set<ObjKey>::erase_null();
    if (ndx != npos) {
        ndx = real2virtual(m_unresolved, ndx);
    }
    return {ndx, x};
}

std::pair<size_t, bool> LnkSet::insert_any(Mixed value)
{
    auto [ndx, x] = Set<ObjKey>::insert_any(value);
    if (ndx != npos) {
        ndx = real2virtual(m_unresolved, ndx);
    }
    return {ndx, x};
}

std::pair<size_t, bool> LnkSet::erase_any(Mixed value)
{
    auto [ndx, x] = Set<ObjKey>::erase_any(value);
    if (ndx != npos) {
        ndx = real2virtual(m_unresolved, ndx);
    }
    return {ndx, x};
}


} // namespace realm
