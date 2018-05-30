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


#include "realm/list.hpp"
#include "realm/cluster_tree.hpp"
#include "realm/array_basic.hpp"
#include "realm/array_integer.hpp"
#include "realm/array_bool.hpp"
#include "realm/array_string.hpp"
#include "realm/array_binary.hpp"
#include "realm/array_timestamp.hpp"
#include "realm/column_type_traits.hpp"
#include "realm/table.hpp"
#include "realm/table_view.hpp"
#include "realm/group.hpp"
#include "realm/replication.hpp"

using namespace realm;

LstBasePtr Obj::get_listbase_ptr(ColKey col_key, DataType type)
{
    switch (type) {
        case type_Int: {
            return std::make_unique<Lst<Int>>(*this, col_key);
        }
        case type_Bool: {
            return std::make_unique<Lst<Bool>>(*this, col_key);
        }
        case type_Float: {
            return std::make_unique<Lst<Float>>(*this, col_key);
        }
        case type_Double: {
            return std::make_unique<Lst<Double>>(*this, col_key);
        }
        case type_String: {
            return std::make_unique<Lst<String>>(*this, col_key);
        }
        case type_Binary: {
            return std::make_unique<Lst<Binary>>(*this, col_key);
        }
        case type_Timestamp: {
            return std::make_unique<Lst<Timestamp>>(*this, col_key);
        }
        case type_LinkList:
            return get_linklist_ptr(col_key);
        case type_Link:
        case type_OldDateTime:
        case type_OldTable:
        case type_OldMixed:
            REALM_ASSERT(false);
            break;
    }
    return {};
}

/********************************* LstBase **********************************/

ConstLstBase::ConstLstBase(ColKey col_key, const ConstObj* obj)
    : m_const_obj(obj)
    , m_col_key(col_key)
{
}

template <class T>
ConstLst<T>::ConstLst(const ConstObj& obj, ColKey col_key)
    : ConstLstBase(col_key, &m_obj)
    , ConstLstIf<T>(obj.get_alloc())
    , m_obj(obj)
{
    this->m_nullable = obj.get_table()->is_nullable(col_key);
    this->init_from_parent();
}

ConstLstBase::~ConstLstBase()
{
}

ref_type ConstLstBase::get_child_ref(size_t) const noexcept
{
    try {
        return to_ref(m_const_obj->get<int64_t>(m_col_key));
    }
    catch (const InvalidKey&) {
        return ref_type(0);
    }
}

std::pair<ref_type, size_t> ConstLstBase::get_to_dot_parent(size_t) const
{
    // TODO
    return {};
}

void ConstLstBase::erase_repl(Replication* repl, size_t ndx) const
{
    repl->list_erase(*this, ndx);
}

void ConstLstBase::move_repl(Replication* repl, size_t from, size_t to) const
{
    repl->list_move(*this, from, to);
}

void ConstLstBase::swap_repl(Replication* repl, size_t ndx1, size_t ndx2) const
{
    repl->list_swap(*this, ndx1, ndx2);
}

void ConstLstBase::clear_repl(Replication* repl) const
{
    repl->list_clear(*this);
}

template <class T>
Lst<T>::Lst(const Obj& obj, ColKey col_key)
    : ConstLstBase(col_key, &m_obj)
    , ConstLstIf<T>(obj.get_alloc())
    , m_obj(obj)
{
    this->m_nullable = obj.m_table->is_nullable(col_key);
    this->init_from_parent();
}

namespace realm {
template ConstLst<int64_t>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<util::Optional<Int>>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<bool>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<util::Optional<bool>>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<float>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<double>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<StringData>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<BinaryData>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<Timestamp>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<ObjKey>::ConstLst(const ConstObj& obj, ColKey col_key);

template Lst<int64_t>::Lst(const Obj& obj, ColKey col_key);
template Lst<util::Optional<Int>>::Lst(const Obj& obj, ColKey col_key);
template Lst<bool>::Lst(const Obj& obj, ColKey col_key);
template Lst<util::Optional<bool>>::Lst(const Obj& obj, ColKey col_key);
template Lst<float>::Lst(const Obj& obj, ColKey col_key);
template Lst<double>::Lst(const Obj& obj, ColKey col_key);
template Lst<StringData>::Lst(const Obj& obj, ColKey col_key);
template Lst<BinaryData>::Lst(const Obj& obj, ColKey col_key);
template Lst<Timestamp>::Lst(const Obj& obj, ColKey col_key);
template Lst<ObjKey>::Lst(const Obj& obj, ColKey col_key);
}

ConstObj ConstLnkLst::get_object(size_t link_ndx) const
{
    return m_const_obj->get_target_table(m_col_key)->get_object(ConstLstIf<ObjKey>::get(link_ndx));
}

/********************************* Lst<Key> *********************************/

template <>
void Lst<ObjKey>::do_set(size_t ndx, ObjKey target_key)
{
    CascadeState state;
    ObjKey old_key = get(ndx);
    bool recurse = m_obj.replace_backlink(m_col_key, old_key, target_key, state);

    m_tree->set(ndx, target_key);

    if (recurse) {
        auto table = const_cast<Table*>(m_obj.get_table());
        _impl::TableFriend::remove_recursive(*table, state); // Throws
    }
}

template <>
void Lst<ObjKey>::do_insert(size_t ndx, ObjKey target_key)
{
    m_obj.set_backlink(m_col_key, target_key);
    m_tree->insert(ndx, target_key);
}

template <>
void Lst<ObjKey>::do_remove(size_t ndx)
{
    CascadeState state;
    ObjKey old_key = get(ndx);
    bool recurse = m_obj.remove_backlink(m_col_key, old_key, state);

    m_tree->erase(ndx);

    if (recurse) {
        auto table = const_cast<Table*>(m_obj.get_table());
        _impl::TableFriend::remove_recursive(*table, state); // Throws
    }
}

template <>
void Lst<ObjKey>::clear()
{
    update_if_needed();
    Table* origin_table = const_cast<Table*>(m_obj.get_table());

    if (Replication* repl = m_const_obj->get_alloc().get_replication())
        repl->list_clear(*this); // Throws

    if (!origin_table->get_column_attr(m_col_key).test(col_attr_StrongLinks)) {
        size_t ndx = size();
        while (ndx--) {
            do_set(ndx, null_key);
            m_tree->erase(ndx);
            ConstLstBase::adj_remove(ndx);
        }
        return;
    }

    TableRef target_table = m_obj.get_target_table(m_col_key);
    TableKey target_table_key = target_table->get_key();
    ColKey backlink_col = target_table->find_backlink_column(m_obj.get_table_key(), m_col_key);

    CascadeState state;
    state.stop_on_link_list_column_key = m_col_key;
    state.stop_on_link_list_key = m_obj.get_key();

    typedef _impl::TableFriend tf;
    size_t num_links = size();
    for (size_t ndx = 0; ndx < num_links; ++ndx) {
        ObjKey target_key = m_tree->get(ndx);
        Obj target_obj = target_table->get_object(target_key);
        target_obj.remove_one_backlink(backlink_col, m_obj.get_key()); // Throws
        size_t num_remaining = target_obj.get_backlink_count(*origin_table, m_col_key);
        if (num_remaining == 0) {
            state.rows.emplace_back(target_table_key, target_key);
        }
    }

    m_tree->clear();
    m_obj.bump_both_versions();

    tf::remove_recursive(*origin_table, state); // Throws
}

#ifdef _WIN32
namespace realm {
// Explicit instantiation required on some windows builds
template void Lst<ObjKey>::do_insert(size_t ndx, ObjKey target_key);
template void Lst<ObjKey>::do_set(size_t ndx, ObjKey target_key);
template void Lst<ObjKey>::do_remove(size_t ndx);
template void Lst<ObjKey>::clear();
}
#endif

Obj LnkLst::get_object(size_t ndx)
{
    ObjKey k = get(ndx);
    return get_target_table().get_object(k);
}

TableView LnkLst::get_sorted_view(SortDescriptor order) const
{
    TableView tv(get_target_table(), clone());
    tv.do_sync();
    tv.sort(std::move(order));
    return tv;
}

TableView LnkLst::get_sorted_view(ColKey column_key, bool ascending) const
{
    TableView v = get_sorted_view(SortDescriptor(get_target_table(), {{column_key}}, {ascending}));
    return v;
}

void LnkLst::remove_target_row(size_t link_ndx)
{
    // Deleting the object will automatically remove all links
    // to it. So we do not have to manually remove the deleted link
    ObjKey k = get(link_ndx);
    get_target_table().remove_object(k);
}

void LnkLst::remove_all_target_rows()
{
    if (is_attached()) {
        _impl::TableFriend::batch_erase_rows(get_target_table(), *this->m_tree);
    }
}

TableVersions LnkLst::sync_if_needed() const
{
    TableVersions versions;
    if (this->is_attached()) {
        const_cast<LnkLst*>(this)->update_if_needed();
        auto table = get_table();
        versions.emplace_back(table->get_key(), table->get_content_version());
    }
    return versions;
}

namespace realm {
/***************************** Lst<T>::set_repl *****************************/
template <>
void Lst<Int>::set_repl(Replication* repl, size_t ndx, int64_t value)
{
    repl->list_set_int(*this, ndx, value);
}

template <>
void Lst<util::Optional<Int>>::set_repl(Replication* repl, size_t ndx, util::Optional<Int> value)
{
    if (value) {
        repl->list_set_int(*this, ndx, *value);
    }
    else {
        repl->list_set_null(*this, ndx);
    }
}

template <>
void Lst<Bool>::set_repl(Replication* repl, size_t ndx, bool value)
{
    repl->list_set_bool(*this, ndx, value);
}

template <>
void Lst<util::Optional<bool>>::set_repl(Replication* repl, size_t ndx, util::Optional<bool> value)
{
    if (value) {
        repl->list_set_bool(*this, ndx, *value);
    }
    else {
        repl->list_set_null(*this, ndx);
    }
}

template <>
void Lst<Float>::set_repl(Replication* repl, size_t ndx, float value)
{
    repl->list_set_float(*this, ndx, value);
}

template <>
void Lst<Double>::set_repl(Replication* repl, size_t ndx, double value)
{
    repl->list_set_double(*this, ndx, value);
}

template <>
void Lst<String>::set_repl(Replication* repl, size_t ndx, StringData value)
{
    repl->list_set_string(*this, ndx, value);
}

template <>
void Lst<Binary>::set_repl(Replication* repl, size_t ndx, BinaryData value)
{
    repl->list_set_binary(*this, ndx, value);
}

template <>
void Lst<Timestamp>::set_repl(Replication* repl, size_t ndx, Timestamp value)
{
    repl->list_set_timestamp(*this, ndx, value);
}

template <>
void Lst<ObjKey>::set_repl(Replication* repl, size_t ndx, ObjKey key)
{
    repl->list_set_link(*this, ndx, key);
}

/*************************** Lst<T>::insert_repl ****************************/
template <>
void Lst<Int>::insert_repl(Replication* repl, size_t ndx, int64_t value)
{
    repl->list_insert_int(*this, ndx, value);
}

template <>
void Lst<util::Optional<Int>>::insert_repl(Replication* repl, size_t ndx, util::Optional<Int> value)
{
    if (value) {
        repl->list_insert_int(*this, ndx, *value);
    }
    else {
        repl->list_insert_null(*this, ndx);
    }
}

template <>
void Lst<Bool>::insert_repl(Replication* repl, size_t ndx, bool value)
{
    repl->list_insert_bool(*this, ndx, value);
}

template <>
void Lst<util::Optional<bool>>::insert_repl(Replication* repl, size_t ndx, util::Optional<bool> value)
{
    if (value) {
        repl->list_insert_bool(*this, ndx, *value);
    }
    else {
        repl->list_insert_null(*this, ndx);
    }
}

template <>
void Lst<Float>::insert_repl(Replication* repl, size_t ndx, float value)
{
    repl->list_insert_float(*this, ndx, value);
}

template <>
void Lst<Double>::insert_repl(Replication* repl, size_t ndx, double value)
{
    repl->list_insert_double(*this, ndx, value);
}

template <>
void Lst<String>::insert_repl(Replication* repl, size_t ndx, StringData value)
{
    repl->list_insert_string(*this, ndx, value);
}

template <>
void Lst<Binary>::insert_repl(Replication* repl, size_t ndx, BinaryData value)
{
    repl->list_insert_binary(*this, ndx, value);
}

template <>
void Lst<Timestamp>::insert_repl(Replication* repl, size_t ndx, Timestamp value)
{
    repl->list_insert_timestamp(*this, ndx, value);
}

template <>
void Lst<ObjKey>::insert_repl(Replication* repl, size_t ndx, ObjKey key)
{
    repl->list_insert_link(*this, ndx, key);
}
}

#ifdef _WIN32
// For some strange reason these functions needs to be explicitly instantiated
// on Visual Studio 2017. Otherwise the code is not generated.
namespace realm {
template void Lst<ObjKey>::add(ObjKey target_key);
template void Lst<ObjKey>::insert(size_t ndx, ObjKey target_key);
template ObjKey Lst<ObjKey>::remove(size_t ndx);
template void Lst<ObjKey>::clear();
}
#endif
