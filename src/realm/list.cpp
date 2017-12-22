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

ListBasePtr Obj::get_listbase_ptr(size_t col_ndx, DataType type)
{
    switch (type) {
        case type_Int: {
            return std::make_unique<List<Int>>(*this, col_ndx);
        }
        case type_Bool: {
            return std::make_unique<List<Bool>>(*this, col_ndx);
        }
        case type_Float: {
            return std::make_unique<List<Float>>(*this, col_ndx);
        }
        case type_Double: {
            return std::make_unique<List<Double>>(*this, col_ndx);
        }
        case type_String: {
            return std::make_unique<List<String>>(*this, col_ndx);
        }
        case type_Binary: {
            return std::make_unique<List<Binary>>(*this, col_ndx);
        }
        case type_Timestamp: {
            return std::make_unique<List<Timestamp>>(*this, col_ndx);
        }
        case type_LinkList:
            return get_linklist_ptr(col_ndx);
        case type_Link:
        case type_OldDateTime:
        case type_OldTable:
        case type_OldMixed:
            REALM_ASSERT(false);
            break;
    }
    return {};
}


/********************************* ListBase **********************************/

template <class T>
ConstList<T>::ConstList(const ConstObj& obj, size_t col_ndx)
    : ConstListIf<T>(col_ndx, obj.get_alloc())
    , m_obj(obj)
{
    this->set_obj(&m_obj);
    this->init_from_parent();
}

ConstListBase::~ConstListBase()
{
}

ref_type ConstListBase::get_child_ref(size_t) const noexcept
{
    return to_ref(m_const_obj->get<int64_t>(m_col_ndx));
}

std::pair<ref_type, size_t> ConstListBase::get_to_dot_parent(size_t) const
{
    // TODO
    return {};
}

void ConstListBase::insert_null_repl(Replication* repl, size_t ndx) const
{
    repl->list_insert_null(*this, ndx);
}

void ConstListBase::erase_repl(Replication* repl, size_t ndx) const
{
    repl->list_erase(*this, ndx);
}

void ConstListBase::move_repl(Replication* repl, size_t from, size_t to) const
{
    repl->list_move(*this, from, to);
}

void ConstListBase::swap_repl(Replication* repl, size_t ndx1, size_t ndx2) const
{
    repl->list_swap(*this, ndx1, ndx2);
}

void ConstListBase::clear_repl(Replication* repl) const
{
    repl->list_clear(*this);
}

template <class T>
List<T>::List(const Obj& obj, size_t col_ndx)
    : ConstListIf<T>(col_ndx, obj.m_tree_top->get_alloc())
    , m_obj(obj)
{
    this->set_obj(&m_obj);
    this->init_from_parent();
    if (!ConstListIf<T>::m_valid) {
        create();
        ref_type ref = m_leaf->get_ref();
        m_obj.set_int(col_ndx, from_ref(ref));
    }
}

namespace realm {
template ConstList<int64_t>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<bool>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<float>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<double>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<StringData>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<BinaryData>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<Timestamp>::ConstList(const ConstObj& obj, size_t col_ndx);
template ConstList<Key>::ConstList(const ConstObj& obj, size_t col_ndx);

template List<int64_t>::List(const Obj& obj, size_t col_ndx);
template List<bool>::List(const Obj& obj, size_t col_ndx);
template List<float>::List(const Obj& obj, size_t col_ndx);
template List<double>::List(const Obj& obj, size_t col_ndx);
template List<StringData>::List(const Obj& obj, size_t col_ndx);
template List<BinaryData>::List(const Obj& obj, size_t col_ndx);
template List<Timestamp>::List(const Obj& obj, size_t col_ndx);
template List<Key>::List(const Obj& obj, size_t col_ndx);
}

ConstObj ConstLinkListIf::get(size_t link_ndx) const
{
    return m_const_obj->get_target_table(m_col_ndx)->get_object(ConstListIf<Key>::get(link_ndx));
}

/********************************* LinkList **********************************/

Obj LinkList::get(size_t link_ndx)
{
    return get_target_table().get_object(List<Key>::get(link_ndx));
}

/********************************* List<Key> *********************************/

template <>
void List<Key>::do_set(size_t ndx, Key target_key)
{
    CascadeState state;
    Key old_key = get(ndx);
    bool recurse = m_obj.update_backlinks(m_col_ndx, old_key, target_key, state);

    m_leaf->set(ndx, target_key);

    if (recurse) {
        auto table = const_cast<Table*>(m_obj.get_table());
        _impl::TableFriend::remove_recursive(*table, state); // Throws
    }
}

template <>
Key List<Key>::remove(size_t ndx)
{
    ensure_writeable();
    do_set(ndx, null_key);
    if (Replication* repl = this->m_const_obj->get_alloc().get_replication()) {
        ConstListBase::erase_repl(repl, ndx);
    }
    Key old = get(ndx);
    m_leaf->erase(ndx);
    m_obj.bump_version();
    ConstListBase::adj_remove(ndx);
    m_obj.bump_version();

    return old;
}

template <>
void List<Key>::clear()
{
    update_if_needed();
    Table* origin_table = const_cast<Table*>(m_obj.get_table());
    const Spec& origin_table_spec = _impl::TableFriend::get_spec(*origin_table);

    if (Replication* repl = m_const_obj->get_alloc().get_replication())
        repl->list_clear(*this); // Throws

    if (!origin_table_spec.get_column_attr(m_col_ndx).test(col_attr_StrongLinks)) {
        size_t ndx = size();
        while (ndx--) {
            do_set(ndx, null_key);
            m_leaf->erase(ndx);
            ConstListBase::adj_remove(ndx);
        }
        return;
    }

    TableRef target_table = m_obj.get_target_table(m_col_ndx);
    TableKey target_table_key = target_table->get_key();
    const Spec& target_table_spec = _impl::TableFriend::get_spec(*target_table);
    size_t backlink_col = target_table_spec.find_backlink_column(m_obj.get_table_key(), m_col_ndx);

    CascadeState state;
    state.stop_on_link_list_column_ndx = m_col_ndx;
    state.stop_on_link_list_key = m_obj.get_key();

    typedef _impl::TableFriend tf;
    size_t num_links = size();
    for (size_t ndx = 0; ndx < num_links; ++ndx) {
        Key target_key = m_leaf->get(ndx);
        Obj target_obj = target_table->get_object(target_key);
        target_obj.remove_one_backlink(backlink_col, m_obj.get_key()); // Throws
        size_t num_remaining = target_obj.get_backlink_count(*origin_table, m_col_ndx);
        if (num_remaining == 0) {
            state.rows.emplace_back(target_table_key, target_key);
        }
    }

    m_leaf->truncate_and_destroy_children(0);
    m_obj.bump_version();

    tf::remove_recursive(*origin_table, state); // Throws
}

TableView LinkList::get_sorted_view(SortDescriptor order) const
{
    TableView tv(get_target_table(), clone());
    tv.do_sync();
    tv.sort(std::move(order));
    return tv;
}

TableView LinkList::get_sorted_view(size_t column_index, bool ascending) const
{
    TableView v = get_sorted_view(SortDescriptor(get_target_table(), {{column_index}}, {ascending}));
    return v;
}

void LinkList::sort(SortDescriptor&& order)
{
    /*  TODO: implement
    if (Replication* repl = m_obj.get_alloc().get_replication()) {
        // todo, write to the replication log that we're doing a sort
        repl->set_link_list(*this, *this->m_leaf); // Throws
    }
    */
    DescriptorOrdering ordering;
    ordering.append_sort(std::move(order));
    update_if_needed();
    do_sort(ordering);
    m_obj.bump_version();
}

void LinkList::sort(size_t column_index, bool ascending)
{
    sort(SortDescriptor(get_target_table(), {{column_index}}, {ascending}));
}

void LinkList::remove_target_row(size_t link_ndx)
{
    // Deleting the object will automatically remove all links
    // to it. So we do not have to manually remove the deleted link
    get(link_ndx).remove();
}

void LinkList::remove_all_target_rows()
{
    if (is_valid()) {
        auto table = const_cast<Table*>(get_table());
        _impl::TableFriend::batch_erase_rows(*table, *this->m_leaf);
    }
}

uint_fast64_t LinkList::sync_if_needed() const
{
    const_cast<LinkList*>(this)->update_if_needed();
    return get_table()->get_version_counter();
}

bool LinkList::is_in_sync() const
{
    return const_cast<LinkList*>(this)->update_if_needed();
}

void LinkList::generate_patch(const LinkList* list, std::unique_ptr<LinkListHandoverPatch>& patch)
{
    if (list) {
        if (list->is_valid()) {
            patch.reset(new LinkListHandoverPatch);
            Table::generate_patch(list->get_table(), patch->m_table);
            patch->m_col_num = list->get_col_ndx();
            patch->m_key_value = list->ConstListBase::get_key().value;
        }
        else {
            // if the LinkView has become detached, indicate it by passing
            // a handover patch with a nullptr in m_table.
            patch.reset(new LinkListHandoverPatch);
            patch->m_table = nullptr;
        }
    }
    else
        patch.reset();
}


LinkListPtr LinkList::create_from_and_consume_patch(std::unique_ptr<LinkListHandoverPatch>& patch, Group& group)
{
    if (patch) {
        if (patch->m_table) {
            TableRef tr = Table::create_from_and_consume_patch(patch->m_table, group);
            auto result = tr->get_object(Key(patch->m_key_value)).get_linklist_ptr(patch->m_col_num);
            patch.reset();
            return result;
        }
        else {
            // We end up here if we're handing over a detached LinkView.
            // This is indicated by a patch with a null m_table.

            // TODO: Should we be able to create a detached LinkView
        }
    }
    return {};
}

/***************************** List<T>::set_repl *****************************/

namespace realm {
template <>
void List<Int>::set_repl(Replication* repl, size_t ndx, int64_t value)
{
    repl->list_set_int(*this, ndx, value);
}

template <>
void List<Bool>::set_repl(Replication* repl, size_t ndx, bool value)
{
    repl->list_set_bool(*this, ndx, value);
}

template <>
void List<Float>::set_repl(Replication* repl, size_t ndx, float value)
{
    repl->list_set_float(*this, ndx, value);
}

template <>
void List<Double>::set_repl(Replication* repl, size_t ndx, double value)
{
    repl->list_set_double(*this, ndx, value);
}

template <>
void List<String>::set_repl(Replication* repl, size_t ndx, StringData value)
{
    repl->list_set_string(*this, ndx, value);
}

template <>
void List<Binary>::set_repl(Replication* repl, size_t ndx, BinaryData value)
{
    repl->list_set_binary(*this, ndx, value);
}

template <>
void List<Timestamp>::set_repl(Replication* repl, size_t ndx, Timestamp value)
{
    repl->list_set_timestamp(*this, ndx, value);
}

template <>
void List<Key>::set_repl(Replication* repl, size_t ndx, Key key)
{
    repl->list_set_link(*this, ndx, key);
}
}

#ifdef _WIN32
// For some strange reason these functions needs to be explicitly instantiated
// on Visual Studio 2017. Otherwise the code is not generated.
namespace realm {
template void List<Key>::add(Key target_key);
template void List<Key>::insert(size_t ndx, Key target_key);
template Key List<Key>::remove(size_t ndx);
template void List<Key>::clear();
}
#endif
