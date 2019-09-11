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
#include "realm/array_decimal128.hpp"
#include "realm/array_object_id.hpp"
#include "realm/array_typed_link.hpp"
#include "realm/array_mixed.hpp"
#include "realm/column_type_traits.hpp"
#include "realm/object_id.hpp"
#include "realm/table.hpp"
#include "realm/table_view.hpp"
#include "realm/group.hpp"
#include "realm/replication.hpp"

namespace realm {

ConstLstBasePtr ConstObj::get_listbase_ptr(ColKey col_key) const
{
    auto attr = get_table()->get_column_attr(col_key);
    REALM_ASSERT(attr.test(col_attr_List));
    bool nullable = attr.test(col_attr_Nullable);

    switch (get_table()->get_column_type(col_key)) {
        case type_Int: {
            if (nullable)
                return std::make_unique<ConstLst<util::Optional<Int>>>(*this, col_key);
            else
                return std::make_unique<ConstLst<Int>>(*this, col_key);
        }
        case type_Bool: {
            if (nullable)
                return std::make_unique<ConstLst<util::Optional<Bool>>>(*this, col_key);
            else
                return std::make_unique<ConstLst<Bool>>(*this, col_key);
        }
        case type_Float: {
            if (nullable)
                return std::make_unique<ConstLst<util::Optional<Float>>>(*this, col_key);
            else
                return std::make_unique<ConstLst<Float>>(*this, col_key);
        }
        case type_Double: {
            if (nullable)
                return std::make_unique<ConstLst<util::Optional<Double>>>(*this, col_key);
            else
                return std::make_unique<ConstLst<Double>>(*this, col_key);
        }
        case type_String: {
            return std::make_unique<ConstLst<String>>(*this, col_key);
        }
        case type_Binary: {
            return std::make_unique<ConstLst<Binary>>(*this, col_key);
        }
        case type_Timestamp: {
            return std::make_unique<ConstLst<Timestamp>>(*this, col_key);
        }
        case type_Decimal: {
            return std::make_unique<ConstLst<Decimal128>>(*this, col_key);
        }
        case type_ObjectId: {
            if (nullable) {
                return std::make_unique<ConstLst<util::Optional<ObjectId>>>(*this, col_key);
            }
            else {
                return std::make_unique<ConstLst<ObjectId>>(*this, col_key);
            }
        }
        case type_Mixed:
            return std::make_unique<ConstLst<Mixed>>(*this, col_key);
        case type_TypedLink:
            return std::make_unique<ConstLst<ObjLink>>(*this, col_key);
        case type_LinkList: {
            const ConstLstBase* clb = get_linklist_ptr(col_key).release();
            return ConstLstBasePtr(const_cast<ConstLstBase*>(clb));
        }
        case type_Link:
        case type_OldDateTime:
        case type_OldTable:
            REALM_ASSERT(false);
            break;
    }
    return {};
}

LstBasePtr Obj::get_listbase_ptr(ColKey col_key) const
{
    auto attr = get_table()->get_column_attr(col_key);
    REALM_ASSERT(attr.test(col_attr_List));
    bool nullable = attr.test(col_attr_Nullable);

    switch (get_table()->get_column_type(col_key)) {
        case type_Int: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<Int>>>(*this, col_key);
            else
                return std::make_unique<Lst<Int>>(*this, col_key);
        }
        case type_Bool: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<Bool>>>(*this, col_key);
            else
                return std::make_unique<Lst<Bool>>(*this, col_key);
        }
        case type_Float: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<Float>>>(*this, col_key);
            else
                return std::make_unique<Lst<Float>>(*this, col_key);
        }
        case type_Double: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<Double>>>(*this, col_key);
            else
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
        case type_Decimal: {
            return std::make_unique<Lst<Decimal128>>(*this, col_key);
        }
        case type_ObjectId: {
            if (nullable)
                return std::make_unique<Lst<util::Optional<ObjectId>>>(*this, col_key);
            else
                return std::make_unique<Lst<ObjectId>>(*this, col_key);
        }
        case type_TypedLink: {
            return std::make_unique<Lst<ObjLink>>(*this, col_key);
        }
        case type_Mixed: {
            return std::make_unique<Lst<Mixed>>(*this, col_key);
        }
        case type_LinkList:
            return get_linklist_ptr(col_key);
        case type_Link:
        case type_OldDateTime:
        case type_OldTable:
            REALM_ASSERT(false);
            break;
    }
    return {};
}

/********************************* LstBase **********************************/

ConstLstBase::ConstLstBase(ColKey col_key, ConstObj* obj)
    : m_const_obj(obj)
    , m_col_key(col_key)
{
    if (!col_key.get_attrs().test(col_attr_List)) {
        throw LogicError(LogicError::list_type_mismatch);
    }
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
        return to_ref(m_const_obj->_get<int64_t>(m_col_key.get_index()));
    }
    catch (const KeyNotFound&) {
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
    if (ndx2 < ndx1)
        std::swap(ndx1, ndx2);
    repl->list_move(*this, ndx2, ndx1);
    if (ndx1 + 1 != ndx2)
        repl->list_move(*this, ndx1 + 1, ndx2);
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
    if (m_obj) {
        this->m_nullable = col_key.get_attrs().test(col_attr_Nullable);
        ConstLstIf<T>::init_from_parent();
    }
}

/****************************** Lst aggregates *******************************/

// This will be defined when using C++17
template <typename... Ts>
struct make_void {
    typedef void type;
};
template <typename... Ts>
using void_t = typename make_void<Ts...>::type;


template <class T, class = void>
struct MinHelper {
    template <class U>
    static Mixed eval(U&, size_t*)
    {
        return Mixed{};
    }
};

template <class T>
struct MinHelper<T, void_t<ColumnMinMaxType<T>>> {
    template <class U>
    static Mixed eval(U& tree, size_t* return_ndx)
    {
        return Mixed(bptree_minimum<T>(tree, return_ndx));
    }
};

template <class T>
Mixed ConstLstIf<T>::min(size_t* return_ndx) const
{
    return MinHelper<T>::eval(*m_tree, return_ndx);
}

template <class T, class Enable = void>
struct MaxHelper {
    template <class U>
    static Mixed eval(U&, size_t*)
    {
        return Mixed{};
    }
};

template <class T>
struct MaxHelper<T, void_t<ColumnMinMaxType<T>>> {
    template <class U>
    static Mixed eval(U& tree, size_t* return_ndx)
    {
        return Mixed(bptree_maximum<T>(tree, return_ndx));
    }
};

template <class T>
Mixed ConstLstIf<T>::max(size_t* return_ndx) const
{
    return MaxHelper<T>::eval(*m_tree, return_ndx);
}

template <class T, class Enable = void>
class SumHelper {
public:
    template <class U>
    static Mixed eval(U&, size_t* return_cnt)
    {
        if (return_cnt)
            *return_cnt = 0;
        return Mixed{};
    }
};

template <class T>
class SumHelper<T, void_t<ColumnSumType<T>>> {
public:
    template <class U>
    static Mixed eval(U& tree, size_t* return_cnt)
    {
        return Mixed(bptree_sum<T>(tree, return_cnt));
    }
};

template <class T>
Mixed ConstLstIf<T>::sum(size_t* return_cnt) const
{
    return SumHelper<T>::eval(*m_tree, return_cnt);
}

template <class T, class = void>
struct AverageHelper {
    template <class U>
    static Mixed eval(U&, size_t* return_cnt)
    {
        if (return_cnt)
            *return_cnt = 0;
        return Mixed{};
    }
};

template <class T>
struct AverageHelper<T, void_t<ColumnSumType<T>>> {
    template <class U>
    static Mixed eval(U& tree, size_t* return_cnt)
    {
        return Mixed(bptree_average<T>(tree, return_cnt));
    }
};

template <class T>
Mixed ConstLstIf<T>::avg(size_t* return_cnt) const
{
    return AverageHelper<T>::eval(*m_tree, return_cnt);
}

template <class T>
void ConstLstIf<T>::sort(std::vector<size_t>& indices, bool ascending) const
{
    auto sz = size();
    auto sz2 = indices.size();

    indices.reserve(sz);
    if (sz < sz2) {
        // If list size has decreased, we have to start all over
        indices.clear();
        sz2 = 0;
    }
    for (size_t i = sz2; i < sz; i++) {
        // If list size has increased, just add the missing indices
        indices.push_back(i);
    }
    auto b = indices.begin();
    auto e = indices.end();
    if (ascending) {
        std::sort(b, e, [this](size_t i1, size_t i2) { return m_tree->get(i1) < m_tree->get(i2); });
    }
    else {
        std::sort(b, e, [this](size_t i1, size_t i2) { return m_tree->get(i1) > m_tree->get(i2); });
    }
}

template <class T>
void ConstLstIf<T>::distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order) const
{
    indices.clear();
    sort(indices, sort_order ? *sort_order : true);
    auto duplicates = std::unique(indices.begin(), indices.end(),
                                  [this](size_t i1, size_t i2) { return m_tree->get(i1) == m_tree->get(i2); });
    // Erase the duplicates
    indices.erase(duplicates, indices.end());

    if (!sort_order) {
        // Restore original order
        std::sort(indices.begin(), indices.end(), std::less<size_t>());
    }
}


/************************* template instantiations ***************************/

template ConstLst<int64_t>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<util::Optional<Int>>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<bool>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<util::Optional<bool>>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<float>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<util::Optional<float>>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<double>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<util::Optional<double>>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<StringData>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<BinaryData>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<Timestamp>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<ObjKey>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<Decimal128>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<ObjectId>::ConstLst(const ConstObj& obj, ColKey col_key);
template ConstLst<util::Optional<ObjectId>>::ConstLst(const ConstObj& obj, ColKey col_key);

template Lst<int64_t>::Lst(const Obj& obj, ColKey col_key);
template Lst<util::Optional<Int>>::Lst(const Obj& obj, ColKey col_key);
template Lst<bool>::Lst(const Obj& obj, ColKey col_key);
template Lst<util::Optional<bool>>::Lst(const Obj& obj, ColKey col_key);
template Lst<float>::Lst(const Obj& obj, ColKey col_key);
template Lst<util::Optional<float>>::Lst(const Obj& obj, ColKey col_key);
template Lst<double>::Lst(const Obj& obj, ColKey col_key);
template Lst<util::Optional<double>>::Lst(const Obj& obj, ColKey col_key);
template Lst<StringData>::Lst(const Obj& obj, ColKey col_key);
template Lst<BinaryData>::Lst(const Obj& obj, ColKey col_key);
template Lst<Timestamp>::Lst(const Obj& obj, ColKey col_key);
template Lst<ObjKey>::Lst(const Obj& obj, ColKey col_key);
template Lst<Decimal128>::Lst(const Obj& obj, ColKey col_key);
template Lst<ObjectId>::Lst(const Obj& obj, ColKey col_key);
template Lst<util::Optional<ObjectId>>::Lst(const Obj& obj, ColKey col_key);
template Lst<Mixed>::Lst(const Obj& obj, ColKey col_key);
template Lst<ObjLink>::Lst(const Obj& obj, ColKey col_key);

ConstObj ConstLnkLst::get_object(size_t link_ndx) const
{
    return m_const_obj->get_target_table(m_col_key)->get_object(get(link_ndx));
}

bool ConstLnkLst::init_from_parent() const
{
    ConstLstIf<ObjKey>::init_from_parent();
    update_unresolved(m_unresolved, *m_tree);

    return m_valid;
}

/********************************* Lst<Key> *********************************/

namespace {
void check_for_last_unresolved(BPlusTree<ObjKey>& tree)
{
    bool no_more_unresolved = true;
    size_t sz = tree.size();
    for (size_t n = 0; n < sz; n++) {
        if (tree.get(n).is_unresolved()) {
            no_more_unresolved = false;
            break;
        }
    }
    if (no_more_unresolved)
        tree.set_context_flag(true);
}
} // namespace

template <>
void Lst<ObjKey>::do_set(size_t ndx, ObjKey target_key)
{
    auto origin_table = m_obj.get_table();
    auto target_table = origin_table->get_opposite_table(m_col_key);
    auto target_table_key = target_table->get_key();
    ObjKey old_key = get(ndx);
    CascadeState state(CascadeState::Mode::Strong);
    bool recurse =
        m_obj.replace_backlink(m_col_key, {target_table_key, old_key}, {target_table_key, target_key}, state);

    m_tree->set(ndx, target_key);

    if (recurse) {
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
    if (target_key.is_unresolved()) {
        if (!old_key.is_unresolved())
            m_tree->set_context_flag(true);
    }
    else if (old_key.is_unresolved()) {
        // We might have removed the last unresolved link - check it
        check_for_last_unresolved(*m_tree);
    }
}

template <>
void Lst<ObjKey>::do_insert(size_t ndx, ObjKey target_key)
{
    auto origin_table = m_obj.get_table();
    auto target_table = origin_table->get_opposite_table(m_col_key);
    auto target_table_key = target_table->get_key();
    m_obj.set_backlink(m_col_key, {target_table_key, target_key});
    m_tree->insert(ndx, target_key);
    if (target_key.is_unresolved()) {
        m_tree->set_context_flag(true);
    }
}

template <>
void Lst<ObjKey>::do_remove(size_t ndx)
{
    auto origin_table = m_obj.get_table();
    auto target_table = origin_table->get_opposite_table(m_col_key);
    auto target_table_key = target_table->get_key();
    ObjKey old_key = get(ndx);
    CascadeState state(old_key.is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);

    bool recurse = m_obj.remove_backlink(m_col_key, {target_table_key, old_key}, state);

    m_tree->erase(ndx);

    if (recurse) {
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
    if (old_key.is_unresolved()) {
        // We might have removed the last unresolved link - check it
        check_for_last_unresolved(*m_tree);
    }
}

template <>
void Lst<ObjKey>::clear()
{
    update_if_needed();
    auto sz = Lst<ObjKey>::size();

    if (sz > 0) {
        auto origin_table = m_obj.get_table();
        TableRef target_table = m_obj.get_target_table(m_col_key);

        if (Replication* repl = m_const_obj->get_replication())
            repl->list_clear(*this); // Throws

        if (!target_table->is_embedded()) {
            size_t ndx = sz;
            while (ndx--) {
                do_set(ndx, null_key);
                m_tree->erase(ndx);
                ConstLstBase::adj_remove(ndx);
            }
            // m_obj.bump_both_versions();
            m_obj.bump_content_version();
            m_tree->set_context_flag(false);
            return;
        }

        TableKey target_table_key = target_table->get_key();
        ColKey backlink_col = origin_table->get_opposite_column(m_col_key);

        CascadeState state;

        typedef _impl::TableFriend tf;
        size_t num_links = size();
        for (size_t ndx = 0; ndx < num_links; ++ndx) {
            ObjKey target_key = m_tree->get(ndx);
            Obj target_obj = target_table->get_object(target_key);
            target_obj.remove_one_backlink(backlink_col, m_obj.get_key()); // Throws
            size_t num_remaining = target_obj.get_backlink_count(*origin_table, m_col_key);
            if (num_remaining == 0) {
                state.m_to_be_deleted.emplace_back(target_table_key, target_key);
            }
        }

        m_tree->clear();
        m_obj.bump_both_versions();
        m_tree->set_context_flag(false);

        tf::remove_recursive(*origin_table, state); // Throws
    }
}

template <>
void Lst<ObjLink>::do_set(size_t ndx, ObjLink target_link)
{
    ObjLink old_link = get(ndx);
    CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);
    bool recurse = m_obj.replace_backlink(m_col_key, old_link, target_link, state);

    m_tree->set(ndx, target_link);

    if (recurse) {
        auto origin_table = m_obj.get_table();
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
}

template <>
void Lst<ObjLink>::do_insert(size_t ndx, ObjLink target_link)
{
    m_obj.set_backlink(m_col_key, target_link);
    m_tree->insert(ndx, target_link);
}

template <>
void Lst<ObjLink>::do_remove(size_t ndx)
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
void Lst<Mixed>::do_set(size_t ndx, Mixed value)
{
    ObjLink old_link;
    ObjLink target_link;
    Mixed old_value = get(ndx);

    if (old_value.get_type() == type_TypedLink) {
        old_link = old_value.get<ObjLink>();
    }
    if (value.get_type() == type_TypedLink) {
        target_link = value.get<ObjLink>();
    }

    CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);
    bool recurse = m_obj.replace_backlink(m_col_key, old_link, target_link, state);

    m_tree->set(ndx, value);

    if (recurse) {
        auto origin_table = m_obj.get_table();
        _impl::TableFriend::remove_recursive(*origin_table, state); // Throws
    }
}

template <>
void Lst<Mixed>::do_insert(size_t ndx, Mixed value)
{
    if (value.get_type() == type_TypedLink) {
        m_obj.set_backlink(m_col_key, value.get<ObjLink>());
    }
    m_tree->insert(ndx, value);
}

template <>
void Lst<Mixed>::do_remove(size_t ndx)
{
    ObjLink old_link;
    Mixed old_value = get(ndx);

    if (old_value.get_type() == type_TypedLink) {
        old_link = old_value.get<ObjLink>();
    }

    CascadeState state(old_link.get_obj_key().is_unresolved() ? CascadeState::Mode::All : CascadeState::Mode::Strong);
    bool recurse = m_obj.remove_backlink(m_col_key, old_link, state);

    m_tree->erase(ndx);

    if (recurse) {
        auto table = m_obj.get_table();
        _impl::TableFriend::remove_recursive(*table, state); // Throws
    }
}

Obj LnkLst::get_object(size_t ndx) const
{
    ObjKey k = get(ndx);
    return get_target_table()->get_object(k);
}

bool LnkLst::init_from_parent() const
{
    ConstLstIf<ObjKey>::init_from_parent();
    update_unresolved(m_unresolved, *m_tree);

    return m_valid;
}

size_t virtual2real(const std::vector<size_t>& vec, size_t ndx)
{
    for (auto i : vec) {
        if (i > ndx)
            break;
        ndx++;
    }
    return ndx;
}

void update_unresolved(std::vector<size_t>& vec, const BPlusTree<ObjKey>& tree)
{
    vec.clear();
    if (tree.is_attached()) {
        // Only do the scan if context flag is set.
        if (tree.get_context_flag()) {
            auto func = [&vec](BPlusTreeNode* node, size_t offset) {
                auto leaf = static_cast<typename BPlusTree<ObjKey>::LeafNode*>(node);
                size_t sz = leaf->size();
                for (size_t i = 0; i < sz; i++) {
                    auto k = leaf->get(i);
                    if (k.is_unresolved()) {
                        vec.push_back(i + offset);
                    }
                }
                return false;
            };

            tree.traverse(func);
        }
    }
}

void LnkLst::set(size_t ndx, ObjKey value)
{
    if (get_target_table()->is_embedded() && value != ObjKey())
        throw LogicError(LogicError::wrong_kind_of_table);

    Lst<ObjKey>::set(virtual2real(m_unresolved, ndx), value);
}

void LnkLst::insert(size_t ndx, ObjKey value)
{
    if (get_target_table()->is_embedded() && value != ObjKey())
        throw LogicError(LogicError::wrong_kind_of_table);

    Lst<ObjKey>::insert(virtual2real(m_unresolved, ndx), value);
}

Obj LnkLst::create_and_insert_linked_object(size_t ndx)
{
    Table& t = *get_target_table();
    auto o = t.is_embedded() ? t.create_linked_object() : t.create_object();
    Lst<ObjKey>::insert(ndx, o.get_key());
    return o;
}

Obj LnkLst::create_and_set_linked_object(size_t ndx)
{
    Table& t = *get_target_table();
    auto o = t.is_embedded() ? t.create_linked_object() : t.create_object();
    Lst<ObjKey>::set(ndx, o.get_key());
    return o;
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
    TableView v = get_sorted_view(SortDescriptor({{column_key}}, {ascending}));
    return v;
}

void LnkLst::remove_target_row(size_t link_ndx)
{
    // Deleting the object will automatically remove all links
    // to it. So we do not have to manually remove the deleted link
    ObjKey k = get(link_ndx);
    get_target_table()->remove_object(k);
}

void LnkLst::remove_all_target_rows()
{
    if (is_attached()) {
        _impl::TableFriend::batch_erase_rows(*get_target_table(), *this->m_tree);
    }
}

LnkLst::LnkLst(const Obj& owner, ColKey col_key)
    : ConstLstBase(col_key, &m_obj)
    , Lst<ObjKey>(owner, col_key)
{
    update_unresolved(m_unresolved, *m_tree);
}

void LnkLst::get_dependencies(TableVersions& versions) const
{
    if (is_attached()) {
        auto table = get_table();
        versions.emplace_back(table->get_key(), table->get_content_version());
    }
}

void LnkLst::sync_if_needed() const
{
    if (this->is_attached()) {
        const_cast<LnkLst*>(this)->update_if_needed();
    }
}

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
void Lst<util::Optional<Float>>::set_repl(Replication* repl, size_t ndx, util::Optional<Float> value)
{
    if (value) {
        repl->list_set_float(*this, ndx, *value);
    }
    else {
        repl->list_set_null(*this, ndx);
    }
}

template <>
void Lst<Double>::set_repl(Replication* repl, size_t ndx, double value)
{
    repl->list_set_double(*this, ndx, value);
}

template <>
void Lst<util::Optional<Double>>::set_repl(Replication* repl, size_t ndx, util::Optional<Double> value)
{
    if (value) {
        repl->list_set_double(*this, ndx, *value);
    }
    else {
        repl->list_set_null(*this, ndx);
    }
}

template <>
void Lst<String>::set_repl(Replication* repl, size_t ndx, StringData value)
{
    if (value.is_null()) {
        repl->list_set_null(*this, ndx);
    }
    else {
        repl->list_set_string(*this, ndx, value);
    }
}

template <>
void Lst<Binary>::set_repl(Replication* repl, size_t ndx, BinaryData value)
{
    if (value.is_null()) {
        repl->list_set_null(*this, ndx);
    }
    else {
        repl->list_set_binary(*this, ndx, value);
    }
}

template <>
void Lst<Timestamp>::set_repl(Replication* repl, size_t ndx, Timestamp value)
{
    if (value.is_null()) {
        repl->list_set_null(*this, ndx);
    }
    else {
        repl->list_set_timestamp(*this, ndx, value);
    }
}

template <>
void Lst<ObjectId>::set_repl(Replication* repl, size_t ndx, ObjectId value)
{
    repl->list_set_object_id(*this, ndx, value);
}

template <>
void Lst<util::Optional<ObjectId>>::set_repl(Replication* repl, size_t ndx, util::Optional<ObjectId> value)
{
    if (value) {
        repl->list_set_object_id(*this, ndx, *value);
    }
    else {
        repl->list_set_null(*this, ndx);
    }
}


template <>
void Lst<ObjKey>::set_repl(Replication* repl, size_t ndx, ObjKey key)
{
    if (key) {
        repl->list_set_link(*this, ndx, key);
    }
    else {
        repl->list_set_null(*this, ndx);
    }
}

template <>
void Lst<ObjLink>::set_repl(Replication*, size_t, ObjLink)
{
    // TODO: Implement
}

template <>
void Lst<Mixed>::set_repl(Replication*, size_t, Mixed)
{
    // TODO: Implement
}

template <>
void Lst<Decimal128>::set_repl(Replication* repl, size_t ndx, Decimal128 value)
{
    if (value.is_null()) {
        repl->list_set_null(*this, ndx);
    }
    else {
        repl->list_set_decimal(*this, ndx, value);
    }
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
void Lst<util::Optional<Float>>::insert_repl(Replication* repl, size_t ndx, util::Optional<Float> value)
{
    if (value) {
        repl->list_insert_float(*this, ndx, *value);
    }
    else {
        repl->list_insert_null(*this, ndx);
    }
}

template <>
void Lst<Double>::insert_repl(Replication* repl, size_t ndx, double value)
{
    repl->list_insert_double(*this, ndx, value);
}

template <>
void Lst<util::Optional<Double>>::insert_repl(Replication* repl, size_t ndx, util::Optional<Double> value)
{
    if (value) {
        repl->list_insert_double(*this, ndx, *value);
    }
    else {
        repl->list_insert_null(*this, ndx);
    }
}

template <>
void Lst<String>::insert_repl(Replication* repl, size_t ndx, StringData value)
{
    if (value.is_null()) {
        repl->list_insert_null(*this, ndx);
    }
    else {
        repl->list_insert_string(*this, ndx, value);
    }
}

template <>
void Lst<Binary>::insert_repl(Replication* repl, size_t ndx, BinaryData value)
{
    if (value.is_null()) {
        repl->list_insert_null(*this, ndx);
    }
    else {
        repl->list_insert_binary(*this, ndx, value);
    }
}

template <>
void Lst<Timestamp>::insert_repl(Replication* repl, size_t ndx, Timestamp value)
{
    if (value.is_null()) {
        repl->list_insert_null(*this, ndx);
    }
    else {
        repl->list_insert_timestamp(*this, ndx, value);
    }
}

template <>
void Lst<ObjectId>::insert_repl(Replication* repl, size_t ndx, ObjectId value)
{
    repl->list_insert_object_id(*this, ndx, value);
}

template <>
void Lst<util::Optional<ObjectId>>::insert_repl(Replication* repl, size_t ndx, util::Optional<ObjectId> value)
{
    if (value) {
        repl->list_insert_object_id(*this, ndx, *value);
    }
    else {
        repl->list_insert_null(*this, ndx);
    }
}


template <>
void Lst<ObjKey>::insert_repl(Replication* repl, size_t ndx, ObjKey key)
{
    if (key) {
        repl->list_insert_link(*this, ndx, key);
    }
    else {
        repl->list_insert_null(*this, ndx);
    }
}

template <>
void Lst<ObjLink>::insert_repl(Replication*, size_t, ObjLink)
{
    // TODO: Implement
}

template <>
void Lst<Mixed>::insert_repl(Replication*, size_t, Mixed)
{
    // TODO: Implement
}

template <>
void Lst<Decimal128>::insert_repl(Replication* repl, size_t ndx, Decimal128 value)
{
    if (value.is_null()) {
        repl->list_insert_null(*this, ndx);
    }
    else {
        repl->list_insert_decimal(*this, ndx, value);
    }
}

#ifdef _WIN32
// For some strange reason these functions needs to be explicitly instantiated
// on Visual Studio 2017. Otherwise the code is not generated.
template void Lst<ObjKey>::add(ObjKey target_key);
template void Lst<ObjKey>::insert(size_t ndx, ObjKey target_key);
template ObjKey Lst<ObjKey>::remove(size_t ndx);
template void Lst<ObjKey>::clear();
template void Lst<ObjKey>::do_insert(size_t ndx, ObjKey target_key);
template void Lst<ObjKey>::do_set(size_t ndx, ObjKey target_key);
template void Lst<ObjKey>::do_remove(size_t ndx);

template void Lst<ObjLink>::do_insert(size_t ndx, ObjLink target_key);
template void Lst<ObjLink>::do_set(size_t ndx, ObjLink target_key);
template void Lst<ObjLink>::do_remove(size_t ndx);

template void Lst<Mixed>::do_insert(size_t ndx, Mixed target_key);
template void Lst<Mixed>::do_set(size_t ndx, Mixed target_key);
template void Lst<Mixed>::do_remove(size_t ndx);
#endif

} // namespace realm
