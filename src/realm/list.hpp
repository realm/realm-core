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

#ifndef REALM_LIST_HPP
#define REALM_LIST_HPP

#include <realm/collection.hpp>

#include <realm/obj.hpp>
#include <realm/bplustree.hpp>
#include <realm/obj_list.hpp>
#include <realm/array_basic.hpp>
#include <realm/array_integer.hpp>
#include <realm/array_key.hpp>
#include <realm/array_bool.hpp>
#include <realm/array_string.hpp>
#include <realm/array_binary.hpp>
#include <realm/array_timestamp.hpp>
#include <realm/array_ref.hpp>
#include <realm/array_fixed_bytes.hpp>
#include <realm/array_decimal128.hpp>
#include <realm/array_mixed.hpp>
#include <realm/array_typed_link.hpp>
#include <realm/replication.hpp>

#ifdef _MSC_VER
#pragma warning(disable : 4250) // Suppress 'inherits ... via dominance' on MSVC
#endif

namespace realm {

class TableView;
class SortDescriptor;
class Group;
class LstBase;

template <class T>
using LstIterator = typename Collection<T, LstBase>::iterator;

/*
 * This class defines a virtual interface to a writable list
 */
class LstBase : public CollectionBase {
public:
    using CollectionBase::CollectionBase;

    virtual ~LstBase() {}
    LstBasePtr clone() const
    {
        return m_obj.get_listbase_ptr(m_col_key);
    }
    virtual void set_null(size_t ndx) = 0;
    virtual void set_any(size_t ndx, Mixed val) = 0;
    virtual void insert_null(size_t ndx) = 0;
    virtual void insert_any(size_t ndx, Mixed val) = 0;
    virtual void resize(size_t new_size) = 0;
    virtual void remove(size_t from, size_t to) = 0;
    virtual void move(size_t from, size_t to) = 0;
    virtual void swap(size_t ndx1, size_t ndx2) = 0;
    virtual void clear() = 0;

protected:
    void swap_repl(Replication* repl, size_t ndx1, size_t ndx2) const;
};

template <class T>
class Lst : public Collection<T, LstBase> {
public:
    using Collection<T, LstBase>::m_tree;
    using Collection<T, LstBase>::get;
    using Collection<T, LstBase>::size;

    Lst() = default;

    Lst(const Obj& owner, ColKey col_key);
    Lst(const Lst& other);

    Lst& operator=(const Lst& other);
    Lst& operator=(const BPlusTree<T>& other);

    void create();

    // Overriding members of CollectionBase:
    Mixed min(size_t* return_ndx = nullptr) const final;
    Mixed max(size_t* return_ndx = nullptr) const final;
    Mixed sum(size_t* return_cnt = nullptr) const final;
    Mixed avg(size_t* return_cnt = nullptr) const final;
    void sort(std::vector<size_t>& indices, bool ascending = true) const final;
    void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const final;

    // Overriding members of LstBase:
    void set_null(size_t ndx) override;
    void set_any(size_t ndx, Mixed val) override;
    void insert_null(size_t ndx) override;
    void insert_any(size_t ndx, Mixed val) override;
    void resize(size_t new_size) override;
    void remove(size_t from, size_t to) override;
    void move(size_t from, size_t to) override;
    void swap(size_t ndx1, size_t ndx2) override;
    void clear() override;

    void add(T value);
    T set(size_t ndx, T value);
    void insert(size_t ndx, T value);
    T remove(const LstIterator<T>& it);
    T remove(size_t ndx);

    using Collection<T, LstBase>::m_col_key;

protected:
    using Collection<T, LstBase>::m_valid;
    using Collection<T, LstBase>::m_nullable;
    using Collection<T, LstBase>::m_obj;
    using Collection<T, LstBase>::init_from_parent;

    bool update_if_needed();
    void ensure_created();
    void do_set(size_t ndx, T value);
    void do_insert(size_t ndx, T value);
    void do_remove(size_t ndx);
};

// Specialization of Lst<ObjKey>:
template <>
void Lst<ObjKey>::clear();
template <>
void Lst<ObjKey>::do_set(size_t, ObjKey);
template <>
void Lst<ObjKey>::do_insert(size_t, ObjKey);
template <>
void Lst<ObjKey>::do_remove(size_t);
extern template class Lst<ObjKey>;

// Specialization of Lst<Mixed>:
template <>
void Lst<Mixed>::do_set(size_t, Mixed);
template <>
void Lst<Mixed>::do_insert(size_t, Mixed);
template <>
void Lst<Mixed>::do_remove(size_t);
extern template class Lst<Mixed>;

// Specialization of Lst<ObjLink>:
template <>
void Lst<ObjLink>::do_set(size_t, ObjLink);
template <>
void Lst<ObjLink>::do_insert(size_t, ObjLink);
template <>
void Lst<ObjLink>::do_remove(size_t);
extern template class Lst<ObjLink>;

class LnkLst : public Lst<ObjKey>, public ObjList {
public:
    LnkLst() = default;

    LnkLst(const Obj& owner, ColKey col_key);
    LnkLst(const LnkLst& other)
        : Lst<ObjKey>(other)
        , m_unresolved(other.m_unresolved)
    {
    }
    LnkLst& operator=(const LnkLst& other)
    {
        Lst<ObjKey>::operator=(other);
        m_unresolved = other.m_unresolved;
        return *this;
    }

    LnkLstPtr clone() const
    {
        if (m_obj.is_valid()) {
            return std::make_unique<LnkLst>(m_obj, m_col_key);
        }
        else {
            return std::make_unique<LnkLst>();
        }
    }
    TableRef get_target_table() const override
    {
        return m_obj.get_target_table(m_col_key);
    }
    bool is_in_sync() const override
    {
        return true;
    }
    size_t size() const override
    {
        auto full_sz = Lst<ObjKey>::size();
        return full_sz - m_unresolved.size();
    }

    bool has_unresolved() const noexcept
    {
        return !m_unresolved.empty();
    }

    bool is_obj_valid(size_t) const noexcept override
    {
        // A link list cannot contain null values
        return true;
    }

    Obj get_object(size_t ndx) const override;

    Obj operator[](size_t ndx)
    {
        return get_object(ndx);
    }

    using Lst<ObjKey>::find_first;
    using Lst<ObjKey>::find_all;
    void add(ObjKey value)
    {
        insert(size(), value);
    }
    void set(size_t ndx, ObjKey value);
    void insert(size_t ndx, ObjKey value);
    ObjKey get(size_t ndx) const;
    ObjKey get_key(size_t ndx) const override;
    void remove(size_t ndx);
    void remove(size_t from, size_t to) override;
    void clear() override;
    // Create a new object in insert a link to it
    Obj create_and_insert_linked_object(size_t ndx);
    // Create a new object and link it. If an embedded object
    // is already set, it will be removed. TBD: If a non-embedded
    // object is already set, we throw LogicError (to prevent
    // dangling objects, since they do not delete automatically
    // if they are not embedded...)
    Obj create_and_set_linked_object(size_t ndx);
    // to be implemented:
    Obj clear_linked_object(size_t ndx);

    TableView get_sorted_view(SortDescriptor order) const;
    TableView get_sorted_view(ColKey column_key, bool ascending = true) const;
    void remove_target_row(size_t link_ndx);
    void remove_all_target_rows();

private:
    friend class ConstTableView;
    friend class Query;

    // Sorted set of indices containing unresolved links.
    mutable std::vector<size_t> m_unresolved;

    void get_dependencies(TableVersions&) const override;
    void sync_if_needed() const override;
    bool init_from_parent() const override;
};


// Implementation:

inline void LstBase::swap_repl(Replication* repl, size_t ndx1, size_t ndx2) const
{
    if (ndx2 < ndx1)
        std::swap(ndx1, ndx2);
    repl->list_move(*this, ndx2, ndx1);
    if (ndx1 + 1 != ndx2)
        repl->list_move(*this, ndx1 + 1, ndx2);
}

template <class T>
inline Lst<T>::Lst(const Lst<T>& other)
    : Collection<T, LstBase>(other)
{
}

template <class T>
inline Lst<T>::Lst(const Obj& obj, ColKey col_key)
    : Collection<T, LstBase>(obj, col_key)
{
    if (m_obj) {
        Collection<T, LstBase>::init_from_parent();
    }
}

template <class T>
inline void Lst<T>::create()
{
    m_tree->create();
    m_valid = true;
}

template <class T>
Lst<T>& Lst<T>::operator=(const Lst& other)
{
    Collection<T, LstBase>::operator=(other);
    return *this;
}

template <class T>
Lst<T>& Lst<T>::operator=(const BPlusTree<T>& other)
{
    *m_tree = other;
    return *this;
}

template <class T>
T Lst<T>::set(size_t ndx, T value)
{
    REALM_ASSERT_DEBUG(!update_if_needed());

    if (value_is_null(value) && !m_nullable)
        throw LogicError(LogicError::column_not_nullable);

    // get will check for ndx out of bounds
    T old = get(ndx);
    if (old != value) {
        this->ensure_writeable();
        do_set(ndx, value);
        m_obj.bump_content_version();
    }
    if (Replication* repl = this->m_obj.get_replication()) {
        repl->list_set(*this, ndx, value);
    }
    return old;
}

template <class T>
T Lst<T>::remove(size_t ndx)
{
    REALM_ASSERT_DEBUG(!update_if_needed());
    this->ensure_writeable();
    if (Replication* repl = this->m_obj.get_replication()) {
        repl->list_erase(*this, ndx);
    }
    T old = get(ndx);
    do_remove(ndx);
    CollectionBase::adj_remove(ndx);
    m_obj.bump_content_version();

    return old;
}

template <class T>
void Lst<T>::insert(size_t ndx, T value)
{
    REALM_ASSERT_DEBUG(!update_if_needed());

    if (value_is_null(value) && !m_nullable)
        throw LogicError(LogicError::column_not_nullable);

    ensure_created();
    if (ndx > m_tree->size()) {
        throw std::out_of_range("Index out of range");
    }
    this->ensure_writeable();
    if (Replication* repl = this->m_obj.get_replication()) {
        repl->list_insert(*this, ndx, value);
    }
    do_insert(ndx, value);
    m_obj.bump_content_version();
}

template <class T>
void Lst<T>::set_null(size_t ndx)
{
    set(ndx, BPlusTree<T>::default_value(m_nullable));
}

template <class T>
void Lst<T>::set_any(size_t ndx, Mixed val)
{
    if constexpr (std::is_same_v<T, Mixed>) {
        set(ndx, val);
    }
    else {
        if (val.is_null()) {
            set_null(ndx);
        }
        else {
            set(ndx, val.get<typename util::RemoveOptional<T>::type>());
        }
    }
}

template <class T>
void Lst<T>::insert_null(size_t ndx)
{
    insert(ndx, BPlusTree<T>::default_value(m_nullable));
}

template <class T>
void Lst<T>::insert_any(size_t ndx, Mixed val)
{
    if constexpr (std::is_same_v<T, Mixed>) {
        insert(ndx, val);
    }
    else {
        if (val.is_null()) {
            insert_null(ndx);
        }
        else {
            insert(ndx, val.get<typename util::RemoveOptional<T>::type>());
        }
    }
}

template <class T>
void Lst<T>::resize(size_t new_size)
{
    update_if_needed();
    size_t current_size = m_tree->size();
    while (new_size > current_size) {
        insert_null(current_size++);
    }
    remove(new_size, current_size);
    m_obj.bump_both_versions();
}

template <class T>
void Lst<T>::add(T value)
{
    insert(size(), value);
}

template <class T>
T Lst<T>::remove(const LstIterator<T>& it)
{
    return remove(CollectionBase::adjust(it.m_ndx));
}

template <class T>
void Lst<T>::remove(size_t from, size_t to)
{
    while (from < to) {
        remove(--to);
    }
}

template <class T>
void Lst<T>::move(size_t from, size_t to)
{
    REALM_ASSERT_DEBUG(!update_if_needed());
    if (from != to) {
        this->ensure_writeable();
        if (Replication* repl = this->m_obj.get_replication()) {
            repl->list_move(*this, from, to);
        }
        if (to > from) {
            to++;
        }
        else {
            from++;
        }
        // We use swap here as it handles the special case for StringData where
        // 'to' and 'from' points into the same array. In this case you cannot
        // set an entry with the result of a get from another entry in the same
        // leaf.
        m_tree->insert(to, BPlusTree<T>::default_value(m_nullable));
        m_tree->swap(from, to);
        m_tree->erase(from);

        m_obj.bump_content_version();
    }
}

template <class T>
void Lst<T>::swap(size_t ndx1, size_t ndx2)
{
    REALM_ASSERT_DEBUG(!update_if_needed());
    if (ndx1 != ndx2) {
        if (Replication* repl = this->m_obj.get_replication()) {
            LstBase::swap_repl(repl, ndx1, ndx2);
        }
        m_tree->swap(ndx1, ndx2);
        m_obj.bump_content_version();
    }
}

template <class T>
void Lst<T>::clear()
{
    static_assert(!std::is_same_v<T, ObjKey>);
    ensure_created();
    update_if_needed();
    this->ensure_writeable();
    if (size() > 0) {
        if (Replication* repl = this->m_obj.get_replication()) {
            repl->list_clear(*this);
        }
        m_tree->clear();
        m_obj.bump_content_version();
    }
}

template <class T>
inline bool Lst<T>::update_if_needed()
{
    if (m_obj.update_if_needed()) {
        return init_from_parent();
    }
    return false;
}

template <class T>
inline void Lst<T>::ensure_created()
{
    if (!m_valid && m_obj.is_valid()) {
        create();
    }
}

template <class T>
inline void Lst<T>::do_set(size_t ndx, T value)
{
    m_tree->set(ndx, value);
}

template <class T>
inline void Lst<T>::do_insert(size_t ndx, T value)
{
    m_tree->insert(ndx, value);
}

template <class T>
inline void Lst<T>::do_remove(size_t ndx)
{
    m_tree->erase(ndx);
}

// Translate from userfacing index to internal index.
size_t virtual2real(const std::vector<size_t>& vec, size_t ndx);
// Scan through the list to find unresolved links
void update_unresolved(std::vector<size_t>& vec, const BPlusTree<ObjKey>& tree);


template <typename U>
Lst<U> Obj::get_list(ColKey col_key) const
{
    return Lst<U>(*this, col_key);
}

template <typename U>
LstPtr<U> Obj::get_list_ptr(ColKey col_key) const
{
    return std::make_unique<Lst<U>>(*this, col_key);
}

template <>
inline LstPtr<ObjKey> Obj::get_list_ptr(ColKey col_key) const
{
    return get_linklist_ptr(col_key);
}

inline LnkLst Obj::get_linklist(ColKey col_key) const
{
    return LnkLst(*this, col_key);
}

inline LnkLstPtr Obj::get_linklist_ptr(ColKey col_key) const
{
    return std::make_unique<LnkLst>(*this, col_key);
}

inline LnkLst Obj::get_linklist(StringData col_name) const
{
    return get_linklist(get_column_key(col_name));
}

inline ObjKey LnkLst::get(size_t ndx) const
{
    return Lst<ObjKey>::get(virtual2real(m_unresolved, ndx));
}

inline ObjKey LnkLst::get_key(size_t ndx) const
{
    return get(ndx);
}

inline void LnkLst::remove(size_t ndx)
{
    Lst<ObjKey>::remove(virtual2real(m_unresolved, ndx));
}

inline void LnkLst::remove(size_t from, size_t to)
{
    while (from < to) {
        remove(--to);
    }
}

inline void LnkLst::clear()
{
    Lst<ObjKey>::clear();
    m_unresolved.clear();
}

template <class T>
inline ColumnSumType<T> list_sum(const Collection<T, LstBase>& list, size_t* return_cnt = nullptr)
{
    return bptree_sum(list.get_tree(), return_cnt);
}

template <class T>
inline ColumnMinMaxType<T> list_maximum(const Collection<T, LstBase>& list, size_t* return_ndx = nullptr)
{
    return bptree_maximum(list.get_tree(), return_ndx);
}

template <class T>
inline ColumnMinMaxType<T> list_minimum(const Collection<T, LstBase>& list, size_t* return_ndx = nullptr)
{
    return bptree_minimum(list.get_tree(), return_ndx);
}

template <class T>
inline ColumnAverageType<T> list_average(const Collection<T, LstBase>& list, size_t* return_cnt = nullptr)
{
    return bptree_average(list.get_tree(), return_cnt);
}

template <class T>
Mixed Lst<T>::min(size_t* return_ndx) const
{
    return MinHelper<T>::eval(*m_tree, return_ndx);
}

template <class T>
Mixed Lst<T>::max(size_t* return_ndx) const
{
    return MaxHelper<T>::eval(*m_tree, return_ndx);
}

template <class T>
Mixed Lst<T>::sum(size_t* return_cnt) const
{
    return SumHelper<T>::eval(*m_tree, return_cnt);
}

template <class T>
Mixed Lst<T>::avg(size_t* return_cnt) const
{
    return AverageHelper<T>::eval(*m_tree, return_cnt);
}

} // namespace realm

#endif /* REALM_LIST_HPP */
