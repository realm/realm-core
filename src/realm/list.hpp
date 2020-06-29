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
#include <realm/array_object_id.hpp>
#include <realm/array_decimal128.hpp>
#include <realm/array_mixed.hpp>
#include <realm/array_typed_link.hpp>

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
    virtual void insert_null(size_t ndx) = 0;
    virtual void insert_any(size_t ndx, Mixed val) = 0;
    virtual void resize(size_t new_size) = 0;
    virtual void remove(size_t from, size_t to) = 0;
    virtual void move(size_t from, size_t to) = 0;
    virtual void swap(size_t ndx1, size_t ndx2) = 0;
    virtual void clear() = 0;

protected:
    void erase_repl(Replication* repl, size_t) const;
    void clear_repl(Replication* repl) const;
    void move_repl(Replication* repl, size_t from, size_t to) const;
    void swap_repl(Replication* repl, size_t from, size_t to) const;
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

    void create()
    {
        m_tree->create();
        m_valid = true;
    }

    // Overriding members of CollectionBase:
    Mixed min(size_t* return_ndx = nullptr) const final;
    Mixed max(size_t* return_ndx = nullptr) const final;
    Mixed sum(size_t* return_cnt = nullptr) const final;
    Mixed avg(size_t* return_cnt = nullptr) const final;
    void sort(std::vector<size_t>& indices, bool ascending = true) const final;
    void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const final;

    void set_null(size_t ndx) override
    {
        set(ndx, BPlusTree<T>::default_value(m_nullable));
    }

    void insert_null(size_t ndx) override
    {
        insert(ndx, BPlusTree<T>::default_value(m_nullable));
    }

    void insert_any(size_t ndx, Mixed val) override
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

    void resize(size_t new_size) override
    {
        update_if_needed();
        size_t current_size = m_tree->size();
        while (new_size > current_size) {
            insert_null(current_size++);
        }
        remove(new_size, current_size);
        m_obj.bump_both_versions();
    }

    void add(T value)
    {
        insert(size(), value);
    }

    T set(size_t ndx, T value);

    void insert(size_t ndx, T value);

    T remove(const LstIterator<T>& it)
    {
        return remove(CollectionBase::adjust(it.m_ndx));
    }

    T remove(size_t ndx);

    void remove(size_t from, size_t to) override
    {
        while (from < to) {
            remove(--to);
        }
    }

    void move(size_t from, size_t to) override
    {
        REALM_ASSERT_DEBUG(!update_if_needed());
        if (from != to) {
            this->ensure_writeable();
            if (Replication* repl = this->m_obj.get_replication()) {
                LstBase::move_repl(repl, from, to);
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

    void swap(size_t ndx1, size_t ndx2) override
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

    void clear() override
    {
        ensure_created();
        update_if_needed();
        this->ensure_writeable();
        if (size() > 0) {
            if (Replication* repl = this->m_obj.get_replication()) {
                LstBase::clear_repl(repl);
            }
            m_tree->clear();
            m_obj.bump_content_version();
        }
    }

    using Collection<T, LstBase>::m_col_key;

protected:
    using Collection<T, LstBase>::m_valid;
    using Collection<T, LstBase>::m_nullable;
    using Collection<T, LstBase>::m_obj;
    using Collection<T, LstBase>::init_from_parent;

    bool update_if_needed()
    {
        if (m_obj.update_if_needed()) {
            return init_from_parent();
        }
        return false;
    }
    void ensure_created()
    {
        if (!m_valid && m_obj.is_valid()) {
            create();
        }
    }
    void do_set(size_t ndx, T value)
    {
        m_tree->set(ndx, value);
    }
    void do_insert(size_t ndx, T value)
    {
        m_tree->insert(ndx, value);
    }
    void do_remove(size_t ndx)
    {
        m_tree->erase(ndx);
    }
    void set_repl(Replication* repl, size_t ndx, T value);
    void insert_repl(Replication* repl, size_t ndx, T value);
};

template <class T>
Lst<T>::Lst(const Lst<T>& other)
    : Collection<T, LstBase>(other)
{
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
        set_repl(repl, ndx, value);
    }
    return old;
}

template <class T>
T Lst<T>::remove(size_t ndx)
{
    REALM_ASSERT_DEBUG(!update_if_needed());
    this->ensure_writeable();
    if (Replication* repl = this->m_obj.get_replication()) {
        LstBase::erase_repl(repl, ndx);
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
        insert_repl(repl, ndx, value);
    }
    do_insert(ndx, value);
    m_obj.bump_content_version();
}

template <>
void Lst<ObjKey>::do_set(size_t ndx, ObjKey target_key);

template <>
void Lst<ObjKey>::do_insert(size_t ndx, ObjKey target_key);

template <>
void Lst<ObjKey>::do_remove(size_t ndx);

template <>
void Lst<ObjKey>::clear();

template <>
void Lst<ObjLink>::do_set(size_t ndx, ObjLink target_key);

template <>
void Lst<ObjLink>::do_insert(size_t ndx, ObjLink target_key);

template <>
void Lst<ObjLink>::do_remove(size_t ndx);

template <>
void Lst<Mixed>::do_set(size_t ndx, Mixed target_key);

template <>
void Lst<Mixed>::do_insert(size_t ndx, Mixed target_key);

template <>
void Lst<Mixed>::do_remove(size_t ndx);

// Translate from userfacing index to internal index.
size_t virtual2real(const std::vector<size_t>& vec, size_t ndx);
// Scan through the list to find unresolved links
void update_unresolved(std::vector<size_t>& vec, const BPlusTree<ObjKey>& tree);


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
    ObjKey get(size_t ndx) const
    {
        return Lst<ObjKey>::get(virtual2real(m_unresolved, ndx));
    }
    ObjKey get_key(size_t ndx) const override
    {
        return get(ndx);
    }
    void remove(size_t ndx)
    {
        Lst<ObjKey>::remove(virtual2real(m_unresolved, ndx));
    }
    void remove(size_t from, size_t to) override
    {
        while (from < to) {
            remove(--to);
        }
    }
    void clear() override
    {
        Lst<ObjKey>::clear();
        m_unresolved.clear();
    }
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
} // namespace realm

#endif /* REALM_LIST_HPP */
