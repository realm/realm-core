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

#include <realm/obj.hpp>
#include <realm/bplustree.hpp>
#include <realm/obj_list.hpp>
#include <realm/array_basic.hpp>
#include <realm/array_key.hpp>
#include <realm/array_bool.hpp>
#include <realm/array_string.hpp>
#include <realm/array_binary.hpp>
#include <realm/array_timestamp.hpp>

namespace realm {

class TableView;
class SortDescriptor;
class Group;

// To be used in query for size. Adds nullability to size so that
// it can be put in a NullableVector
struct SizeOfList {
    static constexpr size_t null_value = size_t(-1);

    SizeOfList(size_t s = null_value)
        : sz(s)
    {
    }
    bool is_null()
    {
        return sz == null_value;
    }
    void set_null()
    {
        sz = null_value;
    }
    size_t size() const
    {
        return sz;
    }
    size_t sz = null_value;
};

inline std::ostream& operator<<(std::ostream& ostr, SizeOfList size_of_list)
{
    if (size_of_list.is_null()) {
        ostr << "null";
    }
    else {
        ostr << size_of_list.sz;
    }
    return ostr;
}

class ConstListBase : public ArrayParent {
public:
    virtual ~ConstListBase();
    /*
     * Operations that makes sense without knowing the specific type
     * can be made virtual.
     */
    virtual size_t size() const = 0;
    virtual bool is_null() const = 0;
    ObjKey get_key() const
    {
        return m_const_obj->get_key();
    }
    bool is_attached() const
    {
        return m_const_obj->is_valid();
    }
    const Table* get_table() const
    {
        return m_const_obj->get_table();
    }
    ColKey get_col_key() const
    {
        return m_col_key;
    }

protected:
    template <class>
    friend class ListIterator;

    const ConstObj* m_const_obj = nullptr;
    const ColKey m_col_key;

    mutable std::vector<size_t> m_deleted;

    ConstListBase(ColKey col_key)
        : m_col_key(col_key)
    {
    }
    virtual void init_from_parent() const = 0;

    void set_obj(const ConstObj* obj)
    {
        m_const_obj = obj;
    }
    ref_type get_child_ref(size_t) const noexcept override;
    std::pair<ref_type, size_t> get_to_dot_parent(size_t) const override;

    void update_if_needed() const
    {
        if (m_const_obj->update_if_needed()) {
            init_from_parent();
        }
    }
    // Increase index by one. I we land on and index that is deleted, keep
    // increasing until we get to a valid entry.
    size_t incr(size_t ndx) const
    {
        ndx++;
        if (!m_deleted.empty()) {
            auto it = m_deleted.begin();
            auto end = m_deleted.end();
            while (it != end && *it < ndx) {
                ++it;
            }
            // If entry is deleted, increase further
            while (it != end && *it == ndx) {
                ++it;
                ++ndx;
            }
        }
        return ndx;
    }
    // Convert from virtual to real index
    size_t adjust(size_t ndx) const
    {
        if (!m_deleted.empty()) {
            // Optimized for the case where the iterator is past that last deleted entry
            auto it = m_deleted.rbegin();
            auto end = m_deleted.rend();
            while (it != end && *it >= ndx) {
                if (*it == ndx) {
                    throw std::out_of_range("Element was deleted");
                }
                ++it;
            }
            auto diff = end - it;
            ndx -= diff;
        }
        return ndx;
    }
    void adj_remove(size_t ndx)
    {
        auto it = m_deleted.begin();
        auto end = m_deleted.end();
        while (it != end && *it <= ndx) {
            ++ndx;
            ++it;
        }
        m_deleted.insert(it, ndx);
    }
    void erase_repl(Replication* repl, size_t ndx) const;
    void move_repl(Replication* repl, size_t from, size_t to) const;
    void swap_repl(Replication* repl, size_t ndx1, size_t ndx2) const;
    void clear_repl(Replication* repl) const;
};

/*
 * This class implements a forward iterator over the elements in a List.
 *
 * The iterator is stable against deletions in the list. If you try to
 * dereference an iterator that points to an element, that is deleted, the
 * call will throw.
 *
 * Values are read into a member variable (m_val). This is the only way to
 * implement operator-> and operator* returning a pointer and a reference resp.
 * There is no overhead compared to the alternative where operator* would have
 * to return T by value.
 */
template <class T>
class ListIterator {
public:
    typedef std::forward_iterator_tag iterator_category;
    typedef const T value_type;
    typedef ptrdiff_t difference_type;
    typedef const T* pointer;
    typedef const T& reference;

    ListIterator(const ConstListIf<T>* l, size_t ndx)
        : m_list(l)
        , m_ndx(ndx)
    {
    }
    pointer operator->()
    {
        m_val = m_list->get(m_list->adjust(m_ndx));
        return &m_val;
    }
    reference operator*()
    {
        return *operator->();
    }
    ListIterator& operator++()
    {
        m_ndx = m_list->incr(m_ndx);
        return *this;
    }
    ListIterator operator++(int)
    {
        ListIterator tmp(*this);
        operator++();
        return tmp;
    }

    bool operator!=(const ListIterator& rhs)
    {
        return m_ndx != rhs.m_ndx;
    }

    bool operator==(const ListIterator& rhs)
    {
        return m_ndx == rhs.m_ndx;
    }

private:
    friend class List<T>;
    T m_val;
    const ConstListIf<T>* m_list;
    size_t m_ndx;
};

/// This class defines the interface to ConstList, except for the constructor
/// The ConstList class has the ConstObj member m_obj, which should not be
/// inherited from List<T>.
template <class T>
class ConstListIf : public ConstListBase {
public:
    /**
     * Only member functions not referring to an index in the list will check if
     * the object is up-to-date. The logic is that the user must always check the
     * size before referring to a particular index, and size() will check for update.
     */
    size_t size() const override
    {
        if (!is_attached())
            return 0;

        update_if_needed();

        return is_null() ? 0 : m_tree->size();
    }
    bool is_null() const override
    {
        return !m_valid;
    }
    T get(size_t ndx) const
    {
        if (ndx >= m_tree->size()) {
            throw std::out_of_range("Index out of range");
        }
        return m_tree->get(ndx);
    }
    T operator[](size_t ndx) const
    {
        return get(ndx);
    }
    ListIterator<T> begin() const
    {
        return ListIterator<T>(this, 0);
    }
    ListIterator<T> end() const
    {
        return ListIterator<T>(this, size() + m_deleted.size());
    }

protected:
    mutable std::unique_ptr<BPlusTree<T>> m_tree;
    mutable bool m_valid = false;

    ConstListIf(ColKey col_key, Allocator& alloc)
        : ConstListBase(col_key)
        , m_tree(new BPlusTree<T>(alloc))
    {
        m_tree->set_parent(this, 0); // ndx not used, implicit in m_owner
    }

    ConstListIf(const ConstListIf&) = delete;
    ConstListIf(ConstListIf&& other)
        : ConstListBase(std::move(other))
        , m_tree(std::move(other.m_tree))
        , m_valid(other.m_valid)
    {
        m_tree->set_parent(this, 0);
    }

    void init_from_parent() const override
    {
        m_valid = m_tree->init_from_parent();
    }
};

template <class T>
class ConstList : public ConstListIf<T> {
public:
    ConstList(const ConstObj& owner, ColKey col_key);
    ConstList(ConstList&& other)
        : ConstListIf<T>(std::move(other))
        , m_obj(std::move(other.m_obj))
    {
        this->set_obj(&m_obj);
    }

private:
    ConstObj m_obj;
    void update_child_ref(size_t, ref_type) override
    {
    }
};
/*
 * This class defines a virtual interface to a writable list
 */
class ListBase {
public:
    virtual ~ListBase()
    {
    }
    virtual size_t size() const = 0;
    virtual void insert_null(size_t ndx) = 0;
    virtual void resize(size_t new_size) = 0;
    virtual void remove(size_t from, size_t to) = 0;
    virtual void move(size_t from, size_t to) = 0;
    virtual void swap(size_t ndx1, size_t ndx2) = 0;
    virtual void clear() = 0;
};

template <class T>
class List : public ConstListIf<T>, public ListBase {
public:
    using ConstListIf<T>::m_tree;
    using ConstListIf<T>::get;

    List(const Obj& owner, ColKey col_key);
    List(List&& other)
        : ConstListIf<T>(std::move(other))
        , m_obj(std::move(other.m_obj))
    {
        this->set_obj(&m_obj);
    }

    List& operator=(const BPlusTree<T>& other)
    {
        *m_tree = other;
        return *this;
    }

    void update_child_ref(size_t, ref_type new_ref) override
    {
        m_obj.set_int(ConstListBase::m_col_key, from_ref(new_ref));
    }

    void create()
    {
        m_tree->create();
        ConstListIf<T>::m_valid = true;
    }

    size_t size() const override
    {
        return ConstListIf<T>::size();
    }

    void insert_null(size_t ndx) override
    {
        insert(ndx, BPlusTree<T>::default_value());
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
        insert(m_tree->size(), value);
    }

    T set(size_t ndx, T value)
    {
        // get will check for ndx out of bounds
        T old = get(ndx);
        if (old != value) {
            ensure_writeable();
            do_set(ndx, value);
            m_obj.bump_content_version();
            if (Replication* repl = this->m_const_obj->get_alloc().get_replication()) {
                set_repl(repl, ndx, value);
            }
        }
        return old;
    }

    void insert(size_t ndx, T value)
    {
        if (ndx > m_tree->size()) {
            throw std::out_of_range("Index out of range");
        }
        ensure_writeable();
        if (Replication* repl = this->m_const_obj->get_alloc().get_replication()) {
            insert_repl(repl, ndx, value);
        }
        do_insert(ndx, value);
        m_obj.bump_both_versions();
    }

    T remove(ListIterator<T>& it)
    {
        return remove(ConstListBase::adjust(it.m_ndx));
    }

    T remove(size_t ndx)
    {
        ensure_writeable();
        if (Replication* repl = this->m_const_obj->get_alloc().get_replication()) {
            ConstListBase::erase_repl(repl, ndx);
        }
        T old = get(ndx);
        do_remove(ndx);
        ConstListBase::adj_remove(ndx);
        m_obj.bump_both_versions();

        return old;
    }

    void remove(size_t from, size_t to) override
    {
        while (from < to) {
            remove(--to);
        }
    }

    void move(size_t from, size_t to) override
    {
        if (from != to) {
            ensure_writeable();
            if (Replication* repl = this->m_const_obj->get_alloc().get_replication()) {
                ConstListBase::move_repl(repl, from, to);
            }
            T tmp = get(from);
            int adj = (from < to) ? 1 : -1;
            while (from != to) {
                size_t neighbour = from + adj;
                T val = m_tree->get(neighbour);
                m_tree->set(from, val);
                from = neighbour;
            }
            m_tree->set(to, tmp);
        }
    }

    void swap(size_t ndx1, size_t ndx2) override
    {
        if (ndx1 != ndx2) {
            if (Replication* repl = this->m_const_obj->get_alloc().get_replication()) {
                ConstListBase::swap_repl(repl, ndx1, ndx2);
            }
            T tmp = m_tree->get(ndx1);
            m_tree->set(ndx1, get(ndx2));
            m_tree->set(ndx2, tmp);
        }
    }

    void clear() override
    {
        update_if_needed();
        ensure_writeable();
        if (Replication* repl = this->m_const_obj->get_alloc().get_replication()) {
            ConstListBase::clear_repl(repl);
        }
        m_tree->clear();
        m_obj.bump_both_versions();
    }

protected:
    Obj m_obj;
    bool update_if_needed()
    {
        if (m_obj.update_if_needed()) {
            m_tree->init_from_parent();
            return true;
        }
        return false;
    }
    void ensure_writeable()
    {
        if (!m_obj.is_writeable()) {
            m_obj.ensure_writeable();
            m_tree->init_from_parent();
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

    friend class Transaction;
};

template <>
void List<ObjKey>::do_set(size_t ndx, ObjKey target_key);

template <>
void List<ObjKey>::do_insert(size_t ndx, ObjKey target_key);

template <>
void List<ObjKey>::do_remove(size_t ndx);

template <>
void List<ObjKey>::clear();

class ConstLinkListIf : public ConstListIf<ObjKey> {
public:
    // Getting links
    ConstObj operator[](size_t link_ndx) const
    {
        return get_object(link_ndx);
    }
    ConstObj get_object(size_t link_ndx) const;

protected:
    ConstLinkListIf(ColKey col_key, Allocator& alloc)
        : ConstListIf<ObjKey>(col_key, alloc)
    {
    }
};

class ConstLinkList : public ConstLinkListIf {
public:
    ConstLinkList(const ConstObj& obj, ColKey col_key)
        : ConstLinkListIf(col_key, obj.get_alloc())
        , m_obj(obj)
    {
        this->set_obj(&m_obj);
        this->init_from_parent();
    }
    ConstLinkList(ConstLinkList&& other)
        : ConstLinkListIf(std::move(other))
        , m_obj(std::move(other.m_obj))
    {
        this->set_obj(&m_obj);
    }
    void update_child_ref(size_t, ref_type) override
    {
    }

private:
    friend class Transaction;

    ConstObj m_obj;
};

class LinkList : public List<ObjKey>, public ObjList {
public:
    LinkList(const Obj& owner, ColKey col_key)
        : List<ObjKey>(owner, col_key)
        , ObjList(*this->m_tree, &get_target_table())
    {
    }
    LinkListPtr clone() const
    {
        return std::make_unique<LinkList>(m_obj, m_col_key);
    }
    Table& get_target_table() const
    {
        return *m_obj.get_target_table(m_col_key);
    }
    bool is_in_sync() const override
    {
        return m_obj.is_in_sync();
    }
    size_t size() const override
    {
        return List<ObjKey>::size();
    }

    using ObjList::operator[];

    TableView get_sorted_view(SortDescriptor order) const;
    TableView get_sorted_view(ColKey column_key, bool ascending = true) const;
    void remove_target_row(size_t link_ndx);
    void remove_all_target_rows();

private:
    friend class DB;
    friend class ConstTableView;
    friend class Query;

    TableVersions sync_if_needed() const override;
};

template <typename U>
ConstList<U> ConstObj::get_list(ColKey col_key) const
{
    return ConstList<U>(*this, col_key);
}

template <typename U>
ConstListPtr<U> ConstObj::get_list_ptr(ColKey col_key) const
{
    Obj obj(*this);
    return std::const_pointer_cast<const List<U>>(obj.get_list_ptr<U>(col_key));
}

template <typename U>
List<U> Obj::get_list(ColKey col_key)
{
    return List<U>(*this, col_key);
}

template <typename U>
ListPtr<U> Obj::get_list_ptr(ColKey col_key)
{
    return std::make_unique<List<U>>(*this, col_key);
}

template <>
inline ListPtr<ObjKey> Obj::get_list_ptr(ColKey col_key)
{
    return get_linklist_ptr(col_key);
}

inline ConstLinkList ConstObj::get_linklist(ColKey col_key)
{
    return ConstLinkList(*this, col_key);
}

inline ConstLinkListPtr ConstObj::get_linklist_ptr(ColKey col_key)
{
    Obj obj(*this);
    return obj.get_linklist_ptr(col_key);
}

inline LinkList Obj::get_linklist(ColKey col_key)
{
    return LinkList(*this, col_key);
}

inline LinkListPtr Obj::get_linklist_ptr(ColKey col_key)
{
    return std::make_unique<LinkList>(*this, col_key);
}
}

#endif /* REALM_LIST_HPP */
