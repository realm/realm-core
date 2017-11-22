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
#include <realm/array_key.hpp>

namespace realm {

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
    Key get_key() const
    {
        return m_const_obj->get_key();
    }
    bool is_valid() const
    {
        return m_const_obj->is_valid();
    }
    const Table* get_table() const
    {
        return m_const_obj->get_table();
    }
    size_t get_col_ndx() const
    {
        return m_col_ndx;
    }

protected:
    template <class>
    friend class ListIterator;

    const ConstObj* m_const_obj = nullptr;
    const size_t m_col_ndx;

    mutable std::vector<size_t> m_deleted;

    ConstListBase(size_t col_ndx)
        : m_col_ndx(col_ndx)
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
    using LeafType = typename ColumnTypeTraits<T>::cluster_leaf_type;

    /**
     * Only member functions not referring to an index in the list will check if
     * the object is up-to-date. The logic is that the user must always check the
     * size before referring to a particular index, and size() will check for update.
     */
    size_t size() const override
    {
        update_if_needed();
        return m_valid ? m_leaf->size() : 0;
    }
    bool is_null() const override
    {
        return !m_valid;
    }
    T get(size_t ndx) const
    {
        if (ndx >= m_leaf->size()) {
            throw std::out_of_range("Index out of range");
        }
        return m_leaf->get(ndx);
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
    mutable std::unique_ptr<LeafType> m_leaf;
    mutable bool m_valid = false;

    ConstListIf(size_t col_ndx, Allocator& alloc);
    ConstListIf(const ConstListIf&) = delete;
    ConstListIf(ConstListIf&& other)
        : ConstListBase(std::move(other))
        , m_leaf(std::move(other.m_leaf))
        , m_valid(other.m_valid)
    {
        m_leaf->set_parent(this, 0);
    }

    void init_from_parent() const override
    {
        ref_type ref = get_child_ref(0);
        if (ref && (!m_valid || ref != m_leaf->get_ref())) {
            m_leaf->init_from_ref(ref);
            m_valid = true;
        }
    }
};

template <class T>
class ConstList : public ConstListIf<T> {
public:
    ConstList(const ConstObj& owner, size_t col_ndx);
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
    virtual void resize(size_t new_size) = 0;
    virtual void remove(size_t from, size_t to) = 0;
    virtual void move(size_t from, size_t to) = 0;
    virtual void swap(size_t ndx1, size_t ndx2) = 0;
    virtual void clear() = 0;
};

template <class T>
class List : public ConstListIf<T>, public ListBase {
public:
    using ConstListIf<T>::m_leaf;
    using ConstListIf<T>::get;

    List(const Obj& owner, size_t col_ndx);
    List(List&& other)
        : ConstListIf<T>(std::move(other))
        , m_obj(std::move(other.m_obj))
    {
        this->set_obj(&m_obj);
    }

    void update_child_ref(size_t, ref_type new_ref) override
    {
        m_obj.set(ConstListBase::m_col_ndx, from_ref(new_ref));
    }

    void create()
    {
        m_leaf->create();
        ConstListIf<T>::m_valid = true;
    }
    void resize(size_t new_size) override
    {
        update_if_needed();
        size_t current_size = m_leaf->size();
        while (new_size > current_size) {
            m_leaf->add(ConstList<T>::LeafType::default_value(false));
            current_size++;
        }
        if (current_size > new_size) {
            m_leaf->truncate_and_destroy_children(new_size);
        }
    }
    void add(T value)
    {
        update_if_needed();
        m_leaf->insert(m_leaf->size(), value);
    }
    T set(size_t ndx, T value)
    {
        // get will check for ndx out of bounds
        T old = get(ndx);
        if (old != value) {
            ensure_writeable();
            m_leaf->set(ndx, value);
            m_obj.bump_version();
        }
        return old;
    }
    void insert(size_t ndx, T value)
    {
        if (ndx > m_leaf->size()) {
            throw std::out_of_range("Index out of range");
        }
        ensure_writeable();
        m_leaf->insert(ndx, value);
        m_obj.bump_version();
    }
    T remove(ListIterator<T>& it)
    {
        return remove(ConstListBase::adjust(it.m_ndx));
    }
    T remove(size_t ndx)
    {
        ensure_writeable();
        T ret = m_leaf->get(ndx);
        m_leaf->erase(ndx);
        ConstListBase::adj_remove(ndx);
        m_obj.bump_version();

        return ret;
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
            T tmp = get(from);
            int adj = (from < to) ? 1 : -1;
            while (from != to) {
                size_t neighbour = from + adj;
                set(from, get(neighbour));
                from = neighbour;
            }
            set(to, tmp);
        }
    }
    void swap(size_t ndx1, size_t ndx2) override
    {
        if (ndx1 != ndx2) {
            T tmp = get(ndx1);
            set(ndx1, get(ndx2));
            set(ndx2, tmp);
        }
    }
    void clear() override
    {
        update_if_needed();
        ensure_writeable();
        m_leaf->truncate_and_destroy_children(0);
        m_obj.bump_version();
    }

protected:
    Obj m_obj;
    void update_if_needed()
    {
        if (m_obj.update_if_needed()) {
            m_leaf->init_from_parent();
        }
    }
    void ensure_writeable()
    {
        if (!m_obj.is_writeable()) {
            m_obj.ensure_writeable();
            m_leaf->init_from_parent();
        }
    }
};

template <>
void List<Key>::add(Key target_key);

template <>
Key List<Key>::set(size_t ndx, Key target_key);

template <>
void List<Key>::insert(size_t ndx, Key target_key);

template <>
Key List<Key>::remove(size_t ndx);

template <>
void List<Key>::clear();

class ConstLinkListIf : public ConstListIf<Key> {
public:
    // Getting links
    ConstObj operator[](size_t link_ndx) const
    {
        return get(link_ndx);
    }
    ConstObj get(size_t link_ndx) const;

protected:
    ConstLinkListIf(size_t col_ndx, Allocator& alloc)
        : ConstListIf<Key>(col_ndx, alloc)
    {
    }
};

class ConstLinkList : public ConstLinkListIf {
public:
    ConstLinkList(const ConstObj& obj, size_t col_ndx)
        : ConstLinkListIf(col_ndx, obj.get_alloc())
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
    ConstObj m_obj;
};

class LinkList : public List<Key> {
public:
    LinkList(Obj& owner, size_t col_ndx)
        : List<Key>(owner, col_ndx)
    {
    }
    // Getting links
    Obj operator[](size_t link_ndx)
    {
        return get(link_ndx);
    }
    Obj get(size_t link_ndx);
};

template <typename U>
ConstList<U> ConstObj::get_list(size_t col_ndx) const
{
    return ConstList<U>(*this, col_ndx);
}

template <typename U>
ConstListPtr<U> ConstObj::get_list_ptr(size_t col_ndx) const
{
    return std::make_unique<ConstList<U>>(*this, col_ndx);
}

template <typename U>
List<U> Obj::get_list(size_t col_ndx)
{
    return List<U>(*this, col_ndx);
}

template <typename U>
ListPtr<U> Obj::get_list_ptr(size_t col_ndx)
{
    return std::make_unique<List<U>>(*this, col_ndx);
}

inline ConstLinkList ConstObj::get_linklist(size_t col_ndx)
{
    return ConstLinkList(*this, col_ndx);
}

inline ConstLinkListPtr ConstObj::get_linklist_ptr(size_t col_ndx)
{
    return std::make_unique<ConstLinkList>(*this, col_ndx);
}

inline LinkList Obj::get_linklist(size_t col_ndx)
{
    return LinkList(*this, col_ndx);
}

inline LinkListPtr Obj::get_linklist_ptr(size_t col_ndx)
{
    return std::make_unique<LinkList>(*this, col_ndx);
}
}

#endif /* REALM_LIST_HPP */
