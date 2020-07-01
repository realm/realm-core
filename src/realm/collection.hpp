#ifndef REALM_COLLECTION_HPP
#define REALM_COLLECTION_HPP

#include <realm/obj.hpp>
#include <realm/bplustree.hpp>

#include <iosfwd>      // std::ostream
#include <type_traits> // std::void_t

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

template <class T>
inline void check_column_type(ColKey col)
{
    if (col && col.get_type() != ColumnTypeTraits<T>::column_id) {
        throw LogicError(LogicError::list_type_mismatch);
    }
}

template <>
inline void check_column_type<Int>(ColKey col)
{
    if (col && (col.get_type() != col_type_Int || col.get_attrs().test(col_attr_Nullable))) {
        throw LogicError(LogicError::list_type_mismatch);
    }
}

template <>
inline void check_column_type<util::Optional<Int>>(ColKey col)
{
    if (col && (col.get_type() != col_type_Int || !col.get_attrs().test(col_attr_Nullable))) {
        throw LogicError(LogicError::list_type_mismatch);
    }
}

template <>
inline void check_column_type<ObjKey>(ColKey col)
{
    if (col && col.get_type() != col_type_LinkList) {
        throw LogicError(LogicError::list_type_mismatch);
    }
}

template <class T, class = void>
struct MinHelper {
    template <class U>
    static Mixed eval(U&, size_t*)
    {
        return Mixed{};
    }
};

template <class T>
struct MinHelper<T, std::void_t<ColumnMinMaxType<T>>> {
    template <class U>
    static Mixed eval(U& tree, size_t* return_ndx)
    {
        return Mixed(bptree_minimum<T>(tree, return_ndx));
    }
};

template <class T, class Enable = void>
struct MaxHelper {
    template <class U>
    static Mixed eval(U&, size_t*)
    {
        return Mixed{};
    }
};

template <class T>
struct MaxHelper<T, std::void_t<ColumnMinMaxType<T>>> {
    template <class U>
    static Mixed eval(U& tree, size_t* return_ndx)
    {
        return Mixed(bptree_maximum<T>(tree, return_ndx));
    }
};

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
class SumHelper<T, std::void_t<ColumnSumType<T>>> {
public:
    template <class U>
    static Mixed eval(U& tree, size_t* return_cnt)
    {
        return Mixed(bptree_sum<T>(tree, return_cnt));
    }
};

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
struct AverageHelper<T, std::void_t<ColumnSumType<T>>> {
    template <class U>
    static Mixed eval(U& tree, size_t* return_cnt)
    {
        return Mixed(bptree_average<T>(tree, return_cnt));
    }
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

class CollectionBase : public ArrayParent {
public:
    virtual ~CollectionBase();
    CollectionBase(const CollectionBase&) = default;

    /*
     * Operations that makes sense without knowing the specific type
     * can be made virtual.
     */
    virtual size_t size() const = 0;
    virtual bool is_null(size_t ndx) const = 0;
    virtual Mixed get_any(size_t ndx) const = 0;

    virtual Mixed min(size_t* return_ndx = nullptr) const = 0;
    virtual Mixed max(size_t* return_ndx = nullptr) const = 0;
    virtual Mixed sum(size_t* return_cnt = nullptr) const = 0;
    virtual Mixed avg(size_t* return_cnt = nullptr) const = 0;

    // Modifies a vector of indices so that they refer to values sorted according
    // to the specified sort order
    virtual void sort(std::vector<size_t>& indices, bool ascending = true) const = 0;
    // Modifies a vector of indices so that they refer to distinct values.
    // If 'sort_order' is supplied, the indices will refer to values in sort order,
    // otherwise the indices will be in original order.
    virtual void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const = 0;

    bool is_empty() const;
    ObjKey get_key() const;
    bool is_attached() const;
    bool has_changed() const;
    void update_child_ref(size_t, ref_type new_ref) override;
    ConstTableRef get_table() const;
    ColKey get_col_key() const;
    bool operator==(const CollectionBase& other) const;

protected:
    friend class Transaction;

    Obj m_obj;
    ColKey m_col_key;
    bool m_nullable = false;

    mutable std::vector<size_t> m_deleted;
    mutable uint_fast64_t m_content_version = 0;
    mutable uint_fast64_t m_last_content_version = 0;

    CollectionBase() = default;
    CollectionBase(const Obj& owner, ColKey col_key);
    CollectionBase& operator=(const CollectionBase& other);

    virtual bool init_from_parent() const = 0;

    ref_type get_child_ref(size_t) const noexcept override;

    void update_if_needed() const;
    void update_content_version() const;
    // Increase index by one. I we land on and index that is deleted, keep
    // increasing until we get to a valid entry.
    size_t incr(size_t ndx) const;
    /// Decrease index by one. If we land on an index that is deleted, keep decreasing until we get to a valid entry.
    size_t decr(size_t ndx) const;
    /// Convert from virtual to real index
    size_t adjust(size_t ndx) const;
    void adj_remove(size_t ndx);
};

/// This class defines the interface to ConstList, except for the constructor
/// The ConstList class has the Obj member m_obj, which should not be
/// inherited from Lst<T>.
template <class T, class Interface = CollectionBase>
class Collection : public Interface {
public:
    struct iterator;

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
        if (!m_valid)
            return 0;

        return m_tree->size();
    }
    bool is_null(size_t ndx) const final
    {
        return m_nullable && get(ndx) == BPlusTree<T>::default_value(true);
    }
    Mixed get_any(size_t ndx) const final
    {
        return Mixed(get(ndx));
    }

    // Ensure that `Interface` implements `CollectionBase`.
    using Interface::avg;
    using Interface::distinct;
    using Interface::is_attached;
    using Interface::max;
    using Interface::min;
    using Interface::size;
    using Interface::sort;
    using Interface::sum;
    using Interface::update_content_version;
    using Interface::update_if_needed;

    T get(size_t ndx) const
    {
        if (ndx >= Collection::size()) {
            throw std::out_of_range("Index out of range");
        }
        return m_tree->get(ndx);
    }
    T operator[](size_t ndx) const
    {
        return get(ndx);
    }
    iterator begin() const
    {
        return iterator(this, 0);
    }
    iterator end() const
    {
        return iterator(this, Collection::size() + m_deleted.size());
    }
    size_t find_first(T value) const
    {
        if (!m_valid && !init_from_parent())
            return not_found;
        return m_tree->find_first(value);
    }
    template <typename Func>
    void find_all(T value, Func&& func) const
    {
        if (m_valid && init_from_parent())
            m_tree->find_all(value, std::forward<Func>(func));
    }
    const BPlusTree<T>& get_tree() const
    {
        return *m_tree;
    }

protected:
    mutable std::unique_ptr<BPlusTree<T>> m_tree;
    mutable bool m_valid = false;

    using Interface::m_col_key;
    using Interface::m_deleted;
    using Interface::m_nullable;
    using Interface::m_obj;

    Collection() = default;

    Collection(const Obj& obj, ColKey col_key)
        : Interface(obj, col_key)
        , m_tree(new BPlusTree<T>(obj.get_alloc()))
    {
        check_column_type<T>(m_col_key);

        m_tree->set_parent(this, 0); // ndx not used, implicit in m_owner
    }

    Collection(const Collection& other)
        : Interface(static_cast<const Interface&>(other))
        , m_valid(other.m_valid)
    {
        if (other.m_tree) {
            Allocator& alloc = other.m_tree->get_alloc();
            m_tree = std::make_unique<BPlusTree<T>>(alloc);
            m_tree->set_parent(this, 0);
            if (m_valid)
                m_tree->init_from_ref(other.m_tree->get_ref());
        }
    }

    Collection& operator=(const Collection& other)
    {
        if (this != &other) {
            CollectionBase::operator=(other);
            m_valid = other.m_valid;
            m_deleted.clear();
            m_tree = nullptr;

            if (other.m_tree) {
                Allocator& alloc = other.m_tree->get_alloc();
                m_tree = std::make_unique<BPlusTree<T>>(alloc);
                m_tree->set_parent(this, 0);
                if (m_valid)
                    m_tree->init_from_ref(other.m_tree->get_ref());
            }
        }
        return *this;
    }

    bool init_from_parent() const override
    {
        m_valid = m_tree->init_from_parent();
        update_content_version();
        return m_valid;
    }

    void ensure_writeable()
    {
        if (m_obj.ensure_writeable()) {
            init_from_parent();
        }
    }
};

/*
 * This class implements a forward iterator over the elements in a Lst.
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
template <class T, class Interface>
struct Collection<T, Interface>::iterator {
    typedef std::bidirectional_iterator_tag iterator_category;
    typedef const T value_type;
    typedef ptrdiff_t difference_type;
    typedef const T* pointer;
    typedef const T& reference;

    iterator(const Collection<T, Interface>* l, size_t ndx)
        : m_list(l)
        , m_ndx(ndx)
    {
    }
    pointer operator->() const
    {
        m_val = m_list->get(m_list->adjust(m_ndx));
        return &m_val;
    }
    reference operator*() const
    {
        return *operator->();
    }
    iterator& operator++()
    {
        m_ndx = m_list->incr(m_ndx);
        return *this;
    }
    iterator operator++(int)
    {
        iterator tmp(*this);
        operator++();
        return tmp;
    }
    iterator& operator--()
    {
        m_ndx = m_list->decr(m_ndx);
        return *this;
    }
    iterator operator--(int)
    {
        iterator tmp(*this);
        operator--();
        return tmp;
    }

    bool operator!=(const iterator& rhs) const
    {
        return m_ndx != rhs.m_ndx;
    }

    bool operator==(const iterator& rhs) const
    {
        return m_ndx == rhs.m_ndx;
    }

private:
    friend class Lst<T>;
    friend class Collection<T, Interface>;

    mutable T m_val;
    const Collection<T, Interface>* m_list;
    size_t m_ndx;
};


// Implementation:

inline CollectionBase& CollectionBase::operator=(const CollectionBase& other)
{
    m_obj = other.m_obj;
    m_col_key = other.m_col_key;
    m_nullable = other.m_nullable;
    return *this;
}

inline bool CollectionBase::is_empty() const
{
    return size() == 0;
}

inline ObjKey CollectionBase::get_key() const
{
    return m_obj.get_key();
}

inline bool CollectionBase::is_attached() const
{
    return m_obj.is_valid();
}

inline bool CollectionBase::has_changed() const
{
    update_if_needed();
    if (m_last_content_version != m_content_version) {
        m_last_content_version = m_content_version;
        return true;
    }
    return false;
}

inline void CollectionBase::update_child_ref(size_t, ref_type new_ref)
{
    m_obj.set_int(CollectionBase::m_col_key, from_ref(new_ref));
}

inline ConstTableRef CollectionBase::get_table() const
{
    return m_obj.get_table();
}

inline ColKey CollectionBase::get_col_key() const
{
    return m_col_key;
}

inline bool CollectionBase::operator==(const CollectionBase& other) const
{
    return get_key() == other.get_key() && get_col_key() == other.get_col_key();
}

inline void CollectionBase::update_if_needed() const
{
    auto content_version = m_obj.get_alloc().get_content_version();
    if (m_obj.update_if_needed() || content_version != m_content_version) {
        init_from_parent();
    }
}

inline void CollectionBase::update_content_version() const
{
    m_content_version = m_obj.get_alloc().get_content_version();
}

inline size_t CollectionBase::incr(size_t ndx) const
{
    // Increase index by one. If we land on and index that is deleted, keep
    // increasing until we get to a valid entry.
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

inline size_t CollectionBase::decr(size_t ndx) const
{
    REALM_ASSERT(ndx != 0);
    ndx--;
    if (!m_deleted.empty()) {
        auto it = m_deleted.rbegin();
        auto rend = m_deleted.rend();
        while (it != rend && *it > ndx) {
            ++it;
        }
        // If entry is deleted, decrease further
        while (it != rend && *it == ndx) {
            ++it;
            --ndx;
        }
    }
    return ndx;
}

inline size_t CollectionBase::adjust(size_t ndx) const
{
    // Convert from virtual to real index
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

inline void CollectionBase::adj_remove(size_t ndx)
{
    auto it = m_deleted.begin();
    auto end = m_deleted.end();
    while (it != end && *it <= ndx) {
        ++ndx;
        ++it;
    }
    m_deleted.insert(it, ndx);
}

} // namespace realm

#endif // REALM_COLLECTION_HPP
