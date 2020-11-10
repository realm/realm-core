#ifndef REALM_COLLECTION_HPP
#define REALM_COLLECTION_HPP

#include <realm/obj.hpp>
#include <realm/bplustree.hpp>
#include <realm/obj_list.hpp>
#include <realm/table.hpp>

#include <iosfwd>      // std::ostream
#include <type_traits> // std::void_t

namespace realm {

template <class L>
struct CollectionIterator;

class CollectionBase {
public:
    virtual ~CollectionBase() {}
    CollectionBase(const CollectionBase&) = default;

    /*
     * Operations that makes sense without knowing the specific type
     * can be made virtual.
     */
    virtual size_t size() const = 0;
    virtual bool is_null(size_t ndx) const = 0;
    virtual Mixed get_any(size_t ndx) const = 0;
    virtual void clear() = 0;

    virtual Mixed min(size_t* return_ndx = nullptr) const = 0;
    virtual Mixed max(size_t* return_ndx = nullptr) const = 0;
    virtual Mixed sum(size_t* return_cnt = nullptr) const = 0;
    virtual Mixed avg(size_t* return_cnt = nullptr) const = 0;
    virtual std::unique_ptr<CollectionBase> clone_collection() const = 0;

    virtual TableRef get_target_table() const = 0;

    // Modifies a vector of indices so that they refer to values sorted according
    // to the specified sort order
    virtual void sort(std::vector<size_t>& indices, bool ascending = true) const = 0;
    // Modifies a vector of indices so that they refer to distinct values.
    // If 'sort_order' is supplied, the indices will refer to values in sort order,
    // otherwise the indices will be in original order.
    virtual void distinct(std::vector<size_t>& indices, util::Optional<bool> sort_order = util::none) const = 0;

    bool is_empty() const
    {
        return size() == 0;
    }

    virtual const Obj& get_obj() const noexcept = 0;
    virtual ObjKey get_key() const = 0;
    virtual bool is_attached() const = 0;
    virtual bool has_changed() const = 0;
    virtual ConstTableRef get_table() const = 0;
    virtual ColKey get_col_key() const = 0;

protected:
    friend class Transaction;
    CollectionBase() = default;

    virtual bool init_from_parent() const = 0;
    virtual bool update_if_needed() const = 0;
};

template <class T>
class CollectionOf {
public:
    using value_type = T;

    virtual ~CollectionOf() {}
    virtual size_t size() const = 0;
    virtual T get(size_t ndx) const = 0;
    virtual bool is_null(size_t ndx) const = 0;
    virtual size_t find_first(const T&) const = 0;
};


template <class T>
inline void check_column_type(ColKey col)
{
    if (col && col.get_type() != ColumnTypeTraits<T>::column_id) {
        throw LogicError(LogicError::collection_type_mismatch);
    }
}

template <>
inline void check_column_type<Int>(ColKey col)
{
    if (col && (col.get_type() != col_type_Int || col.get_attrs().test(col_attr_Nullable))) {
        throw LogicError(LogicError::collection_type_mismatch);
    }
}

template <>
inline void check_column_type<util::Optional<Int>>(ColKey col)
{
    if (col && (col.get_type() != col_type_Int || !col.get_attrs().test(col_attr_Nullable))) {
        throw LogicError(LogicError::collection_type_mismatch);
    }
}

template <>
inline void check_column_type<ObjKey>(ColKey col)
{
    if (col) {
        bool is_link_list = (col.get_type() == col_type_LinkList);
        bool is_link_set = (col.is_set() && col.get_type() == col_type_Link);
        if (!(is_link_list || is_link_set))
            throw LogicError(LogicError::collection_type_mismatch);
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

/// Convenience base class for collections, which implements most of the
/// relevant interfaces for a collection that is bound to an object accessor and
/// representable as a BPlusTree<T>.
template <class Interface>
class CollectionBaseImpl : public Interface, private ArrayParent {
public:
    using value_type = typename Interface::value_type;
    using iterator = CollectionIterator<CollectionBaseImpl<Interface>>;

    static_assert(std::is_base_of_v<CollectionBase, Interface>);
    static_assert(std::is_base_of_v<CollectionOf<value_type>, Interface>);

    // Overriding members of CollectionBase:

    size_t size() const final
    {
        if (!is_attached())
            return 0;
        update_if_needed();
        if (!m_valid) {
            return 0;
        }
        return m_tree->size();
    }

    bool is_null(size_t ndx) const final
    {
        return m_nullable && value_is_null(get(ndx));
    }

    Mixed get_any(size_t ndx) const final
    {
        return get(ndx);
    }

    Mixed min(size_t* return_ndx = nullptr) const override
    {
        return MinHelper<value_type>::eval(*m_tree, return_ndx);
    }

    Mixed max(size_t* return_ndx = nullptr) const override
    {
        return MaxHelper<value_type>::eval(*m_tree, return_ndx);
    }

    Mixed sum(size_t* return_cnt = nullptr) const override
    {
        return SumHelper<value_type>::eval(*m_tree, return_cnt);
    }

    Mixed avg(size_t* return_cnt = nullptr) const override
    {
        return AverageHelper<value_type>::eval(*m_tree, return_cnt);
    }

    ColKey get_col_key() const noexcept final
    {
        return m_col_key;
    }

    TableRef get_target_table() const final
    {
        return m_obj.get_target_table(m_col_key);
    }

    const Obj& get_obj() const noexcept final
    {
        return m_obj;
    }

    ObjKey get_key() const noexcept final
    {
        return m_obj.get_key();
    }

    bool is_attached() const noexcept final
    {
        return m_obj.is_valid();
    }

    bool has_changed() const noexcept final
    {
        update_if_needed();
        if (m_last_content_version != m_content_version) {
            m_last_content_version = m_content_version;
            return true;
        }
        return false;
    }

    ConstTableRef get_table() const final
    {
        return m_obj.get_table();
    }

    // Overriding members of CollectionOf<T>:

    value_type get(size_t ndx) const final
    {
        const auto current_size = size();
        if (ndx >= current_size) {
            throw std::out_of_range("Index out of range");
        }
        return m_tree->get(ndx);
    }

    size_t find_first(const value_type& value) const final
    {
        if (!m_valid && !init_from_parent())
            return not_found;
        update_if_needed();
        return m_tree->find_first(value);
    }

    const BPlusTree<value_type>& get_tree() const
    {
        return *m_tree;
    }

    iterator begin() const
    {
        return iterator{this, 0};
    }

    iterator end() const
    {
        return iterator{this, size()};
    }

protected:
    Obj m_obj;
    ColKey m_col_key;
    bool m_nullable = false;

    mutable uint_fast64_t m_content_version = 0;
    mutable uint_fast64_t m_last_content_version = 0;
    mutable std::unique_ptr<BPlusTree<value_type>> m_tree;
    mutable bool m_valid = false;

    CollectionBaseImpl() = default;

    CollectionBaseImpl(const Obj& obj, ColKey col_key)
        : m_obj(obj)
        , m_col_key(col_key)
        , m_nullable(col_key.is_nullable())
        , m_tree(new BPlusTree<value_type>(obj.get_alloc()))
    {
        if (!col_key.is_collection()) {
            throw LogicError(LogicError::collection_type_mismatch);
        }

        check_column_type<value_type>(m_col_key);

        m_tree->set_parent(this, 0); // ndx not used, implicit in m_owner
    }

    CollectionBaseImpl(const CollectionBaseImpl& other)
        : Interface(static_cast<const Interface&>(other))
        , m_obj(other.m_obj)
        , m_col_key(other.m_col_key)
        , m_nullable(other.m_nullable)
        , m_content_version(other.m_content_version)
        , m_last_content_version(other.m_last_content_version)
        , m_valid(other.m_valid)
    {
        if (other.m_tree) {
            Allocator& alloc = other.m_tree->get_alloc();
            m_tree = std::make_unique<BPlusTree<value_type>>(alloc);
            m_tree->set_parent(this, 0);
            if (m_valid)
                m_tree->init_from_ref(other.m_tree->get_ref());
        }
    }

    CollectionBaseImpl& operator=(const CollectionBaseImpl& other)
    {
        static_cast<Interface&>(*this) = static_cast<const Interface&>(other);

        if (this != &other) {
            m_obj = other.m_obj;
            m_col_key = other.m_col_key;
            m_nullable = other.m_nullable;
            m_content_version = other.m_content_version;
            m_last_content_version = other.m_last_content_version;
            m_valid = other.m_valid;
            m_tree = nullptr;

            if (other.m_tree) {
                Allocator& alloc = other.m_tree->get_alloc();
                m_tree = std::make_unique<BPlusTree<value_type>>(alloc);
                m_tree->set_parent(this, 0);
                if (m_valid)
                    m_tree->init_from_ref(other.m_tree->get_ref());
            }
        }

        return *this;
    }

    bool operator==(const CollectionBaseImpl& other) const noexcept
    {
        return get_key() == other.get_key() && get_col_key() == other.get_col_key();
    }

    bool operator!=(const CollectionBaseImpl& other) const noexcept
    {
        return !(*this == other);
    }

    template <typename Func>
    void find_all(value_type value, Func&& func) const
    {
        if (m_valid && init_from_parent())
            m_tree->find_all(value, std::forward<Func>(func));
    }

    // Overriding members of CollectionBase:
    bool init_from_parent() const final
    {
        m_valid = m_tree->init_from_parent();
        update_content_version();
        return m_valid;
    }

    bool update_if_needed() const override
    {
        auto content_version = m_obj.get_alloc().get_content_version();
        if (m_obj.update_if_needed() || content_version != m_content_version) {
            init_from_parent();
            return true;
        }
        return false;
    }

    void update_content_version() const
    {
        m_content_version = m_obj.get_alloc().get_content_version();
    }

    void ensure_writeable()
    {
        if (m_obj.ensure_writeable()) {
            init_from_parent();
        }
    }

private:
    // Overriding ArrayParent interface:
    ref_type get_child_ref(size_t child_ndx) const noexcept final
    {
        static_cast<void>(child_ndx);
        try {
            return to_ref(m_obj._get<int64_t>(m_col_key.get_index()));
        }
        catch (const KeyNotFound&) {
            return ref_type(0);
        }
    }

    void update_child_ref(size_t child_ndx, ref_type new_ref) final
    {
        REALM_ASSERT(child_ndx == 0);
        m_obj.set_int(m_col_key, from_ref(new_ref));
    }
};

namespace _impl {
// Translate from userfacing index to internal index.
size_t virtual2real(const std::vector<size_t>& vec, size_t ndx) noexcept;
size_t real2virtual(const std::vector<size_t>& vec, size_t ndx) noexcept;
// Scan through the list to find unresolved links
void update_unresolved(std::vector<size_t>& vec, const BPlusTree<ObjKey>& tree);
// Clear the context flag on the tree if there are no more unresolved links.
void check_for_last_unresolved(BPlusTree<ObjKey>& tree);
} // namespace _impl


/// Base class for collections of objects, where unresolved links (tombstones)
/// can occur.
template <class Interface>
class ObjCollectionBase : public Interface, public ObjList {
public:
    static_assert(std::is_base_of_v<CollectionOf<ObjKey>, Interface>);

    using Interface::get;
    using Interface::get_table;
    using Interface::is_attached;
    using Interface::size;
    using Interface::update_if_needed;

    // Overriding methods in ObjList:

    Obj get_object(size_t ndx) const final
    {
        ObjKey key = this->get(ndx);
        return get_target_table()->get_object(key);
    }

    ObjKey get_key(size_t ndx) const final
    {
        return get(ndx);
    }

    void get_dependencies(TableVersions& versions) const final
    {
        if (is_attached()) {
            auto table = this->get_table();
            versions.emplace_back(table->get_key(), table->get_content_version());
        }
    }

    void sync_if_needed() const final
    {
        if (is_attached()) {
            update_if_needed();
        }
    }

    bool is_in_sync() const final
    {
        return true;
    }

    Obj operator[](size_t ndx) const
    {
        return get_object(ndx);
    }

    bool has_unresolved() const noexcept
    {
        return m_unresolved.size() != 0;
    }

protected:
    ObjCollectionBase() = default;
    ObjCollectionBase(const ObjCollectionBase&) = default;
    ObjCollectionBase(ObjCollectionBase&&) = default;
    ObjCollectionBase& operator=(const ObjCollectionBase&) = default;
    ObjCollectionBase& operator=(ObjCollectionBase&&) = default;

    size_t virtual2real(size_t ndx) const noexcept
    {
        return _impl::virtual2real(m_unresolved, ndx);
    }

    size_t real2virtual(size_t ndx) const noexcept
    {
        return _impl::real2virtual(m_unresolved, ndx);
    }

    void update_unresolved(BPlusTree<ObjKey>& tree) const
    {
        _impl::update_unresolved(m_unresolved, tree);
    }

    void check_for_last_unresolved(BPlusTree<ObjKey>& tree)
    {
        _impl::check_for_last_unresolved(tree);
    }

    void clear_unresolved()
    {
        m_unresolved.clear();
    }

    size_t num_unresolved() const noexcept
    {
        return m_unresolved.size();
    }

private:
    // Sorted set of indices containing unresolved links.
    mutable std::vector<size_t> m_unresolved;
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
template <class L>
struct CollectionIterator {
    using iterator_category = std::random_access_iterator_tag;
    using value_type = typename L::value_type;
    using difference_type = ptrdiff_t;
    using pointer = const value_type*;
    using reference = const value_type&;

    static_assert(std::is_base_of_v<CollectionOf<value_type>, L>);

    CollectionIterator(const L* l, size_t ndx) noexcept
        : m_list(l)
        , m_ndx(ndx)
    {
    }

    pointer operator->() const noexcept
    {
        m_val = m_list->get(m_ndx);
        return &m_val;
    }

    reference operator*() const noexcept
    {
        return *operator->();
    }

    CollectionIterator& operator++()
    {
        ++m_ndx;
        return *this;
    }

    CollectionIterator operator++(int)
    {
        auto tmp = *this;
        operator++();
        return tmp;
    }

    CollectionIterator& operator--()
    {
        --m_ndx;
        return *this;
    }

    CollectionIterator operator--(int)
    {
        auto tmp = *this;
        operator--();
        return tmp;
    }

    CollectionIterator& operator+=(ptrdiff_t n) noexcept
    {
        m_ndx += n;
        return *this;
    }

    CollectionIterator& operator-=(ptrdiff_t n) noexcept
    {
        m_ndx -= n;
        return *this;
    }

    friend ptrdiff_t operator-(CollectionIterator& lhs, CollectionIterator& rhs) noexcept
    {
        return ptrdiff_t(lhs.m_ndx) - ptrdiff_t(rhs.m_ndx);
    }

    friend CollectionIterator operator+(CollectionIterator lhs, ptrdiff_t rhs) noexcept
    {
        lhs.m_ndx += rhs;
        return lhs;
    }

    friend CollectionIterator operator+(ptrdiff_t lhs, CollectionIterator rhs) noexcept
    {
        return rhs + lhs;
    }

    bool operator!=(const CollectionIterator& rhs) const noexcept
    {
        REALM_ASSERT_DEBUG(m_list == rhs.m_list);
        return m_ndx != rhs.m_ndx;
    }

    bool operator==(const CollectionIterator& rhs) const noexcept
    {
        REALM_ASSERT_DEBUG(m_list == rhs.m_list);
        return m_ndx == rhs.m_ndx;
    }

    size_t index() const noexcept
    {
        return m_ndx;
    }

private:
    mutable value_type m_val;
    const L* m_list;
    size_t m_ndx = size_t(-1);
};

} // namespace realm

#endif // REALM_COLLECTION_HPP
