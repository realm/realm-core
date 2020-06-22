#ifndef REALM_COLLECTION_HPP
#define REALM_COLLECTION_HPP

#include <realm/obj.hpp>
#include <iosfwd>

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

class CollectionBase : public ArrayParent {
public:
    virtual ~CollectionBase();
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
    template <class>
    friend class LstIterator;
    friend class Transaction;


    Obj m_obj;
    ColKey m_col_key;
    bool m_nullable = false;

    mutable std::vector<size_t> m_deleted;
    mutable uint_fast64_t m_content_version = 0;
    mutable uint_fast64_t m_last_content_version = 0;

    CollectionBase() = default;
    CollectionBase(bool)
    {
        REALM_ASSERT(false);
    }
    CollectionBase(const Obj& owner, ColKey col_key);
    CollectionBase& operator=(const CollectionBase& other);

    virtual bool init_from_parent() const = 0;

    ref_type get_child_ref(size_t) const noexcept override;

    void update_if_needed() const;
    void update_content_version() const;
    // Increase index by one. I we land on and index that is deleted, keep
    // increasing until we get to a valid entry.
    size_t incr(size_t ndx) const;
    // Convert from virtual to real index
    size_t adjust(size_t ndx) const;
    void adj_remove(size_t ndx);
    void erase_repl(Replication* repl, size_t ndx) const;
    void move_repl(Replication* repl, size_t from, size_t to) const;
    void swap_repl(Replication* repl, size_t ndx1, size_t ndx2) const;
    void clear_repl(Replication* repl) const;
};

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
    // Increase index by one. I we land on and index that is deleted, keep
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