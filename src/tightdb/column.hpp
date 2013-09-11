/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef TIGHTDB_COLUMN_HPP
#define TIGHTDB_COLUMN_HPP

#include <stdint.h> // unint8_t etc
#include <cstdlib> // std::size_t

#include <tightdb/array.hpp>
#include <tightdb/query_conditions.hpp>

namespace tightdb {


// Pre-definitions
class Column;

class ColumnBase {
public:
    /// Get the number of entries in this column.
    virtual std::size_t size() const TIGHTDB_NOEXCEPT = 0;

    /// Add an entry to this column using the columns default value.
    virtual void add() = 0;

    /// Insert an entry into this column using the columns default
    /// value.
    virtual void insert(std::size_t ndx) = 0;

    /// Remove all entries from this column.
    virtual void clear() = 0;

    /// Remove the specified entry from this column.
    virtual void erase(std::size_t ndx) = 0;

    virtual void move_last_over(std::size_t ndx) = 0;

    // FIXME: Carefull with this one. It resizes the root node, not
    // the column. Depending on what it is used for, either rename to
    // resize_root() or upgrade to handle proper column
    // resizing. Check if it is used at all. Same for various specific
    // column types such as AdaptiveStringColumn.
    void resize(std::size_t size) { m_array->resize(size); }

    virtual bool IsIntColumn() const TIGHTDB_NOEXCEPT { return false; }

    virtual void destroy() TIGHTDB_NOEXCEPT;

    virtual ~ColumnBase() TIGHTDB_NOEXCEPT {};

    // Indexing
    virtual bool has_index() const TIGHTDB_NOEXCEPT { return false; }
    virtual void set_index_ref(ref_type, ArrayParent*, std::size_t) {}

    virtual void adjust_ndx_in_parent(int diff) TIGHTDB_NOEXCEPT;

    /// Called in the context of Group::commit() to ensure that
    /// attached table accessors stay valid across a commit. Please
    /// note that this works only for non-transactional commits. Table
    /// accessors obtained during a transaction are always detached
    /// when the transaction ends.
    virtual void update_from_parent(std::size_t old_baseline) TIGHTDB_NOEXCEPT;

    virtual void detach_subtable_accessors_virtual() TIGHTDB_NOEXCEPT {}

    Allocator& get_alloc() const TIGHTDB_NOEXCEPT { return m_array->get_alloc(); }

    // FIXME: Should be moved into concrete derivatives, since not all
    // column types have a unique root (string enum).
    ref_type get_ref() const TIGHTDB_NOEXCEPT { return m_array->get_ref(); }

    // FIXME: Should be moved into concrete derivatives, since not all
    // column types have a unique root (string enum).
    void set_parent(ArrayParent* parent, std::size_t ndx_in_parent) TIGHTDB_NOEXCEPT { m_array->set_parent(parent, ndx_in_parent); }

    // FIXME: Should be moved into concrete derivatives, since not all
    // column types have a unique root (string enum).
    const Array* get_root_array() const TIGHTDB_NOEXCEPT { return m_array; }

#ifdef TIGHTDB_DEBUG
    virtual void Verify() const = 0; // Must be upper case to avoid conflict with macro in Objective-C
    virtual void to_dot(std::ostream&, StringData title = StringData()) const;
#endif

    template<class C, class A>
    A* TreeGetArray(std::size_t start, std::size_t* first, std::size_t* last) const;

    template<class T, class C, class F>
    std::size_t TreeFind(T value, std::size_t start, std::size_t end) const;

    const Array* GetBlock(std::size_t ndx, Array& arr, std::size_t& off,
                          bool use_retval = false) const
    {
        return m_array->GetBlock(ndx, arr, off, use_retval);
    }

protected:
    // FIXME: This should not be mutable, the problem is again the
    // const-violating moving copy constructor.
    mutable Array* m_array;

    ColumnBase() TIGHTDB_NOEXCEPT {}
    ColumnBase(Array* root) TIGHTDB_NOEXCEPT: m_array(root) {}

    // Tree functions
    template<class T, class C> void TreeSet(std::size_t ndx, T value);
    template<class T, class C> void TreeDelete(std::size_t ndx);
    template<class T, class C> void TreeFindAll(Array &result, T value, size_t add_offset = 0, size_t start = 0, size_t end = -1) const;
    template<class T, class C> void TreeVisitLeafs(size_t start, size_t end, size_t caller_offset, bool (*call)(T* arr, size_t start, size_t end, size_t caller_offset, void* state), void* state) const;
    template<class T, class C, class S> std::size_t TreeWrite(S& out, size_t& pos) const;

    //@{
    /// \tparam L Any type with an appropriate `value_type`, %size(),
    /// and %get() members.
    template<class L, class T>
    std::size_t lower_bound(const L& list, T value) const TIGHTDB_NOEXCEPT;
    template<class L, class T>
    std::size_t upper_bound(const L& list, T value) const TIGHTDB_NOEXCEPT;
    //@}

    // Node functions
    bool root_is_leaf() const TIGHTDB_NOEXCEPT { return m_array->is_leaf(); }
    Array NodeGetOffsets() const TIGHTDB_NOEXCEPT; // FIXME: Constness is not propagated to the sub-array. This constitutes a real problem, because modifying the returned array genrally causes the parent to be modified too.
    Array NodeGetRefs() const TIGHTDB_NOEXCEPT; // FIXME: Constness is not propagated to the sub-array. This constitutes a real problem, because modifying the returned array genrally causes the parent to be modified too.

    static std::size_t get_size_from_ref(ref_type, Allocator&) TIGHTDB_NOEXCEPT;
    static bool root_is_leaf_from_ref(ref_type, Allocator&) TIGHTDB_NOEXCEPT;

    template <class T, class R, Action action, class condition>
    R aggregate(T target, std::size_t start, std::size_t end, std::size_t* matchcount) const;


#ifdef TIGHTDB_DEBUG
    void array_to_dot(std::ostream&, const Array&) const;
    virtual void leaf_to_dot(std::ostream&, const Array&) const;
#endif

    /// Introduce a new root node which increments the height of the
    /// tree by one.
    void introduce_new_root(ref_type new_sibling_ref, Array::TreeInsertBase& state);

    friend class StringIndex;
};



class Column: public ColumnBase {
public:
    typedef int64_t value_type;

    explicit Column(Allocator&);
    Column(Array::Type, Allocator&);
    explicit Column(Array::Type = Array::type_Normal, ArrayParent* = 0,
                    std::size_t ndx_in_parent = 0, Allocator& = Allocator::get_default());
    explicit Column(ref_type, ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                    Allocator& = Allocator::get_default()); // Throws
    Column(const Column&); // FIXME: Constness violation
    ~Column() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

    bool IsIntColumn() const TIGHTDB_NOEXCEPT { return true; }

    std::size_t size() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    bool is_empty() const TIGHTDB_NOEXCEPT;

    // Getting and setting values
    int64_t get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    ref_type get_as_ref(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    int64_t Back() const TIGHTDB_NOEXCEPT { return get(size()-1); }
    void set(std::size_t ndx, int64_t value);
    void insert(std::size_t ndx) TIGHTDB_OVERRIDE { insert(ndx, 0); }
    void insert(std::size_t ndx, int64_t value);
    void add() TIGHTDB_OVERRIDE { add(0); }
    void add(int64_t value);
    void fill(std::size_t count);

    std::size_t count(int64_t target) const;
    int64_t sum(std::size_t start = 0, std::size_t end = -1) const;
    int64_t maximum(std::size_t start = 0, std::size_t end = -1) const;
    int64_t minimum(std::size_t start = 0, std::size_t end = -1) const;
    double  average(std::size_t start = 0, std::size_t end = -1) const;

    void sort(std::size_t start, std::size_t end);
    void ReferenceSort(std::size_t start, std::size_t end, Column& ref);

    // FIXME: Be careful, clear() currently forgets if the leaf type
    // is Array::type_HasRefs.
    void clear() TIGHTDB_OVERRIDE;

    void erase(std::size_t ndx) TIGHTDB_OVERRIDE;
    void move_last_over(std::size_t ndx) TIGHTDB_OVERRIDE;

    void Increment64(int64_t value, std::size_t start=0, std::size_t end=-1);
    void IncrementIf(int64_t limit, int64_t value);

    size_t find_first(int64_t value, std::size_t start=0, std::size_t end=-1) const;
    void   find_all(Array& result, int64_t value, size_t caller_offset=0, size_t start=0, size_t end=-1) const;

    //@{
    /// Find the lower/upper bound for the specified value assuming
    /// that the elements are already sorted in ascending order
    /// according to ordinary integer comparison.
    std::size_t lower_bound_int(int64_t value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound_int(int64_t value) const TIGHTDB_NOEXCEPT;
    //@}

    // Query support methods
    void LeafFindAll(Array& result, int64_t value, size_t add_offset, size_t start, size_t end) const;

    void sort();

    /// Compare two columns for equality.
    bool compare_int(const Column&) const;

    // Debug
#ifdef TIGHTDB_DEBUG
    void print() const;
    virtual void Verify() const TIGHTDB_OVERRIDE;
    MemStats stats() const;
#endif

protected:
    Column(Array* root);
    void create();

    // Node functions
    void LeafSet(std::size_t ndx, int64_t value) { m_array->set(ndx, value); }
    void LeafDelete(std::size_t ndx) { m_array->erase(ndx); }
    template<class F> size_t LeafFind(int64_t value, std::size_t start, std::size_t end) const
    {
        return m_array->find_first<F>(value, start, end);
    }

    void DoSort(std::size_t lo, std::size_t hi);

private:
    Column &operator=(const Column&); // not allowed

    void do_insert(std::size_t ndx, int64_t value);

    // Called by Array::btree_insert().
    static ref_type leaf_insert(MemRef leaf_mem, ArrayParent&, std::size_t ndx_in_parent,
                                Allocator&, std::size_t insert_ndx, Array::TreeInsert<Column>&);

    friend class Array;
    friend class ColumnBase;
};




// Implementation:

inline void ColumnBase::destroy() TIGHTDB_NOEXCEPT
{
    if (m_array)
        m_array->destroy();
}

template<class L, class T>
std::size_t ColumnBase::lower_bound(const L& list, T value) const TIGHTDB_NOEXCEPT
{
    std::size_t i = 0;
    std::size_t size = list.size();
    while (0 < size) {
        std::size_t half = size / 2;
        std::size_t mid = i + half;
        typename L::value_type probe = list.get(mid);
        if (probe < value) {
            i = mid + 1;
            size -= half + 1;
        }
        else {
            size = half;
        }
    }
    return i;
}

template<class L, class T>
std::size_t ColumnBase::upper_bound(const L& list, T value) const TIGHTDB_NOEXCEPT
{
    size_t i = 0;
    size_t size = list.size();
    while (0 < size) {
        size_t half = size / 2;
        size_t mid = i + half;
        typename L::value_type probe = list.get(mid);
        if (!(value < probe)) {
            i = mid + 1;
            size -= half + 1;
        }
        else {
            size = half;
        }
    }
    return i;
}

inline Column::Column(Allocator& alloc):
    ColumnBase(new Array(Array::type_Normal, 0, 0, alloc))
{
    create();
}

inline Column::Column(Array::Type type, Allocator& alloc):
    ColumnBase(new Array(type, 0, 0, alloc))
{
    create();
}

inline Column::Column(Array::Type type, ArrayParent* parent, std::size_t ndx_in_parent,
                      Allocator& alloc):
    ColumnBase(new Array(type, parent, ndx_in_parent, alloc))
{
    create();
}

inline Column::Column(ref_type ref, ArrayParent* parent, std::size_t ndx_in_parent,
                      Allocator& alloc):
    ColumnBase(new Array(ref, parent, ndx_in_parent, alloc)) {}

inline Column::Column(const Column& column): ColumnBase(column.m_array)
{
    // FIXME: Unfortunate hidden constness violation here
    // we now own array
    column.m_array = 0;       // so detach source
}

inline Column::Column(Array* root): ColumnBase(root) {}

inline Column::~Column() TIGHTDB_NOEXCEPT
{
    delete m_array;
}

inline int64_t Column::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < size());
    if (root_is_leaf())
        return m_array->get(ndx);

    std::pair<MemRef, std::size_t> p = m_array->find_btree_leaf(ndx);
    const char* leaf_header = p.first.m_addr;
    std::size_t ndx_in_leaf = p.second;
    return Array::get(leaf_header, ndx_in_leaf);
}

inline ref_type Column::get_as_ref(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    return to_ref(get(ndx));
}

inline void Column::add(int64_t value)
{
    do_insert(npos, value);
}

inline void Column::insert(std::size_t ndx, int64_t value)
{
    TIGHTDB_ASSERT(ndx <= size());
    if (size() <= ndx)
        ndx = npos;
    do_insert(ndx, value);
}

TIGHTDB_FORCEINLINE
ref_type Column::leaf_insert(MemRef leaf_mem, ArrayParent& parent, std::size_t ndx_in_parent,
                             Allocator& alloc, std::size_t insert_ndx,
                             Array::TreeInsert<Column>& state)
{
    Array leaf(leaf_mem, &parent, ndx_in_parent, alloc);
    return leaf.btree_leaf_insert(insert_ndx, state.m_value, state);
}


inline std::size_t Column::lower_bound_int(int64_t value) const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        return m_array->lower_bound_int(value);
    }
    return ColumnBase::lower_bound(*this, value);
}

inline std::size_t Column::upper_bound_int(int64_t value) const TIGHTDB_NOEXCEPT
{
    if (root_is_leaf()) {
        return m_array->upper_bound_int(value);
    }
    return ColumnBase::upper_bound(*this, value);
}


} // namespace tightdb

// Templates
#include <tightdb/column_tpl.hpp>

#endif // TIGHTDB_COLUMN_HPP
