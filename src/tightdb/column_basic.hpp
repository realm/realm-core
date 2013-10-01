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
#ifndef TIGHTDB_COLUMN_BASIC_HPP
#define TIGHTDB_COLUMN_BASIC_HPP

#include <tightdb/column.hpp>
#include <tightdb/array_basic.hpp>

//
// A BasicColumn can currently only be used for simple unstructured types like float, double.
//

namespace tightdb {

template<class T> struct AggReturnType {
    typedef T sum_type;
};
template<> struct AggReturnType<float> {
    typedef double sum_type;
};


template<class T>
class BasicColumn: public ColumnBase {
public:
    typedef T value_type;

    explicit BasicColumn(Allocator& = Allocator::get_default());
    explicit BasicColumn(ref_type, ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                         Allocator& = Allocator::get_default());
    ~BasicColumn() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;

    size_t size() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    bool is_empty() const TIGHTDB_NOEXCEPT;

    T get(size_t ndx) const TIGHTDB_NOEXCEPT;
    void add() TIGHTDB_OVERRIDE { add(0); }
    void add(T value);
    void set(size_t ndx, T value);
    void insert(size_t ndx) TIGHTDB_OVERRIDE { insert(ndx, 0); }
    void insert(size_t ndx, T value);
    void erase(size_t ndx) TIGHTDB_OVERRIDE;
    void clear() TIGHTDB_OVERRIDE;
    void resize(size_t ndx);
    void fill(size_t count);
    // Experimental. Overwrites the row at ndx with the last row and removes the last row. For unordered tables.
    void move_last_over(size_t ndx) TIGHTDB_OVERRIDE;

    size_t count(T value) const;

    typedef typename AggReturnType<T>::sum_type SumType;
    SumType sum(size_t start = 0, size_t end = -1, size_t limit = size_t(-1)) const;
    double average(size_t start = 0, size_t end = -1, size_t limit = size_t(-1)) const;
    T maximum(size_t start = 0, size_t end = -1, size_t limit = size_t(-1)) const;
    T minimum(size_t start = 0, size_t end = -1, size_t limit = size_t(-1)) const;
    size_t find_first(T value, size_t start=0 , size_t end=-1) const;
    void find_all(Array& result, T value, size_t start = 0, size_t end = -1) const;

    //@{
    /// Find the lower/upper bound for the specified value assuming
    /// that the elements are already sorted in ascending order.
    std::size_t lower_bound(T value) const TIGHTDB_NOEXCEPT;
    std::size_t upper_bound(T value) const TIGHTDB_NOEXCEPT;
    //@{

    /// Compare two columns for equality.
    bool compare(const BasicColumn&) const;

#ifdef TIGHTDB_DEBUG
    void Verify() const TIGHTDB_OVERRIDE {}; // Must be upper case to avoid conflict with macro in ObjC
#endif

private:
    void LeafSet(size_t ndx, T value);
    void LeafDelete(size_t ndx);

    template<class F> size_t LeafFind(T value, size_t start, size_t end) const;
    void LeafFindAll(Array& result, T value, size_t add_offset = 0, size_t start = 0, size_t end = -1) const;

    void do_insert(std::size_t ndx, T value);

    // Called by Array::btree_insert().
    static ref_type leaf_insert(MemRef leaf_mem, ArrayParent&, std::size_t ndx_in_parent,
                                Allocator&, std::size_t insert_ndx,
                                Array::TreeInsert<BasicColumn<T> >&);

#ifdef TIGHTDB_DEBUG
    void leaf_to_dot(std::ostream&, const Array&) const TIGHTDB_OVERRIDE;
#endif

    template <typename R, Action action, class cond>
    R aggregate(T target, size_t start, size_t end, size_t *matchcount = 0) const;

    friend class Array;
    friend class ColumnBase;
};


} // namespace tightdb


// template implementation
#include <tightdb/column_basic_tpl.hpp>


#endif // TIGHTDB_COLUMN_BASIC_HPP
