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


template<typename T>
class BasicColumn : public ColumnBase {
public:
    BasicColumn(Allocator& = Allocator::get_default());
    BasicColumn(size_t ref, ArrayParent* = 0, size_t ndx_in_parent = 0,
                Allocator& = Allocator::get_default());
    ~BasicColumn();

    void Destroy();

    size_t Size() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    bool is_empty() const TIGHTDB_NOEXCEPT;

    T get(size_t ndx) const TIGHTDB_NOEXCEPT;
    void add() TIGHTDB_OVERRIDE { add(0); }
    void add(T value);
    void set(size_t ndx, T value);
    void insert(size_t ndx) TIGHTDB_OVERRIDE { insert(ndx, 0); }
    void insert(size_t ndx, T value);
    void erase(size_t ndx) TIGHTDB_OVERRIDE;
    void Clear() TIGHTDB_OVERRIDE;
    void Resize(size_t ndx);
    void fill(size_t count);

    size_t count(T value) const;

    typedef typename AggReturnType<T>::sum_type SumType;
    SumType sum(size_t start = 0, size_t end = -1) const;
    double average(size_t start = 0, size_t end = -1) const;
    T maximum(size_t start = 0, size_t end = -1) const;
    T minimum(size_t start = 0, size_t end = -1) const;
    size_t find_first(T value, size_t start=0 , size_t end=-1) const;
    void find_all(Array& result, T value, size_t start = 0, size_t end = -1) const;

    // Index
    bool HasIndex() const TIGHTDB_OVERRIDE {return false;}
    void BuildIndex(Index&) {}
    void ClearIndex() {}
    size_t FindWithIndex(int64_t) const {return size_t(-1);}

    size_t GetRef() const TIGHTDB_OVERRIDE {return m_array->GetRef();}
    void SetParent(ArrayParent* parent, size_t pndx) TIGHTDB_OVERRIDE {m_array->SetParent(parent, pndx);}

    /// Compare two columns for equality.
    bool compare(const BasicColumn&) const;

    void foreach(Array::ForEachOp<T>*) const TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void Verify() const {}; // Must be upper case to avoid conflict with macro in ObjC
#endif // TIGHTDB_DEBUG

private:
    friend class ColumnBase;

    void UpdateRef(size_t ref);

    T LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT;
    void LeafSet(size_t ndx, T value);
    void LeafInsert(size_t ndx, T value);
    void LeafDelete(size_t ndx);

    template<class F> size_t LeafFind(T value, size_t start, size_t end) const;
    void LeafFindAll(Array& result, T value, size_t add_offset = 0, size_t start = 0, size_t end = -1) const;

#ifdef TIGHTDB_DEBUG
    virtual void LeafToDot(std::ostream& out, const Array& array) const;
#endif // TIGHTDB_DEBUG

    template <typename R, Action action, class cond>
    R aggregate(T target, size_t start, size_t end, size_t *matchcount = 0) const;

    static void foreach(const Array* parent, Array::ForEachOp<T>*) TIGHTDB_NOEXCEPT;
};


} // namespace tightdb


// template implementation
#include <tightdb/column_basic_tpl.hpp>


#endif // TIGHTDB_COLUMN_BASIC_HPP
