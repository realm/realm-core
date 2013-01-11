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

namespace tightdb {

template<typename T>
class ColumnBasic : public ColumnBase {
public:
    ColumnBasic(Allocator& alloc=GetDefaultAllocator());
    ColumnBasic(size_t ref, ArrayParent* parent=NULL, size_t pndx=0, Allocator& alloc=GetDefaultAllocator());
    ~ColumnBasic();

    void Destroy();

    size_t Size() const;
    bool is_empty() const;

    T Get(size_t ndx) const;
    virtual bool add() {add(0); return true;}
    bool add(T value);
    bool Set(size_t ndx, T value);
    virtual void insert(size_t ndx) { bool ok = Insert(ndx, 0); TIGHTDB_ASSERT(ok);}
    bool Insert(size_t ndx, T value);
    void Delete(size_t ndx);
    void Clear();
    void Resize(size_t ndx);
    void fill(size_t count);

    size_t count(T value) const;
    double sum(size_t start = 0, size_t end = -1) const;
    double average(size_t start = 0, size_t end = -1) const;
    T maximum(size_t start = 0, size_t end = -1) const;
    T minimum(size_t start = 0, size_t end = -1) const;
    size_t find_first(T value, size_t start=0 , size_t end=-1) const;
    void find_all(Array& result, T value, size_t start = 0, size_t end = -1) const;

    // Index
    bool HasIndex() const {return false;}
    void BuildIndex(Index&) {}
    void ClearIndex() {}
    size_t FindWithIndex(int64_t) const {return (size_t)-1;}

    size_t GetRef() const {return m_array->GetRef();}
    void SetParent(ArrayParent* parent, size_t pndx) {m_array->SetParent(parent, pndx);}

//??FIXME: NEEDED?    Allocator& GetAllocator() const {return m_array->GetAllocator();}

    /// Compare two columns for equality.
    bool Compare(const ColumnBasic&) const;

#ifdef TIGHTDB_DEBUG
    void Verify() const {}; // Must be upper case to avoid conflict with macro in ObjC
#endif // TIGHTDB_DEBUG

protected:
    friend class ColumnBase;

    void UpdateRef(size_t ref);

    T LeafGet(size_t ndx) const;
    bool LeafSet(size_t ndx, T value);
    bool LeafInsert(size_t ndx, T value);
    void LeafDelete(size_t ndx);

    template<class F>
    size_t LeafFind(T value, size_t start, size_t end) const;
    void LeafFindAll(Array& result, T value, size_t add_offset = 0, size_t start = 0, size_t end = -1) const;

//FIXME: Remove?    bool FindKeyPos(const char* target, size_t& pos) const;

#ifdef TIGHTDB_DEBUG
    virtual void LeafToDot(std::ostream& out, const Array& array) const;
#endif // TIGHTDB_DEBUG
};


} // namespace tightdb

// Templates
#include <tightdb/column_basic_tpl.hpp>


#endif // TIGHTDB_COLUMN_BASIC_HPP
