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
#include <cstdlib> // size_t

#include <tightdb/array.hpp>
#include <tightdb/query_conditions.hpp>

namespace tightdb {


// Pre-definitions
class Column;
class Index;

class ColumnBase {
public:
    virtual ~ColumnBase() {};
    virtual void Destroy() = 0;

    virtual void SetHasRefs() {};

    virtual bool IsIntColumn() const TIGHTDB_NOEXCEPT {return false;}

    virtual size_t Size() const TIGHTDB_NOEXCEPT = 0;

    virtual void add() = 0; // Add an entry to this column using the columns default value
    virtual void insert(size_t ndx) = 0; // Insert an entry into this column using the columns default value
    virtual void Clear() = 0;
    virtual void erase(size_t ndx) = 0;
    virtual void move_last_over(size_t ndx) = 0;
    void Resize(size_t ndx) {m_array->Resize(ndx);}

    // Indexing
    virtual bool HasIndex() const = 0;
    //virtual Index& GetIndex() = 0;
    //virtual void BuildIndex(Index& index) = 0;
    //virtual void ClearIndex() = 0;
    virtual void SetIndexRef(size_t, ArrayParent*, size_t) {}

    virtual size_t GetRef() const = 0;
    virtual void SetParent(ArrayParent* parent, size_t pndx) {m_array->SetParent(parent, pndx);}
    virtual void UpdateParentNdx(int diff) {m_array->UpdateParentNdx(diff);}
    virtual void UpdateFromParent() {m_array->UpdateFromParent();}

    virtual void invalidate_subtables_virtual() {}

    const Array* get_root_array() const TIGHTDB_NOEXCEPT { return m_array; }

#ifdef TIGHTDB_DEBUG
    virtual void Verify() const = 0; // Must be upper case to avoid conflict with macro in ObjC
    virtual void ToDot(std::ostream& out, StringData title = StringData()) const;
#endif // TIGHTDB_DEBUG

    template<class C, class A>
    A* TreeGetArray(size_t start, size_t *first, size_t *last) const;

    template<typename T, class C, class F>
    size_t TreeFind(T value, size_t start, size_t end) const;

    const Array* GetBlock(size_t ndx, Array& arr, size_t& off, bool use_retval = false) const
    {
        return m_array->GetBlock(ndx, arr, off, use_retval);
    }

protected:
    friend class StringIndex;

    struct NodeChange {
        size_t ref1;
        size_t ref2;
        enum ChangeType { none, insert_before, insert_after, split } type;
        NodeChange(ChangeType t, size_t r1=0, size_t r2=0) : ref1(r1), ref2(r2), type(t) {}
        NodeChange() : ref1(0), ref2(0), type(none) {}
    };

    // Tree functions
public:
    template<typename T, class C> T TreeGet(size_t ndx) const; // FIXME: This one should probably be eliminated or redesiged because it throws due to dynamic memory allocation
protected:
	template<typename T, class C> void TreeSet(size_t ndx, T value);
    template<typename T, class C> void TreeInsert(size_t ndx, T value);
    template<typename T, class C> NodeChange DoInsert(size_t ndx, T value);
    template<typename T, class C> void TreeDelete(size_t ndx);
    template<typename T, class C> void TreeFindAll(Array &result, T value, size_t add_offset = 0, size_t start = 0, size_t end = -1) const;
    template<typename T, class C> void TreeVisitLeafs(size_t start, size_t end, size_t caller_offset, bool (*call)(T *arr, size_t start, size_t end, size_t caller_offset, void *state), void *state) const;
    template<typename T, class C, class S> size_t TreeWrite(S& out, size_t& pos) const;

    // Node functions
    bool IsNode() const TIGHTDB_NOEXCEPT {return m_array->IsNode();} // FIXME: This one should go away. It does not make any sense to think of a column being a node or not a node.
    Array NodeGetOffsets() const TIGHTDB_NOEXCEPT; // FIXME: Constness is not propagated to the sub-array. This constitutes a real problem, because modifying the returned array genrally causes the parent to be modified too.
    Array NodeGetRefs() const TIGHTDB_NOEXCEPT; // FIXME: Constness is not propagated to the sub-array. This constitutes a real problem, because modifying the returned array genrally causes the parent to be modified too.
    template<class C> void NodeInsert(size_t ndx, size_t ref);
    template<class C> void NodeAdd(size_t ref);
    void NodeAddKey(size_t ref);
    void NodeUpdateOffsets(size_t ndx);
    template<class C> void NodeInsertSplit(size_t ndx, size_t newRef);
    size_t GetRefSize(size_t ref) const;

    static size_t get_size_from_ref(size_t ref, Allocator&) TIGHTDB_NOEXCEPT;
    static bool is_node_from_ref(size_t ref, Allocator& alloc) TIGHTDB_NOEXCEPT;

    template <typename T, typename R, Action action, class condition>
        R aggregate(T target, size_t start, size_t end, size_t *matchcount) const;


#ifdef TIGHTDB_DEBUG
    void ArrayToDot(std::ostream& out, const Array& array) const;
    virtual void LeafToDot(std::ostream& out, const Array& array) const;
#endif // TIGHTDB_DEBUG

    // Member variables
    mutable Array* m_array; // FIXME: This should not be mutable, the problem is again the const-violating moving copy constructor
};



class Column : public ColumnBase {
public:
    explicit Column(Allocator&);
    Column(Array::ColumnDef, Allocator&);
    Column(Array::ColumnDef = Array::coldef_Normal, ArrayParent* = 0, size_t ndx_in_parent = 0,
           Allocator& = Allocator::get_default());
    Column(size_t ref, ArrayParent* = 0, size_t ndx_in_parent = 0,
           Allocator& = Allocator::get_default()); // Throws
    Column(const Column&); // FIXME: Constness violation
    ~Column();

    void Destroy();

    bool IsIntColumn() const TIGHTDB_NOEXCEPT {return true;}

    bool operator==(const Column& column) const;

    void UpdateParentNdx(int diff);
    void SetHasRefs();

    size_t Size() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    bool is_empty() const TIGHTDB_NOEXCEPT;

    // Getting and setting values
    int64_t get(size_t ndx) const TIGHTDB_NOEXCEPT;
    size_t GetAsRef(size_t ndx) const TIGHTDB_NOEXCEPT;
    int64_t Back() const TIGHTDB_NOEXCEPT {return get(Size()-1);}
    void set(size_t ndx, int64_t value);
    void insert(size_t ndx) TIGHTDB_OVERRIDE { insert(ndx, 0); }
    void insert(size_t ndx, int64_t value);
    void add() TIGHTDB_OVERRIDE { add(0); }
    void add(int64_t value);
    void fill(size_t count);

    size_t  count(int64_t target) const;
    int64_t sum(size_t start = 0, size_t end = -1) const;
    int64_t maximum(size_t start = 0, size_t end = -1) const;
    int64_t minimum(size_t start = 0, size_t end = -1) const;
    double  average(size_t start = 0, size_t end = -1) const;

    void sort(size_t start, size_t end);
    void ReferenceSort(size_t start, size_t end, Column &ref);

    intptr_t GetPtr(size_t ndx) const {return intptr_t(get(ndx));} // FIXME: intptr_t is not guaranteed to exists, not even in C++11

    void Clear() TIGHTDB_OVERRIDE;
    void erase(size_t ndx) TIGHTDB_OVERRIDE;
    void move_last_over(size_t ndx) TIGHTDB_OVERRIDE;
    //void Resize(size_t len);

    void Increment64(int64_t value, size_t start=0, size_t end=-1);
    void IncrementIf(int64_t limit, int64_t value);

    size_t find_first(int64_t value, size_t start=0, size_t end=-1) const;
    void   find_all(Array& result, int64_t value, size_t caller_offset=0, size_t start=0, size_t end=-1) const;
    void   find_all_hamming(Array& result, uint64_t value, size_t maxdist, size_t offset=0) const;
    size_t find_pos(int64_t value) const TIGHTDB_NOEXCEPT;
    size_t find_pos2(int64_t value) const TIGHTDB_NOEXCEPT;
    bool   find_sorted(int64_t target, size_t& pos) const TIGHTDB_NOEXCEPT;

    // Query support methods
    void LeafFindAll(Array &result, int64_t value, size_t add_offset, size_t start, size_t end) const;


    // Index
    bool HasIndex() const {return m_index != NULL;}
    Index& GetIndex();
    void BuildIndex(Index& index);
    void ClearIndex();
    size_t FindWithIndex(int64_t value) const;

    size_t GetRef() const {return m_array->GetRef();}
    Allocator& GetAllocator() const TIGHTDB_NOEXCEPT {return m_array->GetAllocator();}
    Array* GetArray(void) {return m_array;}

    void sort();

    /// Compare two columns for equality.
    bool compare(const Column&) const;

    // Debug
#ifdef TIGHTDB_DEBUG
    void Print() const;
    virtual void Verify() const;
    MemStats Stats() const;
#endif // TIGHTDB_DEBUG

protected:
    friend class ColumnBase;
    void Create();
    void UpdateRef(size_t ref);

    // Node functions
    int64_t LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT { return m_array->Get(ndx); }
    void LeafSet(size_t ndx, int64_t value) { m_array->Set(ndx, value); }
    void LeafInsert(size_t ndx, int64_t value) { m_array->Insert(ndx, value); }
    void LeafDelete(size_t ndx) { m_array->Delete(ndx); }
    template<class F> size_t LeafFind(int64_t value, size_t start, size_t end) const
    {
        return m_array->find_first<F>(value, start, end);
    }

    void DoSort(size_t lo, size_t hi);

    // Member variables
    Index* m_index;

private:
    Column &operator=(Column const &); // not allowed
};




// Implementation:

inline int64_t Column::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    return m_array->column_get(ndx);
}

inline std::size_t Column::GetAsRef(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    return to_ref(get(ndx));
}


} // namespace tightdb

// Templates
#include <tightdb/column_tpl.hpp>

#endif // TIGHTDB_COLUMN_HPP
