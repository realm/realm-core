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
#ifndef TIGHTDB_COLUMN_STRING_HPP
#define TIGHTDB_COLUMN_STRING_HPP

#include <tightdb/column.hpp>
#include <tightdb/array_string.hpp>
#include <tightdb/array_string_long.hpp>

namespace tightdb {

// Pre-declarations
class StringIndex;

class AdaptiveStringColumn : public ColumnBase {
public:
    AdaptiveStringColumn(Allocator& alloc=Allocator::get_default());
    AdaptiveStringColumn(size_t ref, ArrayParent* parent=NULL, size_t pndx=0,
                         Allocator& alloc=Allocator::get_default());
    ~AdaptiveStringColumn();

    void Destroy();

    size_t Size() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    bool is_empty() const TIGHTDB_NOEXCEPT;

    const char* Get(size_t ndx) const TIGHTDB_NOEXCEPT;
    void add() TIGHTDB_OVERRIDE {return add("");}
    void add(const char* value);
    void Set(size_t ndx, const char* value);
    void insert(size_t ndx) TIGHTDB_OVERRIDE { Insert(ndx, ""); }
    void Insert(size_t ndx, const char* value);
    void Delete(size_t ndx) TIGHTDB_OVERRIDE;
    void Clear() TIGHTDB_OVERRIDE;
    void Resize(size_t ndx);
    void fill(size_t count);

    size_t count(const char* value) const;
    size_t find_first(const char* value, size_t start=0 , size_t end=-1) const;
    void find_all(Array& result, const char* value, size_t start = 0, size_t end = -1) const;

    // Index
    bool HasIndex() const {return m_index != NULL;}
    const StringIndex& GetIndex() const {return *m_index;}
    StringIndex& PullIndex() {StringIndex& ndx = *m_index; m_index = NULL; return ndx;}
    StringIndex& CreateIndex();
    void SetIndexRef(size_t ref, ArrayParent* parent, size_t pndx);
    void RemoveIndex() {m_index = NULL;}

    size_t GetRef() const {return m_array->GetRef();}
    Allocator& GetAllocator() const {return m_array->GetAllocator();}
    void SetParent(ArrayParent* parent, size_t pndx) {m_array->SetParent(parent, pndx);}

    // Optimizing data layout
    bool AutoEnumerate(size_t& ref_keys, size_t& ref_values) const;

    /// Compare two string columns for equality.
    bool Compare(const AdaptiveStringColumn&) const;

#ifdef TIGHTDB_DEBUG
    void Verify() const {}; // Must be upper case to avoid conflict with macro in ObjC
#endif // TIGHTDB_DEBUG

protected:
    friend class ColumnBase;
    void UpdateRef(size_t ref);

    const char* LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT;
    void LeafSet(size_t ndx, const char* value);
    void LeafInsert(size_t ndx, const char* value);
    template<class F> size_t LeafFind(const char* value, size_t start, size_t end) const;
    void LeafFindAll(Array& result, const char* value, size_t add_offset = 0, size_t start = 0, size_t end = -1) const;

    void LeafDelete(size_t ndx);

    // Assumes that this column has only a single leaf node, no
    // internal nodes. In this case HasRefs indicates a long string
    // array.
    bool IsLongStrings() const TIGHTDB_NOEXCEPT {return m_array->HasRefs();}

    bool FindKeyPos(const char* target, size_t& pos) const;

#ifdef TIGHTDB_DEBUG
    virtual void LeafToDot(std::ostream& out, const Array& array) const;
#endif // TIGHTDB_DEBUG

private:
    StringIndex* m_index;
};





// Implementation:

inline const char* AdaptiveStringColumn::Get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < Size());
    return m_array->ColumnStringGet(ndx);
}


} // namespace tightdb

#endif // TIGHTDB_COLUMN_STRING_HPP
