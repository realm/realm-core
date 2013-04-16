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

class AdaptiveStringColumn: public ColumnBase {
public:
    AdaptiveStringColumn(Allocator& = Allocator::get_default());
    AdaptiveStringColumn(std::size_t ref, ArrayParent* = 0, std::size_t ndx_in_parent = 0,
                         Allocator& = Allocator::get_default());
    ~AdaptiveStringColumn();

    void Destroy();

    std::size_t Size() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    bool is_empty() const TIGHTDB_NOEXCEPT;

    StringData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    void add() TIGHTDB_OVERRIDE {return add(StringData());}
    void add(StringData);
    void set(std::size_t ndx, StringData);
    void insert(std::size_t ndx) TIGHTDB_OVERRIDE { insert(ndx, StringData()); }
    void insert(std::size_t ndx, StringData);
    void erase(std::size_t ndx) TIGHTDB_OVERRIDE;
    void Clear() TIGHTDB_OVERRIDE;
    void Resize(std::size_t ndx);
    void fill(std::size_t count);
 
    std::size_t count(StringData value) const;
    std::size_t find_first(StringData value, std::size_t begin = 0 , std::size_t end = -1) const;
    void find_all(Array& result, StringData value, std::size_t start = 0,
                  std::size_t end = -1) const;

    /// Find the lower bound for the specified value assuming that the
    /// elements are already sorted according to
    /// StringData::operator<(). This operation is semantically
    /// identical to std::lower_bound().
    std::size_t lower_bound(StringData value) const TIGHTDB_NOEXCEPT;
    FindRes find_all_indexref(StringData value, size_t& dst) const;

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
    bool compare(const AdaptiveStringColumn&) const;

    bool GetBlock(size_t ndx, ArrayParent** ap, size_t& off) const
    {
        if (IsNode()) {
            std::pair<size_t, size_t> p = m_array->find_leaf_ref(m_array, ndx);
            bool longstr = m_array->get_hasrefs_from_header(static_cast<const char*>(m_array->GetAllocator().Translate(p.first)));
            if (longstr) {
                ArrayStringLong* asl2 = new ArrayStringLong(p.first, NULL, 0, m_array->GetAllocator());
                *ap = asl2;
            }
            else {
                ArrayString* as2 = new ArrayString(p.first, NULL, 0, m_array->GetAllocator());
                *ap = as2;
            }
            off = ndx - p.second;
            return longstr;
        }
        else {
            off = 0;
            if (IsLongStrings()) {
                ArrayStringLong* asl2 = new ArrayStringLong(m_array->GetRef(), NULL, 0, m_array->GetAllocator());
                *ap = asl2;
                return true;
            }
            else {
                ArrayString* as2 = new ArrayString(m_array->GetRef(), NULL, 0, m_array->GetAllocator());
                *ap = as2;
                return false;
            }
        }

        TIGHTDB_ASSERT(false);
    }

#ifdef TIGHTDB_DEBUG
    void Verify() const; // Must be upper case to avoid conflict with macro in ObjC
#endif // TIGHTDB_DEBUG

    // Assumes that this column has only a single leaf node, no
    // internal nodes. In this case HasRefs indicates a long string
    // array.
    bool IsLongStrings() const TIGHTDB_NOEXCEPT {return m_array->HasRefs();}

protected:
    friend class ColumnBase;
    void UpdateRef(size_t ref);

    StringData LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT;
    void LeafSet(size_t ndx, StringData value);
    void LeafInsert(size_t ndx, StringData value);
    template<class F> size_t LeafFind(StringData value, size_t begin, size_t end) const;
    void LeafFindAll(Array& result, StringData value, size_t add_offset = 0,
                     size_t begin = 0, size_t end = -1) const;

    void LeafDelete(size_t ndx);

#ifdef TIGHTDB_DEBUG
    virtual void LeafToDot(std::ostream& out, const Array& array) const;
#endif // TIGHTDB_DEBUG

private:
    StringIndex* m_index;
};





// Implementation:

inline StringData AdaptiveStringColumn::get(std::size_t ndx) const TIGHTDB_NOEXCEPT
{
    TIGHTDB_ASSERT(ndx < Size());
    return m_array->string_column_get(ndx);
}

inline void AdaptiveStringColumn::add(StringData str)
{
    insert(Size(), str);
}

inline std::size_t AdaptiveStringColumn::lower_bound(StringData value) const TIGHTDB_NOEXCEPT
{
    std::size_t i = 0;
    std::size_t size = Size();

    while (0 < size) {
        std::size_t half = size / 2;
        std::size_t mid = i + half;
        StringData probe = get(mid);
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


} // namespace tightdb

#endif // TIGHTDB_COLUMN_STRING_HPP
