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
#ifndef TIGHTDB_COLUMN_STRING_ENUM_HPP
#define TIGHTDB_COLUMN_STRING_ENUM_HPP

#include <tightdb/column_string.hpp>

namespace tightdb {

// Pre-declarations
class StringIndex;

class ColumnStringEnum: public Column {
public:
    ColumnStringEnum(size_t ref_keys, size_t ref_values, ArrayParent* parent=NULL, size_t pndx=0,
                     Allocator& alloc = Allocator::get_default());
    ~ColumnStringEnum();
    void Destroy();

    StringData get(std::size_t ndx) const TIGHTDB_NOEXCEPT;
    void add(StringData value);
    void set(std::size_t ndx, StringData value);
    void insert(std::size_t ndx, StringData value);
    void erase(std::size_t ndx) TIGHTDB_OVERRIDE;
    void Clear() TIGHTDB_OVERRIDE;

    using Column::add;
    using Column::insert;

    size_t count(StringData value) const;
    size_t find_first(StringData value, size_t begin=0, size_t end=-1) const;
    void find_all(Array& res, StringData value, size_t begin=0, size_t end=-1) const;

    size_t count(size_t key_index) const;
    size_t find_first(size_t key_index, size_t begin=0, size_t end=-1) const;
    void find_all(Array& res, size_t key_index, size_t begin=0, size_t end=-1) const;

    void UpdateParentNdx(int diff);
    void UpdateFromParent();

    // Index
    bool HasIndex() const {return m_index != NULL;}
    const StringIndex& GetIndex() const {return *m_index;}
    StringIndex& CreateIndex();
    void SetIndexRef(size_t ref, ArrayParent* parent, size_t pndx);
    void ReuseIndex(StringIndex& index);
    void RemoveIndex() {m_index = NULL;}

    // Compare two string columns for equality
    bool compare(const AdaptiveStringColumn&) const;
    bool compare(const ColumnStringEnum&) const;

    const Array* get_enum_root_array() const TIGHTDB_NOEXCEPT { return m_keys.get_root_array(); }

    void foreach(Array::ForEachOp<StringData>*) const TIGHTDB_NOEXCEPT;

#ifdef TIGHTDB_DEBUG
    void Verify() const; // Must be upper case to avoid conflict with macro in ObjC
    void ToDot(std::ostream& out, StringData title) const;
#endif // TIGHTDB_DEBUG

    size_t GetKeyNdx(StringData value) const;
    size_t GetKeyNdxOrAdd(StringData value);

private:
    // Member variables
    AdaptiveStringColumn m_keys;
    StringIndex* m_index;

    struct ForEachIndexOp;
};




// Implementation:

struct ColumnStringEnum::ForEachIndexOp: Array::ForEachOp<int64_t> {
    void handle_chunk(const int64_t* begin, const int64_t* end) TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    ForEachIndexOp(const AdaptiveStringColumn& k,
                   Array::ForEachOp<StringData>* o) TIGHTDB_NOEXCEPT: m_keys(k), m_op(o) {}
private:
    const AdaptiveStringColumn& m_keys;
    Array::ForEachOp<StringData>* const m_op;
};

inline void ColumnStringEnum::foreach(Array::ForEachOp<StringData>* op) const TIGHTDB_NOEXCEPT
{
    ForEachIndexOp op2(m_keys, op);
    Column::foreach(&op2);
}


} // namespace tightdb

#endif // TIGHTDB_COLUMN_STRING_ENUM_HPP
