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
#ifndef TIGHTDB_COLUMN_BINARY_HPP
#define TIGHTDB_COLUMN_BINARY_HPP

#include <tightdb/column.hpp>
#include <tightdb/binary_data.hpp>
#include <tightdb/array_binary.hpp>

namespace tightdb {


class ColumnBinary : public ColumnBase {
public:
    ColumnBinary(Allocator& alloc = Allocator::get_default());
    ColumnBinary(size_t ref, ArrayParent* parent=NULL, size_t pndx=0,
                 Allocator& alloc = Allocator::get_default());
    ~ColumnBinary();

    void Destroy();

    size_t Size() const TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE;
    bool is_empty() const TIGHTDB_NOEXCEPT;

    BinaryData Get(size_t ndx) const;
    const char* GetData(size_t ndx) const;
    size_t GetLen(size_t ndx) const;

    void add() TIGHTDB_OVERRIDE { add(NULL, 0); }
    void add(const char* value, size_t len);
    void Set(size_t ndx, const char* value, size_t len);
    void insert(size_t ndx) TIGHTDB_OVERRIDE { Insert(ndx, 0, 0); }
    void Insert(size_t ndx, const char* value, size_t len);
    void Delete(size_t ndx) TIGHTDB_OVERRIDE;
    void Resize(size_t ndx);
    void Clear() TIGHTDB_OVERRIDE;
    void fill(size_t count);

    // Index
    bool HasIndex() const {return false;}
    void BuildIndex(Index&) {}
    void ClearIndex() {}
    size_t FindWithIndex(int64_t) const {return (size_t)-1;}

    size_t GetRef() const {return m_array->GetRef();}
    void SetParent(ArrayParent *parent, size_t pndx) {m_array->SetParent(parent, pndx);}
    void UpdateParentNdx(int diff) {m_array->UpdateParentNdx(diff);}

    /// Compare two binary columns for equality.
    bool compare(const ColumnBinary&) const;

#ifdef TIGHTDB_DEBUG
    void Verify() const {}; // Must be upper case to avoid conflict with macro in ObjC
#endif // TIGHTDB_DEBUG

protected:
    friend class ColumnBase;

    void add(BinaryData bin);
    void Set(size_t ndx, BinaryData bin);
    void Insert(size_t ndx, BinaryData bin);

    void UpdateRef(size_t ref);

    BinaryData LeafGet(size_t ndx) const TIGHTDB_NOEXCEPT;
    void LeafSet(size_t ndx, BinaryData value);
    void LeafInsert(size_t ndx, BinaryData value);
    void LeafDelete(size_t ndx);

#ifdef TIGHTDB_DEBUG
    virtual void LeafToDot(std::ostream& out, const Array& array) const;
#endif // TIGHTDB_DEBUG
};


} // namespace tightdb

#endif // TIGHTDB_COLUMN_BINARY_HPP
