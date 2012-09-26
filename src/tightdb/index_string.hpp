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
#ifndef __TDB_INDEX_STRING__
#define __TDB_INDEX_STRING__

#include <tightdb/column.hpp>
#include <tightdb/column_string.hpp>

namespace tightdb {

typedef const char*(*StringGetter)(void*, size_t);

class StringIndex : public Column {
public:
    StringIndex(void* target_column, StringGetter get_func, Allocator& alloc);
    StringIndex(ColumnDef type, Allocator& alloc);
    StringIndex(size_t ref, ArrayParent* parent, size_t pndx, void* target_column, StringGetter get_func, Allocator& alloc);
    void SetTarget(void* target_column, StringGetter get_func);

    void Insert(size_t row_ndx, const char* value, bool isLast=false);
    void Set(size_t row_ndx, const char* oldValue, const char* newValue);
    void Delete(size_t row_ndx, const char* value, bool isLast=false);
    void Clear();

    size_t count(const char* value) const;
    size_t find_first(const char* value) const;
    void   find_all(Array& result, const char* value) const;

#ifdef TIGHTDB_DEBUG
    bool is_empty() const;
    void to_dot(std::ostream& out);
#endif

protected:
    void Create();

    bool InsertWithOffset(size_t row_ndx, size_t offset, const char* value);
    bool InsertRowList(size_t ref, size_t offset, const char* value);
    int32_t GetLastKey() const;
    void UpdateRefs(size_t pos, int diff);

    // B-Tree functions
    bool TreeInsert(size_t row_ndx, int32_t key, size_t offset, const char* value);
    NodeChange DoInsert(size_t ndx, int32_t key, size_t offset, const char* value);
    bool LeafInsert(size_t row_ndx, int32_t key, size_t offset, const char* value, bool noextend=false);
    bool NodeInsertSplit(size_t ndx, size_t new_ref);
    bool NodeInsert(size_t ndx, size_t ref);
    void DoDelete(size_t ndx, const char* value, size_t offset);

    const char* Get(size_t ndx) {return (*m_get_func)(m_target_column, ndx);}

    // Member variables
    void* m_target_column;
    StringGetter m_get_func;

#ifdef TIGHTDB_DEBUG
    void ToDot(std::ostream& out, const char* title=NULL) const;
    void ArrayToDot(std::ostream& out, const Array& array) const;
    void KeysToDot(std::ostream& out, const Array& array, const char* title=NULL) const;
#endif
};

} //namespace tightdb

#endif //__TDB_INDEX_STRING__
