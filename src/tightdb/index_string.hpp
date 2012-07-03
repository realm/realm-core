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

#include "column.hpp"
#include "column_string.hpp"

namespace tightdb {
    
class StringIndex : public Column {
public:
    StringIndex(const AdaptiveStringColumn& c);
    StringIndex(size_t ref, ArrayParent* parent, size_t pndx, const AdaptiveStringColumn& c);
    
    bool Insert(size_t row_ndx, const char* value, bool isLast=false);
    
    size_t find_first(const char* value) const;
    
#ifdef _DEBUG
    void to_dot(std::ostream& out = std::cerr);
#endif
    
protected:
    bool InsertWithOffset(size_t row_ndx, size_t offset, const char* value);
    bool InsertRowList(size_t ref, size_t offset, const char* value);
    int64_t GetLastKey() const;
    
    // B-Tree functions
    bool TreeInsert(size_t row_ndx, int32_t key, size_t offset, const char* value);
    NodeChange DoInsert(size_t ndx, int32_t key, size_t offset, const char* value);
    bool LeafInsert(size_t row_ndx, int32_t key, size_t offset, const char* value, bool noextend=false);
    bool NodeInsertSplit(size_t ndx, size_t new_ref);
    bool NodeInsert(size_t ndx, size_t ref);

    // Member variables
    const AdaptiveStringColumn& m_column;
    
#ifdef _DEBUG
    void ToDot(std::ostream& out, const char* title=NULL) const;
    void ArrayToDot(std::ostream& out, const Array& array) const;
    void KeysToDot(std::ostream& out, const Array& array, const char* title=NULL) const;
#endif
};
    
} //namespace tightdb

#endif //__TDB_INDEX_STRING__
