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
#ifndef TIGHTDB_COLUMN_FLOAT_HPP
#define TIGHTDB_COLUMN_FLOAT_HPP

#include <tightdb/column.hpp>
#include <tightdb/column_generic.hpp>
#include <tightdb/array_float.hpp>

namespace tightdb {
    
class ColumnFloat : public ColumnGeneric<float> {
public:
    ColumnFloat(Allocator& alloc=GetDefaultAllocator()) : 
        ColumnGeneric<float>(alloc) {};
    ColumnFloat(size_t ref, ArrayParent* parent=NULL, size_t pndx=0, Allocator& alloc=GetDefaultAllocator()) :
        ColumnGeneric<float>(ref, parent, pndx, alloc) {};

    bool IsFloatColumn() const {return true;}
};

} // namespace tightdb

#endif // TIGHTDB_COLUMN_FLOAT_HPP
