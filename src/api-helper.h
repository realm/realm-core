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
#ifndef __TIGHTDB_API_HELPER__
#define __TIGHTDB_API_HELPER__

#include "table.hpp"

// These functions are only to be used for making language API wrappers to the C++ library.
// They help getting real Table pointers instead of reference counted smart pointers (TableRef). 


// Below get_ptr methods will get the Table pointer and bind the pointer.
// When finished you have to manually call TableHelper_unbind to release the pointer.
namespace tightdb {

Table* TableHelper_get_subtable_ptr(Table* t, size_t column_ndx, size_t row_ndx);

// const Table* TableHelper_get_const_subtable_ptr(const Table* t, size_t column_ndx, size_t row_ndx);

Table* ViewHelper_get_table_ptr(TableView* tv, size_t column_ndx, size_t row_ndx);

Table* GroupHelper_get_table_ptr(Group* grp, const char* name);

void TableHelper_unbind(Table* t);


// Don't use seperately. Already called by above *get_ptr functions.
void TableHelper_bind(Table* t);

} // namespace tightdb

#endif // __TIGHTDB_API_HELPER__