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
#ifndef TIGHTDB_COLUMN_TYPE_HPP
#define TIGHTDB_COLUMN_TYPE_HPP

namespace tightdb {

// Note: tightdb_objc/Deliv/ColumnType.h must be kept in sync with his file.
// Note: tightdb_java2/src/main/java/ColumnType.java must be kept in sync with his file.
//
// Note: <tightdb/c/column_type.h> must be kept in sync with his file.
// Note: <tightdb/objc/column_type.h> must be kept in sync with his file.
// Note: "com/tightdb/ColumnType.java" must be kept in sync with his file.
enum ColumnType {
    // Column types
    type_Int         =  0,
    type_Bool        =  1,
    type_String      =  2,
    type_Binary      =  4,
    type_Table       =  5,
    type_Mixed       =  6,
    type_Date        =  7,
    type_Float       =  9,
    type_Double      = 10,

    col_type_StringEnum  =  3, // double refs
    col_type_Reserved1   =  8, // DateTime
    col_type_Reserved4   = 11, // Decimal

    // Attributes
    col_attr_Indexed     = 100,
    col_attr_Unique      = 101,
    col_attr_Sorted      = 102,
    col_attr_None        = 103
};


} // namespace tightdb

#endif // TIGHTDB_COLUMN_TYPE_HPP
