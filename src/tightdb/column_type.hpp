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

// FIXME: The namespace of all-upper-case names must be considered
// reserved for macros. Consider renaming 'COLUMN_TYPE_INT' to
// 'type_Int', COLUMN_TYPE_STRING_ENUM to 'type_StringEnum', and so
// forth. That is, a qualifying prefix followed by the enumeration
// name in CamelCase. This is a reasonably common naming scheme for
// enumeration values. Note that I am also suggesting that we drop
// 'column' from the names, since these types a used much more
// generally than as just 'column types'.
//
// Note: <tightdb/c/column_type.h> must be kept in sync with his file.
// Note: <tightdb/objc/column_type.h> must be kept in sync with his file.
// Note: "com/tightdb/ColumnType.java" must be kept in sync with his file.
enum ColumnType {
    // Column types
    COLUMN_TYPE_INT         =  0,
    COLUMN_TYPE_BOOL        =  1,
    COLUMN_TYPE_STRING      =  2,
    COLUMN_TYPE_STRING_ENUM =  3, // double refs
    COLUMN_TYPE_BINARY      =  4,
    COLUMN_TYPE_TABLE       =  5,
    COLUMN_TYPE_MIXED       =  6,
    COLUMN_TYPE_DATE        =  7,
    COLUMN_TYPE_RESERVED1   =  8, // DateTime
    COLUMN_TYPE_FLOAT       =  9, // Float
    COLUMN_TYPE_DOUBLE      = 10, // Double
    COLUMN_TYPE_RESERVED4   = 11, // Decimal

    // Attributes
    COLUMN_ATTR_INDEXED     = 100,
    COLUMN_ATTR_UNIQUE      = 101,
    COLUMN_ATTR_SORTED      = 102,
    COLUMN_ATTR_NONE        = 103
};


} // namespace tightdb

#endif // TIGHTDB_COLUMN_TYPE_HPP
