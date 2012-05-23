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

#ifdef __cplusplus
#define TIGHTDB_QAUL_CC(name) name
#define TIGHTDB_QAUL_UC(name) name
#else
#define TIGHTDB_QAUL_CC(name) Tightdb##name
#define TIGHTDB_QAUL_UC(name) TIGHTDB_##name
#endif

#ifdef __cplusplus
namespace tightdb {
#endif


enum TIGHTDB_QAUL_CC(ColumnType) {
    // Single ref
    TIGHTDB_QAUL_UC(COLUMN_TYPE_INT),
    TIGHTDB_QAUL_UC(COLUMN_TYPE_BOOL),
    TIGHTDB_QAUL_UC(COLUMN_TYPE_STRING),
    TIGHTDB_QAUL_UC(COLUMN_TYPE_DATE), // FIXME: Why do we need a special column type for dates, can we not just use 'int'
    TIGHTDB_QAUL_UC(COLUMN_TYPE_BINARY),
    TIGHTDB_QAUL_UC(COLUMN_TYPE_TABLE),
    TIGHTDB_QAUL_UC(COLUMN_TYPE_MIXED),

    // Double refs
    TIGHTDB_QAUL_UC(COLUMN_TYPE_STRING_ENUM),

    // Attributes
    TIGHTDB_QAUL_UC(COLUMN_ATTR_INDEXED),
    TIGHTDB_QAUL_UC(COLUMN_ATTR_UNIQUE),
    TIGHTDB_QAUL_UC(COLUMN_ATTR_SORTED),
    TIGHTDB_QAUL_UC(COLUMN_ATTR_NONE)
};


#ifdef __cplusplus
} // namespace tightdb
#endif

#endif // TIGHTDB_COLUMN_TYPE_HPP
