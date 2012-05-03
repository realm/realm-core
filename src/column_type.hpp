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
#ifndef __TDB_COLUMNTYPE__
#define __TDB_COLUMNTYPE__

#include <stdlib.h>

enum ColumnType {
    // Single ref
    COLUMN_TYPE_INT,
    COLUMN_TYPE_BOOL,
    COLUMN_TYPE_STRING,
    COLUMN_TYPE_DATE,
    COLUMN_TYPE_BINARY,
    COLUMN_TYPE_TABLE,
    COLUMN_TYPE_MIXED,

    // Double refs
    COLUMN_TYPE_STRING_ENUM,

    // Attributes
    COLUMN_ATTR_INDEXED,
    COLUMN_ATTR_UNIQUE,
    COLUMN_ATTR_SORTED,
    COLUMN_ATTR_NONE
};

struct BinaryData {
    const char* pointer;
    size_t len;
};

#endif //__TDB_COLUMNTYPE__
