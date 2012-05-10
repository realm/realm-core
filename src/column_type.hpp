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

enum ColumnType {           // Can't change order or numbers for serialization compatibility
    // Single ref
    COLUMN_TYPE_INT         = 0,
    COLUMN_TYPE_BOOL        = 1,
    COLUMN_TYPE_STRING      = 2,
    COLUMN_TYPE_DATE        = 3,
    COLUMN_TYPE_BINARY      = 4,
    COLUMN_TYPE_TABLE       = 5,
    COLUMN_TYPE_MIXED       = 6,

    // Double refs
    COLUMN_TYPE_STRING_ENUM = 7,

    // Attributes
    COLUMN_ATTR_INDEXED     = 8,
    COLUMN_ATTR_UNIQUE      = 9,
    COLUMN_ATTR_SORTED      = 10,
    COLUMN_ATTR_NONE        = 11
};

struct BinaryData {
    const char* pointer;
    size_t len;
};

#endif //__TDB_COLUMNTYPE__
