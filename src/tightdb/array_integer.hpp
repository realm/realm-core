/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2014] TightDB Inc
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
***************************************************************************/

#ifndef TIGHTDB_ARRAY_INTEGER_HPP
#define TIGHTDB_ARRAY_INTEGER_HPP

#include <tightdb/array.hpp>

namespace tightdb {

class ArrayInteger: public Array {
public:
    typedef int64_t value_type;

    explicit ArrayInteger(Allocator&) TIGHTDB_NOEXCEPT;
    ~ArrayInteger() TIGHTDB_NOEXCEPT TIGHTDB_OVERRIDE {}
};


// Implementation:

inline ArrayInteger::ArrayInteger(Allocator& alloc) TIGHTDB_NOEXCEPT:
    Array(alloc)
{
}

}

#endif // TIGHTDB_ARRAY_INTEGER_HPP
