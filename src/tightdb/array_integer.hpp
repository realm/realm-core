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

    /// Construct an array of the specified type and size, and return just the
    /// reference to the underlying memory. All elements will be initialized to
    /// the specified value.
    static MemRef create_array(Type, bool context_flag, std::size_t size, int_fast64_t value,
                               Allocator&);
};


// Implementation:

inline ArrayInteger::ArrayInteger(Allocator& alloc) TIGHTDB_NOEXCEPT:
    Array(alloc)
{
}

inline MemRef ArrayInteger::create_array(Type type, bool context_flag, std::size_t size,
                                  int_fast64_t value, Allocator& alloc)
{
    return Array::create(type, context_flag, wtype_Bits, size, value, alloc); // Throws
}

}

#endif // TIGHTDB_ARRAY_INTEGER_HPP
