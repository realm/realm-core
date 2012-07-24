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
#ifndef TIGHTDB_OVERFLOW_HPP
#define TIGHTDB_OVERFLOW_HPP

#include <limits>

#include <tightdb/assert.hpp>
#include <tightdb/meta.hpp>

namespace tightdb {


/// Checks for positive overflow. Works for integers only. 'rval' must
/// not be negative.
template<class L, class R> inline bool add_with_overflow_detect(L& lval, R rval)
{
    TIGHTDB_STATIC_ASSERT((SameType<L,R>::value), "Same type required");
    if (std::numeric_limits<R>::max() - rval < lval) return true;
    lval += rval;
    return false;
}


/// Checks for negative overflow. Works for integers only. 'rval' must
/// not be negative.
template<class L, class R> inline bool subtract_with_overflow_detect(L& lval, R rval)
{
    TIGHTDB_STATIC_ASSERT((SameType<L,R>::value), "Same type required");
    if (lval < std::numeric_limits<R>::min() + rval) return true;
    lval -= rval;
    return false;
}


/// Checks for positive overflow. Works for integers only. 'lval' must
/// not be negative. 'rval' must be stricly greater than zero.
template<class L, class R> inline bool multiply_with_overflow_detect(L& lval, R rval)
{
    TIGHTDB_STATIC_ASSERT((SameType<L,R>::value), "Same type required");
    if (std::numeric_limits<R>::max() / rval < lval) return true;
    lval *= rval;
    return false;
}


/// Checks for positive overflow. Works for integers only. 'lval' must
/// not be negative. 'i' must be such that 'L(1)>>i' has a value that is
/// defined by the standard.
template<class L> inline bool shift_left_with_overflow_detect(L& lval, int i)
{
    if (std::numeric_limits<L>::max() >> i < lval) return true;
    lval <<= i;
    return false;
}


} // namespace tightdb

#endif // TIGHTDB_OVERFLOW_HPP
