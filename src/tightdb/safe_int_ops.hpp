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
#ifndef TIGHTDB_SAFE_INT_OPS_HPP
#define TIGHTDB_SAFE_INT_OPS_HPP

#include <limits>

#include <tightdb/config.h>
#include <tightdb/assert.hpp>
#include <tightdb/meta.hpp>

namespace tightdb {


//@{

/// Compare two integers of the same, or of different type, and
/// produce the expected result according to the natural
/// interpretation of the operation.
///
/// Note that in general a standard comparison between a signed and an
/// unsigned integer type is unsafe, and it often generates a compiler
/// warning. An example is a 'less than' comparison between a negative
/// value of type 'int' and a small positive value of type
/// 'unsigned'. In this case the negative value will be converted to
/// 'unsigned' producing a large positive value which, in turn, will
/// lead to the counter intuitive result of 'false'.
///
/// Please note that these operation incur absolutely no overhead when
/// the two types have the same signedness.
///
/// These functions check at compile time that both types have valid
/// specializations of std::numeric_limits<> and that both are indeed
/// integers.
///
/// These functions make absolutely no assumptions about the platform
/// except that it complies with at least C++03.

template<class A, class B> inline bool int_equal_to(A,B);
template<class A, class B> inline bool int_not_equal_to(A,B);
template<class A, class B> inline bool int_less_than(A,B);
template<class A, class B> inline bool int_less_than_or_equal(A,B);
template<class A, class B> inline bool int_greater_than(A,B);
template<class A, class B> inline bool int_greater_than_or_equal(A,B);

//@}


//@{

/// Check for overflow when adding or subtracting two integers of the
/// same, or of different type.
///
/// These functions check for both positive and negative overflow.
///
/// These functions are especially well suited for cases where \a rval
/// is a compile-time constant.
///
/// These functions check at compile time that both types have valid
/// specializations of std::numeric_limits<> and that both are indeed
/// integers.
///
/// These functions make absolutely no assumptions about the platform
/// except that it complies with at least C++03.

template<class L, class R> inline bool int_add_with_overflow_detect(L& lval, R rval);
template<class L, class R> inline bool int_subtract_with_overflow_detect(L& lval, R rval);

//@}


/// Check for positive overflow when multiplying two positive integers
/// of the same, or of different type.
///
/// \param lval Must not be negative. Both signed and unsigned types
/// can be used.
///
/// \param rval Must be stricly greater than zero. Both signed and
/// unsigned types can be used.
///
/// This function is especially well suited for cases where \a rval is
/// a compile-time constant.
///
/// This function checks at compile time that both types have valid
/// specializations of std::numeric_limits<> and that both are indeed
/// integers.
///
/// This function makes absolutely no assumptions about the platform
/// except that it complies with at least C++03.
template<class L, class R> inline bool int_multiply_with_overflow_detect(L& lval, R rval);


/// Checks for positive overflow when performing a bitwise shift to
/// the left on a non-negative value of arbitrary integer type.
///
/// \param lval Must not be negative. Both signed and unsigned types
/// can be used.
///
/// \param i Must be non-negative and such that <tt>L(1)>>i</tt> has a
/// value that is defined by the C++03 standard.
///
/// This function makes absolutely no assumptions about the platform
/// except that it complies with at least C++03.
template<class L> inline bool int_shift_left_with_overflow_detect(L& lval, int i);


//@{

/// Check for overflow when casting an integer value from one type to
/// another. While the first function is a mere check, the second one
/// also carries out the cast, but only whn there is no overflow. Both
/// return true on overflow.
///
/// These functions check at compile time that both types have valid
/// specializations of std::numeric_limits<> and that both are indeed
/// integers.
///
/// These functions make absolutely no assumptions about the platform
/// except that it complies with at least C++03.

template<class T, class F> bool int_cast_has_overflow(F from);
template<class T, class F> bool int_cast_with_overflow_detect(F from, T& to);

//@}






// Implementation:

template<class A, class B> inline bool int_equal_to(A a, B b)
{
    typedef std::numeric_limits<A> lim_a;
    typedef std::numeric_limits<B> lim_b;
    TIGHTDB_STATIC_ASSERT(lim_a::is_specialized && lim_b::is_specialized,
                          "Both types must be specialized");
    TIGHTDB_STATIC_ASSERT(lim_a::is_integer && lim_b::is_integer,
                          "Both types must be integers");
    if (lim_a::is_signed && !lim_b::is_signed)
        return lim_a::digits < lim_b::digits ? !is_negative(a) && B(a) == b : a == A(b);
    if (lim_b::is_signed && !lim_a::is_signed)
        return lim_b::digits < lim_a::digits ? !is_negative(b) && a == A(b) : B(a) == b;
    return lim_a::digits < lim_b::digits ? B(a) == b : a == A(b);
}

template<class A, class B> inline bool int_not_equal_to(A a, B b)
{
    typedef std::numeric_limits<A> lim_a;
    typedef std::numeric_limits<B> lim_b;
    TIGHTDB_STATIC_ASSERT(lim_a::is_specialized && lim_b::is_specialized,
                          "Both types must be specialized");
    TIGHTDB_STATIC_ASSERT(lim_a::is_integer && lim_b::is_integer,
                          "Both types must be integers");
    if (lim_a::is_signed && !lim_b::is_signed)
        return lim_a::digits < lim_b::digits ? is_negative(a) || B(a) != b : a != A(b);
    if (lim_b::is_signed && !lim_a::is_signed)
        return lim_b::digits < lim_a::digits ? is_negative(b) || a != A(b) : B(a) != b;
    return lim_a::digits < lim_b::digits ? B(a) != b : a != A(b);
}

template<class A, class B> inline bool int_less_than(A a, B b)
{
    typedef std::numeric_limits<A> lim_a;
    typedef std::numeric_limits<B> lim_b;
    TIGHTDB_STATIC_ASSERT(lim_a::is_specialized && lim_b::is_specialized,
                          "Both types must be specialized");
    TIGHTDB_STATIC_ASSERT(lim_a::is_integer && lim_b::is_integer,
                          "Both types must be integers");
    if (lim_a::is_signed && !lim_b::is_signed)
        return lim_a::digits < lim_b::digits ?  is_negative(a) || B(a) < b : a < A(b);
    if (lim_b::is_signed && !lim_a::is_signed)
        return lim_b::digits < lim_a::digits ? !is_negative(b) && a < A(b) : B(a) < b;
    return lim_a::digits < lim_b::digits ? B(a) < b : a < A(b);
}

template<class A, class B> inline bool int_less_than_or_equal(A a, B b)
{
    typedef std::numeric_limits<A> lim_a;
    typedef std::numeric_limits<B> lim_b;
    TIGHTDB_STATIC_ASSERT(lim_a::is_specialized && lim_b::is_specialized,
                          "Both types must be specialized");
    TIGHTDB_STATIC_ASSERT(lim_a::is_integer && lim_b::is_integer,
                          "Both types must be integers");
    if (lim_a::is_signed && !lim_b::is_signed)
        return lim_a::digits < lim_b::digits ?  is_negative(a) || B(a) <= b : a <= A(b);
    if (lim_b::is_signed && !lim_a::is_signed)
        return lim_b::digits < lim_a::digits ? !is_negative(b) && a <= A(b) : B(a) <= b;
    return lim_a::digits < lim_b::digits ? B(a) <= b : a <= A(b);
}

template<class A, class B> inline bool int_greater_than(A a, B b)
{
    return int_less_than(b,a);
}

template<class A, class B> inline bool int_greater_than_or_equal(A a, B b)
{
    return int_less_than_or_equal(b,a);
}

template<class L, class R> inline bool int_add_with_overflow_detect(L& lval, R rval)
{
    if (is_negative(rval)) {
        if (int_less_than(lval, std::numeric_limits<R>::min() - rval)) return true;
    }
    else {
        if (int_less_than(std::numeric_limits<R>::max() - rval, lval)) return true;
    }
    lval = L(lval + rval);
    return false;
}

template<class L, class R> inline bool int_subtract_with_overflow_detect(L& lval, R rval)
{
    if (is_negative(rval)) {
        if (int_less_than(std::numeric_limits<R>::max() + rval, lval)) return true;
    }
    else {
        if (int_less_than(lval, std::numeric_limits<R>::min() + rval)) return true;
    }
    lval = L(lval - rval);
    return false;
}

template<class L, class R> inline bool int_multiply_with_overflow_detect(L& lval, R rval)
{
    typedef std::numeric_limits<L> lim_l;
    typedef std::numeric_limits<R> lim_r;
    TIGHTDB_STATIC_ASSERT(lim_l::is_specialized && lim_r::is_specialized,
                          "Both types must be specialized");
    TIGHTDB_STATIC_ASSERT(lim_l::is_integer && lim_r::is_integer,
                          "Both types must be integers");
    if (int_less_than(lim_r::max() / rval, lval)) return true;
    lval = L(lval * rval);
    return false;
}

template<class L> inline bool int_shift_left_with_overflow_detect(L& lval, int i)
{
    if (std::numeric_limits<L>::max() >> i < lval) return true;
    lval <<= i;
    return false;
}

template<class T, class F> inline bool int_cast_has_overflow(F from)
{
    typedef std::numeric_limits<T> lim_to;
    return int_less_than(from, lim_to::min()) || int_less_than(lim_to::max(), from);
}

template<class T, class F> inline bool int_cast_with_overflow_detect(F from, T& to)
{
    if (TIGHTDB_LIKELY(!int_cast_has_overflow<T>(from))) {
        to = T(from);
        return false;
    }
    return true;
}


} // namespace tightdb

#endif // TIGHTDB_SAFE_INT_OPS_HPP
