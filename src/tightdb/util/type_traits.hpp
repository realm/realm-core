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
#ifndef TIGHTDB_UTIL_TYPE_TRAITS_HPP
#define TIGHTDB_UTIL_TYPE_TRAITS_HPP

#include <climits>
#include <cwchar>
#include <limits>

#ifdef TIGHTDB_HAVE_CXX11_TYPE_TRAITS
#  include <type_traits>
#endif

#include <tightdb/util/features.h>
#include <tightdb/util/assert.hpp>
#include <tightdb/util/meta.hpp>
#include <tightdb/util/type_list.hpp>

namespace tightdb {
namespace util {


template<class T> struct IsConst          { static const bool value = false; };
template<class T> struct IsConst<const T> { static const bool value = true;  };

template<class From, class To> struct CopyConstness                 { typedef       To type; };
template<class From, class To> struct CopyConstness<const From, To> { typedef const To type; };

template<class T> struct DerefType {};
template<class T> struct DerefType<T*> { typedef T type; };



/// Determine whether a type is an integral type.
#ifdef TIGHTDB_HAVE_CXX11_TYPE_TRAITS
template<class T> struct IsIntegral { static const bool value = std::is_integral<T>::value; };
#else // !TIGHTDB_HAVE_CXX11_TYPE_TRAITS
template<class T> struct IsIntegral { static const bool value = false; };
template<> struct IsIntegral<bool>               { static const bool value = true; };
template<> struct IsIntegral<char>               { static const bool value = true; };
template<> struct IsIntegral<signed char>        { static const bool value = true; };
template<> struct IsIntegral<unsigned char>      { static const bool value = true; };
template<> struct IsIntegral<wchar_t>            { static const bool value = true; };
template<> struct IsIntegral<short>              { static const bool value = true; };
template<> struct IsIntegral<unsigned short>     { static const bool value = true; };
template<> struct IsIntegral<int>                { static const bool value = true; };
template<> struct IsIntegral<unsigned>           { static const bool value = true; };
template<> struct IsIntegral<long>               { static const bool value = true; };
template<> struct IsIntegral<unsigned long>      { static const bool value = true; };
template<> struct IsIntegral<long long>          { static const bool value = true; };
template<> struct IsIntegral<unsigned long long> { static const bool value = true; };
#endif // !TIGHTDB_HAVE_CXX11_TYPE_TRAITS



/// Determine the type resulting from integral promotion.
///
/// \note Enum types are supported only when the compiler supports the
/// C++11 'decltype' feature.
#ifdef TIGHTDB_HAVE_CXX11_DECLTYPE
template<class T> struct IntegralPromote { typedef decltype(+T()) type; };
#else // !TIGHTDB_HAVE_CXX11_DECLTYPE
template<class T> struct IntegralPromote;
template<> struct IntegralPromote<bool> { typedef int type; };
template<> struct IntegralPromote<char> {
    typedef CondType<INT_MIN <= CHAR_MIN && CHAR_MAX <= INT_MAX, int, unsigned>::type type;
};
template<> struct IntegralPromote<signed char> {
    typedef CondType<INT_MIN <= SCHAR_MIN && SCHAR_MAX <= INT_MAX, int, unsigned>::type type;
};
template<> struct IntegralPromote<unsigned char> {
    typedef CondType<UCHAR_MAX <= INT_MAX, int, unsigned>::type type;
};
template<> struct IntegralPromote<wchar_t> {
private:
    typedef CondType<LLONG_MIN <= WCHAR_MIN && WCHAR_MAX <= LLONG_MAX, long long, unsigned long long>::type type_1;
    typedef CondType<0 <= WCHAR_MIN && WCHAR_MAX <= ULONG_MAX, unsigned long, type_1>::type type_2;
    typedef CondType<LONG_MIN <= WCHAR_MIN && WCHAR_MAX <= LONG_MAX, long, type_2>::type type_3;
    typedef CondType<0 <= WCHAR_MIN && WCHAR_MAX <= UINT_MAX, unsigned, type_3>::type type_4;
public:
    typedef CondType<INT_MIN <= WCHAR_MIN && WCHAR_MAX <= INT_MAX, int, type_4>::type type;
};
template<> struct IntegralPromote<short> {
    typedef CondType<INT_MIN <= SHRT_MIN && SHRT_MAX <= INT_MAX, int, unsigned>::type type;
};
template<> struct IntegralPromote<unsigned short> {
    typedef CondType<USHRT_MAX <= INT_MAX, int, unsigned>::type type;
};
template<> struct IntegralPromote<int> { typedef int type; };
template<> struct IntegralPromote<unsigned> { typedef unsigned type; };
template<> struct IntegralPromote<long> { typedef long type; };
template<> struct IntegralPromote<unsigned long> { typedef unsigned long type; };
template<> struct IntegralPromote<long long> { typedef long long type; };
template<> struct IntegralPromote<unsigned long long> { typedef unsigned long long type; };
template<> struct IntegralPromote<float> { typedef float type; };
template<> struct IntegralPromote<double> { typedef double type; };
template<> struct IntegralPromote<long double> { typedef long double type; };
#endif // !TIGHTDB_HAVE_CXX11_DECLTYPE



/// Determine the type of the result of a binary arithmetic (or
/// bitwise) operation (+, -, *, /, %, |, &, ^). The type of the
/// result of a shift operation (<<, >>) can instead be found as the
/// type resulting from integral promotion of the left operand. The
/// type of the result of a unary arithmetic (or bitwise) operation
/// can be found as the type resulting from integral promotion of the
/// operand.
///
/// \note Enum types are supported only when the compiler supports the
/// C++11 'decltype' feature.
#ifdef TIGHTDB_HAVE_CXX11_DECLTYPE
template<class A, class B> struct ArithBinOpType { typedef decltype(A()+B()) type; };
#else // !TIGHTDB_HAVE_CXX11_DECLTYPE
template<class A, class B> struct ArithBinOpType {
private:
    typedef typename IntegralPromote<A>::type A2;
    typedef typename IntegralPromote<B>::type B2;

    typedef typename CondType<UINT_MAX <= LONG_MAX, long, unsigned long>::type type_l_u;
    typedef typename CondType<EitherTypeIs<unsigned, A2, B2>::value, type_l_u, long>::type type_l;

    typedef typename CondType<UINT_MAX <= LLONG_MAX, long long, unsigned long long>::type type_ll_u;
    typedef typename CondType<ULONG_MAX <= LLONG_MAX, long long, unsigned long long>::type type_ll_ul;
    typedef typename CondType<EitherTypeIs<unsigned, A2, B2>::value, type_ll_u, long long>::type type_ll_1;
    typedef typename CondType<EitherTypeIs<unsigned long, A2, B2>::value, type_ll_ul, type_ll_1>::type type_ll;

    typedef typename CondType<EitherTypeIs<unsigned, A2, B2>::value, unsigned, int>::type type_1;
    typedef typename CondType<EitherTypeIs<long, A2, B2>::value, type_l, type_1>::type type_2;
    typedef typename CondType<EitherTypeIs<unsigned long, A2, B2>::value, unsigned long, type_2>::type type_3;
    typedef typename CondType<EitherTypeIs<long long, A2, B2>::value, type_ll, type_3>::type type_4;
    typedef typename CondType<EitherTypeIs<unsigned long long, A2, B2>::value, unsigned long long, type_4>::type type_5;
    typedef typename CondType<EitherTypeIs<float, A, B>::value, float, type_5>::type type_6;
    typedef typename CondType<EitherTypeIs<double, A, B>::value, double, type_6>::type type_7;

public:
    typedef typename CondType<EitherTypeIs<long double, A, B>::value, long double, type_7>::type type;
};
#endif // !TIGHTDB_HAVE_CXX11_DECLTYPE


/// Choose the first of `unsigned char`, `unsigned short`, `unsigned
/// int`, `unsigned long`, and `unsigned long long` that has at least
/// `bits` value bits.
template<int bits> struct LeastUnsigned {
private:
    typedef void                                          types_0;
    typedef TypeAppend<types_0, unsigned char>::type      types_1;
    typedef TypeAppend<types_1, unsigned short>::type     types_2;
    typedef TypeAppend<types_2, unsigned int>::type       types_3;
    typedef TypeAppend<types_3, unsigned long>::type      types_4;
    typedef TypeAppend<types_4, unsigned long long>::type types_5;
    typedef types_5 types;
    // The `dummy<>` template is there to work around a bug in
    // VisualStudio (seen in versions 2010 and 2012). Without the
    // `dummy<>` template, The C++ compiler in Visual Studio would
    // attempt to instantiate `FindType<type, pred>` before the
    // instantiation of `LeastUnsigned<>` which obviously fails
    // because `pred` depends on `bits`.
    template<int> struct dummy {
        template<class T> struct pred {
            static const bool value = std::numeric_limits<T>::digits >= bits;
        };
    };
public:
    typedef typename FindType<types, dummy<bits>::template pred>::type type;
    TIGHTDB_STATIC_ASSERT(!(SameType<type, void>::value), "No unsigned type is that wide");
};


/// Choose `B` if `B` has more value bits than `A`, otherwise choose
/// `A`.
template<class A, class B> struct ChooseWidestInt {
private:
    typedef std::numeric_limits<A> lim_a;
    typedef std::numeric_limits<B> lim_b;
    TIGHTDB_STATIC_ASSERT(lim_a::is_specialized && lim_b::is_specialized,
                          "std::numeric_limits<> must be specialized for both types");
    TIGHTDB_STATIC_ASSERT(lim_a::is_integer && lim_b::is_integer,
                          "Both types must be integers");
public:
    typedef typename CondType<(lim_a::digits >= lim_b::digits), A, B>::type type;
};


} // namespace util
} // namespace tightdb

#endif // TIGHTDB_UTIL_TYPE_TRAITS_HPP
