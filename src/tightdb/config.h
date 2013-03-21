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
#ifndef TIGHTDB_CONFIG_H
#define TIGHTDB_CONFIG_H


/* This one is needed to allow tightdb-config to know whether a
 * nondefault value is in effect. */
#define TIGHTDB_DEFAULT_MAX_LIST_SIZE 5

/* The maximum number of elements in a B-tree node. Allow this value
 * to be overridden on the command-line. */
#ifndef TIGHTDB_MAX_LIST_SIZE
#  define TIGHTDB_MAX_LIST_SIZE TIGHTDB_DEFAULT_MAX_LIST_SIZE
#endif


#if __cplusplus >= 201103 || __GXX_EXPERIMENTAL_CXX0X__
#  define TIGHTDB_HAVE_CXX11 1
#endif


#if __GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 3
#  define TIGHTDB_HAVE_GCC_GE_4_3 1
#endif
#if __GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 4
#  define TIGHTDB_HAVE_GCC_GE_4_4 1
#endif
#if __GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 5
#  define TIGHTDB_HAVE_GCC_GE_4_5 1
#endif
#if __GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 6
#  define TIGHTDB_HAVE_GCC_GE_4_6 1
#endif
#if __GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 7
#  define TIGHTDB_HAVE_GCC_GE_4_7 1
#endif
#if __GNUC__ > 4 || __GNUC__ == 4 && __GNUC_MINOR__ >= 8
#  define TIGHTDB_HAVE_GCC_GE_4_8 1
#endif


/* Support for C++11 static_assert(). */
#if TIGHTDB_HAVE_CXX11 && TIGHTDB_HAVE_GCC_GE_4_3 || \
    _MSC_VER >= 1600
#  define TIGHTDB_HAVE_CXX11_STATIC_ASSERT 1
#endif


/* Support for C++11 r-value references.
 *
 * NOTE: Not yet fully supported in MSVC++ 11.0. */
#if TIGHTDB_HAVE_CXX11 && TIGHTDB_HAVE_GCC_GE_4_3
#  define TIGHTDB_HAVE_CXX11_RVALUE_REFERENCE 1
#endif


/* Support for the C++11 'decltype' keyword.
 *
 * NOTE: Not yet fully supported in MSVC++ 11.0. */
#if TIGHTDB_HAVE_CXX11 && TIGHTDB_HAVE_GCC_GE_4_3
#  define TIGHTDB_HAVE_CXX11_DECLTYPE 1
#endif


/* Support for C++11 initializer lists.
 *
 * NOTE: Not yet fully supported in MSVC++ 11.0. */
#if TIGHTDB_HAVE_CXX11 && TIGHTDB_HAVE_GCC_GE_4_4
#  define TIGHTDB_HAVE_CXX11_INITIALIZER_LISTS 1
#endif


/* Support for C++11 atomics. */
#if TIGHTDB_HAVE_CXX11 && TIGHTDB_HAVE_GCC_GE_4_4 || \
    _MSC_VER >= 1700
#  define TIGHTDB_HAVE_CXX11_ATOMIC 1
#endif


/* Support for C++11 explicit conversion operators.
 * NOTE: Not yet fully supported in MSVC++ 11.0. */
#if TIGHTDB_HAVE_CXX11 && TIGHTDB_HAVE_GCC_GE_4_5
#  define TIGHTDB_HAVE_CXX11_EXPLICIT_CONV_OPERATORS 1
#endif


/* Support for the C++11 'constexpr' keyword.
 *
 * NOTE: Not yet fully supported in MSVC++ 11.0. */
#if TIGHTDB_HAVE_CXX11 && TIGHTDB_HAVE_GCC_GE_4_6
#  define TIGHTDB_HAVE_CXX11_CONSTEXPR 1
#endif


/* Support for the C++11 'noexcept' specifier.
 *
 * NOTE: Not yet fully supported in MSVC++ 11.0. */
#if TIGHTDB_HAVE_CXX11 && TIGHTDB_HAVE_GCC_GE_4_6
#  define TIGHTDB_NOEXCEPT noexcept
#elif defined TIGHTDB_DEBUG
#  define TIGHTDB_NOEXCEPT throw()
#else
#  define TIGHTDB_NOEXCEPT
#endif


/* Support for C++11 explicit virtual overrides */
#if TIGHTDB_HAVE_CXX11 && TIGHTDB_HAVE_GCC_GE_4_7 || \
    _MSC_VER >= 1700
#  define TIGHTDB_OVERRIDE override
#else
#  define TIGHTDB_OVERRIDE
#endif


/* How to specify that a function never returns.
 *
 * NOTE: C++11 generalized attributes are not yet fully supported in
 * MSVC++ 11.0. */
#if defined TIGHTDB_HAVE_CXX11 && defined TIGHTDB_HAVE_GCC_GE_4_8
#  define TIGHTDB_NORETURN [[noreturn]]
#elif __GNUC__
#  define TIGHTDB_NORETURN __attribute__((noreturn))
#elif _MSC_VER
#  define TIGHTDB_NORETURN __declspec(noreturn)
#else
#  define TIGHTDB_NORETURN
#endif


#if __GNUC__ || defined __INTEL_COMPILER
#  define TIGHTDB_UNLIKELY(expr) __builtin_expect(!!(expr), 0)
#  define TIGHTDB_LIKELY(expr)   __builtin_expect(!!(expr), 1)
#else
#  define TIGHTDB_UNLIKELY(expr) (expr)
#  define TIGHTDB_LIKELY(expr)   (expr)
#endif


#endif /* TIGHTDB_CONFIG_H */
