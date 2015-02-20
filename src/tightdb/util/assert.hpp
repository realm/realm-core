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
#ifndef TIGHTDB_UTIL_ASSERT_HPP
#define TIGHTDB_UTIL_ASSERT_HPP

#include <tightdb/util/features.h>
#include <tightdb/util/terminate.hpp>
#include <tightdb/version.hpp>

#define TIGHTDB_ASSERT_RELEASE(condition) \
    ((condition) ? static_cast<void>(0) : \
    tightdb::util::terminate(TIGHTDB_VER_CHUNK " Assertion failed: " #condition, __FILE__, __LINE__))

#if defined(TIGHTDB_ENABLE_ASSERTIONS) || defined(TIGHTDB_DEBUG)
#  define TIGHTDB_ASSERT(condition) TIGHTDB_ASSERT_RELEASE(condition)
#else
#  define TIGHTDB_ASSERT(condition) static_cast<void>(0)
#endif

#ifdef TIGHTDB_DEBUG
#  define TIGHTDB_ASSERT_DEBUG(condition) TIGHTDB_ASSERT_RELEASE(condition)
#else
#  define TIGHTDB_ASSERT_DEBUG(condition) static_cast<void>(0)
#endif

// Becase the assert is used in noexcept methods, it's a bad idea to allocate buffer space for the message
// so therefore we must pass it to terminate which will 'cerr' it for us without needing any buffer
#if defined(TIGHTDB_ENABLE_ASSERTIONS) || defined(TIGHTDB_DEBUG)
#  define TIGHTDB_ASSERT_3(left, condition, right) \
    ((left condition right) ? static_cast<void>(0) : \
        tightdb::util::terminate(TIGHTDB_VER_CHUNK " Assertion failed: " #left #condition #right, \
                                 __FILE__, __LINE__, true, (uint64_t)left, (uint64_t)right))
#else
#  define TIGHTDB_ASSERT_3(left, condition, right) static_cast<void>(0)
#endif

#ifdef TIGHTDB_HAVE_CXX11_STATIC_ASSERT
#  define TIGHTDB_STATIC_ASSERT(condition, message) static_assert(condition, message)
#else
#  define TIGHTDB_STATIC_ASSERT(condition, message) typedef \
    tightdb::util::static_assert_dummy<sizeof(tightdb::util:: \
        TIGHTDB_STATIC_ASSERTION_FAILURE<bool(condition)>)> \
    TIGHTDB_JOIN(_tightdb_static_assert_, __LINE__) TIGHTDB_UNUSED
#  define TIGHTDB_JOIN(x,y) TIGHTDB_JOIN2(x,y)
#  define TIGHTDB_JOIN2(x,y) x ## y
namespace tightdb {
namespace util {
    template<bool> struct TIGHTDB_STATIC_ASSERTION_FAILURE;
    template<> struct TIGHTDB_STATIC_ASSERTION_FAILURE<true> {};
    template<int> struct static_assert_dummy {};
}
}
#endif


#endif // TIGHTDB_UTIL_ASSERT_HPP
