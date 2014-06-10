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
#ifndef TIGHTDB_TEST_UTIL_RESOURCE_LIMITS_HPP
#define TIGHTDB_TEST_UTIL_RESOURCE_LIMITS_HPP

namespace tightdb {
namespace test_util {


enum Resource {
    resource_NumOpenFiles
};

bool system_has_rlimit(Resource) TIGHTDB_NOEXCEPT;

//@{

/// Get or set resouce limits. A negative value means 'unlimited' both when
/// getting and when setting.
long get_hard_rlimit(Resource);
long get_soft_rlimit(Resource);
void set_soft_rlimit(Resource, long value);

//@}


} // namespace test_util
} // namespace tightdb

#endif // TIGHTDB_TEST_UTIL_RESOURCE_LIMITS_HPP
