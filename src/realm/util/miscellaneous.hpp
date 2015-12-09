/*************************************************************************
 *
 * REALM CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2015] Realm Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Realm Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Realm Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Realm Incorporated.
 *
 **************************************************************************/
#ifndef REALM_UTIL_MISCELLANEOUS_HPP
#define REALM_UTIL_MISCELLANEOUS_HPP

namespace realm {
namespace util {

template <typename T>
const T& as_const(const T& v)
{
    return v;
}

} // namespace util
} // namespace realm

#endif // REALM_UTIL_MISCELLANEOUS_HPP
