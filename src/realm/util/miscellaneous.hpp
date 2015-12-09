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

// FIXME: Replace this with std::as_const when we switch over to C++17 by
// default.
template <class T>
const T& as_const(const T& v) noexcept
{
    return v;
}

// FIXME: C++17 also defines
//
//     template <class T>
//     const T& as_const(const T&&) = delete;
//
// Though, we are unsure as to why. As we do not understand the underlying
// reason for this deleted function, we are choosing not to add it in our
// codebase. If somebody understands the reasoning, feel free to add it (with
// comments).

} // namespace util
} // namespace realm

#endif // REALM_UTIL_MISCELLANEOUS_HPP
