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

#ifndef REALM_UTIL_INSPECT_HPP
#define REALM_UTIL_INSPECT_HPP

#include <string>
#include <typeinfo>

namespace realm {
namespace util {

// LCOV_EXCL_START

std::string inspect(const std::string& str);
std::string inspect(const char* str);
std::string inspect(const void*);
std::string inspect_pointer(const char* type_name, const void*);

inline
std::string inspect(void* ptr)
{
    return inspect(static_cast<const void*>(ptr));
}

template<class T>
std::string inspect(T* ptr)
{
    return inspect_pointer(typeid(T).name(), ptr);
}

template<class T>
std::string inspect(const T& value)
{
    return std::to_string(value);
}

inline
std::string inspect(const char* str)
{
    return inspect(std::string{str});
}

// LCOV_EXCL_STOP

} // namespace util
} // namespace realm

#endif // REALM_UTIL_INSPECT_HPP
