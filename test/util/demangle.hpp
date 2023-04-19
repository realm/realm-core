/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#ifndef REALM_TEST_UTIL_DEMANGLE_HPP
#define REALM_TEST_UTIL_DEMANGLE_HPP

#include <typeinfo>
#include <string>

namespace realm {
namespace test_util {


/// Demangle the specified name.
///
/// See for example
/// http://gcc.gnu.org/onlinedocs/libstdc++/latest-doxygen/namespaceabi.html
std::string demangle(const std::string&);


/// Get the demangled name of the specified type.
template <class T>
inline std::string get_type_name()
{
    return demangle(typeid(T).name());
}


/// Get the demangled name of the type of the specified argument.
template <typename T>
inline std::string get_type_name(const T& v)
{
    return demangle(typeid(v).name());
}


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_DEMANGLE_HPP
