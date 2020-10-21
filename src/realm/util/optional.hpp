/*************************************************************************
 *
 * Copyright 2020 Realm Inc.
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

#ifndef REALM_OPTIONAL_HPP
#define REALM_OPTIONAL_HPP

#include <optional>

namespace realm {
namespace util {

template <typename>
struct is_optional : std::false_type {};

template <typename T>
struct is_optional<std::optional<T>> : std::true_type {};

template <class T>
struct RemoveOptional {
  using type = T;
};

template <class T>
struct RemoveOptional<std::optional<T>> {
  using type = T;
};

template <class T>
T unwrap(T&& value)
{
    return value;
}

template <class T>
T unwrap(std::optional<T>&& value)
{
    return *value;
}

template <class T>
T unwrap(const std::optional<T>& value)
{
    return *value;
}

template <class T>
T unwrap(std::optional<T>& value)
{
    return *value;
}

} // namespace util
} // namespace realm

#endif /* REALM_OPTIONAL_HPP */
