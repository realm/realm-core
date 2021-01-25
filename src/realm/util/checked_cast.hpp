/*************************************************************************
 *
 * Copyright 2021 Realm Inc.
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

#ifndef REALM_UTIL_CHECKED_CAST_HPP
#define REALM_UTIL_CHECKED_CAST_HPP

#include <memory>
#include <type_traits>
#include <realm/util/assert.hpp>

namespace realm {
namespace util {

/**
 * Similar to static_cast, but in debug builds uses RTTI to confirm that the cast
 * is legal at runtime.
 */
template <typename T, typename U,
          // Final template argument is to limit overload resolution it to cases where static_cast<T> would compile.
          // This results in much better error messages than waiting and letting it blow up inside the body.
          // TODO use requires clause in C++20
          typename = std::void_t<decltype(static_cast<T>(std::declval<U>()))>>
inline T checked_cast(U&& u) noexcept
{
#if REALM_ASSERTIONS_ENABLED
    if constexpr (std::is_pointer_v<std::remove_reference_t<U>>) {
        if (!u)
            return nullptr;
        REALM_ASSERT(dynamic_cast<T>(u));
    }
    else {
        REALM_ASSERT(dynamic_cast<std::add_pointer_t<T>>(&u));
    }
#endif
    return static_cast<T>(std::forward<U>(u));
}

namespace checked_cast_detail {
template <typename T, typename U>
inline std::shared_ptr<T> checked_pointer_cast(U&& u) noexcept
{
#if REALM_ASSERTIONS_ENABLED
    if (!u)
        return nullptr;
    REALM_ASSERT(std::dynamic_pointer_cast<T>(std::forward<U>(u)));
#endif
    return std::static_pointer_cast<T>(std::forward<U>(u));
}
} // namespace checked_cast_detail

/**
 * Similar to static_pointer_cast, but in debug builds uses RTTI to confirm that the cast
 * is legal at runtime.
 */
template <typename T, typename U>
inline std::shared_ptr<T> checked_pointer_cast(const std::shared_ptr<U>& u) noexcept
{
    return checked_cast_detail::checked_pointer_cast<T>(u);
}

template <typename T, typename U>
inline std::shared_ptr<T> checked_pointer_cast(std::shared_ptr<U>&& u) noexcept
{
    return checked_cast_detail::checked_pointer_cast<T>(std::move(u));
}

} // namespace util
} // namespace realm

#endif // REALM_UTIL_CHECKED_CAST_HPP
