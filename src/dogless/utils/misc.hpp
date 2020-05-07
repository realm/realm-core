/*
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DATADOG_CXX_UTILS_MISC_HPP_20160128
#define DATADOG_CXX_UTILS_MISC_HPP_20160128

#include <cstddef>
#include <memory>
#include <type_traits>
#include <string>
#include <utility>

namespace dogless {
namespace utils {

template <typename T>
std::string to_string(T value)
{
    return std::to_string(value);
}

template <>
inline std::string to_string<double>(double value)
{
    auto str = std::to_string(value);
    size_t dot_idx = str.find('.');
    size_t idx = str.find_last_not_of('0');
    if (idx > dot_idx) {
        str.erase(str.begin() + idx + 1, str.end());
    }
    else {
        str.erase(str.begin() + dot_idx, str.end());
    }
    return str.empty() ? "0" : str;
}

template <>
inline std::string to_string<float>(float value)
{
    return to_string(static_cast<double>(value));
}

} // namespace utils
} // namespace dogless

#endif // DATADOG_CXX_UTILS_MISC_HPP_20160128
