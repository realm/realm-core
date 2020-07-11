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

#ifndef REALM_TEST_TYPES_HELPER_HPP
#define REALM_TEST_TYPES_HELPER_HPP

#include <realm.hpp>

namespace realm {
namespace test_util {

template <typename T>
inline T convert_for_test(int64_t v)
{
    return static_cast<T>(v);
}

template <>
inline Timestamp convert_for_test<Timestamp>(int64_t v)
{
    return Timestamp{v, 0};
}

template <>
inline BinaryData convert_for_test<BinaryData>(int64_t v)
{
    static const char letters[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    static const size_t num_letters = 10 + 26;
    return BinaryData(letters, v % num_letters);
}

template <>
inline StringData convert_for_test<StringData>(int64_t v)
{
    static const char letters[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    static const size_t num_letters = 10 + 26;
    return StringData(letters, v % num_letters);
}

template <>
inline ObjectId convert_for_test<ObjectId>(int64_t v)
{
    static const char hex_digits[] = "0123456789abcdef";
    std::string value;
    uint64_t cur = static_cast<uint64_t>(v);
    for (size_t i = 0; i < 24; ++i) {
        value += char(hex_digits[cur % 16]);
        cur -= (cur % 16);
        if (cur == 0) {
            cur += static_cast<uint64_t>(v);
        }
    }
    return ObjectId(value.c_str());
}

template <>
inline util::Optional<ObjectId> convert_for_test(int64_t v)
{
    return util::Optional<ObjectId>(convert_for_test<ObjectId>(v));
}

template <typename T, typename U>
std::vector<T> values_from_int(const std::vector<int64_t>& values)
{
    std::vector<T> ret;
    for (size_t i = 0; i < values.size(); ++i) {
        ret.push_back(convert_for_test<U>(values[i]));
    }
    return ret;
}

struct less {
    template <typename T>
    auto operator()(T&& a, T&& b) const noexcept
    {
        return Mixed(a).compare(Mixed(b)) < 0;
    }
};
struct greater {
    template <typename T>
    auto operator()(T&& a, T&& b) const noexcept
    {
        return Mixed(a).compare(Mixed(b)) > 0;
    }
};

} // namespace test_util
} // namespace realm

#endif // REALM_TEST_TYPES_HELPER
