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

#ifndef REALM_TEST_UTIL_WILDCARD_HPP
#define REALM_TEST_UTIL_WILDCARD_HPP

#include <cstddef>
#include <cstring>
#include <string>
#include <vector>

#include <realm/util/features.h>


namespace realm {
namespace test_util {


class wildcard_pattern {
public:
    explicit wildcard_pattern(const std::string& text);

    bool match(const char* begin, const char* end) const noexcept;

    bool match(const char* c_str) const noexcept;

private:
    std::string m_text;

    struct card {
        size_t m_offset, m_size;
        card(size_t begin, size_t end) noexcept;
    };

    // Must contain at least one card. The first, and the last card
    // may be empty strings. All other cards must be non-empty. If
    // there is exactly one card, the pattern matches a string if, and
    // only if the string is equal to the card. Otherwise, the first
    // card must be a prefix of the string, and the last card must be
    // a suffix.
    std::vector<card> m_cards;
};


// Implementation

inline bool wildcard_pattern::match(const char* c_str) const noexcept
{
    const char* begin = c_str;
    const char* end = begin + std::strlen(c_str);
    return match(begin, end);
}

inline wildcard_pattern::card::card(size_t begin, size_t end) noexcept
{
    m_offset = begin;
    m_size = end - begin;
}


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_WILDCARD_HPP
