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

#include <realm/util/bson/regular_expression.hpp>
#include <numeric>

namespace realm {
namespace bson {

RegularExpression::RegularExpression(const std::string& pattern, const std::string& options)
    : m_pattern(pattern)
    , m_options(std::accumulate(options.begin(), options.end(), RegularExpression::Option::None,
                                [](RegularExpression::Option a, char b) {
                                    return a | option_char_to_option(b);
                                }))
{
}

RegularExpression::RegularExpression(const std::string& pattern, Option options)
    : m_pattern(pattern)
    , m_options(options)
{
}

const std::string& RegularExpression::pattern() const
{
    return m_pattern;
}

RegularExpression::Option RegularExpression::options() const
{
    return m_options;
}

constexpr RegularExpression::Option RegularExpression::option_char_to_option(const char option)
{
    switch (option) {
        case 'i':
            return Option::IgnoreCase;
        case 'l':
            return Option::Locale;
        case 'm':
            return Option::Multiline;
        case 's':
            return Option::Dotall;
        case 'u':
            return Option::Unicode;
        case 'x':
            return Option::Extended;
        default:
            throw std::runtime_error("invalid regex option type");
    }
}

std::string RegularExpression::options_str() const
{
    std::string ret;
    using Option = RegularExpression::Option;

    if ((m_options & Option::IgnoreCase) != Option::None)
        ret += 'i';
    if ((m_options & Option::Locale) != Option::None)
        ret += 'l';
    if ((m_options & Option::Multiline) != Option::None)
        ret += 'm';
    if ((m_options & Option::Dotall) != Option::None)
        ret += 's';
    if ((m_options & Option::Unicode) != Option::None)
        ret += 'u';
    if ((m_options & Option::Extended) != Option::None)
        ret += 'x';

    return ret;
}

} // namespace bson
} // namespace realm
