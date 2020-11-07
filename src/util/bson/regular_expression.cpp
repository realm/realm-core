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

#include "util/bson/regular_expression.hpp"
#include <numeric>

namespace realm {
namespace bson {

RegularExpression::RegularExpression(const std::string pattern,
                                     const std::string& options) :
m_pattern(pattern)
, m_options(
            std::accumulate(options.begin(),
                            options.end(),
                            RegularExpression::Option::None,
                            [](RegularExpression::Option a, char b) { return a | option_char_to_option(b); }))
{
}

RegularExpression::RegularExpression(const std::string pattern,
                                     Option options) :
m_pattern(pattern),
m_options(options) {}

const std::string RegularExpression::pattern() const
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
        case 'm':
            return Option::Multiline;
        case 's':
            return Option::Dotall;
        case 'x':
            return Option::Extended;
        default:
            throw std::runtime_error("invalid regex option type");
    }
};

std::ostream& operator<<(std::ostream& out, const RegularExpression::Option& option)
{
    using Option = RegularExpression::Option;

    if ((option & Option::IgnoreCase) != Option::None) out << 'i';
    if ((option & Option::Multiline) != Option::None) out << 'm';
    if ((option & Option::Dotall) != Option::None) out << 's';
    if ((option & Option::Extended) != Option::None) out << 'x';

    return out;
}

} // namespace bson
} // namespace realm
