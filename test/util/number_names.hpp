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

#ifndef REALM_TEST_UTIL_NUMBER_NAMES_HPP
#define REALM_TEST_UTIL_NUMBER_NAMES_HPP

#include <cstddef>
#include <string>

namespace realm {
namespace test_util {

std::string number_name(size_t n);

std::string number_name(size_t n)
{
    static const char* ones[] = {"zero",     "one",     "two",     "three",     "four",     "five",    "six",
                                 "seven",    "eight",   "nine",    "ten",       "eleven",   "twelve",  "thirteen",
                                 "fourteen", "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"};
    static const char* tens[] = {"",      "ten",   "twenty",  "thirty", "forty",
                                 "fifty", "sixty", "seventy", "eighty", "ninety"};

    std::string txt;
    if (n >= 1000) {
        txt = number_name(n / 1000) + " thousand ";
        n %= 1000;
    }
    if (n >= 100) {
        txt += ones[n / 100];
        txt += " hundred ";
        n %= 100;
    }
    if (n >= 20) {
        txt += tens[n / 10];
        n %= 10;
    }
    else {
        txt += " ";
        txt += ones[n];
    }

    return txt;
}


} // namespace test_util
} // namespace realm

#endif // REALM_TEST_UTIL_NUMBER_NAMES_HPP
