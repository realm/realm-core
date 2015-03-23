/*************************************************************************
 *
 * TIGHTDB CONFIDENTIAL
 * __________________
 *
 *  [2011] - [2012] TightDB Inc
 *  All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of TightDB Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to TightDB Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from TightDB Incorporated.
 *
 **************************************************************************/
#ifndef REALM_TEST_UTIL_NUMBER_NAMES_HPP
#define REALM_TEST_UTIL_NUMBER_NAMES_HPP

#include <cstddef>
#include <string>

namespace tightdb {
namespace test_util {

std::string number_name(std::size_t n);

std::string number_name(std::size_t n)
{
    static const char* ones[] = {"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
                                 "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen",
                                 "eighteen", "nineteen"};
    static const char* tens[] = {"", "ten", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"};

    std::string txt;
    if (n >= 1000) {
        txt = number_name(n/1000) + " thousand ";
        n %= 1000;
    }
    if (n >= 100) {
        txt += ones[n/100];
        txt += " hundred ";
        n %= 100;
    }
    if (n >= 20) {
        txt += tens[n/10];
        n %= 10;
    }
    else {
        txt += " ";
        txt += ones[n];
    }

    return txt;
}


} // namespace test_util
} // namespace tightdb

#endif // REALM_TEST_UTIL_NUMBER_NAMES_HPP
