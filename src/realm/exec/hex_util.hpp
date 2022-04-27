/*
 * hex_util.hpp
 *
 *  Created on: Apr 25, 2022
 *      Author: jed
 */

#ifndef SRC_REALM_EXEC_HEX_UTIL_HPP_
#define SRC_REALM_EXEC_HEX_UTIL_HPP_

#include <iostream>
#include <cstring>

inline int hex_to_int(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    throw std::runtime_error("Invalid hex");
    return 0;
}

inline void hex_to_bin(const char* in_str, char* out_str)
{
    if (strlen(in_str) != 128) {
        throw std::runtime_error("Key length is expected to be a 128 byte hexencoded string");
    }
    for (size_t i = 0; i < 64; i++) {
        out_str[i] = char(hex_to_int(in_str[2 * i]) * 16 + hex_to_int(in_str[2 * i + 1]));
    }
}

#endif /* SRC_REALM_EXEC_HEX_UTIL_HPP_ */
