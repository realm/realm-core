#ifndef VER_STR_H
#define VER_STR_H

#include <vector>
#include <string>
#include <algorithm>
#ifdef _MSC_VER
    #include <win32\stdint.h>
#endif
#include <stdio.h>
#include <tightdb/column_string.hpp>

class VerifiedString {
    std::vector<std::string> v;
    tightdb::AdaptiveStringColumn u;
public:
    void add(tightdb::StringData value);
    void insert(std::size_t ndx, tightdb::StringData value);
    tightdb::StringData get(std::size_t ndx);
    void set(std::size_t ndx, tightdb::StringData value);
    void erase(std::size_t ndx);
    void clear();
    std::size_t find_first(tightdb::StringData value);
    void find_all(tightdb::Array& c, tightdb::StringData value, std::size_t start = 0, std::size_t end = -1);
    std::size_t size();
    bool Verify();
    bool conditional_verify();
    void verify_neighbours(std::size_t ndx);
    void destroy();

};

#endif
