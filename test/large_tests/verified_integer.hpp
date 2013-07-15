#include <vector>
#include <string>
#include <algorithm>
#ifdef _MSC_VER
    #include <win32\stdint.h>
#endif
#include <stdio.h>
#include <tightdb/column.hpp>

class VerifiedInteger {
    std::vector<int64_t> v;
    tightdb::Column u;
public:
    void add(int64_t value);
    void Insert(std::size_t ndx, int64_t value);
    void Insert(std::size_t ndx, const char *value);
    int64_t get(std::size_t ndx);
    void set(std::size_t ndx, int64_t value);
    void Delete(std::size_t ndx);
    void Clear();
    size_t find_first(int64_t value);
    void find_all(tightdb::Array &c, int64_t value, std::size_t start = 0, std::size_t end = -1);
    std::size_t size();
    int64_t Sum(std::size_t start = 0, std::size_t end = -1);
    int64_t maximum(std::size_t start = 0, std::size_t end = -1);
    int64_t minimum(std::size_t start = 0, std::size_t end = -1);
    bool Verify();
    bool ConditionalVerify();
    void VerifyNeighbours(std::size_t ndx);
    void Destroy();
};
