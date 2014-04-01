#include <cstddef>
#include <algorithm>
#include <vector>
#include <string>

#ifdef _MSC_VER
#  include <win32\stdint.h>
#endif

#include <tightdb/column.hpp>

#include "../util/random.hpp"

class VerifiedInteger {
public:
    VerifiedInteger(tightdb::test_util::Random&);
    void add(int64_t value);
    void insert(std::size_t ndx, int64_t value);
    void insert(std::size_t ndx, const char *value);
    int64_t get(std::size_t ndx);
    void set(std::size_t ndx, int64_t value);
    void erase(std::size_t ndx);
    void clear();
    size_t find_first(int64_t value);
    void find_all(tightdb::Array &c, int64_t value, std::size_t start = 0, std::size_t end = -1);
    std::size_t size();
    int64_t sum(std::size_t start = 0, std::size_t end = -1);
    int64_t maximum(std::size_t start = 0, std::size_t end = -1);
    int64_t minimum(std::size_t start = 0, std::size_t end = -1);
    bool Verify();
    bool occasional_verify();
    void verify_neighbours(std::size_t ndx);
    void destroy();
private:
    std::vector<int64_t> v;
    tightdb::Column u;
    tightdb::test_util::Random& m_random;
};



// Implementation

inline VerifiedInteger::VerifiedInteger(tightdb::test_util::Random& random):
    m_random(random)
{
}
