#include "tightdb/array_integer.hpp"

#include <vector>

using namespace tightdb;


std::vector<int64_t> ArrayInteger::ToVector() const
{
    std::vector<int64_t> v;
    const size_t count = size();
    for (size_t t = 0; t < count; ++t)
        v.push_back(get_data(t));
    return v;
}
