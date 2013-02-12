#include <functional>

#include <tightdb/table.hpp>

using namespace std;
using namespace tightdb;

int64_t sum1(const Table& t)
{
    return t.sum(0);
}

int64_t sum2(const Table& t)
{
    return t.foldl_int(0, plus<int64_t>(), int64_t(0));
}
