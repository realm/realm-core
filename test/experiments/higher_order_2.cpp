#include <cstring>
#include <algorithm>
#include <functional>

#include <tightdb/table.hpp>

using namespace std;
using namespace tightdb;


int64_t sum_int_1(const Table& t)
{
    return t.sum(0);
}

int64_t sum_int_2(const Table& t)
{
    return t.foldl_int(0, plus<int64_t>(), int64_t());
}

int64_t sum_int_3(const Table& t)
{
    size_t n = t.size();
    int64_t sum = 0;
    for (size_t i=0; i<n; ++i) sum += t.get_int(0, i);
    return sum;
}



double sum_double_1(const Table& t)
{
    return t.sum_double(1);
}

double sum_double_2(const Table& t)
{
    return t.foldl_double(1, plus<double>(), double());
}

double sum_double_3(const Table& t)
{
    size_t n = t.size();
    double sum = 0;
    for (size_t i=0; i<n; ++i) sum += t.get_double(1, i);
    return sum;
}



namespace {
struct MaxSize {
    size_t operator()(size_t w, const char* s) const
    {
        return max(w, strlen(s));
    }
};
}

size_t max_string_size_2(const Table& t)
{
    return t.foldl_string(2, MaxSize(), size_t());
}

size_t max_string_size_3(const Table& t)
{
    size_t n = t.size();
    size_t max_size = 0;
    for (size_t i=0; i<n; ++i) {
        size_t s = strlen(t.get_string(2, i));
        if (max_size < s) max_size = s;
    }
    return max_size;
}



size_t max_long_string_size_2(const Table& t)
{
    return t.foldl_string(3, MaxSize(), size_t());
}

size_t max_long_string_size_2_2(const Table& t)
{
    return t.foldl2_string(3, MaxSize(), size_t());
}

size_t max_long_string_size_3(const Table& t)
{
    size_t n = t.size();
    size_t max_size = 0;
    for (size_t i=0; i<n; ++i) {
        size_t s = strlen(t.get_string(3, i));
        if (max_size < s) max_size = s;
    }
    return max_size;
}
