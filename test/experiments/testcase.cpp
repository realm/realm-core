#include <cstring>
#include <iostream>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

#define CHECK(v) if (!(v)) cerr << __LINE__ << ": CHECK failed" << endl
#define CHECK_EQUAL(a, b) if (!check_equal((a),(b))) cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl

using namespace tightdb;
using namespace std;


namespace {

template<class A, class B> inline bool check_equal(const A& a, const B& b) { return a == b; }
inline bool check_equal(const char* a, const char* b) { return strcmp(a, b) == 0; }

} // anonymous namespace


namespace {

TIGHTDB_TABLE_1(TestTableShared,
                first,  Int)

} // anonymous namespace


int main()
{
    Array a;
    for (int i=0; i<16; ++i) a.add(4300000000ULL);
    a.Destroy();

    return 0;
}
