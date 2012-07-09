#include <iostream>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

using namespace tightdb;
using namespace std;

namespace {

#define CHECK(v) if (!(v)) cerr << __LINE__ << ": CHECK failed" << endl
#define CHECK_EQUAL(a, b) if ((a)!=(b)) cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl

} // namespace

int main()
{
    // Load the group and let it clean up without loading
    // any tables
    Group fromDisk("table_test.tbl", GROUP_READONLY);
    CHECK(fromDisk.is_valid());

    return 0;
}
