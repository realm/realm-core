#include <iostream>
#include <tightdb.hpp>
#include <tightdb/group_shared.hpp>

using namespace tightdb;
using namespace std;

namespace {

#define CHECK(v) if (!(v)) cerr << __LINE__ << ": CHECK failed" << endl
#define CHECK_EQUAL(a, b) if ((a)!=(b)) cerr << __LINE__ << ": CHECK_EQUAL failed: " << (a) << " vs " << (b) << endl

enum Days {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun
};

TIGHTDB_TABLE_4(TestTableGroup,
                first,  String,
                second, Int,
                third,  Bool,
                fourth, Enum<Days>)

TIGHTDB_TABLE_1(TestTableGroup2,
                second, Subtable<TestTableGroup>)

} // namespace

int main()
{
    TestTableGroup2::Ref table;
    {
        Group group;
        table = group.get_table<TestTableGroup2>("foo");
    }

    return 0;
}
